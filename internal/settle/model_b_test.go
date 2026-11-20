package settle

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/ciranda-dev/kolo-braid/internal/dag"
)

// forkWorld builds scope b's DAG: seats in lockstep through round fr-1, then seat e signs two histories A and B, and every
// other seat follows the side it is given, linking only entries whose past holds no entry of the other history.
func forkWorld(n, e, fr, steps int, side []int) (w *world, hist [2]bitset) {
	w = newWorld()
	keys := seq(n)
	heads := w.lockstep(keys, map[int]*ent{}, fr, 0, nil)
	eh := [2]*ent{heads[e], heads[e]}
	var mark [2]bitset
	clean := func(x *ent, s int) bool {
		if x == nil {
			return true
		}
		for _, v := range []int{1 - s} {
			for i, m := range mark[v] {
				if i < len(x.past) && x.past[i]&m != 0 {
					return false
				}
			}
		}
		return true
	}
	for range steps {
		prev := map[int]*ent{}
		for k, v := range heads {
			prev[k] = v
		}
		pe := eh
		for _, k := range keys {
			if k == e {
				for h := range 2 {
					var links []*ent
					for _, o := range keys {
						if o != e && clean(prev[o], h) {
							links = append(links, prev[o])
						}
					}
					eh[h] = w.pulse(e, pe[h], 0, nil, links...)
					mark[h].set(eh[h].n)
				}
				continue
			}
			s := side[k]
			links := []*ent{pe[s]}
			for _, o := range keys {
				if o != k && o != e && clean(prev[o], s) {
					links = append(links, prev[o])
				}
			}
			heads[k] = w.pulse(k, prev[k], 0, nil, links...)
		}
	}
	for h := range 2 {
		for _, x := range w.ents {
			if clean(x, h) {
				hist[h].set(x.n)
			}
		}
	}
	return w, hist
}

type bCase struct {
	n    int
	rule Rule
	// conflict says whether some built case must show conflicting prefixes between one-sided peers
	conflict bool
}

// runB checks every built case of one configuration and reports whether a conflict was found.
func runB(t *testing.T, c bCase, dev rules) (cases int, conflict bool) {
	for e := range c.n {
		for fr := 1; fr <= 4; fr++ {
			for mask := range 1 << c.n {
				if mask&(1<<e) != 0 {
					continue
				}
				side := make([]int, c.n)
				for k := range c.n {
					side[k] = (mask >> k) & 1
				}
				w, hist := forkWorld(c.n, e, fr, 8, side)
				cfg := cfg0(w, c.rule, seq(c.n)...)
				var lists [2][]dag.ID
				for h := range 2 {
					p := newPeer(t, w, cfg, nil, dev)
					p.feed(w.in(hist[h])...)
					lists[h] = p.ids()
					if p.s.Halted() != nil {
						t.Fatalf("a one-sided peer halted: %v", p.s.Halted())
					}
				}
				cases++
				if !isPrefix(lists[0], lists[1]) {
					conflict = true
				}
				// a peer holding both histories stops; what it committed before is a prefix of a one-sided peer's list
				for _, order := range [][]*ent{w.ents, reversed(w.ents)} {
					p := newPeer(t, w, cfg, nil, dev)
					p.feed(order...)
					if dev.noHalt {
						continue
					}
					var he *HaltError
					if err := p.s.Halted(); !errors.As(err, &he) || he.Reason != SeatFork || he.Key != keyOf(e) {
						t.Fatalf("n=%d e=%d fork %d sides %v: a peer holding both histories did not stop: %v", c.n, e, fr, side, err)
					}
					if !isPrefix(p.ids(), lists[0]) && !isPrefix(p.ids(), lists[1]) {
						t.Fatalf("a stopped peer's list conflicts with both sides")
					}
					if !c.conflict && (!isPrefix(p.ids(), lists[0]) || !isPrefix(p.ids(), lists[1])) {
						t.Fatalf("n=%d %v: a stopped peer's list conflicts with a side", c.n, c.rule)
					}
				}
			}
		}
	}
	return cases, conflict
}

func reversed(es []*ent) []*ent {
	out := slices.Clone(es)
	slices.Reverse(out)
	return out
}

// TestModelScopeB is scope b: one seat signs two histories. Majority n = 3 and n = 5 (q = 3) must show a conflict between
// one-sided peers; n = 4 q = 3 and n = 5 TwoThirds must not; peers holding both stop.
func TestModelScopeB(t *testing.T) {
	sc := newScope(t, "b", time.Minute)
	total := 0
	for _, c := range []bCase{{3, Majority, true}, {5, Majority, true}, {4, Majority, false}, {4, TwoThirds, false}, {5, TwoThirds, false}} {
		n, conflict := runB(t, c, rules{})
		total += n
		if conflict != c.conflict {
			t.Fatalf("scope b n=%d q=%d: conflict %v, want %v", c.n, Quorum(c.rule, c.n), conflict, c.conflict)
		}
		t.Logf("scope b n=%d q=%d 2q-n=%d: %d built cases, conflict between one-sided peers %v", c.n, Quorum(c.rule, c.n), 2*Quorum(c.rule, c.n)-c.n, n, conflict)
	}
	sc.done("%d built cases", total)
}

// scenario is one of scope b's settings (n, q) with its rule.
type scenario struct {
	n    int
	rule Rule
}

var bPrimeCases = []scenario{{3, Majority}, {4, Majority}, {5, TwoThirds}, {5, Majority}}

// closureWorld is the formerseat model with a second config: config 1 is three new keys 100 to 102 (q 2), set by an admin's
// Settlers entry that round 2's pivot settles; their readiness pulses reach the round-10 pivot first, so config 0 closes
// there. claims config-1 pulses name it. After that a retired seat (round 4's pivot seat) signs a second round-4 pulse.
func closureWorld(sc scenario, claims int) (w *world, honest bitset, fake *ent, real10 *ent) {
	w = newWorld()
	keys := seq(sc.n)
	heads := map[int]*ent{}
	set := w.settlersEntry(50, 50, nil, Majority, []int{100, 101, 102})
	heads = w.lockstep(keys, heads, 1, 0, nil, set)
	heads = w.lockstep(keys, heads, 4, 0, nil)
	cid := configID(1, Majority, w.seats(100, 101, 102))
	var ready []*ent
	for k := range 3 {
		ready = append(ready, w.readiness(100+k, nil, 0, nil, cid, heads[0]))
	}
	heads = w.lockstep(keys, heads, 5, 0, nil)
	pivotSeat := (10 / 2) % sc.n
	prev := map[int]*ent{}
	for k, v := range heads {
		prev[k] = v
	}
	for _, k := range keys {
		var links []*ent
		for _, o := range keys {
			if o != k {
				links = append(links, prev[o])
			}
		}
		if k == pivotSeat {
			links = append(links, ready...)
		}
		heads[k] = w.pulse(k, prev[k], 0, nil, links...)
		if k == pivotSeat {
			real10 = heads[k]
		}
	}
	w.lockstep(keys, heads, 1, 0, nil)
	for k := range claims {
		w.pulse(100+k, ready[k], 1, real10, real10)
	}
	honest = w.all()
	victim := (4 / 2) % sc.n
	var r3 []*ent
	for _, e := range w.ents {
		if e.pulse && e.ready == nil && e.key < sc.n && e.key != victim && e.index == 3 {
			r3 = append(r3, e)
		}
	}
	fake = w.pulse(victim, w.byStrand[victim][3], 0, nil, r3...)
	fake.fake = true
	return w, honest, fake, real10
}

func roundsOf(p *peer, cfg uint64) []uint64 {
	var out []uint64
	for _, x := range p.pivots {
		if x.Config == cfg {
			out = append(out, x.Round)
		}
	}
	return out
}

var oldList = []uint64{0, 2, 4, 6, 8, 10}

// TestModelScopeBPrime is scope b': a retired seat's late second history with 1, 2 and 3 config-1 claims, and a former
// admin seat's forged certificate seen with and without the real one, and by a joiner with a welcome anchor.
func TestModelScopeBPrime(t *testing.T) {
	sc := newScope(t, "b'", time.Minute)
	cases := 0
	for _, s := range bPrimeCases {
		q := Quorum(s.rule, s.n)
		for claims := 1; claims <= 3; claims++ {
			w, honest, fake, _ := closureWorld(s, claims)
			cfg := cfg0(w, s.rule, seq(s.n)...)
			old := newPeer(t, w, cfg, nil, rules{})
			old.feed(w.in(honest)...)
			if got := roundsOf(old, 0); !slices.Equal(got, oldList) || old.s.Config().Number != 1 {
				t.Fatalf("n=%d q=%d: old peers commit %v in config %d", s.n, q, got, old.s.Config().Number)
			}
			// the negative control: without the halting rule and with the walk taking the first vertex in the whole DAG, a joiner
			// that gets the late history first settles [0 2 6 8 10]
			order := append([]*ent{}, w.ents[:fake.prev.n+1]...)
			order = append(order, fake)
			order = append(order, w.ents[fake.prev.n+1:fake.n]...)
			nh := newPeer(t, w, cfg, nil, rules{noHalt: true, globalWalk: true})
			nh.feed(order...)
			if got := roundsOf(nh, 0); !slices.Equal(got, []uint64{0, 2, 6, 8, 10}) {
				t.Fatalf("n=%d q=%d without the halting rule: %v, want [0 2 6 8 10]", s.n, q, got)
			}
			for oi, ord := range [][]*ent{order, w.ents, reversed(w.ents), shuffled(w.ents, uint64(claims))} {
				j := newPeer(t, w, cfg, nil, rules{})
				j.feed(ord...)
				got := roundsOf(j, 0)
				if !isPrefix(j.ids(), old.ids()) {
					t.Fatalf("n=%d q=%d claims %d order %d: the joiner settled %v, a wrong list", s.n, q, claims, oi, got)
				}
				// with one claim there is no certificate: a joiner that holds the late history before it settled config 0 waits there
				waits := j.s.Config().Number == 0 && j.s.Halted() != nil
				if claims < 2 && (oi == 0 && !waits || !waits && !slices.Equal(got, oldList)) {
					t.Fatalf("n=%d q=%d with %d claim order %d: config %d %v, halted %v", s.n, q, claims, oi, j.s.Config().Number, got, j.s.Halted())
				}
				if claims >= 2 && (!slices.Equal(got, oldList) || j.s.Config().Number != 1) {
					t.Fatalf("n=%d q=%d with %d claims order %d: joiner %v in config %d; want the certificate used", s.n, q, claims, oi, got, j.s.Config().Number)
				}
				cases++
			}
		}
		cases += forgedCert(t, s)
	}
	sc.done("%d built cases", cases)
}

func shuffled(es []*ent, seed uint64) []*ent {
	out := slices.Clone(es)
	r := rand.New(rand.NewPCG(seed, 99))
	r.Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })
	return out
}

// forgedCert: the real config 1 is seats 0 and 1 and a new key 50, closing config 0 at the real round-10 pivot. The pivot
// seat of round 10, an admin of config 0, signs a second history from that index: a Settlers entry naming keys 60 and 61 it
// owns, their readiness pulses, its fake round-10 pivot and their config-1 pulses naming it.
func forgedCert(t *testing.T, s scenario) int {
	w := newWorld()
	keys := seq(s.n)
	attacker := (10 / 2) % s.n
	set := w.settlersEntry(70, 70, nil, Majority, []int{0, 1, 50})
	heads := w.lockstep(keys, map[int]*ent{}, 2, 0, nil)
	heads = w.lockstep(keys, heads, 1, 0, nil, set)
	heads = w.lockstep(keys, heads, 4, 0, nil)
	ready50 := w.readiness(50, nil, 0, nil, configID(1, Majority, w.seats(0, 1, 50)), heads[0])
	heads = w.lockstep(keys, heads, 3, 0, nil)
	prev := map[int]*ent{}
	for k, v := range heads {
		prev[k] = v
	}
	var real10, fc *ent
	var fakes []*ent
	for _, k := range keys {
		var links []*ent
		for _, o := range keys {
			if o != k {
				links = append(links, prev[o])
			}
		}
		if k == attacker {
			f1 := w.settlersEntry(k, k, prev[k], Majority, []int{60, 61})
			fcid := configID(1, Majority, w.seats(60, 61))
			r60 := w.readiness(60, nil, 0, nil, fcid, prev[0])
			r61 := w.readiness(61, nil, 0, nil, fcid, prev[0])
			fc = w.pulse(k, f1, 0, nil, append(slices.Clone(links), r60, r61)...)
			c60 := w.pulse(60, r60, 1, fc, fc)
			c61 := w.pulse(61, r61, 1, fc, fc)
			fakes = append(fakes, f1, r60, r61, fc, c60, c61)
			links = append(links, ready50)
		}
		heads[k] = w.pulse(k, prev[k], 0, nil, links...)
		if k == attacker {
			real10 = heads[k]
		}
	}
	heads = w.lockstep(keys, heads, 1, 0, nil)
	var realClaims []*ent
	for _, k := range []int{0, 1} {
		realClaims = append(realClaims, w.pulse(k, heads[k], 1, real10, heads[1-k], real10))
	}
	for _, f := range fakes {
		f.fake = true
	}
	var honest, forgedOnly bitset
	for _, e := range w.ents {
		if !e.fake {
			honest.set(e.n)
		}
		if !slices.Contains(realClaims, e) {
			forgedOnly.set(e.n)
		}
	}
	cfg := cfg0(w, s.rule, seq(s.n)...)
	q := Quorum(s.rule, s.n)
	old := newPeer(t, w, cfg, nil, rules{})
	old.feed(w.in(honest)...)
	if got := roundsOf(old, 0); !slices.Equal(got, oldList) || old.s.Config().Number != 1 || old.s.Config().Activating != real10.id {
		t.Fatalf("forgedcert n=%d q=%d: old peers %v config %d", s.n, q, got, old.s.Config().Number)
	}
	// a peer that holds both certificates uses the real one: the forged pivot has no votes of config 0
	both := newPeer(t, w, cfg, nil, rules{})
	both.place(w.ents...)
	both.advance()
	if got := roundsOf(both, 0); !slices.Equal(got, oldList) || both.s.Config().Activating != real10.id {
		t.Fatalf("forgedcert n=%d q=%d: a peer holding both certificates: %v config %d, halted %v", s.n, q, got, both.s.Config().Number, both.s.Halted())
	}
	// a peer that sees only the forged certificate stops on the fork instead of being misled
	fo := newPeer(t, w, cfg, nil, rules{})
	fo.place(w.in(forgedOnly)...)
	fo.advance()
	var he *HaltError
	if !isPrefix(fo.ids(), old.ids()) || fo.s.Config().Number != 0 || !errors.As(fo.s.Halted(), &he) || he.Reason != SeatFork {
		t.Fatalf("forgedcert n=%d q=%d: the forged-only peer %v config %d, halted %v", s.n, q, roundsOf(fo, 0), fo.s.Config().Number, fo.s.Halted())
	}
	// the negative control: without the votes rule both certificates are valid, so a peer holding both stops and a peer that
	// sees only the forged one is led to a fake prefix
	nb := newPeer(t, w, cfg, nil, rules{certNoVotes: true})
	nb.place(w.ents...)
	nb.advance()
	if err := nb.s.Halted(); !errors.As(err, &he) || he.Reason != TwoCertificates || he.Key != keyOf(attacker) || !isPrefix(nb.ids(), old.ids()) {
		t.Fatalf("forgedcert n=%d q=%d without votes: a peer holding both certificates: %v", s.n, q, err)
	}
	nf := newPeer(t, w, cfg, nil, rules{certNoVotes: true})
	nf.place(w.in(forgedOnly)...)
	nf.advance()
	if isPrefix(nf.ids(), old.ids()) || nf.s.Config().Activating != fc.id {
		t.Fatalf("forgedcert n=%d q=%d without votes: the forged-only peer was not misled: %v", s.n, q, roundsOf(nf, 0))
	}
	// a joiner with the welcome anchor settles exactly the old list, whatever it holds and in any order
	for oi, ord := range [][]*ent{w.ents, reversed(w.ents), shuffled(w.ents, 5), w.in(forgedOnly)} {
		j := newPeer(t, w, cfg, &Anchor{ID: real10.id, Config: 0}, rules{})
		j.feed(ord...)
		if oi == 3 {
			// without the real claims the anchor still names the real pivot
			j.feed(realClaims...)
		}
		if got := roundsOf(j, 0); !slices.Equal(got, oldList) || j.s.Config().Activating != real10.id {
			t.Fatalf("forgedcert n=%d q=%d order %d: anchored joiner %v (config %d, halted %v)", s.n, q, oi, got, j.s.Config().Number, j.s.Halted())
		}
	}
	t.Logf("scope b' forgedcert n=%d q=%d: old %v; both certificates: the real one used; forged only: halted at %v; without votes misled %v; anchored joiner %v",
		s.n, q, roundsOf(old, 0), roundsOf(fo, 0), roundsOf(nf, 0), oldList)
	return 9 + currentForgery(t, s)
}

// currentForgery is the forged certificate of a seat of the current config: a peer that committed through round 8 and then
// holds every entry must stop, never settle the fake pivot; without the votes rule it settles a conflicting prefix.
func currentForgery(t *testing.T, s scenario) int {
	w, honest, real10, fc := currentSeatForgery(s.n)
	cfg := cfg0(w, s.rule, seq(s.n)...)
	old := newPeer(t, w, cfg, nil, rules{})
	old.feed(w.in(honest)...)
	if !slices.Contains(old.ids(), real10.id) {
		t.Fatalf("currentforgery n=%d: the honest peer did not commit the real pivot: %v", s.n, old.rounds())
	}
	for _, dev := range []rules{{}, {certNoVotes: true}} {
		v := newPeer(t, w, cfg, nil, dev)
		for _, e := range w.in(honest) {
			if e.n < real10.n {
				v.feed(e)
			}
		}
		v.feed(w.ents...)
		misled := !isPrefix(v.ids(), old.ids()) || v.s.Config().Activating == fc.id
		if misled != dev.certNoVotes {
			t.Fatalf("currentforgery n=%d q=%d certNoVotes %v: honest %v, victim %v config %d, halted %v", s.n, Quorum(s.rule, s.n),
				dev.certNoVotes, old.rounds(), v.rounds(), v.s.Config().Number, v.s.Halted())
		}
	}
	return 2
}

func TestFormerSeatAnchor(t *testing.T) {
	for _, s := range bPrimeCases {
		w, honest, fake, real10 := closureWorld(s, 1)
		cfg := cfg0(w, s.rule, seq(s.n)...)
		for _, ord := range [][]*ent{w.ents, reversed(w.ents)} {
			j := newPeer(t, w, cfg, &Anchor{ID: real10.id, Config: 0}, rules{})
			j.feed(ord...)
			if got := roundsOf(j, 0); !slices.Equal(got, oldList) {
				t.Fatalf("n=%d: anchored joiner %v", s.n, got)
			}
		}
		_ = honest
		_ = fake
	}
}

func TestBadAnchor(t *testing.T) {
	w := newWorld()
	keys := seq(3)
	heads := w.lockstep(keys, map[int]*ent{}, 6, 0, nil)
	cfg := cfg0(w, Majority, keys...)
	for i, a := range []Anchor{
		{ID: heads[2].id, Config: 0},           // not a pivot
		{ID: heads[1].prev.prev.id, Config: 1}, // wrong config number
		{ID: w.byStrand[0][1].id, Config: 0},   // an odd round
	} {
		p := newPeer(t, w, cfg, &a, rules{})
		p.feed(w.ents...)
		var he *HaltError
		if err := p.s.Halted(); !errors.As(err, &he) || he.Reason != BadTrust || len(p.pivots) != 0 {
			t.Fatalf("anchor %d: %v, %d pivots", i, err, len(p.pivots))
		}
	}
	// an anchor that is not a pulse: placed, never added
	d := w.add(9, 9, nil)
	p := newPeer(t, w, cfg, &Anchor{ID: d.id}, rules{})
	p.feed(w.ents...)
	if p.s.Halted() == nil {
		t.Fatal("anchor on a data entry")
	}
	// an anchor not held yet: nothing is committed by direct commits meanwhile
	p = newPeer(t, w, cfg, &Anchor{ID: dag.ID{1}}, rules{})
	p.feed(w.ents...)
	if len(p.pivots) != 0 || p.s.Halted() != nil || p.s.Predicted() != nil {
		t.Fatalf("pending anchor: %d pivots, %v", len(p.pivots), p.s.Halted())
	}
	_ = fmt.Sprint()
}

// TestCertificateKeepsCommittedList: a certificate whose walk does not pass through the last committed pivot is ignored,
// even when q of the next config's seats claim it.
func TestCertificateKeepsCommittedList(t *testing.T) {
	w := newWorld()
	keys := seq(3)
	w.lockstep(keys, map[int]*ent{}, 4, 0, nil)
	honest := len(w.ents)
	s := func(k, i int) *ent { return w.byStrand[k][i] }
	f1 := w.pulse(1, s(1, 1), 0, nil, s(0, 1), s(2, 1))
	f2a := w.pulse(2, s(2, 1), 0, nil, s(0, 1), f1)
	f2b := w.pulse(2, f2a, 0, nil, f1)
	f1b := w.pulse(1, f1, 0, nil, f2a)
	set := w.settlersEntry(70, 70, nil, Majority, []int{0})
	v4 := w.pulse(2, f2b, 0, nil, f1b, set)
	w.pulse(0, s(0, 3), 1, v4, v4)
	p := newPeer(t, w, cfg0(w, Majority, keys...), nil, rules{})
	p.feed(w.ents[:honest]...)
	before := p.ids()
	if len(before) != 2 {
		t.Fatalf("honest part committed %v", p.rounds())
	}
	p.feed(w.ents[honest:]...)
	if !slices.Equal(p.ids(), before) || p.s.Config().Number != 0 {
		t.Fatalf("a certificate off the committed list was used: %v, config %d", p.rounds(), p.s.Config().Number)
	}
}
