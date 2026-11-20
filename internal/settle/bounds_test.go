package settle

import (
	"fmt"
	"testing"

	"github.com/ciranda-dev/kolo-braid/internal/dag"
)

func (s *Settler) retained() (recs, ids int) {
	for _, st := range s.seats {
		recs += len(st.recs)
	}
	return recs, len(s.byID)
}

// TestBoundsPrune: with every seat pulsing, the pulses kept do not grow with the rounds.
func TestBoundsPrune(t *testing.T) {
	for _, n := range []int{4, 7} {
		w := newWorld()
		heads := map[int]*ent{}
		p := newPeer(t, w, cfg0(w, Majority, seq(n)...), nil, rules{})
		most := 0
		for range 400 {
			from := len(w.ents)
			heads = w.lockstep(seq(n), heads, 1, 0, nil)
			p.feed(w.ents[from:]...)
			recs, ids := p.s.retained()
			most = max(most, recs, ids)
		}
		if len(p.pivots) != 200 || most > 4*n {
			t.Fatalf("n=%d: %d pivots, up to %d pulses kept", n, len(p.pivots), most)
		}
		t.Logf("n=%d: 400 rounds, at most %d pulses kept", n, most)
	}
}

// TestBoundsSilentSeat: a seat that never pulses keeps the rounds of the others, one pulse per seat per round, until it
// pulses or the config changes; the record keeps these lists in the entry index.
func TestBoundsSilentSeat(t *testing.T) {
	w := newWorld()
	heads := map[int]*ent{}
	p := newPeer(t, w, cfg0(w, Majority, seq(4)...), nil, rules{})
	for i := range 200 {
		from := len(w.ents)
		heads = w.lockstep(seq(3), heads, 1, 0, nil)
		p.feed(w.ents[from:]...)
		if recs, _ := p.s.retained(); recs != 3*(i+1) {
			t.Fatalf("round %d: %d pulses kept, want %d", i, recs, 3*(i+1))
		}
	}
	last := w.pulse(3, nil, 0, nil, heads[0], heads[1], heads[2])
	heads[3] = last
	p.feed(last)
	from := len(w.ents)
	w.lockstep(seq(4), heads, 4, 0, nil)
	p.feed(w.ents[from:]...)
	if recs, _ := p.s.retained(); recs > 16 {
		t.Fatalf("%d pulses kept after the silent seat caught up", recs)
	}
}

// TestBoundsFloods: a seat writing many pulses of one round, pulses of later configs and claims for one pivot are all
// dropped once they settle, and claims cost at most a roster call per candidate pivot and commit.
func TestBoundsFloods(t *testing.T) {
	w := newWorld()
	keys := seq(4)
	heads := w.lockstep(keys, map[int]*ent{}, 2, 0, nil)
	p := newPeer(t, w, cfg0(w, Majority, keys...), nil, rules{})
	p.feed(w.ents...)
	const flood = 5000
	x := heads[0]
	fl := map[int]*ent{}
	for i := range flood {
		x = w.pulse(0, x, 0, nil)
		if i%2 == 0 {
			k := 20 + i%7
			fl[k] = w.pulse(k, fl[k], uint64(3+i%5), x, x)
		}
	}
	heads[0] = x
	p.feed(w.ents[len(w.ents)-3*flood/2:]...)
	if recs, _ := p.s.retained(); recs < flood {
		t.Fatalf("flood not held while unsettled: %d", recs)
	}
	v2 := heads[1]
	var c *ent
	for range 300 {
		c = w.pulse(40, c, 1, v2, v2)
		p.feed(c)
	}
	calls := p.r.calls
	from := len(w.ents)
	extra := []*ent{c}
	for _, e := range fl {
		extra = append(extra, e)
	}
	w.lockstep(keys, heads, 8, 0, nil, extra...)
	p.feed(w.ents[from:]...)
	recs, ids := p.s.retained()
	if recs > 16 || len(p.s.future) != 0 || ids > 16 {
		t.Fatalf("after settling: %d pulses, %d IDs, %d later-config pulses kept", recs, ids, len(p.s.future))
	}
	if extra := p.r.calls - calls; extra > 2*len(p.pivots) {
		t.Fatalf("%d roster calls for %d pivots", extra, len(p.pivots))
	}
	t.Logf("%d flood pulses, 300 claims: %d roster calls in all, %d pulses kept after settling", flood, p.r.calls, recs)
}

func benchWorld(n, rounds int) *world {
	w := newWorld()
	w.lockstep(seq(n), map[int]*ent{}, rounds, 0, nil)
	return w
}

// BenchmarkPulse is the cost of adding one placed pulse and advancing, in lockstep rounds of n seats.
func BenchmarkPulse(b *testing.B) {
	for _, n := range []int{4, 7, 64} {
		w := benchWorld(n, 64)
		b.Run(fmt.Sprintf("seats=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			done := 0
			for b.Loop() {
				b.StopTimer()
				p := newPeer(b, w, cfg0(w, Majority, seq(n)...), nil, rules{})
				for _, e := range w.ents {
					if _, err := p.d.Add(w.node(e)); err != nil {
						b.Fatal(err)
					}
				}
				b.StartTimer()
				for _, e := range w.ents {
					if err := p.s.Add(e.asPulse()); err != nil {
						b.Fatal(err)
					}
					p.advance()
				}
				done += len(w.ents)
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(done), "ns/pulse")
		})
	}
}

// BenchmarkRound is Round for a past that holds every seat's newest pulse.
func BenchmarkRound(b *testing.B) {
	for _, n := range []int{4, 64} {
		w := benchWorld(n, 16)
		p := newPeer(b, w, cfg0(w, Majority, seq(n)...), nil, rules{})
		p.feed(w.ents...)
		var cs []dag.Clock
		for k := range n {
			c, _ := p.d.Clock(w.byStrand[k][len(w.byStrand[k])-1].id)
			cs = append(cs, c)
		}
		u := union(cs...)
		b.Run(fmt.Sprintf("seats=%d", n), func(b *testing.B) {
			for b.Loop() {
				p.s.Round(u)
			}
		})
	}
}

func BenchmarkSave(b *testing.B) {
	w := benchWorld(7, 40)
	p := newPeer(b, w, cfg0(w, Majority, seq(7)...), nil, rules{})
	p.feed(w.ents...)
	b.ReportAllocs()
	for b.Loop() {
		p.s.Save()
	}
}

// TestBoundsReadinessOfAMember: readiness pulses a member writes for IDs that no pending config has are dropped once they
// settle, and so are settled pulses of the next config, so neither grows the state saved with every settled batch.
func TestBoundsReadinessOfAMember(t *testing.T) {
	w := newWorld()
	heads := map[int]*ent{}
	p := newPeer(t, w, cfg0(w, Majority, seq(3)...), nil, rules{})
	var junk, later *ent
	for round := range 10 {
		from := len(w.ents)
		for i := range 40 {
			junk = w.readiness(9, junk, 0, nil, configID(uint64(1000+40*round+i), Majority, nil))
		}
		extra := []*ent{junk}
		if s0 := w.byStrand[0]; len(s0) > 0 {
			for i := range 40 {
				later = w.pulse(8, later, 1, s0[max(0, len(s0)-1-i%3)])
			}
			extra = append(extra, later)
		}
		heads = w.lockstep(seq(3), heads, 3, 0, nil, extra...)
		p.feed(w.ents[from:]...)
	}
	if len(p.pivots) == 0 || len(p.s.ready) != 0 || len(p.s.cands) != 0 {
		t.Fatalf("%d pivots; after 400 settled readiness pulses %d are kept, %d claim lists", len(p.pivots), len(p.s.ready), len(p.s.cands))
	}
	t.Logf("state %d bytes", len(p.s.Save()))
}

// TestReadinessBeforeItsPending: a readiness pulse settled before its config became pending counts for nothing: the seat
// is awaited until it writes one later, which then activates the config.
func TestReadinessBeforeItsPending(t *testing.T) {
	w := newWorld()
	cid := configID(1, Majority, w.seats(0, 1, 2, 9))
	early := w.readiness(9, nil, 0, nil, cid)
	set := w.settlersEntry(70, 70, nil, Majority, []int{0, 1, 2, 9})
	heads := w.lockstep(seq(3), map[int]*ent{}, 2, 0, nil, early)
	heads = w.lockstep(seq(3), heads, 4, 0, nil, set)
	// one peer settles as the pulses arrive, the other everything in one walk: the rule, not the pruning, decides
	p := newPeer(t, w, cfg0(w, Majority, seq(3)...), nil, rules{})
	p.feed(w.ents...)
	all := newPeer(t, w, cfg0(w, Majority, seq(3)...), nil, rules{})
	all.place(w.ents...)
	all.advance()
	for i, x := range []*peer{p, all} {
		if _, ok := x.s.Pending(); !ok || x.s.Config().Number != 0 || len(x.s.Awaiting()) != 1 {
			t.Fatalf("peer %d: an early readiness pulse counted: config %d, awaiting %v", i, x.s.Config().Number, x.s.Awaiting())
		}
	}
	from := len(w.ents)
	ready := w.readiness(9, early, 0, nil, cid, heads[0])
	w.lockstep(seq(3), heads, 4, 0, nil, ready)
	p.feed(w.ents[from:]...)
	if p.s.Config().Number != 1 {
		t.Fatalf("the readiness pulse after the pending config did not activate it: config %d", p.s.Config().Number)
	}
}

// TestBoundsReadinessOfAnAddedSeat: an added seat that writes many readiness pulses for its pending config, which waits for
// another added seat, keeps one settled record.
func TestBoundsReadinessOfAnAddedSeat(t *testing.T) {
	w := newWorld()
	set := w.settlersEntry(70, 70, nil, Majority, []int{0, 1, 2, 8, 9})
	heads := w.lockstep(seq(3), map[int]*ent{}, 4, 0, nil, set)
	cid := configID(1, Majority, w.seats(0, 1, 2, 8, 9))
	var r9 *ent
	for range 5 {
		for range 8 {
			r9 = w.readiness(9, r9, 0, nil, cid, heads[0])
		}
		heads = w.lockstep(seq(3), heads, 3, 0, nil, r9)
	}
	p := newPeer(t, w, cfg0(w, Majority, seq(3)...), nil, rules{})
	p.feed(w.ents...)
	// added again, as by a restored DAG, the settled ones stay one
	for _, e := range w.byStrand[9] {
		if err := p.s.Add(e.asPulse()); err != nil {
			t.Fatal(err)
		}
	}
	n := 0
	for _, r := range p.s.ready {
		if r.key == keyOf(9) {
			n++
		}
	}
	if aw := p.s.Awaiting(); p.s.Config().Number != 0 || len(aw) != 1 || aw[0] != w.seat(8) || n != 1 {
		t.Fatalf("config %d, awaiting %v, %d records of the ready seat kept", p.s.Config().Number, aw, n)
	}
}

// TestPendingAgeSurvivesRestart: after a restart, a readiness pulse settled before its config became pending still counts
// for nothing, also when the restored DAG adds it again.
func TestPendingAgeSurvivesRestart(t *testing.T) {
	w := newWorld()
	cid := configID(1, Majority, w.seats(0, 1, 2, 9))
	early := w.readiness(9, nil, 0, nil, cid)
	set := w.settlersEntry(70, 70, nil, Majority, []int{0, 1, 2, 9})
	heads := w.lockstep(seq(3), map[int]*ent{}, 2, 0, nil, early)
	heads = w.lockstep(seq(3), heads, 4, 0, nil, set)
	p := newPeer(t, w, cfg0(w, Majority, seq(3)...), nil, rules{})
	p.feed(w.ents...)
	q := restart(t, p, w.ents)
	from := len(w.ents)
	w.lockstep(seq(3), heads, 4, 0, nil)
	q.feed(w.ents[from:]...)
	if _, ok := q.s.Pending(); !ok || q.s.Config().Number != 0 || len(q.s.Awaiting()) != 1 {
		t.Fatalf("after a restart an early readiness pulse counted: config %d, awaiting %v", q.s.Config().Number, q.s.Awaiting())
	}
}

// TestSettledReadinessNotKeptAgain: a settled readiness pulse that cannot count is not kept when it is added again.
func TestSettledReadinessNotKeptAgain(t *testing.T) {
	w := newWorld()
	junk := w.readiness(9, nil, 0, nil, configID(7, Majority, nil))
	w.lockstep(seq(3), map[int]*ent{}, 4, 0, nil, junk)
	p := newPeer(t, w, cfg0(w, Majority, seq(3)...), nil, rules{})
	p.feed(w.ents...)
	if len(p.s.ready) != 0 {
		t.Fatalf("%d settled readiness records kept", len(p.s.ready))
	}
	if err := p.s.Add(junk.asPulse()); err != nil || len(p.s.ready) != 0 {
		t.Fatalf("added again: %v, %d records", err, len(p.s.ready))
	}
}
