package settle

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/ciranda-dev/kolo-braid/internal/dag"
)

// splitGen builds a random braid world with Settlers changes (a share of them repeating an earlier set, for undo and redo),
// readiness pulses of every kind and config changes, fed to a driver peer entry by entry so that later configs' seats write
// pulses of their config.
type splitGen struct {
	t       testing.TB
	w       *world
	r       *rand.Rand
	drv     *peer
	extras  []*ent
	admin   *ent
	sets    []Proposal
	offline map[int]bool
	pool    []int
	redo    float64
}

func (g *splitGen) last(strand int) *ent {
	es := g.w.byStrand[strand]
	if len(es) == 0 {
		return nil
	}
	return es[len(es)-1]
}

func (g *splitGen) emit(e *ent) {
	g.drv.feed(e)
}

func (g *splitGen) cur() (uint64, []int, *ent) {
	c := g.drv.s.Config()
	var act *ent
	if c.Number > 0 {
		act = g.w.byID[c.Activating]
	}
	return c.Number, keysOf(c.Seats), act
}

func (g *splitGen) randHeads(keys []int, self int, p float64) []*ent {
	var out []*ent
	for _, k := range keys {
		if k == self {
			continue
		}
		if h := g.last(g.w.seatStrand(k)); h != nil && g.r.Float64() < p {
			out = append(out, h)
		}
	}
	return out
}

func (g *splitGen) takeExtras(p float64) []*ent {
	var out, keep []*ent
	for _, e := range g.extras {
		if g.r.Float64() < p {
			out = append(out, e)
		} else {
			keep = append(keep, e)
		}
	}
	g.extras = keep
	return out
}

func (g *splitGen) build(steps int) {
	for step := 0; step < steps && len(g.w.ents) < 230; step++ {
		cfgN, seats, act := g.cur()
		order := slices.Clone(seats)
		g.r.Shuffle(len(order), func(i, j int) { order[i], order[j] = order[j], order[i] })
		for _, k := range order {
			if g.r.Float64() < 0.12 {
				continue
			}
			links := g.randHeads(seats, k, 0.75)
			links = append(links, g.takeExtras(0.4)...)
			if act != nil {
				links = append(links, act)
			}
			g.emit(g.w.pulse(k, g.last(g.w.seatStrand(k)), cfgN, act, links...))
			if g.drv.s.Config().Number != cfgN {
				cfgN, seats, act = g.cur()
			}
		}
		if g.r.Float64() < 0.10 {
			var keys []int
			for _, k := range g.pool {
				if g.r.Float64() < 0.55 {
					keys = append(keys, k)
				}
			}
			if len(keys) == 0 {
				keys = []int{g.pool[g.r.IntN(len(g.pool))]}
			}
			rule := Majority
			if g.r.Float64() < 0.25 {
				rule = TwoThirds
			}
			if len(g.sets) > 0 && g.r.Float64() < g.redo {
				// undo or redo: an earlier set again
				p := g.sets[g.r.IntN(len(g.sets))]
				keys, rule = keysOf(p.Seats), p.Rule
			}
			g.admin = g.w.settlersEntry(70, 70, g.admin, rule, keys, g.randHeads(seats, -1, 0.6)...)
			g.sets = append(g.sets, Proposal{Rule: rule, Seats: g.w.seats(keys...)})
			g.emit(g.admin)
			g.extras = append(g.extras, g.admin)
		}
		nr := 0
		if g.r.Float64() < 0.45 {
			nr = 1 + g.r.IntN(2)
		}
		for range nr {
			cfgN, seats, _ = g.cur()
			var k int
			switch x := g.r.Float64(); {
			case x < 0.7:
				var cands []int
				for _, p := range g.pool {
					if !slices.Contains(seats, p) {
						cands = append(cands, p)
					}
				}
				if len(cands) == 0 {
					continue
				}
				k = cands[g.r.IntN(len(cands))]
			case x < 0.85:
				k = seats[g.r.IntN(len(seats))]
			default:
				k = 20
			}
			if g.offline[k] {
				continue
			}
			var id [32]byte
			pend, ok := g.drv.s.Pending()
			switch x := g.r.Float64(); {
			case x < 0.6 && ok:
				id = configID(cfgN+1, pend.Rule, pend.Seats)
			case x < 0.8 && len(g.sets) > 0:
				p := g.sets[g.r.IntN(len(g.sets))]
				id = configID(cfgN+1, p.Rule, p.Seats)
			case x < 0.9:
				id = [32]byte{byte(g.r.IntN(256)), 0xee}
			default:
				if ok {
					id = configID(cfgN+2, pend.Rule, pend.Seats)
				}
			}
			e := g.w.readiness(k, g.last(g.w.seatStrand(k)), 0, nil, id, g.randHeads(seats, k, 0.7)...)
			g.emit(e)
			g.extras = append(g.extras, e)
		}
		if g.r.Float64() < 0.05 {
			cfgN, seats, act = g.cur()
			links := g.randHeads(seats, -1, 0.7)
			if act != nil {
				links = append(links, act)
			}
			e := g.w.pulse(20, g.last(20), cfgN, act, links...)
			g.emit(e)
			g.extras = append(g.extras, e)
		}
	}
	// tail: current seats keep pulsing so that what can settle settles
	for range 10 {
		cfgN, seats, act := g.cur()
		for _, k := range seats {
			links := g.randHeads(seats, k, 1)
			links = append(links, g.takeExtras(1)...)
			if act != nil {
				links = append(links, act)
			}
			g.emit(g.w.pulse(k, g.last(g.w.seatStrand(k)), cfgN, act, links...))
		}
	}
}

type splitState struct {
	pivots   []Pivot
	cfg      Config
	pend     *Proposal
	awaiting []Seat
	count    uint64
}

func snap(p *peer) splitState {
	z := splitState{pivots: slices.Clone(p.pivots), cfg: p.s.Config(), awaiting: p.s.Awaiting(), count: p.s.Count()}
	if pr, ok := p.s.Pending(); ok {
		z.pend = &pr
	}
	return z
}

func (a splitState) diff(b splitState) string {
	if len(a.pivots) != len(b.pivots) {
		return fmt.Sprintf("pivots %d vs %d", len(a.pivots), len(b.pivots))
	}
	for i := range a.pivots {
		if a.pivots[i] != b.pivots[i] {
			return fmt.Sprintf("pivot %d: %+v vs %+v", i, a.pivots[i], b.pivots[i])
		}
	}
	if a.cfg.Number != b.cfg.Number || a.cfg.Rule != b.cfg.Rule || a.cfg.Activating != b.cfg.Activating || !slices.Equal(a.cfg.Seats, b.cfg.Seats) {
		return fmt.Sprintf("config %d vs %d", a.cfg.Number, b.cfg.Number)
	}
	if !sameProposal(a.pend, b.pend) {
		return "pending differs"
	}
	if !slices.Equal(a.awaiting, b.awaiting) {
		return fmt.Sprintf("awaiting %d vs %d", len(a.awaiting), len(b.awaiting))
	}
	if a.count != b.count {
		return "count differs"
	}
	return ""
}

// splitRestart saves and loads p into a new DAG holding the entries given so far.
func splitRestart(t testing.TB, p *peer, given []*ent) *peer {
	b := p.s.Save()
	d, err := dag.New(dag.Config{MaxLinks: 63, MaxPendingBytes: 1 << 30})
	if err != nil {
		t.Fatal(err)
	}
	q := &peer{w: p.w, d: d}
	q.r = newRoster(p.w, d, p.r.base)
	q.r.committed = slices.Clone(p.r.committed)
	q.pivots = slices.Clone(p.pivots)
	q.s, err = Load(d, q.r.fn, b)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if err := q.s.RestoreMarks(p.s.Marks()); err != nil {
		t.Fatalf("restore marks: %v", err)
	}
	if !bytes.Equal(q.s.Save(), b) {
		t.Fatal("Save after Load differs")
	}
	q.place(given...)
	return q
}

// readyBound checks that at most one settled readiness record per seat is kept and each kept one can count.
func readyBound(p *peer) error {
	s := p.s
	if s.last == nil {
		return nil
	}
	seen := map[Seat]bool{}
	for _, r := range s.ready {
		if !in(s.last.clock, r) {
			continue
		}
		st := Seat{Key: r.key, Strand: r.strand}
		if seen[st] {
			return fmt.Errorf("two settled readiness records of one seat kept")
		}
		seen[st] = true
		if !s.keepReady(r) {
			return fmt.Errorf("settled readiness that cannot count kept")
		}
	}
	return nil
}

func splitWorld(t testing.TB, seed uint64) (*world, Config, *peer, *splitGen) {
	r := rand.New(rand.NewPCG(seed, 0xc0f6))
	w := newWorld()
	n := 3 + r.IntN(3)
	rule := Majority
	if r.IntN(4) == 0 {
		rule = TwoThirds
	}
	cfg := cfg0(w, rule, seq(n)...)
	g := &splitGen{t: t, w: w, r: r, offline: map[int]bool{}, redo: []float64{0, 0.3, 0.4}[seed%3]}
	g.pool = seq(8)
	if r.IntN(3) == 0 {
		g.offline[n+r.IntN(8-n)] = true
	}
	g.drv = newPeer(t, w, cfg, nil, rules{})
	g.build(30 + r.IntN(30))
	return w, cfg, g.drv, g
}

// TestPropertyBatchSplit: random worlds with settler changes and readiness pulses of every kind; peers that receive them in
// creation or random order, settle in random batches (one walk included), restart at a random point, or join through an
// anchor, end with the driver's pivots, config, pending config, awaited seats and count, and keep at most one settled
// readiness record per seat, one that can count. Every third world is also checked against the reference.
func TestPropertyBatchSplit(t *testing.T) {
	seeds, limit := 100, 2*time.Minute
	if raceEnabled {
		seeds = 10
	}
	if *fullModel {
		seeds, limit = 30_000, 6*time.Hour
	}
	sc := newScope(t, "batch split", limit)
	stats := map[string]int{}
	for seed := uint64(0); seed < uint64(seeds); seed++ {
		if sc.over() {
			t.Fatalf("batch split: %v after %d worlds", errIncomplete, seed)
		}
		w, cfg, drv, _ := splitWorld(t, seed)
		ref := snap(drv)
		stats["worlds"]++
		stats[fmt.Sprintf("cfg%d", ref.cfg.Number)]++
		if ref.pend != nil {
			stats["pending at end"]++
		}
		for _, x := range ref.pivots {
			if x.Activates {
				stats["activations"]++
			}
		}
		if err := readyBound(drv); err != nil {
			t.Fatalf("seed %d driver: %v", seed, err)
		}
		// the reference model
		if seed%3 == 0 {
			rf := newRef(w, w.all(), cfg.Rule, seq(len(cfg.Seats)), dagRoster(t, w, w.all(), Proposal{Rule: cfg.Rule, Seats: cfg.Seats}))
			want := refIDs(rf.committed())
			if !slices.Equal(want, drv.ids()) || rf.cfgN != ref.cfg.Number {
				t.Fatalf("seed %d: driver %d pivots config %d, reference %d config %d", seed, len(drv.pivots), ref.cfg.Number, len(want), rf.cfgN)
			}
			stats["ref checked"]++
		}
		r := rand.New(rand.NewPCG(seed, 0x5eed))
		for v := 0; v < 12; v++ {
			p := newPeer(t, w, cfg, nil, rules{})
			order := slices.Clone(w.ents)
			mode := v % 4
			if mode >= 2 {
				r.Shuffle(len(order), func(i, j int) { order[i], order[j] = order[j], order[i] })
			}
			pAdv := []float64{0, 0.05, 0.3, 0.7}[r.IntN(4)]
			restartAt := -1
			if mode == 1 || mode == 3 {
				restartAt = r.IntN(len(order))
			}
			for i, e := range order {
				p.place(e)
				if r.Float64() < pAdv {
					p.advance()
					if err := readyBound(p); err != nil {
						t.Fatalf("seed %d variant %d: %v", seed, v, err)
					}
				}
				if i == restartAt {
					p.advance()
					p = splitRestart(t, p, order[:i+1])
					p.t = t
					p.advance()
					stats["restarts"]++
				}
			}
			p.advance()
			if err := readyBound(p); err != nil {
				t.Fatalf("seed %d variant %d: %v", seed, v, err)
			}
			if d := snap(p).diff(ref); d != "" {
				t.Fatalf("seed %d variant %d (mode %d, pAdv %.2f, restart %d): %s", seed, v, mode, pAdv, restartAt, d)
			}
			stats["variants"]++
		}
		// joiners anchored at a committed pivot, settling in random batches
		for v := 0; v < 3 && len(ref.pivots) > 0; v++ {
			a := ref.pivots[r.IntN(len(ref.pivots))]
			p := newPeer(t, w, cfg, &Anchor{ID: a.ID, Config: a.Config}, rules{})
			order := slices.Clone(w.ents)
			if v > 0 {
				r.Shuffle(len(order), func(i, j int) { order[i], order[j] = order[j], order[i] })
			}
			pAdv := []float64{0, 0.1, 0.5}[v]
			for _, e := range order {
				p.place(e)
				if r.Float64() < pAdv {
					p.advance()
				}
			}
			p.advance()
			if err := readyBound(p); err != nil {
				t.Fatalf("seed %d joiner %d: %v", seed, v, err)
			}
			if p.s.Halted() != nil {
				t.Fatalf("seed %d joiner %d: halted %v", seed, v, p.s.Halted())
			}
			if d := snap(p).diff(ref); d != "" {
				t.Fatalf("seed %d joiner %d anchored at config %d round %d: %s", seed, v, a.Config, a.Round, d)
			}
			stats["joiners"]++
		}
	}
	sc.done("%v", stats)
}

// TestBoundsReadinessSpamWhilePending: a current seat, an added seat and a plain member each write 20 readiness pulses per round for 20
// rounds while a config stays pending; the kept readiness records and the state size stay flat.
func TestBoundsReadinessSpamWhilePending(t *testing.T) {
	for _, oneWalk := range []bool{false, true} {
		w := newWorld()
		last := func(k int) *ent {
			if es := w.byStrand[k]; len(es) > 0 {
				return es[len(es)-1]
			}
			return nil
		}
		set := w.settlersEntry(70, 70, nil, Majority, []int{0, 1, 2, 8, 9})
		heads := w.lockstep(seq(3), map[int]*ent{}, 4, 0, nil, set)
		cid := configID(1, Majority, w.seats(0, 1, 2, 8, 9))
		p := newPeer(t, w, cfg0(w, Majority, seq(3)...), nil, rules{})
		if !oneWalk {
			p.feed(w.ents...)
		}
		var sizes, readies []int
		for round := range 20 {
			from := len(w.ents)
			var extra []*ent
			for _, k := range []int{8, 20, 1} {
				for i := range 20 {
					id := cid
					if i%2 == 1 {
						id = configID(uint64(500+round*20+i), Majority, nil)
					}
					w.readiness(k, last(k), 0, nil, id, heads[0])
				}
				extra = append(extra, last(k))
			}
			heads[1] = last(1)
			heads = w.lockstep(seq(3), heads, 3, 0, nil, extra...)
			if !oneWalk {
				p.feed(w.ents[from:]...)
				sizes = append(sizes, len(p.s.Save()))
				readies = append(readies, len(p.s.ready))
			}
		}
		if oneWalk {
			p.place(w.ents...)
			p.advance()
		}
		if err := readyBound(p); err != nil {
			t.Fatal(err)
		}
		t.Logf("one walk %v: config %d, %d pivots, awaiting %d, ready %d, state %d bytes, sizes %v, ready %v", oneWalk,
			p.s.Config().Number, len(p.pivots), len(p.s.Awaiting()), len(p.s.ready), len(p.s.Save()), sizes, readies)
		if len(p.s.ready) > 1 || p.s.Config().Number != 0 || len(p.s.Awaiting()) != 1 {
			t.Fatal("not bounded")
		}
	}
}
