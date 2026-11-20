package settle

import (
	"errors"
	"slices"
	"testing"
)

// shrinkWorld: four seats and a removal to seats 0 to 2 that the round-0 pivot settles, so config 1 starts there; config 1
// then runs six lockstep rounds. pre entries are linked in the first round of config 0, extra in the first of config 1.
func shrinkWorld(pre func(w *world) []*ent, extra func(w *world, act *ent) []*ent) (*world, Config, *ent) {
	w := newWorld()
	set := w.settlersEntry(70, 70, nil, Majority, []int{0, 1, 2})
	first := []*ent{set}
	if pre != nil {
		first = append(first, pre(w)...)
	}
	heads := w.lockstep(seq(4), map[int]*ent{}, 2, 0, nil, first...)
	act := w.byStrand[0][0]
	var ex []*ent
	if extra != nil {
		ex = extra(w, act)
	}
	h1 := map[int]*ent{0: heads[0], 1: heads[1], 2: heads[2]}
	w.lockstep([]int{0, 1, 2}, h1, 6, 1, act, append(ex, act)...)
	return w, cfg0(w, Majority, seq(4)...), act
}

// TestConfigPulseRules: a pulse of config 1 counts only when it names config 1's activating pivot and holds it in its past.
func TestConfigPulseRules(t *testing.T) {
	w, cfg, act := shrinkWorld(nil, nil)
	wrongAct := w.pulse(0, w.byStrand[0][len(w.byStrand[0])-1], 1, w.byStrand[1][1], act)
	noPast := w.pulse(2, nil, 1, act)
	p := newPeer(t, w, cfg, nil, rules{})
	p.feed(w.ents...)
	if p.s.Config().Number != 1 || p.s.Config().Activating != act.id {
		t.Fatalf("config %+v", p.s.Config())
	}
	for _, e := range []*ent{wrongAct, noPast} {
		if r := p.s.byID[e.id]; r != nil && r.seat >= 0 {
			t.Fatalf("pulse %d counts", e.n)
		}
	}
}

// TestEmptyConfigNeverActivates: a Settlers entry that leaves no seat makes a pending config that never activates.
func TestEmptyConfigNeverActivates(t *testing.T) {
	w := newWorld()
	set := w.settlersEntry(70, 70, nil, Majority, []int{})
	w.lockstep(seq(3), map[int]*ent{}, 8, 0, nil, set)
	p := newPeer(t, w, cfg0(w, Majority, seq(3)...), nil, rules{})
	p.feed(w.ents...)
	if pd, ok := p.s.Pending(); !ok || len(pd.Seats) != 0 || p.s.Config().Number != 0 || len(p.pivots) != 4 {
		t.Fatalf("pending %v %v, config %d, %d pivots", pd, ok, p.s.Config().Number, len(p.pivots))
	}
}

// TestCertificateNeedsSeats: claims from keys that are not seats of the next config never certify it.
func TestCertificateNeedsSeats(t *testing.T) {
	s := scenario{3, Majority}
	w, _, _, real10 := closureWorld(s, 0)
	for k := range 3 {
		w.pulse(200+k, nil, 1, real10, real10)
	}
	cfg := cfg0(w, s.rule, seq(s.n)...)
	j := newPeer(t, w, cfg, nil, rules{})
	j.feed(w.ents...)
	honest := newPeer(t, w, cfg, nil, rules{})
	honest.feed(w.ents...)
	if j.s.Config().Number != 1 {
		t.Fatalf("without the late history the joiner settles directly: config %d", j.s.Config().Number)
	}
	// with the late history first, only the non-seat claims are there: the joiner waits
	w2, _, fake, real := closureWorld(s, 0)
	for k := range 3 {
		w2.pulse(200+k, nil, 1, real, real)
	}
	order := append([]*ent{}, w2.ents[:fake.prev.n+1]...)
	order = append(order, fake)
	for _, e := range w2.ents[fake.prev.n+1:] {
		if e != fake {
			order = append(order, e)
		}
	}
	j2 := newPeer(t, w2, cfg0(w2, s.rule, seq(s.n)...), nil, rules{})
	j2.feed(order...)
	if j2.s.Config().Number != 0 || j2.s.Halted() == nil {
		t.Fatalf("claims from non-seats certified config 0: config %d", j2.s.Config().Number)
	}
	// claims by the seats that do not hold the pivot in their past do not certify either
	w3, _, fake3, real3 := closureWorld(s, 0)
	for _, e := range slices.Clone(w3.ents) {
		if e.ready != nil {
			w3.pulse(e.key, e, 1, real3)
		}
	}
	order3 := append([]*ent{}, w3.ents[:fake3.prev.n+1]...)
	order3 = append(order3, fake3)
	for _, e := range w3.ents[fake3.prev.n+1:] {
		if e != fake3 {
			order3 = append(order3, e)
		}
	}
	j3 := newPeer(t, w3, cfg0(w3, s.rule, seq(s.n)...), nil, rules{})
	j3.feed(order3...)
	if j3.s.Config().Number != 0 || j3.s.Halted() == nil {
		t.Fatalf("claims without the pivot in their past certified config 0: config %d", j3.s.Config().Number)
	}
}

// TestRoundWithForkedSingleSeat: with one seat (q 1) whose strand holds two histories, Round scans both.
func TestRoundWithForkedSingleSeat(t *testing.T) {
	w := newWorld()
	var real *ent
	for range 10 {
		real = w.pulse(0, real, 0, nil)
	}
	fake := w.pulse(0, w.byStrand[0][2], 0, nil)
	d := w.add(9, 9, nil, real)
	p := newPeer(t, w, cfg0(w, Majority, 0), nil, rules{})
	p.feed(w.ents...)
	cd, _ := p.d.Clock(d.id)
	cf, _ := p.d.Clock(fake.id)
	rf := newRef(w, w.all(), Majority, []int{0}, nil)
	probe := w.pulse(1, nil, 0, nil, d, fake)
	if got, want := p.s.Round(union(cd, cf)), uint64(rf.roundOf(probe)); got != want {
		t.Fatalf("Round over both histories: %d, the definition gives %d", got, want)
	}
}

// TestBadAnchorChains: an anchor whose chain skips a config, or that is reached after an activation the rules make
// earlier, stops the joiner before it commits anything.
func TestBadAnchorChains(t *testing.T) {
	w, _, act := shrinkWorld(nil, nil)
	skip := w.pulse(1, w.byStrand[1][len(w.byStrand[1])-1], 2, act, act)
	// a world where config 0 goes on to round 5: its round-4 pivot comes after the activation at round 0
	w2 := newWorld()
	set := w2.settlersEntry(70, 70, nil, Majority, []int{0, 1, 2})
	w2.lockstep(seq(4), map[int]*ent{}, 6, 0, nil, set)
	late := w2.byStrand[2][4]
	for i, c := range []struct {
		w *world
		a Anchor
	}{{w, Anchor{ID: skip.id, Config: 2}}, {w2, Anchor{ID: late.id, Config: 0}}} {
		a := c.a
		w, cfg := c.w, cfg0(c.w, Majority, seq(4)...)
		p := newPeer(t, w, cfg, &a, rules{})
		p.feed(w.ents...)
		var he *HaltError
		if err := p.s.Halted(); !errors.As(err, &he) || he.Reason != BadTrust || len(p.pivots) != 0 {
			t.Fatalf("anchor %d: %v, %d pivots", i, err, len(p.pivots))
		}
	}
}

// TestReadinessBeforeActivation: a readiness pulse in the past of the current config's activating pivot never counts for
// a later config, also after a restart that adds it again.
func TestReadinessBeforeActivation(t *testing.T) {
	w, cfg, act := shrinkWorld(func(w *world) []*ent {
		return []*ent{w.readiness(9, nil, 0, nil, configID(2, Majority, w.seats(0, 1, 2, 9)))}
	}, func(w *world, _ *ent) []*ent {
		return []*ent{w.settlersEntry(70, 70, w.byStrand[70][0], Majority, []int{0, 1, 2, 9})}
	})
	cut := act.n + 8
	p := newPeer(t, w, cfg, nil, rules{})
	p.feed(w.ents[:cut]...)
	if p.s.Config().Number != 1 {
		t.Fatalf("config %d before the restart", p.s.Config().Number)
	}
	q := restart(t, p, w.ents[:cut])
	p.feed(w.ents[cut:]...)
	q.feed(w.ents[cut:]...)
	for i, pp := range []*peer{p, q} {
		if _, ok := pp.s.Pending(); !ok || pp.s.Config().Number != 1 || len(pp.s.Awaiting()) != 1 {
			t.Fatalf("peer %d: config %d, awaiting %v", i, pp.s.Config().Number, pp.s.Awaiting())
		}
	}
	if !slices.Equal(p.ids(), q.ids()) {
		t.Fatal("restart changed the list")
	}
}

// walkAgrees checks that a peer that settles entry by entry and one that settles the first mark entries in one walk commit
// the reference's list.
func walkAgrees(t *testing.T, w *world, mark int) {
	t.Helper()
	x := newPeer(t, w, cfg0(w, Majority, seq(3)...), nil, rules{})
	x.feed(w.ents...)
	y := newPeer(t, w, cfg0(w, Majority, seq(3)...), nil, rules{})
	y.place(w.ents[:mark]...)
	y.advance()
	y.feed(w.ents[mark:]...)
	rf := newRef(w, w.all(), Majority, seq(3), dagRoster(t, w, w.all(), Proposal{Rule: Majority, Seats: w.seats(seq(3)...)}))
	want := refIDs(rf.committed())
	if !slices.Equal(x.ids(), want) || !slices.Equal(y.ids(), want) || x.s.Config().Number != rf.cfgN || y.s.Config().Number != rf.cfgN {
		t.Fatalf("reference: %d pivots, config %d; by entry: %d, config %d; one walk: %d, config %d",
			len(want), rf.cfgN, len(x.pivots), x.s.Config().Number, len(y.pivots), y.s.Config().Number)
	}
}

// TestReadinessSettledInOneWalk: a seat's readiness pulse that cannot count and its later one that can, settled in one
// batch, leave the later one counting, as for a peer that settles them in two.
func TestReadinessSettledInOneWalk(t *testing.T) {
	t.Run("junk then ready", func(t *testing.T) {
		w := newWorld()
		junk := w.readiness(9, nil, 0, nil, configID(77, Majority, nil))
		heads := w.lockstep(seq(3), map[int]*ent{}, 3, 0, nil, junk)
		set := w.settlersEntry(70, 70, nil, Majority, []int{0, 1, 2, 8, 9})
		heads = w.lockstep(seq(3), heads, 4, 0, nil, set)
		cid := configID(1, Majority, w.seats(0, 1, 2, 8, 9))
		heads = w.lockstep(seq(3), heads, 4, 0, nil, w.readiness(9, junk, 0, nil, cid, heads[0]))
		mark := len(w.ents)
		w.lockstep(seq(3), heads, 4, 0, nil, w.readiness(8, nil, 0, nil, cid, heads[0]))
		walkAgrees(t, w, mark)
	})
	t.Run("pending change", func(t *testing.T) {
		w := newWorld()
		set1 := w.settlersEntry(70, 70, nil, Majority, []int{0, 1, 2, 7, 9})
		heads := w.lockstep(seq(3), map[int]*ent{}, 4, 0, nil, set1)
		r9 := w.readiness(9, nil, 0, nil, configID(1, Majority, w.seats(0, 1, 2, 7, 9)), heads[0])
		heads = w.lockstep(seq(3), heads, 4, 0, nil, r9)
		set2 := w.settlersEntry(70, 70, set1, Majority, []int{0, 1, 2, 8, 9}, heads[0])
		heads = w.lockstep(seq(3), heads, 4, 0, nil, set2)
		cid := configID(1, Majority, w.seats(0, 1, 2, 8, 9))
		heads = w.lockstep(seq(3), heads, 4, 0, nil, w.readiness(9, r9, 0, nil, cid, heads[0]))
		mark := len(w.ents)
		w.lockstep(seq(3), heads, 4, 0, nil, w.readiness(8, nil, 0, nil, cid, heads[0]))
		walkAgrees(t, w, mark)
	})
	t.Run("undo and redo", func(t *testing.T) {
		w := newWorld()
		set := w.settlersEntry(70, 70, nil, Majority, []int{0, 1, 2, 8, 9})
		heads := w.lockstep(seq(3), map[int]*ent{}, 4, 0, nil, set)
		cid := configID(1, Majority, w.seats(0, 1, 2, 8, 9))
		r9 := w.readiness(9, nil, 0, nil, cid, heads[0])
		heads = w.lockstep(seq(3), heads, 4, 0, nil, r9)
		undo := w.settlersEntry(70, 70, set, Majority, []int{0, 1, 2}, heads[0])
		heads = w.lockstep(seq(3), heads, 4, 0, nil, undo)
		heads = w.lockstep(seq(3), heads, 4, 0, nil, w.settlersEntry(70, 70, undo, Majority, []int{0, 1, 2, 8, 9}, heads[0]))
		heads = w.lockstep(seq(3), heads, 4, 0, nil, w.readiness(9, r9, 0, nil, cid, heads[0]))
		mark := len(w.ents)
		w.lockstep(seq(3), heads, 4, 0, nil, w.readiness(8, nil, 0, nil, cid, heads[0]))
		walkAgrees(t, w, mark)
	})
}
