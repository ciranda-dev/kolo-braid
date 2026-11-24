package settle

import (
	"errors"
	"slices"
	"testing"

	"github.com/ciranda-dev/kolo-braid/internal/dag"
)

func wantPivots(t *testing.T, p *peer, want ...*ent) {
	t.Helper()
	var ids []dag.ID
	for _, e := range want {
		ids = append(ids, e.id)
	}
	if !slices.Equal(p.ids(), ids) {
		t.Fatalf("pivots %v, want %d pivots %v", p.rounds(), len(want), ids)
	}
}

func batch(t *testing.T, p *peer, prev, pivot *ent) []int {
	t.Helper()
	var out []int
	var pid dag.ID
	if prev != nil {
		pid = prev.id
	}
	if err := p.d.Batch(pid, pivot.id, func(id dag.ID) error {
		out = append(out, p.r.byID[id].n)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	slices.Sort(out)
	return out
}

// TestWorkedExample reproduces the record's settlement worked example: config 1 of S1, S2, S3 after both new settlers
// wrote their readiness pulses; S1 goes offline after round 0.
func TestWorkedExample(t *testing.T) {
	w := newWorld()
	const S1, S2, S3, W = 0, 1, 2, 9
	set := w.settlersEntry(S1, S1, nil, Majority, []int{S1, S2, S3})
	x0 := w.pulse(S1, set, 0, nil)
	x1 := w.pulse(S1, x0, 0, nil)
	cid := configID(1, Majority, w.seats(S1, S2, S3))
	r2 := w.readiness(S2, nil, 0, nil, cid, x1)
	r3 := w.readiness(S3, nil, 0, nil, cid, x1)
	x2 := w.pulse(S1, x1, 0, nil, r2, r3)
	x3 := w.pulse(S1, x2, 0, nil)
	d1 := w.add(W, W, nil)
	// besides the table's links, the first pulses of S2 and S3 link S1's newest entry (rule 1), which holds the activating pivot
	p10 := w.pulse(S1, x3, 1, x2, d1)
	p20 := w.pulse(S2, r2, 1, x2, x3)
	p30 := w.pulse(S3, r3, 1, x2, x3)
	p21 := w.pulse(S2, p20, 1, x2, p10, p30)
	p31 := w.pulse(S3, p30, 1, x2, p10, p20)

	p := newPeer(t, w, cfg0(w, Majority, S1), nil, rules{})
	p.feed(set, x0, x1)
	wantPivots(t, p, x0)
	if pd, ok := p.s.Pending(); !ok || len(pd.Seats) != 3 {
		t.Fatalf("pending %v %v", pd, ok)
	}
	if aw := p.s.Awaiting(); len(aw) != 2 {
		t.Fatalf("awaiting %d seats, want 2", len(aw))
	}
	p.feed(r2, r3, x2, x3)
	wantPivots(t, p, x0, x2)
	if !p.pivots[1].Activates || p.s.Config().Number != 1 || p.s.Config().Activating != x2.id || p.s.Config().Quorum() != 2 {
		t.Fatalf("config 1 not active at x2: %+v", p.s.Config())
	}
	p.feed(d1, p10, p20, p30, p21)
	wantPivots(t, p, x0, x2)
	p.feed(p31)
	wantPivots(t, p, x0, x2, p10)
	if got, want := batch(t, p, x2, p10), []int{x3.n, d1.n, p10.n}; !slices.Equal(got, want) {
		t.Fatalf("batch of v0 %v, want %v", got, want)
	}
	for _, c := range []struct {
		e *ent
		r uint64
	}{{p10, 0}, {p20, 0}, {p30, 0}, {p21, 1}, {p31, 1}} {
		if got := p.s.byID[c.e.id].round; got != c.r {
			t.Fatalf("round of entry %d is %d, want %d", c.e.n, got, c.r)
		}
	}

	// S1 is offline from here on.
	p22 := w.pulse(S2, p21, 1, x2, p31)
	p32 := w.pulse(S3, p31, 1, x2, p21)
	p23 := w.pulse(S2, p22, 1, x2, p32)
	p33 := w.pulse(S3, p32, 1, x2, p22)
	p.feed(p22, p32, p23)
	wantPivots(t, p, x0, x2, p10)
	p.feed(p33)
	wantPivots(t, p, x0, x2, p10, p22)
	p24 := w.pulse(S2, p23, 1, x2, p33)
	p34 := w.pulse(S3, p33, 1, x2, p23)
	p25 := w.pulse(S2, p24, 1, x2, p34)
	p35 := w.pulse(S3, p34, 1, x2, p24)
	p.feed(p24, p34, p25, p35)
	wantPivots(t, p, x0, x2, p10, p22, p34)
	// round 6's pivot seat is S1: no v6, the round-7 pulses are written without it
	p26 := w.pulse(S2, p25, 1, x2, p35)
	p36 := w.pulse(S3, p35, 1, x2, p25)
	p27 := w.pulse(S2, p26, 1, x2, p36)
	p37 := w.pulse(S3, p36, 1, x2, p26)
	p28 := w.pulse(S2, p27, 1, x2, p37)
	p38 := w.pulse(S3, p37, 1, x2, p27)
	p29 := w.pulse(S2, p28, 1, x2, p38)
	p39 := w.pulse(S3, p38, 1, x2, p28)
	p.feed(p26, p36, p27, p37, p28, p38, p29, p39)
	wantPivots(t, p, x0, x2, p10, p22, p34, p28)
	if got := p.rounds(); !slices.Equal(got, []uint64{0, 2, 0, 2, 4, 8}) {
		t.Fatalf("rounds %v", got)
	}
	for _, e := range []*ent{p22, p34, p28} {
		if r := p.s.byID[e.id]; r == nil || r.round%2 != 0 {
			t.Fatalf("pivot %d not at an even round", e.n)
		}
	}

	// the same pivots on a peer that receives everything in reverse order
	q := newPeer(t, w, cfg0(w, Majority, S1), nil, rules{})
	rev := slices.Clone(w.ents)
	slices.Reverse(rev)
	q.feed(rev...)
	if !slices.Equal(q.ids(), p.ids()) {
		t.Fatalf("reverse delivery: %v, want %v", q.rounds(), p.rounds())
	}
}

// counterexample builds the record's DAG: n = 4, q = 3, S1 to S3 vote for S3's round-2 pulse, S4 writes a round-3 pulse
// without it, and round-4 pulses of S1, S2 and S4 hold S4's.
func counterexample() (w *world, P, Q bitset) {
	w = newWorld()
	keys := seq(4)
	heads := w.lockstep(keys, map[int]*ent{}, 3, 0, nil)
	r2 := map[int]*ent{}
	for k, v := range heads {
		r2[k] = v
	}
	a3 := w.pulse(0, r2[0], 0, nil, r2[1], r2[2], r2[3])
	b3 := w.pulse(1, r2[1], 0, nil, r2[0], r2[2], r2[3])
	c3 := w.pulse(2, r2[2], 0, nil, r2[0], r2[1], r2[3])
	d3 := w.pulse(3, r2[3], 0, nil, r2[0], r2[1])
	P = closure(a3, b3, c3)
	a4 := w.pulse(0, a3, 0, nil, b3, d3)
	b4 := w.pulse(1, b3, 0, nil, a3, d3)
	d4 := w.pulse(3, d3, 0, nil, a3, b3)
	Q = closure(a4, b4, d4)
	return w, P, Q
}

func TestEvenRoundCounterexample(t *testing.T) {
	w, P, Q := counterexample()
	run := func(dev rules, D bitset) *peer {
		p := newPeer(t, w, cfg0(w, Majority, seq(4)...), nil, dev)
		p.feed(w.in(D)...)
		return p
	}
	odd := rules{everyRound: true}
	pp, qq := run(odd, P), run(odd, Q)
	if isPrefix(pp.ids(), qq.ids()) {
		t.Fatalf("pivots on every round must conflict: P %v, Q %v", pp.rounds(), qq.rounds())
	}
	if !slices.Equal(pp.rounds(), []uint64{0, 1, 2}) || !slices.Equal(qq.rounds(), []uint64{0, 1, 3}) {
		t.Fatalf("every-round lists P %v, Q %v", pp.rounds(), qq.rounds())
	}
	pp, qq = run(rules{}, P), run(rules{}, Q)
	if !isPrefix(pp.ids(), qq.ids()) || len(pp.ids()) != 2 {
		t.Fatalf("even rounds: P %v, Q %v", pp.rounds(), qq.rounds())
	}
}

// TestSeatRule is the double-strand case: key 2's seat is its new strand 3, and it also pulses on its old strand 2.
func TestSeatRule(t *testing.T) {
	for _, byKey := range []bool{true, false} {
		w := newWorld()
		w.strandOf[2] = 3
		a0 := w.pulseOn(0, 0, nil, 0, nil)
		b0 := w.pulseOn(1, 1, nil, 0, nil)
		c0 := w.pulseOn(2, 2, nil, 0, nil)
		c0n := w.pulseOn(2, 3, nil, 0, nil)
		a1 := w.pulseOn(0, 0, a0, 0, nil, c0)
		c1 := w.pulseOn(2, 2, c0, 0, nil, a0)
		b1 := w.pulseOn(1, 1, b0, 0, nil, c0n)
		c1n := w.pulseOn(2, 3, c0n, 0, nil, b0)
		P := closure(a1, c1)
		b2 := w.pulseOn(1, 1, b1, 0, nil, c1n)
		c2n := w.pulseOn(2, 3, c1n, 0, nil, b1)
		b3 := w.pulseOn(1, 1, b2, 0, nil, c2n)
		c3n := w.pulseOn(2, 3, c2n, 0, nil, b2)
		b4 := w.pulseOn(1, 1, b3, 0, nil, c3n)
		c4n := w.pulseOn(2, 3, c3n, 0, nil, b3)
		w.pulseOn(0, 0, a1, 0, nil, b4)
		c5n := w.pulseOn(2, 3, c4n, 0, nil, b4)
		b5 := w.pulseOn(1, 1, b4, 0, nil, c4n)
		Q := closure(c5n, b5)
		lists := [2][]dag.ID{}
		for i, D := range []bitset{P, Q} {
			p := newPeer(t, w, cfg0(w, Majority, 0, 1, 2), nil, rules{byKey: byKey})
			p.feed(w.in(D)...)
			lists[i] = p.ids()
		}
		consistent := isPrefix(lists[0], lists[1])
		if byKey && (consistent || len(lists[0]) != 1 || len(lists[1]) != 2) {
			t.Fatalf("without the seat rule: P %d, Q %d pivots, consistent %v; want 1, 2, a conflict", len(lists[0]), len(lists[1]), consistent)
		}
		if !byKey && (!consistent || len(lists[0]) != 0 || len(lists[1]) != 2) {
			t.Fatalf("with the seat rule: P %d, Q %d pivots, consistent %v; want 0, 2, consistent", len(lists[0]), len(lists[1]), consistent)
		}
	}
}

// h3 is the readiness model: config 0 is S0 alone, a settled change makes the pending config {S0, S1}, and S1 comes online
// (writes its readiness pulse) after online steps of S0, if ever.
func h3(t *testing.T, online, steps int) (p *peer, w *world, s0 *ent) {
	w = newWorld()
	set := w.settlersEntry(0, 0, nil, Majority, []int{0, 1})
	s0 = set
	p = newPeer(t, w, cfg0(w, Majority, 0), nil, rules{})
	p.feed(set)
	var ready *ent
	for k := range steps {
		if k == online {
			ready = w.readiness(1, nil, 0, nil, configID(1, Majority, w.seats(0, 1)), s0)
			p.feed(ready)
		}
		s0 = w.pulse(0, s0, 0, nil, ready)
		ready = nil
		p.feed(s0)
	}
	return p, w, s0
}

func TestActivationWaitsForReadiness(t *testing.T) {
	p, _, _ := h3(t, -1, 40)
	if p.s.Config().Number != 0 || len(p.pivots) != 20 {
		t.Fatalf("offline new settler: config %d, %d commits; want config 0 settling on (20)", p.s.Config().Number, len(p.pivots))
	}
	if aw := p.s.Awaiting(); len(aw) != 1 || aw[0].Key != keyOf(1) {
		t.Fatalf("awaiting %v", aw)
	}
	p, w, s0 := h3(t, 20, 40)
	if p.s.Config().Number != 1 {
		t.Fatalf("readiness written: config %d, want 1", p.s.Config().Number)
	}
	act := 0
	for i, x := range p.pivots {
		if x.Activates {
			act = i
		}
	}
	if act != 10 {
		t.Fatalf("activated after pivot %d, want 10 (the first whose past holds the readiness pulse)", act)
	}
	// config 1 settles with both seats
	h := map[int]*ent{0: s0}
	cfg := p.s.Config()
	var actEnt *ent
	for _, e := range w.ents {
		if e.id == cfg.Activating {
			actEnt = e
		}
	}
	h[1] = w.pulse(1, w.byStrand[1][0], 1, actEnt, s0)
	h[0] = w.pulse(0, s0, 1, actEnt, h[1])
	p.feed(h[1], h[0])
	before := len(p.pivots)
	for range 10 {
		next0 := w.pulse(0, h[0], 1, actEnt, h[1])
		next1 := w.pulse(1, h[1], 1, actEnt, h[0])
		h[0], h[1] = next0, next1
		p.feed(next0, next1)
	}
	if len(p.pivots)-before < 4 {
		t.Fatalf("config 1 committed %d pivots in 10 lockstep rounds", len(p.pivots)-before)
	}
}

// TestPendingReplaced: a change that adds an offline settler is undone, which activates at once because it adds nobody.
func TestPendingReplaced(t *testing.T) {
	w := newWorld()
	add := w.settlersEntry(0, 0, nil, Majority, []int{0, 1})
	x0 := w.pulse(0, add, 0, nil)
	x1 := w.pulse(0, x0, 0, nil)
	p := newPeer(t, w, cfg0(w, Majority, 0), nil, rules{})
	p.feed(add, x0, x1)
	if _, ok := p.s.Pending(); !ok {
		t.Fatal("no pending config after the change settled")
	}
	undo := w.settlersEntry(0, 0, x1, Majority, []int{0})
	x2 := w.pulse(0, undo, 0, nil)
	x3 := w.pulse(0, x2, 0, nil)
	p.feed(undo, x2, x3)
	if _, ok := p.s.Pending(); ok || p.s.Config().Number != 0 || len(p.pivots) != 2 {
		t.Fatalf("undo: pending kept or config changed: %d pivots, config %d", len(p.pivots), p.s.Config().Number)
	}
	rule := w.settlersEntry(0, 0, x3, TwoThirds, []int{0})
	x4 := w.pulse(0, rule, 0, nil)
	x5 := w.pulse(0, x4, 0, nil)
	p.feed(rule, x4, x5)
	if p.s.Config().Number != 1 || p.s.Config().Rule != TwoThirds || !p.pivots[len(p.pivots)-1].Activates {
		t.Fatalf("a rule change that adds nobody must activate at its batch's pivot: config %+v", p.s.Config())
	}
}

// TestRemovalActivatesAtOnce: removing a seat needs no readiness, and pivots of the old config after the activating one are
// ignored.
func TestRemovalActivatesAtOnce(t *testing.T) {
	w := newWorld()
	keys := seq(4)
	heads := w.lockstep(keys, map[int]*ent{}, 2, 0, nil)
	rm := w.settlersEntry(0, 10, nil, Majority, []int{0, 1, 2})
	heads = w.lockstep(keys, heads, 6, 0, nil, rm)
	p := newPeer(t, w, cfg0(w, Majority, keys...), nil, rules{})
	p.feed(w.ents...)
	if p.s.Config().Number != 1 || len(p.s.Config().Seats) != 3 {
		t.Fatalf("config %+v", p.s.Config())
	}
	last := p.pivots[len(p.pivots)-1]
	if !last.Activates || last.Config != 0 {
		t.Fatalf("the activating pivot must be the last committed one: %+v", p.pivots)
	}
	// the batch of the activating pivot holds the Settlers entry
	var act *ent
	for _, e := range w.ents {
		if e.id == last.ID {
			act = e
		}
	}
	if !act.past.has(rm.n) {
		t.Fatal("activating pivot's past lacks the removal")
	}
	prev := p.pivots[len(p.pivots)-2]
	for _, e := range w.ents {
		if e.id == prev.ID && e.past.has(rm.n) {
			t.Fatal("the removal activated a pivot late")
		}
	}
	// config 1 settles with the remaining three seats
	before := len(p.pivots)
	cfg1 := map[int]*ent{0: heads[0], 1: heads[1], 2: heads[2]}
	w.lockstep([]int{0, 1, 2}, cfg1, 6, 1, act)
	p.feed(w.ents...)
	if len(p.pivots)-before < 2 {
		t.Fatalf("config 1 committed %d pivots", len(p.pivots)-before)
	}
	for _, x := range p.pivots[before:] {
		if x.Config != 1 {
			t.Fatalf("pivot of config %d after the activation", x.Config)
		}
	}
}

func TestSeatForkHalts(t *testing.T) {
	w := newWorld()
	keys := seq(4)
	heads := w.lockstep(keys, map[int]*ent{}, 4, 0, nil)
	p := newPeer(t, w, cfg0(w, TwoThirds, keys...), nil, rules{})
	p.feed(w.ents...)
	n := len(p.pivots)
	if n == 0 || p.s.Halted() != nil {
		t.Fatalf("%d pivots, halted %v", n, p.s.Halted())
	}
	// seat 3 signs a second entry at the index of its newest pulse
	fork := w.pulse(3, heads[3].prev, 0, nil)
	p.feed(fork)
	var he *HaltError
	if err := p.s.Halted(); !errors.As(err, &he) || he.Reason != SeatFork || he.Key != keyOf(3) || !errors.Is(err, ErrHalted) {
		t.Fatalf("halted %v", err)
	}
	w.lockstep(keys[:3], heads, 4, 0, nil)
	p.feed(w.ents...)
	if len(p.pivots) != n {
		t.Fatalf("a halted config committed %d more pivots", len(p.pivots)-n)
	}
	if p.s.Predicted() != nil {
		t.Fatal("predicted pivots while halted")
	}

	// evidence against a seat halts too; against a non-seat strand it does nothing
	w2 := newWorld()
	w2.lockstep(keys, map[int]*ent{}, 4, 0, nil)
	q := newPeer(t, w2, cfg0(w2, Majority, keys...), nil, rules{})
	q.s.Equivocated(strandID(40))
	q.feed(w2.ents[:8]...)
	if q.s.Halted() != nil || len(q.pivots) == 0 {
		t.Fatalf("evidence against a non-seat halted: %v", q.s.Halted())
	}
	q.s.Equivocated(strandID(2))
	if q.s.Halted() == nil {
		t.Fatal("evidence against a seat did not halt")
	}
	n = len(q.pivots)
	q.feed(w2.ents[8:]...)
	if len(q.pivots) != n {
		t.Fatal("committed after evidence against a seat")
	}
}

func TestHelpers(t *testing.T) {
	w := newWorld()
	keys := seq(4)
	heads := w.lockstep(keys, map[int]*ent{}, 3, 0, nil)
	p := newPeer(t, w, cfg0(w, Majority, keys...), nil, rules{})
	p.feed(w.ents...)
	c, _ := p.d.Clock(heads[0].id)
	if r := p.s.Round(c); r != 2 {
		t.Fatalf("Round after one round-2 pulse: %d, want 2", r)
	}
	all := closure(heads[0], heads[1], heads[2])
	if all.count() == 0 {
		t.Fatal("closure")
	}
	c3 := slices.Clone(c)
	for _, k := range []int{1, 2} {
		ck, _ := p.d.Clock(heads[k].id)
		for i, v := range ck {
			for len(c3) <= i {
				c3 = append(c3, 0)
			}
			c3[i] = max(c3[i], v)
		}
	}
	if r := p.s.Round(c3); r != 3 {
		t.Fatalf("Round after q round-2 pulses: %d, want 3", r)
	}
	if h, ok := p.s.Highest(c); !ok || h != 2 {
		t.Fatalf("Highest %d %v", h, ok)
	}
	if _, ok := p.s.Highest(nil); ok {
		t.Fatal("Highest of an empty past")
	}
	if p.s.Round(nil) != 0 {
		t.Fatal("Round of an empty past")
	}
	for r, want := range []int{0, -1, 1, -1, 2, -1, 3, -1, 0} {
		got, ok := p.s.PivotSeat(uint64(r))
		if (want < 0) == ok || (ok && got != want) {
			t.Fatalf("PivotSeat(%d) = %d %v", r, got, ok)
		}
	}
	if id, ok := p.s.Vertex(1, 2); !ok || id != heads[1].id {
		t.Fatal("Vertex(1, 2)")
	}
	if _, ok := p.s.Vertex(1, 9); ok {
		t.Fatal("Vertex of a missing round")
	}
	if _, ok := p.s.Vertex(-1, 0); ok {
		t.Fatal("Vertex of a bad seat")
	}
	if i, ok := p.s.SeatOf(keyOf(2), strandID(2)); !ok || i != 2 {
		t.Fatal("SeatOf")
	}
	if _, ok := p.s.SeatOf(keyOf(2), strandID(7)); ok {
		t.Fatal("SeatOf another strand of a seat key")
	}
	last, ok := p.s.Last()
	if !ok || last.Round != 0 || p.s.Count() != 1 {
		t.Fatalf("Last %+v %v, count %d", last, ok, p.s.Count())
	}
	// the newest candidate is round 2's pivot (seat 1); its walk predicts it alone
	if pr := p.s.Predicted(); len(pr) != 1 || pr[0] != heads[1].id {
		t.Fatalf("Predicted %v", pr)
	}
}

// TestHoldBack checks the inputs of the hold-back rule: a pivot seat that links only entries without pulses of its pivot
// round gets that round, and an entry with such a pulse in its past would skip it.
func TestHoldBack(t *testing.T) {
	w := newWorld()
	keys := seq(3)
	heads := w.lockstep(keys, map[int]*ent{}, 2, 0, nil)
	// seats 0 and 2 run ahead to round 2; seat 1 is round 2's pivot seat
	a2 := w.pulse(0, heads[0], 0, nil, heads[1], heads[2])
	c2 := w.pulse(2, heads[2], 0, nil, heads[0], heads[1])
	a3 := w.pulse(0, a2, 0, nil, c2)
	p := newPeer(t, w, cfg0(w, Majority, keys...), nil, rules{})
	p.feed(w.ents...)
	pivot, _ := p.s.PivotSeat(2)
	if pivot != 1 {
		t.Fatalf("round 2 pivot seat %d", pivot)
	}
	ca3, _ := p.d.Clock(a3.id)
	if h, _ := p.s.Highest(ca3); h < 2 {
		t.Fatal("a3's past must hold a round-2 pulse")
	}
	// linking a3 would give seat 1 round 3: no round-2 vertex
	c := slices.Clone(ca3)
	cb, _ := p.d.Clock(heads[1].id)
	for i, v := range cb {
		for len(c) <= i {
			c = append(c, 0)
		}
		c[i] = max(c[i], v)
	}
	if r := p.s.Round(c); r != 3 {
		t.Fatalf("without hold-back the pivot seat gets round %d, want 3", r)
	}
	// the hold-back set: the newest entries whose past holds no counting pulse of round >= 2
	hb := map[int]*ent{0: heads[0], 2: heads[2]}
	held := closure(hb[0], hb[2], heads[1])
	b2 := w.pulse(1, heads[1], 0, nil, hb[0], hb[2])
	if !held.has(heads[0].n) {
		t.Fatal("hold-back set")
	}
	p.feed(b2)
	if p.s.byID[b2.id].round != 2 {
		t.Fatalf("held-back pivot pulse has round %d, want 2", p.s.byID[b2.id].round)
	}
	if id, ok := p.s.Vertex(1, 2); !ok || id != b2.id {
		t.Fatal("the held-back pulse is not round 2's pivot")
	}
}

// TestSixtyFiveSeats: a config above the old ceiling of 64 seats settles lockstep rounds, as the reference does.
func TestSixtyFiveSeats(t *testing.T) {
	w := newWorld()
	keys := seq(65)
	w.lockstep(keys, map[int]*ent{}, 6, 0, nil)
	cfg := cfg0(w, Majority, keys...)
	want := refIDs(newRef(w, w.all(), Majority, keys, staticRoster(Proposal{Rule: Majority, Seats: cfg.Seats})).committed())
	p := newPeer(t, w, cfg, nil, rules{})
	p.feed(w.ents...)
	if len(want) == 0 || !slices.Equal(p.ids(), want) || p.s.Halted() != nil {
		t.Fatalf("%d pivots, reference %d, halted %v", len(p.ids()), len(want), p.s.Halted())
	}
}

func TestNewAndRosterErrors(t *testing.T) {
	w := newWorld()
	d, _ := dag.New(dag.Config{MaxLinks: 63, MaxPendingBytes: 1 << 20})
	r := func(uint64, []Pivot) ([]Proposal, error) { return nil, nil }
	good := cfg0(w, Majority, 0, 1)
	bad := []Config{
		{Rule: 3, Seats: good.Seats},
		{Rule: Majority},
		{Rule: Majority, Seats: []Seat{good.Seats[1], good.Seats[0]}},
		{Rule: Majority, Seats: []Seat{good.Seats[0], {Key: keyOf(5), Strand: good.Seats[0].Strand}}},
		{Rule: Majority, Seats: good.Seats, Activating: dag.ID{1}},
		{Number: 1, Rule: Majority, Seats: good.Seats, Activating: dag.ID{1}},
		{Rule: Majority, Seats: make([]Seat, 2)},
	}
	for i, c := range bad {
		if _, err := New(d, r, c, nil); !errors.Is(err, ErrInvalid) {
			t.Fatalf("config %d: %v", i, err)
		}
	}
	if _, err := New(nil, r, good, nil); !errors.Is(err, ErrInvalid) {
		t.Fatal("nil dag")
	}
	if _, err := New(d, nil, good, nil); !errors.Is(err, ErrInvalid) {
		t.Fatal("nil roster")
	}
	heads := w.lockstep([]int{0, 1}, map[int]*ent{}, 3, 0, nil)
	_ = heads
	for i, roster := range []Roster{
		func(uint64, []Pivot) ([]Proposal, error) { return nil, nil },
		func(_ uint64, ps []Pivot) ([]Proposal, error) {
			return slices.Repeat([]Proposal{{Rule: 9, Seats: good.Seats}}, len(ps)), nil
		},
		func(_ uint64, ps []Pivot) ([]Proposal, error) {
			return slices.Repeat([]Proposal{{Rule: Majority, Seats: []Seat{good.Seats[1], good.Seats[0]}}}, len(ps)), nil
		},
		func(uint64, []Pivot) ([]Proposal, error) { return nil, errTest },
	} {
		d, _ := dag.New(dag.Config{MaxLinks: 63, MaxPendingBytes: 1 << 20})
		s, err := New(d, roster, good, nil)
		if err != nil {
			t.Fatal(err)
		}
		var got error
		for _, e := range w.ents {
			placed, _ := d.Add(w.node(e))
			for range placed {
				if err := s.Add(e.asPulse()); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := s.Advance(); err != nil {
				got = err
			}
		}
		if got == nil || (i == 3 && !errors.Is(got, errTest)) || (i < 3 && !errors.Is(got, ErrInvalid)) {
			t.Fatalf("roster %d: %v", i, got)
		}
	}
	s, _ := New(d, r, good, nil)
	if err := s.Add(Pulse{ID: dag.ID{7}}); !errors.Is(err, ErrNotPlaced) {
		t.Fatalf("unplaced pulse: %v", err)
	}
	if (&HaltError{Reason: Conflict}).Error() == "" {
		t.Fatal("empty halt message")
	}
}

var errTest = errors.New("test roster error")

// TestHostilePulses: forged and replayed pulses never count and never panic.
func TestHostilePulses(t *testing.T) {
	w := newWorld()
	keys := seq(4)
	heads := w.lockstep(keys, map[int]*ent{}, 2, 0, nil)
	// a non-settler writes pulses that link everything: they are not votes and do not raise rounds
	x := w.pulse(20, nil, 0, nil, heads[0], heads[1], heads[2], heads[3])
	x2 := w.pulse(20, x, 0, nil)
	// a seat key pulses on another strand, claims a config that does not exist, and names an activating pivot in config 0
	y := w.pulseOn(1, 30, nil, 0, nil, heads[0])
	z := w.pulse(2, heads[2], 5, heads[0])
	z2 := w.pulse(2, z, 0, heads[0])
	// a readiness pulse from a seat
	rd := w.readiness(3, heads[3], 0, nil, [32]byte{9})
	heads[2], heads[3] = z2, rd
	heads = w.lockstep(keys, heads, 6, 0, nil)
	clean := newWorld()
	clean.lockstep(keys, map[int]*ent{}, 2, 0, nil)
	p := newPeer(t, w, cfg0(w, Majority, keys...), nil, rules{})
	p.feed(w.ents...)
	for _, e := range []*ent{x, x2, y, z, z2, rd} {
		if r := p.s.byID[e.id]; r != nil && r.seat >= 0 {
			t.Fatalf("hostile pulse %d counts", e.n)
		}
	}
	// replay: adding every pulse again changes nothing
	n := len(p.pivots)
	for _, e := range w.ents {
		if e.pulse {
			if err := p.s.Add(e.asPulse()); err != nil {
				t.Fatal(err)
			}
		}
	}
	p.advance()
	if len(p.pivots) != n || n < 3 {
		t.Fatalf("pivots %d after replay, %d before", len(p.pivots), n)
	}
	for _, x := range p.pivots {
		if x.Round%2 != 0 {
			t.Fatalf("pivot at round %d", x.Round)
		}
	}
	_ = heads
}

// TestAnchorAcrossConfigs: a joiner anchored at a pivot of config 1 settles config 0 inside the activating pivot that the
// anchor names, then config 1 inside the anchor, and gets the old peers' list.
func TestAnchorAcrossConfigs(t *testing.T) {
	for _, c := range []dCase{{true, 1, 1, 0, 4, -1, -1, dNone}, {false, 2, 2, -1, -1, -1, -1, dNone}} {
		w, cfg := c.build(t)
		old := newPeer(t, w, cfg, nil, rules{})
		old.feed(w.ents...)
		last := old.pivots[len(old.pivots)-1]
		if last.Config != 1 {
			t.Fatalf("%+v: the old peer ends in config %d", c, last.Config)
		}
		for _, ord := range [][]*ent{w.ents, reversed(w.ents), shuffled(w.ents, 3)} {
			j := newPeer(t, w, cfg, &Anchor{ID: last.ID, Config: 1}, rules{})
			j.feed(ord...)
			if !slices.Equal(j.ids(), old.ids()) || j.s.Halted() != nil {
				t.Fatalf("%+v: anchored joiner %v (halted %v), old %v", c, j.rounds(), j.s.Halted(), old.rounds())
			}
		}
		// an anchor whose chain names a pulse of the wrong config
		mid := old.pivots[0]
		bad := newPeer(t, w, cfg, &Anchor{ID: mid.ID, Config: 1}, rules{})
		bad.feed(w.ents...)
		if bad.s.Halted() == nil {
			t.Fatal("anchor with a wrong config number")
		}
	}
}

// TestAnchorWrongActivation: an anchor of config 1 whose activating pivot is a real pivot where no config activates stops
// the joiner.
func TestAnchorWrongActivation(t *testing.T) {
	w := newWorld()
	keys := seq(3)
	heads := w.lockstep(keys, map[int]*ent{}, 6, 0, nil)
	cfg := cfg0(w, Majority, keys...)
	old := newPeer(t, w, cfg, nil, rules{})
	old.feed(w.ents...)
	v := w.byID[old.pivots[len(old.pivots)-1].ID]
	fake := w.pulse(0, heads[0], 1, v, v)
	j := newPeer(t, w, cfg, &Anchor{ID: fake.id, Config: 1}, rules{})
	j.feed(w.ents...)
	var he *HaltError
	if err := j.s.Halted(); !errors.As(err, &he) || he.Reason != BadTrust {
		t.Fatalf("halted %v", err)
	}
	// a chain through a data entry
	d := w.add(9, 9, nil)
	f2 := w.pulse(1, heads[1], 1, d, d)
	j = newPeer(t, w, cfg, &Anchor{ID: f2.id, Config: 1}, rules{})
	j.feed(w.ents...)
	if j.s.Halted() == nil {
		t.Fatal("chain through a data entry")
	}
	// a chain through a readiness pulse
	rd := w.readiness(2, heads[2], 0, nil, [32]byte{1})
	f3 := w.pulse(1, f2, 1, rd, rd)
	j = newPeer(t, w, cfg, &Anchor{ID: f3.id, Config: 1}, rules{})
	j.feed(w.ents...)
	if j.s.Halted() == nil {
		t.Fatal("chain through a readiness pulse")
	}
}
