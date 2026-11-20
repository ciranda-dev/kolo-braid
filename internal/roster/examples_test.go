package roster

import (
	"bytes"
	"math/rand/v2"
	"testing"
)

func seedOf(i int) [32]byte { return eid("seed " + string(rune('a'+i%26)) + string(rune('a'+i/26))) }

func perms(n int) [][]int {
	var out [][]int
	var rec func(p []int, used []bool)
	rec = func(p []int, used []bool) {
		if len(p) == n {
			out = append(out, append([]int(nil), p...))
			return
		}
		for i := range n {
			if !used[i] {
				used[i] = true
				rec(append(p, i), used)
				used[i] = false
			}
		}
	}
	rec(nil, make([]bool, n))
	return out
}

// arrivalFold is the rule the record rejects: each entry checked against the roster as it is when the entry arrives.
func arrivalFold(t *testing.T, b *braid, arrival []*item) []bool {
	t.Helper()
	r := b.roster(t)
	out := make([]bool, len(arrival))
	for p, it := range arrival {
		out[p] = r.Apply(uint64(p), &it.e).Effective()
	}
	return out
}

// TestDivergenceExample is the record's worked example: W's w1 is concurrent with its removal a2. Peer P receives w1
// before a2, peer Q a2 before w1. Checked at arrival, P keeps w1 and Q does not, forever; at its position in the order,
// both order a0, a1, w0, a2, w1 and w1 is void on both.
func TestDivergenceExample(t *testing.T) {
	kW, sW := kid(9), sid(9)
	b := newBraid(kA, sA, 0)
	a0 := b.items[0]
	a1 := b.add("a1", sA, addW(kW, sW, 0))
	w0 := b.add("w0", sW, dataE(), a1)
	a2 := b.add("a2", sA, removeW(kW), w0)
	w1 := b.add("w1", sW, dataE())

	p := arrivalFold(t, b, []*item{a0, a1, w0, w1, a2})
	q := arrivalFold(t, b, []*item{a0, a1, w0, a2, w1})
	if !p[3] || q[4] {
		t.Fatalf("arrival-time rule: P keeps w1 %v, Q keeps w1 %v; the example needs P to keep it and Q not", p[3], q[4])
	}

	want := []*item{a0, a1, w0, a2, w1}
	for s := range 64 {
		for _, perm := range perms(len(b.items)) {
			steps := fold(t, b.roster(t), orderOf(t, b.nodes(), perm, seedOf(s)), b.byID)
			for i, it := range want {
				if steps[i].id != it.e.ID {
					t.Fatalf("seed %d: position %d is not %s", s, i, it.name)
				}
			}
			if steps[3].change != Removed || steps[4].reason != Inactive {
				t.Fatalf("seed %d: a2 %v, w1 %v", s, steps[3].reason, steps[4].reason)
			}
		}
	}
}

// TestKeptCaseExample is the record's kept case: a2 also links c3, which W never saw, so a2 is ready only after c3, and
// w1 is kept exactly when it is emitted before a2. Both outcomes occur for some seed, each the same for every delivery
// order.
func TestKeptCaseExample(t *testing.T) {
	kW, sW := kid(9), sid(9)
	b := newBraid(kA, sA, 0)
	a1 := b.add("a1", sA, addW(kW, sW, 0))
	a1c := b.add("a1c", sA, addW(kC, sC, 0))
	w0 := b.add("w0", sW, dataE(), a1)
	c := b.add("c0", sC, dataE(), a1c)
	for _, n := range []string{"c1", "c2", "c3"} {
		c = b.add(n, sC, dataE())
	}
	a2 := b.add("a2", sA, removeW(kW), w0, c)
	w1 := b.add("w1", sW, dataE())

	kept, dropped := 0, 0
	rng := rand.New(rand.NewPCG(1, 2))
	for s := range 200 {
		first := b.run(t, seedOf(s))
		iw, sw := at(t, first, w1)
		ia, sa := at(t, first, a2)
		if sa.change != Removed {
			t.Fatalf("seed %d: a2 %v", s, sa.reason)
		}
		if sw.effective != (iw < ia) {
			t.Fatalf("seed %d: w1 at %d effective %v, removal at %d", s, iw, sw.effective, ia)
		}
		if sw.effective {
			kept++
		} else {
			dropped++
		}
		for range 20 {
			perm := rng.Perm(len(b.items))
			if err := sameSteps(first, fold(t, b.roster(t), orderOf(t, b.nodes(), perm, seedOf(s)), b.byID)); err != nil {
				t.Fatalf("seed %d: %v", s, err)
			}
		}
	}
	if kept == 0 || dropped == 0 {
		t.Fatalf("kept %d, dropped %d: both cases must occur", kept, dropped)
	}
	t.Logf("w1 kept under %d seeds, void under %d", kept, dropped)
}

// TestMutualRemoval: admins A and B remove each other concurrently. The removal with the smaller order key goes first and
// takes effect; the other is void because its author is no longer active.
func TestMutualRemoval(t *testing.T) {
	b := newBraid(kA, sA, 0)
	a1 := b.add("a1", sA, addW(kB, sB, Admin))
	a2 := b.add("a2", sA, settlersE(Majority, kA, kB))
	b0 := b.add("b0", sB, dataE(), a2)
	ra := b.add("a removes b", sA, removeW(kB), b0)
	rb := b.add("b removes a", sB, removeW(kA), a1)
	wins := map[string]int{}
	for s := range 100 {
		steps := b.run(t, seedOf(s))
		ia, sa := at(t, steps, ra)
		ib, sb := at(t, steps, rb)
		ka, kb := orderKey(seedOf(s), ra.e.ID), orderKey(seedOf(s), rb.e.ID)
		aFirst := bytes.Compare(ka[:], kb[:]) < 0
		if aFirst != (ia < ib) {
			t.Fatalf("seed %d: the smaller order key is not first", s)
		}
		first, second := sa, sb
		if !aFirst {
			first, second = sb, sa
		}
		if first.change != Removed || second.reason != Inactive {
			t.Fatalf("seed %d: first %v, second %v", s, first.reason, second.reason)
		}
		last := steps[len(steps)-1]
		if len(last.members) != 1 || len(last.settlers) != 1 || last.members[0].Key != last.settlers[0] {
			t.Fatalf("seed %d: %v", s, last)
		}
		wins[string(last.members[0].Key[:])]++
	}
	if len(wins) != 2 {
		t.Fatalf("one side always wins: %v", wins)
	}
}

// TestLastAdminRemovesItself: the only admin cannot remove itself; with a second admin it can, and when the other admin
// removes it at the same time only the first of the two takes effect.
func TestLastAdminRemovesItself(t *testing.T) {
	b := newBraid(kA, sA, 0)
	b.add("a1", sA, addW(kB, sB, 0))
	self := b.add("self", sA, removeW(kA))
	steps := b.run(t, seedOf(0))
	if _, s := at(t, steps, self); s.reason != LastAdmin {
		t.Fatalf("last admin self removal: %v", s.reason)
	}

	b = newBraid(kA, sA, 0)
	a1 := b.add("a1", sA, addW(kB, sB, Admin))
	a2 := b.add("a2", sA, settlersE(Majority, kA, kB))
	self = b.add("self", sA, removeW(kA), b.add("b0", sB, dataE(), a2))
	other := b.add("other", sB, removeW(kA), a1)
	for s := range 50 {
		steps := b.run(t, seedOf(s))
		is, ss := at(t, steps, self)
		io, so := at(t, steps, other)
		if is < io && (ss.change != Removed || so.reason != NotActive) ||
			io < is && (so.change != Removed || ss.reason != Inactive) {
			t.Fatalf("seed %d: self %v at %d, other %v at %d", s, ss.reason, is, so.reason, io)
		}
		if last := steps[len(steps)-1]; len(last.members) != 1 || last.members[0].Key != kB {
			t.Fatalf("seed %d: %v", s, last)
		}
	}
}

// TestConcurrentAddRemove: B adds D while A removes B. D's addition is kept exactly when it is ordered before the
// removal, and D's own entries follow.
func TestConcurrentAddRemove(t *testing.T) {
	b := newBraid(kA, sA, 0)
	a1 := b.add("a1", sA, addW(kB, sB, Admin))
	b0 := b.add("b0", sB, dataE(), a1)
	rm := b.add("rm", sA, removeW(kB), b0)
	ad := b.add("ad", sB, addW(kD, sD, 0))
	d0 := b.add("d0", sD, dataE(), ad)
	both := map[bool]int{}
	for s := range 100 {
		steps := b.run(t, seedOf(s))
		ir, _ := at(t, steps, rm)
		ia, sa := at(t, steps, ad)
		_, sd := at(t, steps, d0)
		if sa.effective != (ia < ir) || sd.effective != sa.effective {
			t.Fatalf("seed %d: add at %d %v, removal at %d, d0 %v", s, ia, sa.reason, ir, sd.reason)
		}
		both[sa.effective]++
	}
	if len(both) != 2 {
		t.Fatalf("one outcome only: %v", both)
	}
}

// TestRemovedMemberLateEntries: after the removal is ordered, every later entry of the strand is void however late it
// arrives, and entries in the removal's past keep their effect.
func TestRemovedMemberLateEntries(t *testing.T) {
	kW, sW := kid(9), sid(9)
	b := newBraid(kA, sA, 0)
	a1 := b.add("a1", sA, addW(kW, sW, 0))
	var past []*item
	w := b.add("w0", sW, dataE(), a1)
	past = append(past, w)
	for _, n := range []string{"w1", "w2"} {
		w = b.add(n, sW, dataE())
		past = append(past, w)
	}
	rm := b.add("rm", sA, removeW(kW), w)
	var late []*item
	for _, n := range []string{"w3", "w4", "w5"} {
		late = append(late, b.add(n, sW, dataE()))
	}
	late = append(late, b.add("w6", sW, addW(kE, sE, Admin), rm))
	for s := range 20 {
		steps := b.run(t, seedOf(s))
		for _, it := range past {
			if _, st := at(t, steps, it); !st.effective {
				t.Fatalf("%s void", it.name)
			}
		}
		for _, it := range late {
			if _, st := at(t, steps, it); st.reason != Inactive {
				t.Fatalf("%s: %v", it.name, st.reason)
			}
		}
	}
}
