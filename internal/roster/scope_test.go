package roster

import (
	"flag"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/crypto/blake2b"
)

var full = flag.Bool("full", false, "run model-checking scope e in full")

// Scope e operations of B's and C's entries.
var scopeOps = []struct {
	name string
	e    func() Entry
}{
	{"data", dataE},
	{"remove A", func() Entry { return removeW(kA) }},
	{"remove B", func() Entry { return removeW(kB) }},
	{"remove C", func() Entry { return removeW(kC) }},
	{"promote C", func() Entry { return addW(kC, sC, Admin) }},
	{"settlers A B", func() Entry { return settlersE(Majority, kA, kB) }},
}

// scopeSlot is one enumerated entry: its strand, operation and links.
type scopeSlot struct {
	onB   bool
	op    int
	aLink int // 1: a1, 2: a2
	cross bool
}

// TestScopeE is model-checking scope e: members A (founder, admin), B (admin) and C, the fixed entries a0 genesis,
// a1 AddMember(B, admin) and a2 AddMember(C) on A's strand, then up to two entries each of B and C (four in -full, three
// otherwise) in every interleaving, with every operation of scopeOps, a link to a1 or a2, and a link to the newest earlier
// entry of the other strand or none: DAGs of up to 7 entries. Every linear extension is a delivery order. Effective and
// void decisions and the roster at every position must be equal for every delivery order and equal to the reference.
func TestScopeE(t *testing.T) {
	maxEach, maxTotal, limit := 2, 3, time.Minute
	if *full {
		maxTotal, limit = 4, time.Hour
	}
	if testing.Short() || raceEnabled {
		maxTotal = 2
	}
	start := time.Now()
	var dags, orders atomic.Int64
	t.Run("e", func(t *testing.T) {
		for nb := 0; nb <= maxEach; nb++ {
			for nc := 0; nc <= maxEach && nb+nc <= maxTotal; nc++ {
				for _, inter := range interleavings(nb, nc) {
					t.Run(fmt.Sprint(inter), func(t *testing.T) {
						t.Parallel()
						slots := make([]scopeSlot, len(inter))
						var rec func(i int)
						rec = func(i int) {
							if t.Failed() {
								return
							}
							if i == len(slots) {
								dags.Add(1)
								orders.Add(int64(checkScopeDAG(t, slots)))
								return
							}
							slots[i].onB = inter[i]
							other := false
							for j := range i {
								if inter[j] != inter[i] {
									other = true
								}
							}
							for op := range scopeOps {
								for a := 1; a <= 2; a++ {
									for _, cross := range []bool{false, true} {
										if cross && !other {
											continue
										}
										slots[i].op, slots[i].aLink, slots[i].cross = op, a, cross
										rec(i + 1)
									}
								}
							}
						}
						rec(0)
					})
				}
			}
		}
	})
	took := time.Since(start)
	t.Logf("scope e (full %v, at most %d entries of B and C): %d DAGs, %d delivery orders, %v", *full, maxTotal, dags.Load(),
		orders.Load(), took.Round(time.Millisecond))
	if took > limit {
		t.Fatalf("scope e took %v, over its limit %v", took, limit)
	}
}

func interleavings(nb, nc int) [][]bool {
	if nb == 0 && nc == 0 {
		return [][]bool{{}}
	}
	var out [][]bool
	if nb > 0 {
		for _, r := range interleavings(nb-1, nc) {
			out = append(out, append([]bool{true}, r...))
		}
	}
	if nc > 0 {
		for _, r := range interleavings(nb, nc-1) {
			out = append(out, append([]bool{false}, r...))
		}
	}
	return out
}

// checkScopeDAG builds the DAG of the slots and checks every linear extension against the reference. It returns the number
// of delivery orders checked.
func checkScopeDAG(t *testing.T, slots []scopeSlot) int {
	b := newBraid(kA, sA, 0)
	a1 := b.add("a1", sA, addW(kB, sB, Admin))
	a2 := b.add("a2", sA, addW(kC, sC, 0))
	var last [2]*item
	desc := ""
	for i, s := range slots {
		strand, own, other := sC, 1, 0
		if s.onB {
			strand, own, other = sB, 0, 1
		}
		links := []*item{a1}
		if s.aLink == 2 {
			links[0] = a2
		}
		if s.cross {
			links = append(links, last[other])
		}
		desc += fmt.Sprintf("|%v %d %d %v", s.onB, s.op, s.aLink, s.cross)
		name := fmt.Sprint("s", i)
		last[own] = b.addID(name, blake2b.Sum256([]byte(desc)), strand, scopeOps[s.op].e(), links...)
	}
	seed := eid("scope e")
	want := refFold(b.g, naiveOrder(b.items, seed), b.byID)
	nodes := b.nodes()
	n := 0
	var first []step
	extensions(b.items, func(perm []int) bool {
		n++
		got := fold(t, b.roster(t), orderOf(t, nodes, perm, seed), b.byID)
		if first == nil {
			first = got
			if i, ok := sameAsRef(got, want); !ok {
				t.Errorf("%s: position %d: %v, reference %v", desc, i, got[i], want[i])
				return false
			}
		} else if err := sameSteps(first, got); err != nil {
			t.Errorf("%s: %v", desc, err)
			return false
		}
		return true
	})
	return n
}

// extensions calls f with every linear extension of the items, as indexes, until f returns false.
func extensions(items []*item, f func([]int) bool) {
	done := map[ID]bool{}
	perm := make([]int, 0, len(items))
	var rec func() bool
	rec = func() bool {
		if len(perm) == len(items) {
			return f(perm)
		}
		for i, it := range items {
			if done[it.e.ID] || !readyIn(it, done) {
				continue
			}
			done[it.e.ID] = true
			perm = append(perm, i)
			ok := rec()
			perm = perm[:len(perm)-1]
			done[it.e.ID] = false
			if !ok {
				return false
			}
		}
		return true
	}
	rec()
}
