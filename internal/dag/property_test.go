package dag

import (
	"bytes"
	"math/rand/v2"
	"slices"
	"testing"
)

// shuffles is 1,000 delivery orders per DAG; the race detector's slowdown makes it 100 there.
func shuffles() int {
	if raceEnabled {
		return 100
	}
	return 1000
}

type propCase struct {
	nodes     []Node
	settled   []ID
	predicted []ID
	group     [32]byte
}

// propCases are random DAGs with 3 to 8 writers and 50 to 300 entries, some with forked strands, and pivot chains.
func propCases(t testing.TB) []propCase {
	t.Helper()
	var out []propCase
	for w := 3; w <= 8; w++ {
		seed := uint64(1000 + w)
		nodes := randomDAG(seed, w, 50*(w-2), (w-3)%3)
		d := newDAG(t, w-1)
		addAll(t, d, nodes)
		r := rand.New(rand.NewPCG(seed, 1))
		chain := pivotChain(r, d, nodes, 4)
		if len(chain) < 4 {
			t.Fatalf("writers %d: pivot chain of %d", w, len(chain))
		}
		out = append(out, propCase{nodes: nodes, settled: chain[:2], predicted: chain[2:], group: [32]byte(idOf("group"))})
	}
	return out
}

func (pc *propCase) order(t testing.TB, in []Node) []ID {
	t.Helper()
	d := newDAG(t, MaxLinksLimit)
	addAll(t, d, in)
	return sequence(t, d, pc.settled, pc.predicted, pc.group)
}

// batches checks every batch of an order with the naive Kahn check: each step takes the smallest ready entry under the
// batch's seed, so concurrent entries are ordered by the tie-break rule only.
func (pc *propCase) checkBatches(t testing.TB, order []ID) {
	t.Helper()
	nv := newNaive(pc.nodes)
	nv.checkCausal(t, order)
	prev := map[ID]bool{}
	at := 0
	for _, p := range append(slices.Clone(pc.settled), pc.predicted...) {
		P := nv.past(p)
		B := minusSet(P, prev)
		nv.checkKahn(t, B, p, order[at:at+len(B)])
		at += len(B)
		prev = P
	}
	all := setOf()
	for _, n := range pc.nodes {
		all[n.ID] = true
	}
	rest := minusSet(all, prev)
	nv.checkKahn(t, rest, pc.group, order[at:])
}

func TestPropertyDeliveryOrder(t *testing.T) {
	for i, pc := range propCases(t) {
		want := pc.order(t, pc.nodes)
		if len(want) != len(pc.nodes) {
			t.Fatalf("case %d: %d ordered of %d", i, len(want), len(pc.nodes))
		}
		pc.checkBatches(t, want)
		r := rand.New(rand.NewPCG(uint64(i), 99))
		for k := range shuffles() {
			if got := pc.order(t, shuffled(r, pc.nodes)); !slices.Equal(got, want) {
				t.Fatalf("case %d shuffle %d: differs at %d", i, k, FirstChange(got, want))
			}
		}
	}
}

// TestPropertyAdversarialOrders delivers entries in orders chosen against the implementation: sorted by ID either way,
// newest first (every entry waits until the oldest arrives), and by order key.
func TestPropertyAdversarialOrders(t *testing.T) {
	for i, pc := range propCases(t) {
		want := pc.order(t, pc.nodes)
		byID := slices.Clone(pc.nodes)
		slices.SortFunc(byID, func(a, b Node) int { return bytes.Compare(a.ID[:], b.ID[:]) })
		desc := slices.Clone(byID)
		slices.Reverse(desc)
		newest := slices.Clone(pc.nodes)
		slices.Reverse(newest)
		byKey := slices.Clone(pc.nodes)
		slices.SortFunc(byKey, func(a, b Node) int {
			ka, kb := keyOf(pc.group, a.ID), keyOf(pc.group, b.ID)
			return bytes.Compare(ka[:], kb[:])
		})
		for j, in := range [][]Node{byID, desc, newest, byKey} {
			if got := pc.order(t, in); !slices.Equal(got, want) {
				t.Fatalf("case %d order %d: differs at %d", i, j, FirstChange(got, want))
			}
		}
	}
}

// TestPropertyPure checks that the order is a function of the placed entry set alone: entries that never get placed,
// duplicates and the history of earlier calls change nothing.
func TestPropertyPure(t *testing.T) {
	for i, pc := range propCases(t) {
		want := pc.order(t, pc.nodes)
		r := rand.New(rand.NewPCG(uint64(i), 5))
		var junk []Node
		for k := range 40 {
			n := pc.nodes[r.IntN(len(pc.nodes))]
			j := Node{ID: idOf("junk" + string(rune(i*100+k))), Strand: n.Strand, Index: n.Index + 1, Prev: idOf("missing"), Class: Membership}
			junk = append(junk, j)
		}
		in := append(shuffled(r, pc.nodes), junk...)
		in = append(in, pc.nodes[:10]...)
		in = shuffled(r, in)
		d := newDAG(t, MaxLinksLimit)
		for k, n := range in {
			addAll(t, d, []Node{n})
			if k%37 == 0 {
				if _, err := d.Tail(ID{}, nil, pc.group); err != nil {
					t.Fatal(err)
				}
			}
		}
		if got := sequence(t, d, pc.settled, pc.predicted, pc.group); !slices.Equal(got, want) {
			t.Fatalf("case %d: differs at %d", i, FirstChange(got, want))
		}
		if again := sequence(t, d, pc.settled, pc.predicted, pc.group); !slices.Equal(again, want) {
			t.Fatalf("case %d: a second call differs", i)
		}
	}
}

// linearExtension returns the entries in a random order in which every entry follows its predecessors.
func linearExtension(r *rand.Rand, nodes []Node) []Node {
	nv := newNaive(nodes)
	done := map[ID]bool{}
	var out []Node
	for len(out) < len(nodes) {
		var ready []Node
		for _, n := range nodes {
			if done[n.ID] {
				continue
			}
			ok := true
			for _, p := range nv.preds(n.ID) {
				if !done[p] {
					ok = false
				}
			}
			if ok {
				ready = append(ready, n)
			}
		}
		n := ready[r.IntN(len(ready))]
		done[n.ID] = true
		out = append(out, n)
	}
	return out
}

// TestPropertyLateArrivals checks the reorg boundary. A peer holds an ideal S of the DAG with settled and predicted pivots
// inside S. When the other entries arrive with the same pivots, the settled batches and the predicted batches keep their
// positions, the first changed position (FirstChange) is at or after the start of the part ordered under the group ID, the
// entry there is a late one, and the entries of S keep their relative order. A newer predicted pivot among the late entries
// changes nothing before that start either.
func TestPropertyLateArrivals(t *testing.T) {
	newerTested := 0
	for seed := range uint64(12) {
		r := rand.New(rand.NewPCG(seed, 77))
		w := 3 + int(seed%6)
		nodes := randomDAG(3000+seed, w, 120+int(seed)*15, int(seed%3))
		ext := linearExtension(r, nodes)
		k := len(ext)/3 + r.IntN(len(ext)/3)
		S, late := ext[:k], ext[k:]
		inS := map[ID]bool{}
		for _, n := range S {
			inS[n.ID] = true
		}
		d := newDAG(t, w-1)
		addAll(t, d, S)
		chain := pivotChain(r, d, S, 3)
		if len(chain) == 0 {
			t.Fatal("no pivot")
		}
		settled, predicted := chain[:1], chain[1:]
		group := [32]byte(idOf("g"))
		old := sequence(t, d, settled, predicted, group)
		restStart := len(newNaive(nodes).past(chain[len(chain)-1]))
		settledLen := len(newNaive(nodes).past(settled[0]))

		prev := old
		for _, n := range shuffled(r, late) {
			addAll(t, d, []Node{n})
			cur := sequence(t, d, settled, predicted, group)
			fc := FirstChange(prev, cur)
			if fc < restStart {
				t.Fatalf("seed %d: order changed at %d, before the unpredicted part at %d", seed, fc, restStart)
			}
			if fc < len(prev) && inS[cur[fc]] {
				t.Fatalf("seed %d: the first changed position holds an entry of S", seed)
			}
			if !slices.Equal(cur[:settledLen], old[:settledLen]) {
				t.Fatalf("seed %d: settled prefix changed", seed)
			}
			var kept []ID
			for _, id := range cur {
				if inS[id] {
					kept = append(kept, id)
				}
			}
			if !slices.Equal(kept, old) {
				t.Fatalf("seed %d: entries of S changed their relative order", seed)
			}
			prev = cur
		}
		if d.Len() != len(nodes) {
			t.Fatalf("seed %d: %d placed", seed, d.Len())
		}
		last := chain[len(chain)-1]
		for _, n := range late {
			c, _ := d.Clock(n.ID)
			if !d.Contains(c, last) {
				continue
			}
			newer := sequence(t, d, settled, append(slices.Clone(predicted), n.ID), group)
			if fc := FirstChange(prev, newer); fc < restStart {
				t.Fatalf("seed %d: a newer predicted pivot changed position %d before %d", seed, fc, restStart)
			}
			if fc := FirstChange(old, newer); fc < restStart {
				t.Fatalf("seed %d: a newer predicted pivot changed the old tail at %d before %d", seed, fc, restStart)
			}
			newerTested++
			break
		}
	}
	if newerTested < 6 {
		t.Fatalf("a newer predicted pivot was tried in %d of 12 DAGs", newerTested)
	}
}

// TestPropertyAdversarialIDs uses IDs and strand IDs that differ only in their last byte, delivered in many orders to
// fresh DAGs, whose maps all iterate differently.
func TestPropertyAdversarialIDs(t *testing.T) {
	const writers = 5
	g := newGen(42, writers)
	for w := range g.strands {
		g.strands[w] = strandOf(0xaa)
		g.strands[w][31] = byte(w)
	}
	g.pLink = 0.7
	for range 150 {
		g.add(g.r.IntN(writers), 0)
	}
	// replace the random IDs by ones that share their first 30 bytes
	ren := map[ID]ID{}
	for i := range g.nodes {
		var id ID
		for j := range id {
			id[j] = 0x55
		}
		id[30], id[31] = byte(i>>8), byte(i)
		ren[g.nodes[i].ID] = id
	}
	nodes := make([]Node, len(g.nodes))
	for i, n := range g.nodes {
		n.ID = ren[n.ID]
		if n.Index > 0 {
			n.Prev = ren[n.Prev]
		}
		n.Links = slices.Clone(n.Links)
		for j := range n.Links {
			n.Links[j].ID = ren[n.Links[j].ID]
		}
		nodes[i] = n
	}
	pc := propCase{nodes: nodes, group: [32]byte(idOf("adv"))}
	want := pc.order(t, nodes)
	pc.checkBatches(t, want)
	r := rand.New(rand.NewPCG(8, 8))
	for k := range 50 {
		if got := pc.order(t, shuffled(r, nodes)); !slices.Equal(got, want) {
			t.Fatalf("delivery %d: differs at %d", k, FirstChange(got, want))
		}
	}
}

// permute calls f with every permutation of nodes (Heap's algorithm).
func permute(nodes []Node, f func([]Node)) {
	a := slices.Clone(nodes)
	c := make([]int, len(a))
	f(a)
	for i := 0; i < len(a); {
		if c[i] < i {
			if i%2 == 0 {
				a[0], a[i] = a[i], a[0]
			} else {
				a[c[i]], a[i] = a[i], a[c[i]]
			}
			f(a)
			c[i]++
			i = 0
		} else {
			c[i] = 0
			i++
		}
	}
}

// TestPropertyEveryDeliveryOrder delivers 8-entry DAGs, one of them forked, in all 40,320 orders.
func TestPropertyEveryDeliveryOrder(t *testing.T) {
	for i, nodes := range [][]Node{randomDAG(51, 3, 8, 0), randomDAG(52, 4, 8, 1), randomDAG(53, 3, 8, 2)} {
		pc := propCase{nodes: nodes, group: [32]byte(idOf("perm"))}
		want := pc.order(t, nodes)
		pc.checkBatches(t, want)
		n := 0
		permute(nodes, func(in []Node) {
			n++
			if got := pc.order(t, in); !slices.Equal(got, want) {
				t.Fatalf("DAG %d permutation %d: differs at %d", i, n, FirstChange(got, want))
			}
		})
		if n != 40320 {
			t.Fatalf("%d permutations", n)
		}
	}
}

// TestPropertyManyOrdersLarge delivers a 500-entry DAG in 10,000 random orders (1,000 under the race detector).
func TestPropertyManyOrdersLarge(t *testing.T) {
	nodes := randomDAG(54, 6, 500, 2)
	d := newDAG(t, 5)
	addAll(t, d, nodes)
	r := rand.New(rand.NewPCG(54, 1))
	chain := pivotChain(r, d, nodes, 3)
	pc := propCase{nodes: nodes, settled: chain[:1], predicted: chain[1:], group: [32]byte(idOf("large"))}
	want := pc.order(t, nodes)
	pc.checkBatches(t, want)
	orders := 10 * shuffles()
	for k := range orders {
		if got := pc.order(t, shuffled(r, nodes)); !slices.Equal(got, want) {
			t.Fatalf("order %d: differs at %d", k, FirstChange(got, want))
		}
	}
}
