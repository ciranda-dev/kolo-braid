package dag

import (
	"bytes"
	"math/rand/v2"
	"slices"
	"testing"

	"golang.org/x/crypto/blake2b"
)

func newDAG(t testing.TB, maxLinks int) *DAG {
	t.Helper()
	d, err := New(Config{MaxLinks: maxLinks, MaxPendingBytes: 1 << 30})
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func addAll(t testing.TB, d *DAG, nodes []Node) {
	t.Helper()
	for _, n := range nodes {
		if _, err := d.Add(n); err != nil {
			t.Fatalf("add %x: %v", n.ID[:4], err)
		}
	}
}

func idOf(s string) ID { return blake2b.Sum256([]byte(s)) }

func strandOf(b byte) StrandID {
	var s StrandID
	for i := range s {
		s[i] = b
	}
	return s
}

func linkTo(n Node) Link { return Link{Strand: n.Strand, Index: n.Index, ID: n.ID} }

// node builds a node with the given links, sorted by strand.
func node(id ID, strand StrandID, index uint64, prev ID, class Class, links ...Link) Node {
	links = slices.Clone(links)
	slices.SortFunc(links, func(a, b Link) int { return bytes.Compare(a.Strand[:], b.Strand[:]) })
	return Node{ID: id, Strand: strand, Index: index, Prev: prev, Links: links, Class: class}
}

// gen builds random DAGs: writers append to one of their histories, link random entries of other strands, and sometimes
// sign a second history.
type gen struct {
	r        *rand.Rand
	strands  []StrandID
	hist     [][][]int
	byStrand [][]int
	nodes    []Node
	pLink    float64
	pZero    float64
	pMem     float64
}

func newGen(seed uint64, writers int) *gen {
	g := &gen{r: rand.New(rand.NewPCG(seed, 7)), pLink: 0.5, pZero: 0.1, pMem: 0.2}
	for range writers {
		var s StrandID
		for i := range s {
			s[i] = byte(g.r.UintN(256))
		}
		g.strands = append(g.strands, s)
		g.hist = append(g.hist, [][]int{nil})
		g.byStrand = append(g.byStrand, nil)
	}
	return g
}

func (g *gen) id() ID {
	var id ID
	for i := range id {
		id[i] = byte(g.r.UintN(256))
	}
	return id
}

// fork starts a second history of writer w from a random prefix of one of its histories.
func (g *gen) fork(w int) int {
	h := g.hist[w][g.r.IntN(len(g.hist[w]))]
	k := 0
	if len(h) > 0 {
		k = g.r.IntN(len(h))
	}
	g.hist[w] = append(g.hist[w], slices.Clone(h[:k]))
	return len(g.hist[w]) - 1
}

func (g *gen) add(w, hi int) Node {
	h := g.hist[w][hi]
	n := Node{ID: g.id(), Strand: g.strands[w], Index: uint64(len(h)), Class: Other}
	if len(h) > 0 {
		n.Prev = g.nodes[h[len(h)-1]].ID
	}
	if g.r.Float64() < g.pMem {
		n.Class = Membership
	}
	if g.r.Float64() >= g.pZero {
		for s := range g.byStrand {
			if s != w && len(g.byStrand[s]) > 0 && g.r.Float64() < g.pLink {
				n.Links = append(n.Links, linkTo(g.nodes[g.byStrand[s][g.r.IntN(len(g.byStrand[s]))]]))
			}
		}
	}
	slices.SortFunc(n.Links, func(a, b Link) int { return bytes.Compare(a.Strand[:], b.Strand[:]) })
	g.hist[w][hi] = append(h, len(g.nodes))
	g.byStrand[w] = append(g.byStrand[w], len(g.nodes))
	g.nodes = append(g.nodes, n)
	return n
}

// randomDAG returns entries in creation order, a valid insertion order.
func randomDAG(seed uint64, writers, entries, forks int) []Node {
	g := newGen(seed, writers)
	forkAt := map[int]bool{}
	for range forks {
		forkAt[1+g.r.IntN(entries-1)] = true
	}
	for i := 0; i < entries; i++ {
		w := g.r.IntN(writers)
		hi := g.r.IntN(len(g.hist[w]))
		if forkAt[i] {
			hi = g.fork(w)
		}
		g.add(w, hi)
	}
	return g.nodes
}

func shuffled(r *rand.Rand, nodes []Node) []Node {
	out := slices.Clone(nodes)
	r.Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })
	return out
}

// sequence is the full order of a DAG: the settled batches of the pivots, then the tail with the predicted pivots.
func sequence(t testing.TB, d *DAG, settled, predicted []ID, group [32]byte) []ID {
	t.Helper()
	var out []ID
	var prev ID
	for _, p := range settled {
		if err := d.Batch(prev, p, func(id ID) error { out = append(out, id); return nil }); err != nil {
			t.Fatal(err)
		}
		prev = p
	}
	tail, err := d.Tail(prev, predicted, group)
	if err != nil {
		t.Fatal(err)
	}
	return append(out, tail...)
}

// naive is a direct reading of the record's definitions, for checks.
type naive struct {
	nodes map[ID]Node
}

func newNaive(nodes []Node) *naive {
	m := map[ID]Node{}
	for _, n := range nodes {
		m[n.ID] = n
	}
	return &naive{nodes: m}
}

func (nv *naive) preds(id ID) []ID {
	n := nv.nodes[id]
	var p []ID
	if n.Index > 0 {
		p = append(p, n.Prev)
	}
	for _, l := range n.Links {
		p = append(p, l.ID)
	}
	return p
}

func (nv *naive) past(id ID) map[ID]bool {
	seen := map[ID]bool{}
	stack := []ID{id}
	for len(stack) > 0 {
		x := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if seen[x] {
			continue
		}
		seen[x] = true
		stack = append(stack, nv.preds(x)...)
	}
	return seen
}

func keyOf(seed [32]byte, id ID) [32]byte {
	b := []byte{byte(len(orderTag))}
	b = append(b, orderTag...)
	b = append(b, seed[:]...)
	b = append(b, id[:]...)
	return blake2b.Sum256(b)
}

func (nv *naive) less(seed [32]byte, a, b ID) bool {
	na, nb := nv.nodes[a], nv.nodes[b]
	if na.Class != nb.Class {
		return na.Class < nb.Class
	}
	ka, kb := keyOf(seed, a), keyOf(seed, b)
	if c := bytes.Compare(ka[:], kb[:]); c != 0 {
		return c < 0
	}
	return bytes.Compare(a[:], b[:]) < 0
}

// checkKahn checks that order is batch B (a set) emitted so that every step takes the smallest ready entry, and returns the
// ready set of every step.
func (nv *naive) checkKahn(t testing.TB, B map[ID]bool, seed [32]byte, order []ID) [][]ID {
	t.Helper()
	if len(order) != len(B) {
		t.Fatalf("batch has %d entries, order %d", len(B), len(order))
	}
	done := map[ID]bool{}
	var steps [][]ID
	for i, got := range order {
		var ready []ID
		for id := range B {
			if done[id] {
				continue
			}
			ok := true
			for _, p := range nv.preds(id) {
				if B[p] && !done[p] {
					ok = false
				}
			}
			if ok {
				ready = append(ready, id)
			}
		}
		slices.SortFunc(ready, func(a, b ID) int {
			if nv.less(seed, a, b) {
				return -1
			}
			return 1
		})
		if len(ready) == 0 || ready[0] != got {
			t.Fatalf("step %d: emitted %x, smallest ready is not it (%d ready)", i, got[:4], len(ready))
		}
		steps = append(steps, ready)
		done[got] = true
	}
	return steps
}

// checkCausal checks that every predecessor of an entry of order that is in order comes before it.
func (nv *naive) checkCausal(t testing.TB, order []ID) {
	t.Helper()
	at := map[ID]int{}
	for i, id := range order {
		if _, dup := at[id]; dup {
			t.Fatalf("%x emitted twice", id[:4])
		}
		at[id] = i
	}
	for i, id := range order {
		for _, p := range nv.preds(id) {
			j, ok := at[p]
			if !ok || j >= i {
				t.Fatalf("%x at %d before its predecessor %x (%v)", id[:4], i, p[:4], ok)
			}
		}
	}
}

func setOf(ids ...ID) map[ID]bool {
	m := map[ID]bool{}
	for _, id := range ids {
		m[id] = true
	}
	return m
}

func minusSet(a, b map[ID]bool) map[ID]bool {
	r := map[ID]bool{}
	for id := range a {
		if !b[id] {
			r[id] = true
		}
	}
	return r
}

// pivotChain picks k entries, each strictly inside the past of the next, among those d holds.
func pivotChain(r *rand.Rand, d *DAG, nodes []Node, k int) []ID {
	var out []ID
	var last ID
	for range k {
		var cand []ID
		for _, n := range nodes {
			c, ok := d.Clock(n.ID)
			if !ok || n.ID == last || (last != ID{}) && !d.Contains(c, last) {
				continue
			}
			cand = append(cand, n.ID)
		}
		if len(cand) == 0 {
			break
		}
		slices.SortFunc(cand, func(a, b ID) int {
			ca, _ := d.Clock(a)
			cb, _ := d.Clock(b)
			return sum(ca) - sum(cb)
		})
		last = cand[r.IntN((len(cand)+3)/4)]
		out = append(out, last)
	}
	return out
}

func sum(c Clock) int {
	n := 0
	for _, v := range c {
		n += int(v)
	}
	return n
}
