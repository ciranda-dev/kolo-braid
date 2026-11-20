package dag

import (
	"bytes"
	"errors"
	"math"
	"math/rand/v2"
	"slices"
	"testing"
)

func TestNewConfig(t *testing.T) {
	for _, c := range []Config{
		{MaxLinks: 0, MaxPendingBytes: 1},
		{MaxLinks: MaxLinksLimit + 1, MaxPendingBytes: 1},
		{MaxLinks: 1, MaxPendingBytes: 0},
		{MaxLinks: 1, MaxPendingBytes: 1, Segments: []Segment{{Start: 1, Parent: -1}}},
		{MaxLinks: 1, MaxPendingBytes: 1, Segments: []Segment{{Start: 0, Parent: 0}}},
		{MaxLinks: 1, MaxPendingBytes: 1, Segments: []Segment{{Start: 0, Parent: -1, Released: 2, Base: Clock{2}}}},
		{MaxLinks: 1, MaxPendingBytes: 1, Lookup: noLookup, Segments: []Segment{{Start: 0, Parent: -1, Released: 2, Base: Clock{3}}}},
		{MaxLinks: 1, MaxPendingBytes: 1, Lookup: noLookup, Segments: []Segment{{Start: 0, Parent: -1, Released: 2}}},
		{MaxLinks: 1, MaxPendingBytes: 1, Lookup: noLookup, Segments: []Segment{{Start: 0, Parent: -1, Released: 0, Base: Clock{}}}},
		{MaxLinks: 1, MaxPendingBytes: 1, Lookup: noLookup, Segments: []Segment{{Start: 0, Parent: -1, Released: 2, Base: Clock{2, 1}}}},
		{MaxLinks: 1, MaxPendingBytes: 1, Lookup: noLookup, Segments: []Segment{{Start: 0, Parent: -1}, {Start: 0, Parent: 1}}},
		{MaxLinks: 1, MaxPendingBytes: 1, Lookup: noLookup, Segments: []Segment{{Start: 0, Parent: -1, Released: math.MaxUint64, Base: Clock{math.MaxUint64}}}},
	} {
		if _, err := New(c); !errors.Is(err, ErrInvalid) {
			t.Errorf("New(%+v) = %v", c, err)
		}
	}
	for _, c := range []Config{{MaxLinks: 1, MaxPendingBytes: 1}, {MaxLinks: MaxLinksLimit, MaxPendingBytes: 1}} {
		if _, err := New(c); err != nil {
			t.Errorf("New(%+v) = %v", c, err)
		}
	}
}

func noLookup(Link) (int, Clock, bool, error) { return 0, nil, false, nil }

func TestAddInvalid(t *testing.T) {
	a, b, c := strandOf(1), strandOf(2), strandOf(3)
	id := idOf("x")
	other := idOf("y")
	lb := Link{Strand: b, ID: other}
	lc := Link{Strand: c, ID: other}
	for name, n := range map[string]Node{
		"zero ID":             {Strand: a},
		"unknown class":       {ID: id, Strand: a, Class: 2},
		"index out of range":  {ID: id, Strand: a, Index: math.MaxUint64, Prev: other},
		"zero prev at 1":      {ID: id, Strand: a, Index: 1},
		"prev at 0":           {ID: id, Strand: a, Prev: other},
		"too many links":      {ID: id, Strand: a, Links: []Link{lb, lc}},
		"own strand link":     {ID: id, Strand: a, Links: []Link{{Strand: a, ID: other}}},
		"unsorted links":      {ID: id, Strand: a, Links: []Link{lc, lb}},
		"two links per stand": {ID: id, Strand: a, Links: []Link{lb, lb}},
		"negative size":       {ID: id, Strand: a, Size: -1},
	} {
		d := newDAG(t, 1)
		if name == "unsorted links" || name == "two links per stand" {
			d = newDAG(t, 2)
		}
		if _, err := d.Add(n); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: %v", name, err)
		}
		if d.Len() != 0 {
			t.Errorf("%s: stored", name)
		}
		if p, _ := d.Pending(); p != 0 {
			t.Errorf("%s: pending", name)
		}
	}
}

// TestLinkBound checks MaxLinks links exactly: accepted, one more refused.
func TestLinkBound(t *testing.T) {
	const k = 5
	d := newDAG(t, k)
	var links []Link
	for i := range k + 1 {
		n := node(idOf(string(rune('a'+i))), strandOf(byte(10+i)), 0, ID{}, Other)
		addAll(t, d, []Node{n})
		links = append(links, linkTo(n))
	}
	if _, err := d.Add(node(idOf("over"), strandOf(1), 0, ID{}, Other, links...)); !errors.Is(err, ErrInvalid) {
		t.Fatalf("%d links: %v", k+1, err)
	}
	p, err := d.Add(node(idOf("at"), strandOf(1), 0, ID{}, Other, links[:k]...))
	if err != nil || len(p) != 1 {
		t.Fatalf("%d links: %v %v", k, p, err)
	}
}

func TestHugeFanIn(t *testing.T) {
	d := newDAG(t, MaxLinksLimit)
	var links []Link
	var nodes []Node
	for i := range MaxLinksLimit {
		var s StrandID
		s[0], s[1] = byte(i>>8), byte(i)
		n := node(idOf("w"+string(rune(i))), s, 0, ID{}, Other)
		nodes = append(nodes, n)
		links = append(links, linkTo(n))
	}
	fan := node(idOf("fan"), strandOf(0xff), 0, ID{}, Other, links...)
	if _, err := d.Add(fan); err != nil {
		t.Fatal(err)
	}
	if n, _ := d.Pending(); n != 1 {
		t.Fatal("fan-in entry is not pending")
	}
	addAll(t, d, shuffled(rand.New(rand.NewPCG(3, 3)), nodes))
	c, ok := d.Clock(fan.ID)
	if !ok || len(c) != MaxLinksLimit+1 {
		t.Fatalf("fan-in clock has %d components", len(c))
	}
	tail, err := d.Tail(ID{}, nil, [32]byte{})
	if err != nil || len(tail) != MaxLinksLimit+1 || tail[len(tail)-1] != fan.ID {
		t.Fatalf("fan-in entry not last: %v", err)
	}
}

func TestDuplicates(t *testing.T) {
	a, b := strandOf(1), strandOf(2)
	a0 := node(idOf("a0"), a, 0, ID{}, Other)
	b0 := node(idOf("b0"), b, 0, ID{}, Other, Link{Strand: a, Index: 0, ID: a0.ID})
	d := newDAG(t, 1)
	addAll(t, d, []Node{b0, b0})
	if n, _ := d.Pending(); n != 1 {
		t.Fatalf("pending duplicate counted: %d", n)
	}
	moved := b0
	moved.Index, moved.Prev = 1, idOf("p")
	if _, err := d.Add(moved); !errors.Is(err, ErrConflict) {
		t.Fatalf("pending ID with other fields: %v", err)
	}
	relinked := b0
	relinked.Links = []Link{{Strand: a, Index: 1, ID: a0.ID}}
	if _, err := d.Add(relinked); !errors.Is(err, ErrConflict) {
		t.Fatalf("pending ID with other links: %v", err)
	}
	p, err := d.Add(a0)
	if err != nil || len(p) != 2 {
		t.Fatalf("placed %d: %v", len(p), err)
	}
	for _, n := range []Node{a0, b0} {
		if p, err := d.Add(n); err != nil || len(p) != 0 {
			t.Fatalf("placed duplicate: %v %v", p, err)
		}
	}
	if d.Len() != 2 {
		t.Fatalf("%d placed", d.Len())
	}
	other := a0
	other.Strand = b
	if _, err := d.Add(other); !errors.Is(err, ErrConflict) {
		t.Fatalf("placed ID on another strand: %v", err)
	}
	reclassed := a0
	reclassed.Class = Membership
	if _, err := d.Add(reclassed); !errors.Is(err, ErrConflict) {
		t.Fatalf("placed ID with another class: %v", err)
	}
}

func TestUnknownLinks(t *testing.T) {
	a, b, c := strandOf(1), strandOf(2), strandOf(3)
	a0 := node(idOf("a0"), a, 0, ID{}, Other)
	a1 := node(idOf("a1"), a, 1, a0.ID, Other)
	b0 := node(idOf("b0"), b, 0, ID{}, Other, linkTo(a1))
	c0 := node(idOf("c0"), c, 0, ID{}, Other, linkTo(b0))
	wrongIndex := node(idOf("w"), c, 0, ID{}, Other, Link{Strand: a, Index: 5, ID: a0.ID})
	never := node(idOf("n"), b, 0, ID{}, Other, Link{Strand: a, Index: 0})
	d := newDAG(t, 2)
	addAll(t, d, []Node{c0, b0, a1, wrongIndex, never})
	if d.Len() != 0 {
		t.Fatal("placed with missing predecessors")
	}
	want := []Link{{Strand: a, Index: 0}, linkTo(a0), {Strand: a, Index: 5, ID: a0.ID}}
	if got := d.Unresolved(); !slices.Equal(got, want) {
		t.Fatalf("unresolved %v", got)
	}
	p, err := d.Add(a0)
	if err != nil || len(p) != 4 || p[0].ID != a0.ID || p[1].ID != a1.ID || p[2].ID != b0.ID || p[3].ID != c0.ID {
		t.Fatalf("cascade %v %v", p, err)
	}
	if got := d.Unresolved(); !slices.Equal(got, []Link{want[0], want[2]}) {
		t.Fatalf("unresolved after a0: %v", got)
	}
	if n, _ := d.Pending(); n != 2 {
		t.Fatalf("%d pending, want the two that link entries that never exist", n)
	}
	tail, _ := d.Tail(ID{}, nil, [32]byte{})
	if len(tail) != 4 {
		t.Fatalf("tail of %d", len(tail))
	}
}

func TestForgedCycle(t *testing.T) {
	a, b := strandOf(1), strandOf(2)
	x := node(idOf("x"), a, 0, ID{}, Other, Link{Strand: b, Index: 0, ID: idOf("y")})
	y := node(idOf("y"), b, 0, ID{}, Other, Link{Strand: a, Index: 0, ID: idOf("x")})
	self := node(idOf("s"), strandOf(3), 1, idOf("s"), Other)
	d := newDAG(t, 1)
	addAll(t, d, []Node{x, y, self})
	if d.Len() != 0 {
		t.Fatal("a cycle was placed")
	}
	if n, _ := d.Pending(); n != 3 {
		t.Fatalf("%d pending", n)
	}
	tail, err := d.Tail(ID{}, nil, [32]byte{})
	if err != nil || len(tail) != 0 {
		t.Fatalf("tail %v %v", tail, err)
	}
	// every entry of the cycle is held, so nothing is fetched for it
	if got := d.Unresolved(); len(got) != 0 {
		t.Fatalf("unresolved %v", got)
	}
}

func TestPendingBound(t *testing.T) {
	a := strandOf(1)
	cost := int64(pendingNodeCost + 100 + pendingLinkCost)
	d, err := New(Config{MaxLinks: 1, MaxPendingBytes: 3 * cost})
	if err != nil {
		t.Fatal(err)
	}
	root := node(idOf("root"), strandOf(2), 0, ID{}, Other)
	for i := range 3 {
		n := node(idOf("p"+string(rune(i))), a, 0, ID{}, Other, linkTo(root))
		n.Size = 100
		n.Strand[1] = byte(i)
		if _, err := d.Add(n); err != nil {
			t.Fatalf("pending %d: %v", i, err)
		}
	}
	if n, b := d.Pending(); n != 3 || b != 3*cost {
		t.Fatalf("pending %d, %d bytes", n, b)
	}
	over := node(idOf("over"), strandOf(9), 0, ID{}, Other, linkTo(root))
	if _, err := d.Add(over); !errors.Is(err, ErrPendingFull) {
		t.Fatalf("over the bound: %v", err)
	}
	if p, err := d.Add(over); !errors.Is(err, ErrPendingFull) || p != nil {
		t.Fatal("refused node was stored")
	}
	free := node(idOf("free"), strandOf(8), 0, ID{}, Other)
	if p, err := d.Add(free); err != nil || len(p) != 1 {
		t.Fatalf("placeable node refused at the bound: %v", err)
	}
	if p, err := d.Add(root); err != nil || len(p) != 4 {
		t.Fatalf("cascade %d: %v", len(p), err)
	}
	if n, b := d.Pending(); n != 0 || b != 0 {
		t.Fatalf("pending %d, %d bytes after the cascade", n, b)
	}
	if _, err := d.Add(over); err != nil {
		t.Fatalf("room again: %v", err)
	}
}

func TestFork(t *testing.T) {
	a, b, c := strandOf(1), strandOf(2), strandOf(3)
	var hist []Node
	prev := ID{}
	for i := range 4 {
		n := node(idOf("a"+string(rune('0'+i))), a, uint64(i), prev, Other)
		hist = append(hist, n)
		prev = n.ID
	}
	b2 := node(idOf("b2"), a, 2, hist[1].ID, Other)
	b3 := node(idOf("b3"), a, 3, b2.ID, Other)
	w0 := node(idOf("w0"), b, 0, ID{}, Other, linkTo(b3))
	w1 := node(idOf("w1"), b, 1, w0.ID, Other, linkTo(hist[3]))
	x0 := node(idOf("x0"), c, 0, ID{}, Membership, linkTo(hist[2]))
	all := append(slices.Clone(hist), b2, b3, w0, w1, x0)
	var first []ID
	for i, in := range [][]Node{all, {hist[0], hist[1], b2, b3, w0, hist[2], hist[3], x0, w1}} {
		d := newDAG(t, 2)
		addAll(t, d, in)
		if !d.Forked(a) || d.Forked(b) || d.Forked(c) {
			t.Fatal("fork not seen as two segments")
		}
		sa, _, _ := d.Pos(hist[2].ID)
		sb, _, _ := d.Pos(b2.ID)
		s1, _, _ := d.Pos(hist[1].ID)
		if sa == sb || (s1 != sa && s1 != sb) {
			t.Fatalf("segments: a2 %d, b2 %d, a1 %d", sa, sb, s1)
		}
		cb3, _ := d.Clock(b3.ID)
		cw1, _ := d.Clock(w1.ID)
		cx0, _ := d.Clock(x0.ID)
		switch {
		case !d.Contains(cb3, hist[1].ID) || d.Contains(cb3, hist[2].ID):
			t.Fatal("b3's past")
		case !d.Contains(cw1, b3.ID) || !d.Contains(cw1, hist[3].ID) || !d.Contains(cw1, hist[0].ID):
			t.Fatal("w1's past")
		case d.Contains(cx0, b2.ID) || !d.Contains(cx0, hist[2].ID):
			t.Fatal("x0's past")
		}
		tail, err := d.Tail(ID{}, nil, [32]byte{7})
		if err != nil || len(tail) != len(all) {
			t.Fatalf("tail %d: %v", len(tail), err)
		}
		nv := newNaive(all)
		nv.checkCausal(t, tail)
		nv.checkKahn(t, setOf(tail...), [32]byte{7}, tail)
		if i == 0 {
			first = tail
		} else if !slices.Equal(first, tail) {
			t.Fatal("fork order depends on which history arrived first")
		}
	}
}

func TestForkAtZero(t *testing.T) {
	a, b := strandOf(1), strandOf(2)
	x0 := node(idOf("x0"), a, 0, ID{}, Other)
	y0 := node(idOf("y0"), a, 0, ID{}, Other)
	y1 := node(idOf("y1"), a, 1, y0.ID, Other)
	w := node(idOf("w"), b, 0, ID{}, Other, linkTo(y1))
	d := newDAG(t, 1)
	addAll(t, d, []Node{x0, y0, y1, w})
	if !d.Forked(a) || d.Len() != 4 {
		t.Fatal("two roots of one strand")
	}
	cw, _ := d.Clock(w.ID)
	if d.Contains(cw, x0.ID) || !d.Contains(cw, y0.ID) {
		t.Fatal("past across roots")
	}
}

func TestZeroLinks(t *testing.T) {
	var nodes []Node
	for i := range 40 {
		s := strandOf(byte(i % 5))
		var prev ID
		if i >= 5 {
			prev = nodes[i-5].ID
		}
		nodes = append(nodes, node(idOf("z"+string(rune(i))), s, uint64(i/5), prev, Class(i%2)))
	}
	d := newDAG(t, 4)
	addAll(t, d, nodes)
	tail, err := d.Tail(ID{}, nil, [32]byte{3})
	if err != nil {
		t.Fatal(err)
	}
	nv := newNaive(nodes)
	nv.checkCausal(t, tail)
	nv.checkKahn(t, setOf(tail...), [32]byte{3}, tail)
}

// TestClocksMatchPasts compares every clock with the past found by search, on forked random DAGs.
func TestClocksMatchPasts(t *testing.T) {
	for seed := range uint64(6) {
		nodes := randomDAG(100+seed, 3+int(seed), 150, 3)
		d := newDAG(t, 8)
		addAll(t, d, nodes)
		nv := newNaive(nodes)
		for _, y := range nodes {
			c, ok := d.Clock(y.ID)
			if !ok {
				t.Fatal("not placed")
			}
			p := nv.past(y.ID)
			for _, x := range nodes {
				if d.Contains(c, x.ID) != p[x.ID] {
					t.Fatalf("seed %d: Contains disagrees with the past", seed)
				}
			}
		}
	}
}

// store keeps index records as a durable store would, and rebuilds clocks from them.
type store struct {
	recs  map[Link]Record
	prev  map[Link]Link
	segs  func() int
	steps int
}

func newStore() *store { return &store{recs: map[Link]Record{}, prev: map[Link]Link{}} }

func (s *store) keep(t testing.TB, nodes map[ID]Node, placed []Placed) {
	t.Helper()
	for _, p := range placed {
		r, err := DecodeRecord(p.Record)
		if err != nil {
			t.Fatal(err)
		}
		n := nodes[p.ID]
		l := linkTo(n)
		s.recs[l] = r
		if n.Index > 0 {
			s.prev[l] = Link{Strand: n.Strand, Index: n.Index - 1, ID: n.Prev}
		}
	}
}

func (s *store) clock(l Link, depth int) (Clock, bool, error) {
	r, ok := s.recs[l]
	if !ok {
		return nil, false, nil
	}
	s.steps = max(s.steps, depth)
	var prev Clock
	if !r.Full {
		var err error
		if prev, _, err = s.clock(s.prev[l], depth+1); err != nil {
			return nil, false, err
		}
	}
	c, err := r.Clock(prev, s.segs())
	return c, true, err
}

func (s *store) lookup(l Link) (int, Clock, bool, error) {
	c, ok, err := s.clock(l, 0)
	if !ok || err != nil {
		return 0, nil, false, err
	}
	return s.recs[l].Seg, c, true, nil
}

func TestCheckpoints(t *testing.T) {
	g := newGen(41, 3)
	for range 1500 {
		g.add(g.r.IntN(3), 0)
	}
	hi := g.fork(0)
	for range 400 {
		g.add(0, hi)
	}
	nodes := g.nodes
	st := newStore()
	d := newDAG(t, 2)
	st.segs = func() int { return len(d.segs) }
	byID := map[ID]Node{}
	for _, n := range nodes {
		byID[n.ID] = n
		p, err := d.Add(n)
		if err != nil {
			t.Fatal(err)
		}
		st.keep(t, byID, p)
	}
	for _, n := range nodes {
		r := st.recs[linkTo(n)]
		seg, _, _ := d.Pos(n.ID)
		first := n.Index == d.segs[seg].start
		if r.Full != (n.Index%checkpointEvery == 0 || first) || r.Seg != seg || r.Index != n.Index {
			t.Fatalf("record of index %d: %+v", n.Index, r)
		}
		got, ok, err := st.clock(linkTo(n), 0)
		want, _ := d.Clock(n.ID)
		if !ok || err != nil || !slices.Equal(got, want) {
			t.Fatalf("clock rebuilt from records differs at index %d", n.Index)
		}
	}
	if st.steps > checkpointEvery-1 {
		t.Fatalf("a clock needed %d deltas", st.steps)
	}
}

func TestRelease(t *testing.T) {
	r := rand.New(rand.NewPCG(5, 5))
	for seed := range uint64(8) {
		nodes := randomDAG(200+seed, 4+int(seed%4), 260, int(seed%3))
		byID := map[ID]Node{}
		for _, n := range nodes {
			byID[n.ID] = n
		}
		half := nodes[:180]
		ref := newDAG(t, 8)
		addAll(t, ref, half)
		piv := pivotChain(r, ref, half, 2)
		if len(piv) < 2 {
			continue
		}
		settled, pred := piv[0], piv[1:]
		st := newStore()
		d, err := New(Config{MaxLinks: 8, MaxPendingBytes: 1 << 30, Lookup: st.lookup})
		if err != nil {
			t.Fatal(err)
		}
		st.segs = func() int { return len(d.segs) }
		add := func(d *DAG, ns []Node) {
			for _, n := range ns {
				p, err := d.Add(n)
				if err != nil {
					t.Fatal(err)
				}
				st.keep(t, byID, p)
			}
		}
		add(d, half)
		front, _ := d.Clock(settled)
		before := d.Len()
		if err := d.Release(front); err != nil {
			t.Fatal(err)
		}
		if d.Len() >= before {
			t.Fatal("nothing left memory")
		}
		// a second release of the same frontier is a no-op
		if err := d.Release(front); err != nil {
			t.Fatal(err)
		}
		addAll(t, ref, nodes[180:])
		add(d, nodes[180:])
		want, err := ref.Tail(settled, pred, [32]byte{9})
		if err != nil {
			t.Fatal(err)
		}
		// d no longer holds the settled pivot: its clock is the frontier
		got := orderFrom(t, d, front, pred, [32]byte{9})
		if !slices.Equal(got, want) {
			t.Fatalf("seed %d: tail after release differs at %d", seed, FirstChange(got, want))
		}
		// restart: the segment table and the records, then the entries still in memory again
		re, err := New(Config{MaxLinks: 8, MaxPendingBytes: 1 << 30, Lookup: st.lookup, Segments: d.Segments()})
		if err != nil {
			t.Fatal(err)
		}
		st.segs = func() int { return len(re.segs) }
		var kept []Node
		for _, n := range nodes {
			if _, ok := d.byID[n.ID]; ok {
				kept = append(kept, n)
			}
		}
		add(re, shuffled(r, kept))
		if re.Len() != d.Len() {
			t.Fatalf("restart holds %d, want %d", re.Len(), d.Len())
		}
		if got := orderFrom(t, re, front, pred, [32]byte{9}); !slices.Equal(got, want) {
			t.Fatalf("seed %d: tail after restart differs at %d", seed, FirstChange(got, want))
		}
		if err := re.Order(nil, nil, [32]byte{}, func(ID) error { return nil }); !errors.Is(err, ErrNotHeld) {
			t.Fatalf("order over released entries: %v", err)
		}
		if _, err := re.Tail(ID{}, nil, [32]byte{}); !errors.Is(err, ErrNotHeld) {
			t.Fatalf("tail over released entries: %v", err)
		}
		// a released entry added again is a duplicate
		sc, _ := ref.Clock(settled)
		for _, n := range nodes {
			if !ref.Contains(sc, n.ID) {
				continue
			}
			if p, err := re.Add(n); err != nil || len(p) != 0 {
				t.Fatalf("released duplicate: %v %v", p, err)
			}
			if re.Len() != d.Len() {
				t.Fatal("released duplicate stored")
			}
		}
	}
}

// orderFrom is Tail with the settled frontier given as a clock.
func orderFrom(t testing.TB, d *DAG, front Clock, pred []ID, group [32]byte) []ID {
	t.Helper()
	var out []ID
	add := func(id ID) error { out = append(out, id); return nil }
	from := front
	for _, p := range pred {
		to, ok := d.Clock(p)
		if !ok {
			t.Fatal("predicted pivot not held")
		}
		if err := d.Order(from, to, p, add); err != nil {
			t.Fatal(err)
		}
		from = to
	}
	if err := d.Order(from, nil, group, add); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestReleaseErrors(t *testing.T) {
	nodes := randomDAG(77, 3, 40, 0)
	plain := newDAG(t, 2)
	addAll(t, plain, nodes)
	c, _ := plain.Clock(nodes[20].ID)
	if err := plain.Release(c); !errors.Is(err, ErrInvalid) {
		t.Fatalf("release without lookup: %v", err)
	}
	st := newStore()
	d, _ := New(Config{MaxLinks: 2, MaxPendingBytes: 1 << 20, Lookup: st.lookup})
	st.segs = func() int { return len(d.segs) }
	addAll(t, d, nodes)
	if err := d.Release(append(slices.Clone(c), 0, 0, 1)); !errors.Is(err, ErrInvalid) {
		t.Fatalf("unknown segment: %v", err)
	}
	big := slices.Clone(c)
	big[0] += 1000
	if err := d.Release(big); !errors.Is(err, ErrNotHeld) {
		t.Fatalf("beyond held: %v", err)
	}
	// not downward-closed: one segment's last entry links beyond the frontier of another
	for _, n := range nodes {
		cn, _ := d.Clock(n.ID)
		seg, idx, _ := d.Pos(n.ID)
		cut := make(Clock, len(cn))
		cut[seg] = idx + 1
		if slices.Equal(cut, cn) {
			continue
		}
		if err := d.Release(cut); !errors.Is(err, ErrInvalid) {
			t.Fatalf("not downward-closed: %v", err)
		}
		if d.Len() != len(nodes) {
			t.Fatal("a refused release changed the DAG")
		}
		return
	}
	t.Fatal("no entry with links")
}

func TestLookupAnswers(t *testing.T) {
	a, b := strandOf(1), strandOf(2)
	a0 := node(idOf("a0"), a, 0, ID{}, Other)
	a1 := node(idOf("a1"), a, 1, a0.ID, Other)
	b0 := node(idOf("b0"), b, 0, ID{}, Other, linkTo(a0))
	boom := errors.New("disk")
	for name, lk := range map[string]Lookup{
		"error":         func(Link) (int, Clock, bool, error) { return 0, nil, false, boom },
		"bad segment":   func(Link) (int, Clock, bool, error) { return 5, Clock{1}, true, nil },
		"bad clock":     func(Link) (int, Clock, bool, error) { return 0, Clock{7}, true, nil },
		"long clock":    func(Link) (int, Clock, bool, error) { return 0, Clock{1, 0, 0, 0}, true, nil },
		"other strand":  func(Link) (int, Clock, bool, error) { return 1, Clock{0, 1}, true, nil },
		"wrong index":   func(Link) (int, Clock, bool, error) { return 0, Clock{2}, true, nil },
		"not released":  func(Link) (int, Clock, bool, error) { return 2, Clock{0, 0, 1}, true, nil },
		"negative seg":  func(Link) (int, Clock, bool, error) { return -1, Clock{1}, true, nil },
		"answers false": func(Link) (int, Clock, bool, error) { return 0, nil, false, nil },
	} {
		d, _ := New(Config{MaxLinks: 1, MaxPendingBytes: 1 << 20, Lookup: lk})
		addAll(t, d, []Node{a0, node(idOf("x"), b, 0, ID{}, Other), node(idOf("a0 again"), a, 0, ID{}, Other)})
		if err := d.Release(Clock{1}); err != nil {
			t.Fatal(err)
		}
		_, err := d.Add(b0)
		switch name {
		case "error":
			if !errors.Is(err, boom) {
				t.Errorf("%s: %v", name, err)
			}
		case "answers false", "not released":
			if n, _ := d.Pending(); err != nil || n != 1 {
				t.Errorf("%s: %v, pending %d", name, err, n)
			}
		default:
			if !errors.Is(err, ErrInvalid) {
				t.Errorf("%s: %v", name, err)
			}
		}
		if name == "error" {
			if _, err := d.Add(a1); !errors.Is(err, boom) {
				t.Errorf("predecessor lookup error: %v", err)
			}
		}
	}
}

// TestLookupDuringCascade checks a Lookup that fails or forgets an entry while waiting entries are placed.
func TestLookupDuringCascade(t *testing.T) {
	a, b, c := strandOf(1), strandOf(2), strandOf(3)
	a0 := node(idOf("a0"), a, 0, ID{}, Other)
	b0 := node(idOf("b0"), b, 0, ID{}, Other)
	b1 := node(idOf("b1"), b, 1, b0.ID, Other)
	boom := errors.New("disk")
	for _, mode := range []string{"error", "forgets", "prev error", "prev forgets"} {
		answer := true
		st := newStore()
		d, _ := New(Config{MaxLinks: 2, MaxPendingBytes: 1 << 20, Lookup: func(l Link) (int, Clock, bool, error) {
			if !answer {
				if mode == "error" || mode == "prev error" {
					return 0, nil, false, boom
				}
				return 0, nil, false, nil
			}
			return st.lookup(l)
		}})
		st.segs = func() int { return len(d.segs) }
		byID := map[ID]Node{a0.ID: a0, b0.ID: b0, b1.ID: b1}
		for _, n := range []Node{a0, b0} {
			p, _ := d.Add(n)
			st.keep(t, byID, p)
		}
		if err := d.Release(Clock{1}); err != nil {
			t.Fatal(err)
		}
		// c0 links the released a0 and waits for b1; a1 has a0 as its predecessor and waits for b1
		c0 := node(idOf("c0"), c, 0, ID{}, Other, linkTo(a0), linkTo(b1))
		if mode == "prev error" || mode == "prev forgets" {
			c0 = node(idOf("a1"), a, 1, a0.ID, Other, linkTo(b1))
		}
		if p, err := d.Add(c0); err != nil || p != nil {
			t.Fatalf("%s: %v %v", mode, p, err)
		}
		answer = false
		p, err := d.Add(b1)
		if len(p) != 1 || p[0].ID != b1.ID {
			t.Fatalf("%s: placed %v", mode, p)
		}
		fails := mode == "error" || mode == "prev error"
		if fails && !errors.Is(err, boom) || !fails && !errors.Is(err, ErrInvalid) {
			t.Fatalf("%s: %v", mode, err)
		}
	}
}

func TestUnresolvedOrder(t *testing.T) {
	a, b := strandOf(1), strandOf(2)
	want := []Link{{Strand: a, Index: 1, ID: idOf("x")}, {Strand: a, Index: 1, ID: idOf("y")}, {Strand: a, Index: 7, ID: idOf("x")},
		{Strand: b, Index: 0, ID: idOf("x")}}
	if bytes.Compare(want[0].ID[:], want[1].ID[:]) > 0 {
		want[0].ID, want[1].ID = want[1].ID, want[0].ID
	}
	d := newDAG(t, 2)
	for i, l := range []Link{want[3], want[2], want[1], want[0]} {
		addAll(t, d, []Node{node(idOf("w"+string(rune(i))), strandOf(byte(9+i)), 0, ID{}, Other, l)})
	}
	if got := d.Unresolved(); !slices.Equal(got, want) {
		t.Fatalf("unresolved %v", got)
	}
}

func TestRecords(t *testing.T) {
	for _, r := range []Record{
		{},
		{Seg: 3, Index: 300, Full: true, Comps: []Comp{{Seg: 0, Value: 1}, {Seg: 7, Value: 1 << 40}}},
		{Seg: maxSeg, Index: math.MaxUint64 - 1, Comps: []Comp{{Seg: 0, Value: math.MaxUint64}}},
	} {
		got, err := DecodeRecord(r.Encode())
		if err != nil || got.Seg != r.Seg || got.Index != r.Index || got.Full != r.Full || !slices.Equal(got.Comps, r.Comps) {
			t.Fatalf("round trip of %+v: %+v %v", r, got, err)
		}
	}
	for name, b := range map[string][]byte{
		"empty":              {},
		"long seg":           {0x80, 0x00, 0, 0, 0},
		"seg over int32":     {0x80, 0x80, 0x80, 0x80, 0x08, 0, 0, 0},
		"index max":          {0, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01, 0, 0},
		"bad bool":           {0, 0, 2, 0},
		"count over bytes":   {0, 0, 0, 2, 1, 1},
		"own segment":        {1, 0, 0, 1, 1, 1},
		"zero value":         {0, 0, 0, 1, 1, 0},
		"not ascending":      {0, 0, 0, 2, 2, 1, 1, 1},
		"equal segments":     {0, 0, 0, 2, 1, 1, 1, 1},
		"trailing":           {0, 0, 0, 0, 9},
		"truncated":          {0, 0, 0, 1, 1},
		"long value":         {0, 0, 0, 1, 1, 0x81, 0x00},
		"comp seg too large": {0, 0, 0, 1, 0x80, 0x80, 0x80, 0x80, 0x08, 1},
	} {
		if _, err := DecodeRecord(b); !errors.Is(err, ErrMalformed) {
			t.Errorf("%s: %v", name, err)
		}
	}
	r := Record{Seg: 1, Index: 4, Comps: []Comp{{Seg: 0, Value: 2}}}
	if c, err := r.Clock(Clock{1, 4}, 2); err != nil || !slices.Equal(c, Clock{2, 5}) {
		t.Fatalf("delta clock %v %v", c, err)
	}
	full := Record{Seg: 1, Index: 4, Full: true, Comps: []Comp{{Seg: 0, Value: 2}}}
	if c, err := full.Clock(Clock{9, 9}, 2); err != nil || !slices.Equal(c, Clock{2, 5}) {
		t.Fatalf("full clock %v %v", c, err)
	}
	for _, bad := range []struct {
		r    Record
		prev Clock
		segs int
	}{
		{Record{Seg: 2}, nil, 2},
		{Record{Seg: -1}, nil, 2},
		{Record{Seg: 0, Comps: []Comp{{Seg: 5, Value: 1}}}, nil, 2},
		{Record{Seg: 0, Comps: []Comp{{Seg: -1, Value: 1}}}, nil, 2},
		{Record{Seg: 0}, Clock{1, 1, 1}, 2},
		{Record{Seg: 0, Index: math.MaxUint64}, nil, 2},
		{Record{Seg: 1, Index: 4}, Clock{0, 3}, 2},
		{Record{Seg: 1, Index: 4}, Clock{0, 5}, 2},
	} {
		if _, err := bad.r.Clock(bad.prev, bad.segs); !errors.Is(err, ErrMalformed) {
			t.Errorf("Clock of %+v: %v", bad.r, err)
		}
	}
}
