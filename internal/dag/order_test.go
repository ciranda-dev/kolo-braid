package dag

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math/rand/v2"
	"os"
	"slices"
	"testing"

	"golang.org/x/crypto/blake2b"
)

// vectors are computed by an independent naive program written from the decision record; see design/STATUS.md.
type vectorCase struct {
	Name    string   `json:"name"`
	Strands []string `json:"strands"`
	Group   string   `json:"group"`
	Entries []struct {
		Name     string `json:"name"`
		ID       string `json:"id"`
		Strand   int    `json:"strand"`
		Index    uint64 `json:"index"`
		Prev     int    `json:"prev"`
		Links    []int  `json:"links"`
		Dangling []struct {
			Strand int    `json:"strand"`
			Index  uint64 `json:"index"`
			ID     string `json:"id"`
		} `json:"dangling"`
		Class int `json:"class"`
	} `json:"entries"`
	Settled    []int  `json:"settled"`
	Predicted  []int  `json:"predicted"`
	Order      []int  `json:"order"`
	Placed     int    `json:"placed"`
	SettledLen int    `json:"settledLen"`
	RestStart  int    `json:"restStart"`
	Digest     string `json:"digest"`
}

func hex32(t testing.TB, s string) (b [32]byte) {
	t.Helper()
	v, err := hex.DecodeString(s)
	if err != nil || len(v) != 32 {
		t.Fatalf("bad hex %q", s)
	}
	copy(b[:], v)
	return b
}

func loadVectors(t testing.TB) []vectorCase {
	t.Helper()
	raw, err := os.ReadFile("testdata/order.json")
	if err != nil {
		t.Fatal(err)
	}
	var v struct{ Cases []vectorCase }
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatal(err)
	}
	if len(v.Cases) == 0 {
		t.Fatal("no vectors")
	}
	return v.Cases
}

func (c *vectorCase) nodes(t testing.TB) []Node {
	t.Helper()
	strands := make([]StrandID, len(c.Strands))
	for i, s := range c.Strands {
		strands[i] = hex32(t, s)
	}
	out := make([]Node, len(c.Entries))
	for i, e := range c.Entries {
		n := Node{ID: hex32(t, e.ID), Strand: strands[e.Strand], Index: e.Index, Class: Class(e.Class)}
		if e.Prev >= 0 {
			n.Prev = hex32(t, c.Entries[e.Prev].ID)
		}
		for _, l := range e.Links {
			x := c.Entries[l]
			n.Links = append(n.Links, Link{Strand: strands[x.Strand], Index: x.Index, ID: hex32(t, x.ID)})
		}
		for _, l := range e.Dangling {
			n.Links = append(n.Links, Link{Strand: strands[l.Strand], Index: l.Index, ID: hex32(t, l.ID)})
		}
		slices.SortFunc(n.Links, func(a, b Link) int { return bytes.Compare(a.Strand[:], b.Strand[:]) })
		out[i] = n
	}
	return out
}

func (c *vectorCase) ids(t testing.TB, idx []int) []ID {
	out := make([]ID, len(idx))
	for i, k := range idx {
		out[i] = hex32(t, c.Entries[k].ID)
	}
	return out
}

func digestOf(order []ID) string {
	h, _ := blake2b.New256(nil)
	for _, id := range order {
		h.Write(id[:])
	}
	return hex.EncodeToString(h.Sum(nil))
}

func TestVectors(t *testing.T) {
	r := rand.New(rand.NewPCG(11, 12))
	for _, c := range loadVectors(t) {
		t.Run(c.Name, func(t *testing.T) {
			nodes := c.nodes(t)
			orders := [][]Node{nodes, slices.Clone(nodes)}
			slices.Reverse(orders[1])
			for range 50 {
				orders = append(orders, shuffled(r, nodes))
			}
			want := c.ids(t, c.Order)
			for i, in := range orders {
				d := newDAG(t, len(c.Strands)-1)
				addAll(t, d, in)
				if d.Len() != c.Placed {
					t.Fatalf("insertion order %d: %d placed, want %d", i, d.Len(), c.Placed)
				}
				got := sequence(t, d, c.ids(t, c.Settled), c.ids(t, c.Predicted), hex32(t, c.Group))
				if !slices.Equal(got, want) {
					t.Fatalf("insertion order %d: order differs from the reference at %d", i, FirstChange(got, want))
				}
				if dg := digestOf(got); dg != c.Digest {
					t.Fatalf("digest %s, want %s", dg, c.Digest)
				}
			}
		})
	}
}

// TestWorkedExample reproduces the record's Kahn example: keys a0 0x91, b0 0x2c, a1 0x07, b1 0xe4, c0 0x55.
func TestWorkedExample(t *testing.T) {
	c := loadVectors(t)[0]
	if c.Name != "worked example" {
		t.Fatal("first vector is not the worked example")
	}
	nodes := c.nodes(t)
	name := map[ID]string{}
	byName := map[string]Node{}
	for i, e := range c.Entries {
		name[nodes[i].ID] = e.Name
		byName[e.Name] = nodes[i]
	}
	seed := hex32(t, c.Group)
	for n, first := range map[string]byte{"a0": 0x91, "b0": 0x2c, "a1": 0x07, "b1": 0xe4, "c0": 0x55} {
		if k := keyOf(seed, byName[n].ID); k[0] != first {
			t.Fatalf("%s key starts with %#x, want %#x", n, k[0], first)
		}
	}
	d := newDAG(t, 2)
	addAll(t, d, nodes)
	got, err := d.Tail(ID{}, nil, seed)
	if err != nil {
		t.Fatal(err)
	}
	names := func(ids []ID) []string {
		var s []string
		for _, id := range ids {
			s = append(s, name[id])
		}
		return s
	}
	if g := names(got); !slices.Equal(g, []string{"b0", "a0", "a1", "c0", "b1"}) {
		t.Fatalf("order %v", g)
	}
	steps := newNaive(nodes).checkKahn(t, setOf(got...), seed, got)
	table := [][]string{{"b0", "a0"}, {"a0"}, {"a1", "c0", "b1"}, {"c0", "b1"}, {"b1"}}
	for i, ready := range steps {
		if g := names(ready); !slices.Equal(g, table[i]) {
			t.Fatalf("step %d ready %v, want %v", i+1, g, table[i])
		}
	}

	early := newDAG(t, 2)
	addAll(t, early, []Node{byName["a0"], byName["c0"]})
	old, err := early.Tail(ID{}, nil, seed)
	if err != nil {
		t.Fatal(err)
	}
	if g := names(old); !slices.Equal(g, []string{"a0", "c0"}) {
		t.Fatalf("early order %v", g)
	}
	addAll(t, early, []Node{byName["b1"], byName["a1"], byName["b0"]})
	now, err := early.Tail(ID{}, nil, seed)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(now, got) || FirstChange(old, now) != 0 {
		t.Fatalf("after the late entries: %v, reorg from %d", names(now), FirstChange(old, now))
	}
}

func TestFirstChange(t *testing.T) {
	a, b, c := idOf("a"), idOf("b"), idOf("c")
	for _, tc := range []struct {
		x, y []ID
		want int
	}{
		{nil, nil, 0},
		{[]ID{a, b}, []ID{a, b}, 2},
		{[]ID{a, b}, []ID{a, b, c}, 2},
		{[]ID{a, b, c}, []ID{a, b}, 2},
		{[]ID{a, b}, []ID{a, c}, 1},
		{[]ID{a}, []ID{c, a}, 0},
	} {
		if got := FirstChange(tc.x, tc.y); got != tc.want {
			t.Errorf("FirstChange(%d, %d entries) = %d, want %d", len(tc.x), len(tc.y), got, tc.want)
		}
	}
}

// run is a Run over a fixed list of items.
func run(lo uint64, items ...Item) Run {
	i := 0
	return Run{Lo: lo, Hi: lo + uint64(len(items)), Next: func() (Item, error) {
		it := items[i]
		i++
		return it, nil
	}}
}

func collect(runs []Run) ([]ID, error) {
	var out []ID
	err := Merge(runs, [32]byte{}, func(it Item) error { out = append(out, it.ID); return nil })
	return out, err
}

func TestMergeHostile(t *testing.T) {
	x, y := idOf("x"), idOf("y")
	for _, tc := range []struct {
		name string
		runs []Run
		want error
	}{
		{"cycle across segments", []Run{
			run(0, Item{ID: x, Class: Other, Seg: 0, Index: 0, Waits: []Pos{{Seg: 1, Index: 0}}}),
			run(0, Item{ID: y, Class: Other, Seg: 1, Index: 0, Waits: []Pos{{Seg: 0, Index: 0}}}),
		}, ErrStuck},
		{"wait on itself", []Run{run(0, Item{ID: x, Class: Other, Seg: 0, Index: 0, Waits: []Pos{{Seg: 0, Index: 0}}})}, ErrStuck},
		{"wait on a later own entry", []Run{run(0,
			Item{ID: x, Class: Other, Seg: 0, Index: 0, Waits: []Pos{{Seg: 0, Index: 1}}},
			Item{ID: y, Class: Other, Seg: 0, Index: 1})}, ErrStuck},
		{"wait beyond the batch", []Run{
			run(0, Item{ID: x, Class: Other, Seg: 0, Index: 0, Waits: []Pos{{Seg: 1, Index: 5}}}),
			run(0, Item{ID: y, Class: Other, Seg: 1, Index: 0}),
		}, ErrStuck},
		{"wait on an unknown segment", []Run{run(0, Item{ID: x, Class: Other, Seg: 0, Index: 0, Waits: []Pos{{Seg: 3}}})}, ErrInvalid},
		{"negative segment", []Run{run(0, Item{ID: x, Class: Other, Seg: 0, Index: 0, Waits: []Pos{{Seg: -1}}})}, ErrInvalid},
		{"item of another segment", []Run{run(0, Item{ID: x, Class: Other, Seg: 1, Index: 0}), run(0)}, ErrInvalid},
		{"item out of index order", []Run{run(0, Item{ID: x, Class: Other, Seg: 0, Index: 1})}, ErrInvalid},
		{"unknown class", []Run{run(0, Item{ID: x, Class: 2, Seg: 0, Index: 0})}, ErrInvalid},
		{"own wait found after a wake", []Run{
			run(0, Item{ID: x, Class: Other, Seg: 0, Index: 0, Waits: []Pos{{Seg: 1, Index: 0}, {Seg: 0, Index: 0}}}),
			run(0, Item{ID: y, Class: Other, Seg: 1, Index: 0}),
		}, ErrStuck},
		{"lo above hi", []Run{{Lo: 2, Hi: 1}}, ErrInvalid},
		{"no source", []Run{{Lo: 0, Hi: 1}}, ErrInvalid},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := collect(tc.runs); !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
		})
	}
	boom := errors.New("source failed")
	if _, err := collect([]Run{{Lo: 0, Hi: 1, Next: func() (Item, error) { return Item{}, boom }}}); !errors.Is(err, boom) {
		t.Fatalf("source error: %v", err)
	}
	calls := 0
	second := Run{Lo: 0, Hi: 2, Next: func() (Item, error) {
		if calls++; calls == 2 {
			return Item{}, boom
		}
		return Item{ID: x, Class: Other}, nil
	}}
	if _, err := collect([]Run{second}); !errors.Is(err, boom) {
		t.Fatalf("source error after an emit: %v", err)
	}
	if err := Merge([]Run{run(0, Item{ID: x, Class: Other})}, [32]byte{}, func(Item) error { return boom }); !errors.Is(err, boom) {
		t.Fatalf("emit error: %v", err)
	}
	if out, err := collect(nil); err != nil || len(out) != 0 {
		t.Fatalf("empty batch: %v %v", out, err)
	}
}

// TestMergeLowerBound checks that entries below a run's Lo count as ordered.
func TestMergeLowerBound(t *testing.T) {
	x, y := idOf("x"), idOf("y")
	out, err := collect([]Run{
		run(3, Item{ID: x, Class: Other, Seg: 0, Index: 3, Waits: []Pos{{Seg: 1, Index: 6}}}),
		run(7, Item{ID: y, Class: Other, Seg: 1, Index: 7, Waits: []Pos{{Seg: 0, Index: 2}}}),
	})
	if err != nil || len(out) != 2 {
		t.Fatalf("%v %v", out, err)
	}
}

// TestMergeDuplicateIDs checks that the order stays a strict total order when a hostile source repeats one entry ID.
func TestMergeDuplicateIDs(t *testing.T) {
	var runs []Run
	for s := range 10 {
		runs = append(runs, run(0, Item{ID: idOf("same"), Class: Other, Seg: s}))
	}
	var segs []int
	if err := Merge(runs, [32]byte{}, func(it Item) error { segs = append(segs, it.Seg); return nil }); err != nil {
		t.Fatal(err)
	}
	if !slices.IsSorted(segs) || len(segs) != 10 {
		t.Fatalf("segments emitted as %v", segs)
	}
}

func TestMembershipFirst(t *testing.T) {
	seed := [32]byte{1}
	var runs []Run
	for i := range 64 {
		id := idOf(string(rune('A' + i)))
		cl := Other
		if i%3 == 0 {
			cl = Membership
		}
		runs = append(runs, run(0, Item{ID: id, Class: cl, Seg: i}))
	}
	var got []Item
	if err := Merge(runs, seed, func(it Item) error { got = append(got, it); return nil }); err != nil {
		t.Fatal(err)
	}
	for i := 1; i < len(got); i++ {
		a, b := got[i-1], got[i]
		ka, kb := keyOf(seed, a.ID), keyOf(seed, b.ID)
		if a.Class > b.Class || a.Class == b.Class && bytes.Compare(ka[:], kb[:]) >= 0 {
			t.Fatalf("position %d: not ascending by class then key", i)
		}
	}
	if got[0].Class != Membership || got[len(got)-1].Class != Other {
		t.Fatal("membership entries do not lead")
	}
}

// TestSeedIsPivot checks that a settled batch is ordered under its pivot's ID and that a different seed gives another order.
func TestSeedIsPivot(t *testing.T) {
	nodes := randomDAG(5, 6, 120, 0)
	d := newDAG(t, 5)
	addAll(t, d, nodes)
	r := rand.New(rand.NewPCG(1, 2))
	piv := pivotChain(r, d, nodes, 2)
	if len(piv) != 2 {
		t.Fatal("no pivot chain")
	}
	var got []ID
	if err := d.Batch(piv[0], piv[1], func(id ID) error { got = append(got, id); return nil }); err != nil {
		t.Fatal(err)
	}
	from, _ := d.Clock(piv[0])
	to, _ := d.Clock(piv[1])
	nv := newNaive(nodes)
	B := minusSet(nv.past(piv[1]), nv.past(piv[0]))
	nv.checkKahn(t, B, piv[1], got)
	var other []ID
	if err := d.Order(from, to, piv[0], func(id ID) error { other = append(other, id); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(got) > 8 && slices.Equal(got, other) {
		t.Fatal("the order does not depend on the seed")
	}
}

func TestOrderRangeErrors(t *testing.T) {
	nodes := randomDAG(9, 3, 30, 0)
	d := newDAG(t, 2)
	addAll(t, d, nodes)
	c, _ := d.Clock(nodes[len(nodes)-1].ID)
	noop := func(ID) error { return nil }
	if err := d.Order(c, Clock{}, [32]byte{}, noop); !errors.Is(err, ErrInvalid) {
		t.Fatalf("from outside to: %v", err)
	}
	if err := d.Order(nil, append(slices.Clone(c), 0, 1), [32]byte{}, noop); !errors.Is(err, ErrInvalid) {
		t.Fatalf("unknown segment: %v", err)
	}
	big := slices.Clone(c)
	big[0] += 5
	if err := d.Order(nil, big, [32]byte{}, noop); !errors.Is(err, ErrNotHeld) {
		t.Fatalf("beyond held: %v", err)
	}
	if err := d.Batch(ID{}, idOf("unknown"), noop); !errors.Is(err, ErrNotHeld) {
		t.Fatalf("unknown pivot: %v", err)
	}
	if err := d.Batch(idOf("unknown"), nodes[0].ID, noop); !errors.Is(err, ErrNotHeld) {
		t.Fatalf("unknown previous pivot: %v", err)
	}
	if _, err := d.Tail(idOf("unknown"), nil, [32]byte{}); !errors.Is(err, ErrNotHeld) {
		t.Fatalf("unknown settled pivot: %v", err)
	}
	if _, err := d.Tail(ID{}, []ID{idOf("unknown")}, [32]byte{}); !errors.Is(err, ErrNotHeld) {
		t.Fatalf("unknown predicted pivot: %v", err)
	}
	boom := errors.New("stop")
	if err := d.Order(nil, nil, [32]byte{}, func(ID) error { return boom }); !errors.Is(err, boom) {
		t.Fatalf("emit error: %v", err)
	}
	// a predicted pivot outside the past of the next one
	var a, b ID
	for _, x := range nodes {
		for _, y := range nodes {
			cx, _ := d.Clock(x.ID)
			cy, _ := d.Clock(y.ID)
			if !d.Contains(cy, x.ID) && !d.Contains(cx, y.ID) {
				a, b = x.ID, y.ID
			}
		}
	}
	if (a == ID{}) {
		t.Fatal("no concurrent pair")
	}
	if _, err := d.Tail(ID{}, []ID{a, b}, [32]byte{}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("pivots not nested: %v", err)
	}
}
