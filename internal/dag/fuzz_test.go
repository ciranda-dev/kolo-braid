package dag

import (
	"bytes"
	"encoding/binary"
	"errors"
	"slices"
	"testing"
)

// fuzzNodes turns bytes into entries with unique IDs: appends, second histories, links to placed entries, and forged
// links to any (strand, index, ID), cycles and dangling targets included.
func fuzzNodes(data []byte) []Node {
	if len(data) == 0 {
		return nil
	}
	writers := 2 + int(data[0]%5)
	data = data[1:]
	strands := make([]StrandID, writers)
	for w := range strands {
		strands[w][0] = byte(0x10 + w)
	}
	idAt := func(k int) ID {
		var id ID
		binary.BigEndian.PutUint32(id[:], uint32(k)+1)
		id[31] = 0x99
		return id
	}
	var nodes []Node
	byStrand := make([][]int, writers)
	next := func() byte {
		if len(data) == 0 {
			return 0
		}
		b := data[0]
		data = data[1:]
		return b
	}
	for len(data) > 0 && len(nodes) < 200 {
		op := next()
		w := int(op>>4) % writers
		n := Node{ID: idAt(len(nodes)), Strand: strands[w], Class: Class(op & 1)}
		if own := byStrand[w]; len(own) > 0 {
			p := nodes[own[len(own)-1]]
			if op&2 != 0 {
				p = nodes[own[int(next())%len(own)]]
			}
			n.Index, n.Prev = p.Index+1, p.ID
			if op&4 != 0 {
				n.Index, n.Prev = 0, ID{}
			}
		}
		mask := next()
		for s := range writers {
			if s == w || mask&(1<<s) == 0 {
				continue
			}
			if op&8 != 0 {
				n.Links = append(n.Links, Link{Strand: strands[s], Index: uint64(next() % 8), ID: idAt(int(next()))})
			} else if len(byStrand[s]) > 0 {
				n.Links = append(n.Links, linkTo(nodes[byStrand[s][int(next())%len(byStrand[s])]]))
			}
		}
		slices.SortFunc(n.Links, func(a, b Link) int { return bytes.Compare(a.Strand[:], b.Strand[:]) })
		byStrand[w] = append(byStrand[w], len(nodes))
		nodes = append(nodes, n)
	}
	return nodes
}

func FuzzOrder(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{3, 0x01, 0xff, 0x11, 0xff, 0, 0x21, 0xff, 1, 0x03, 0xff, 0, 0})
	f.Add([]byte{0, 0x08, 0xff, 2, 5, 0x18, 0xff, 0, 0, 0x02, 0, 0x06, 0xff})
	f.Add(bytes.Repeat([]byte{4, 0x31, 0x7f, 1, 2, 3}, 30))
	f.Add(bytes.Repeat([]byte{2, 0x0e, 0x55, 0x9c, 0xff, 7, 1, 9, 3}, 20))
	f.Fuzz(func(t *testing.T, data []byte) {
		nodes := fuzzNodes(data)
		build := func(in []Node) *DAG {
			d, err := New(Config{MaxLinks: 6, MaxPendingBytes: 1 << 30})
			if err != nil {
				t.Fatal(err)
			}
			for _, n := range in {
				if _, err := d.Add(n); err != nil {
					t.Fatalf("add: %v", err)
				}
			}
			return d
		}
		rev := slices.Clone(nodes)
		slices.Reverse(rev)
		d1, d2 := build(nodes), build(rev)
		if d1.Len() != d2.Len() {
			t.Fatalf("placed %d and %d", d1.Len(), d2.Len())
		}
		seed := [32]byte{0x42}
		t1, err1 := d1.Tail(ID{}, nil, seed)
		t2, err2 := d2.Tail(ID{}, nil, seed)
		if err1 != nil || err2 != nil {
			t.Fatalf("tail: %v %v", err1, err2)
		}
		if !slices.Equal(t1, t2) || len(t1) != d1.Len() {
			t.Fatal("order depends on the delivery order")
		}
		var placed []Node
		for _, n := range nodes {
			if _, _, ok := d1.Pos(n.ID); ok {
				placed = append(placed, n)
			}
		}
		nv := newNaive(placed)
		nv.checkCausal(t, t1)
		if len(t1) <= 64 {
			nv.checkKahn(t, setOf(t1...), seed, t1)
		}
	})
}

func FuzzMerge(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{2, 2, 1, 1, 0, 0, 1, 1, 1, 0, 0})
	f.Add([]byte{3, 4, 4, 4, 1, 2, 1, 0, 2, 1, 3, 9, 0, 1, 2, 0, 1, 2, 3})
	f.Add(bytes.Repeat([]byte{5, 7, 1, 0, 3, 2, 1}, 12))
	f.Fuzz(func(t *testing.T, data []byte) {
		next := func() byte {
			if len(data) == 0 {
				return 0
			}
			b := data[0]
			data = data[1:]
			return b
		}
		segs := int(next() % 7)
		runs := make([]Run, segs)
		total := 0
		for s := range segs {
			lo := uint64(next() % 4)
			var items []Item
			for i := range int(next() % 9) {
				it := Item{Seg: s, Index: lo + uint64(i), Class: Class(next() % 3)}
				it.ID[0], it.ID[1] = next(), byte(s)
				if b := next(); b&0x80 != 0 {
					it.Seg = int(b % 8)
				}
				for range int(next() % 4) {
					it.Waits = append(it.Waits, Pos{Seg: int(next()%8) - 1, Index: uint64(next() % 12)})
				}
				items = append(items, it)
			}
			runs[s] = run(lo, items...)
			total += len(items)
		}
		cursor := make([]uint64, segs)
		for s := range runs {
			cursor[s] = runs[s].Lo
		}
		emitted := 0
		err := Merge(runs, [32]byte{}, func(it Item) error {
			for _, w := range it.Waits {
				if cursor[w.Seg] <= w.Index {
					t.Fatalf("emitted before a wait")
				}
			}
			if cursor[it.Seg] != it.Index {
				t.Fatal("emitted out of segment order")
			}
			cursor[it.Seg]++
			emitted++
			return nil
		})
		if err == nil && emitted != total {
			t.Fatalf("emitted %d of %d without an error", emitted, total)
		}
		if err != nil && !errors.Is(err, ErrStuck) && !errors.Is(err, ErrInvalid) {
			t.Fatalf("unexpected error %v", err)
		}
	})
}

func FuzzDecodeRecord(f *testing.F) {
	f.Add([]byte{})
	f.Add(Record{Seg: 1, Index: 300, Comps: []Comp{{Seg: 0, Value: 9}, {Seg: 4, Value: 1}}}.Encode())
	f.Add(Record{Seg: 0, Index: 256, Full: true, Comps: []Comp{{Seg: 2, Value: 1 << 33}}}.Encode())
	f.Add([]byte{0, 0, 0, 2, 2, 1, 1, 1})
	f.Fuzz(func(t *testing.T, data []byte) {
		r, err := DecodeRecord(data)
		if err != nil {
			if !errors.Is(err, ErrMalformed) {
				t.Fatalf("error %v", err)
			}
			return
		}
		if !bytes.Equal(r.Encode(), data) {
			t.Fatal("decoded record does not encode to the same bytes")
		}
		prev := Clock{3, 1, 4, 1, 5}
		if r.Seg < 16 {
			for len(prev) <= r.Seg {
				prev = append(prev, 0)
			}
			prev[r.Seg] = r.Index
		}
		c, err := r.Clock(prev, 16)
		if err == nil && c.At(r.Seg) != r.Index+1 {
			t.Fatal("own component")
		}
	})
}
