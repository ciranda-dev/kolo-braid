package dag

import (
	"slices"
	"testing"
)

// TestRestoredSegmentsKeepTheirNumbers re-adds the entries of a table restored before anything was released, in another
// order: every entry must land in the segment number it had, with the same clock, so state saved with those numbers stays
// valid.
func TestRestoredSegmentsKeepTheirNumbers(t *testing.T) {
	a, b := StrandID{1}, StrandID{2}
	id := func(s, i byte) ID { return ID{s, i, 7} }
	nodes := []Node{
		{ID: id(2, 0), Strand: b, Index: 0, Class: Other},
		{ID: id(1, 0), Strand: a, Index: 0, Class: Other},
		{ID: id(1, 1), Strand: a, Index: 1, Prev: id(1, 0), Links: []Link{{Strand: b, Index: 0, ID: id(2, 0)}}, Class: Other},
		// a second history of strand a at index 1
		{ID: id(9, 1), Strand: a, Index: 1, Prev: id(1, 0), Class: Other},
		{ID: id(2, 1), Strand: b, Index: 1, Prev: id(2, 0), Links: []Link{{Strand: a, Index: 1, ID: id(9, 1)}}, Class: Other},
	}
	d, err := New(Config{MaxLinks: 4, MaxPendingBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range nodes {
		if _, err := d.Add(n); err != nil {
			t.Fatal(err)
		}
	}
	segs := d.Segments()
	for _, s := range segs {
		if s.First == (ID{}) {
			t.Fatalf("segment without its first entry: %+v", s)
		}
	}
	re, err := New(Config{MaxLinks: 4, MaxPendingBytes: 1 << 20, Segments: segs})
	if err != nil {
		t.Fatal(err)
	}
	for _, i := range []int{1, 3, 0, 2, 4} {
		if _, err := re.Add(nodes[i]); err != nil {
			t.Fatal(err)
		}
	}
	if len(re.Segments()) != len(segs) {
		t.Fatalf("restart has %d segments, want %d", len(re.Segments()), len(segs))
	}
	for _, n := range nodes {
		s1, i1, _ := d.Pos(n.ID)
		s2, i2, ok := re.Pos(n.ID)
		c1, _ := d.Clock(n.ID)
		c2, _ := re.Clock(n.ID)
		if !ok || s1 != s2 || i1 != i2 || !slices.Equal(c1, c2) {
			t.Fatalf("entry %x: segment %d clock %v before, segment %d clock %v after", n.ID[:2], s1, c1, s2, c2)
		}
	}
	// a restored segment whose first entry differs is not taken
	other := Node{ID: id(5, 0), Strand: a, Index: 0, Class: Other}
	fresh, err := New(Config{MaxLinks: 4, MaxPendingBytes: 1 << 20, Segments: segs})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fresh.Add(other); err != nil {
		t.Fatal(err)
	}
	if s, _, _ := fresh.Pos(other.ID); s != len(segs) {
		t.Fatalf("entry with another ID took restored segment %d", s)
	}
}
