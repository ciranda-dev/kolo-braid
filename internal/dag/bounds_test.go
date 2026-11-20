package dag

import (
	"encoding/binary"
	"math/rand/v2"
	"runtime"
	"testing"
	"time"
)

func liveHeap() uint64 {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

// ladder is a synthetic batch of n entries over segs segments, generated on the fly: entry i of segment s waits on entry
// i-1 of the next segment and on entry i/2 of the segment three further on.
func ladder(segs, n int) []Run {
	per := uint64(n / segs)
	runs := make([]Run, segs)
	for s := range segs {
		i := uint64(0)
		runs[s] = Run{Lo: 0, Hi: per, Next: func() (Item, error) {
			var id ID
			binary.BigEndian.PutUint64(id[:], uint64(s))
			binary.BigEndian.PutUint64(id[8:], i)
			it := Item{ID: id, Class: Class(i % 2), Seg: s, Index: i}
			if i >= 1 {
				it.Waits = []Pos{{Seg: (s + 1) % segs, Index: i - 1}, {Seg: (s + 3) % segs, Index: i / 2}}
			}
			i++
			return it, nil
		}}
	}
	return runs
}

// TestMergeMillionBoundedMemory orders a batch of 10^6 entries and checks that the live heap stays within a bound that
// depends on the segment count only.
func TestMergeMillionBoundedMemory(t *testing.T) {
	n := 1_000_000
	if raceEnabled {
		n = 200_000
	}
	for _, segs := range []int{8, 64} {
		runs := ladder(segs, n)
		done := make([]uint64, segs)
		base := liveHeap()
		var peak uint64
		emitted := 0
		start := time.Now()
		err := Merge(runs, [32]byte{1}, func(it Item) error {
			for _, w := range it.Waits {
				if done[w.Seg] <= w.Index {
					t.Fatalf("emitted before its wait at segment %d index %d", w.Seg, w.Index)
				}
			}
			done[it.Seg]++
			if emitted++; emitted%(1<<16) == 0 {
				peak = max(peak, liveHeap())
			}
			return nil
		})
		elapsed := time.Since(start)
		if err != nil {
			t.Fatal(err)
		}
		if emitted != n/segs*segs {
			t.Fatalf("emitted %d", emitted)
		}
		grow := int64(peak) - int64(base)
		t.Logf("%d segments: %d entries in %v, live heap grew %d bytes", segs, emitted, elapsed, grow)
		if limit := int64(64<<10 + segs*1024); grow > limit {
			t.Fatalf("%d segments: live heap grew %d bytes, bound %d", segs, grow, limit)
		}
	}
}

// honest builds entries the way honest writers do: each links the newest entry of a few other strands.
func honest(seed uint64, writers, n int) []Node {
	r := rand.New(rand.NewPCG(seed, 3))
	heads := make([]*Node, writers)
	strands := make([]StrandID, writers)
	for w := range strands {
		strands[w][0] = byte(w + 1)
	}
	out := make([]Node, 0, n)
	for i := range n {
		w := r.IntN(writers)
		nd := Node{Strand: strands[w], Class: Other}
		binary.BigEndian.PutUint64(nd.ID[:], uint64(i)+1)
		nd.ID[31] = 0x77
		if h := heads[w]; h != nil {
			nd.Index, nd.Prev = h.Index+1, h.ID
		}
		for s := range writers {
			if s != w && heads[s] != nil && r.IntN(3) == 0 {
				nd.Links = append(nd.Links, linkTo(*heads[s]))
			}
		}
		out = append(out, nd)
		heads[w] = &out[len(out)-1]
	}
	return out
}

// maxPlacedBytes is the stated in-memory cost of one placed entry at 8 writers, its index record and clock deltas included.
const maxPlacedBytes = 400

func TestHundredThousandEntries(t *testing.T) {
	const n = 100_000
	nodes := honest(1, 8, n)
	base := liveHeap()
	d := newDAG(t, 7)
	start := time.Now()
	for _, nd := range nodes {
		if _, err := d.Add(nd); err != nil {
			t.Fatal(err)
		}
	}
	added := time.Since(start)
	used := int64(liveHeap()) - int64(base)
	runtime.KeepAlive(nodes)
	start = time.Now()
	tail, err := d.Tail(ID{}, nil, [32]byte{2})
	ordered := time.Since(start)
	if err != nil || len(tail) != n {
		t.Fatalf("%d ordered: %v", len(tail), err)
	}
	t.Logf("%d entries: add %v, order %v, %d bytes per entry in memory", n, added, ordered, used/n)
	if used/n > maxPlacedBytes {
		t.Fatalf("%d bytes per placed entry, bound %d", used/n, maxPlacedBytes)
	}
	if !raceEnabled && (added > 10*time.Second || ordered > 10*time.Second) {
		t.Fatalf("too slow: add %v, order %v", added, ordered)
	}
	runtime.KeepAlive(d)
}

// TestPendingCost checks that pendingNodeCost and pendingLinkCost cover what a pending node really takes on the heap.
func TestPendingCost(t *testing.T) {
	const n = 2000
	for _, links := range []int{0, 1, 8, 63} {
		nodes := make([]Node, n)
		for i := range nodes {
			nd := Node{Strand: StrandID{0xee}, Index: 1, Class: Other}
			binary.BigEndian.PutUint64(nd.ID[:], uint64(i)+1)
			binary.BigEndian.PutUint64(nd.Prev[:], uint64(i)+1<<40)
			nd.Strand[1], nd.Strand[2] = byte(i), byte(i>>8)
			for l := range links {
				var s StrandID
				s[0] = byte(l + 1)
				lk := Link{Strand: s, Index: uint64(i)}
				binary.BigEndian.PutUint64(lk.ID[:], uint64(i*64+l)+1<<50)
				nd.Links = append(nd.Links, lk)
			}
			nodes[i] = nd
		}
		d := newDAG(t, 63)
		base := liveHeap()
		for _, nd := range nodes {
			if _, err := d.Add(nd); err != nil {
				t.Fatal(err)
			}
		}
		nodes = nil
		used := (int64(liveHeap()) - int64(base)) / n
		limit := int64(pendingNodeCost + links*pendingLinkCost)
		t.Logf("pending node with %d links: %d bytes on the heap, counted %d", links, used, limit)
		if used > limit {
			t.Fatalf("pending node with %d links takes %d bytes, counted %d", links, used, limit)
		}
		runtime.KeepAlive(d)
	}
}

func BenchmarkAdd(b *testing.B) {
	nodes := honest(2, 8, 50_000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; {
		b.StopTimer()
		d := newDAG(b, 7)
		b.StartTimer()
		for _, nd := range nodes {
			if i == b.N {
				break
			}
			if _, err := d.Add(nd); err != nil {
				b.Fatal(err)
			}
			i++
		}
	}
}

func BenchmarkTail2048(b *testing.B) {
	d := newDAG(b, 7)
	addAll(b, d, honest(3, 8, 2048))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := d.Tail(ID{}, nil, [32]byte{}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMergePerEntry(b *testing.B) {
	b.ReportAllocs()
	runs := ladder(8, max(b.N, 8)*8/8)
	b.ResetTimer()
	if err := Merge(runs, [32]byte{}, func(Item) error { return nil }); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkClock(b *testing.B) {
	nodes := honest(4, 8, 20_000)
	d := newDAG(b, 7)
	addAll(b, d, nodes)
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		d.Clock(nodes[i%len(nodes)].ID)
	}
}
