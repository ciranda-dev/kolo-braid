package dag

import (
	"errors"
	"fmt"
	"runtime"
	"testing"
)

// floodForks adds a strand of two entries and then n alternative entries at index 1, each its own fork, with maxForks as bound.
func floodForks(t testing.TB, maxForks, n int) *DAG {
	t.Helper()
	d, err := New(Config{MaxLinks: 2, MaxPendingBytes: 1 << 30, MaxForks: maxForks})
	if err != nil {
		t.Fatal(err)
	}
	w := strandOf(1)
	e0 := node(idOf("e0"), w, 0, ID{}, Other)
	e1 := node(idOf("e1"), w, 1, e0.ID, Other)
	addAll(t, d, []Node{e0, e1})
	for i := range n {
		alt := node(idOf(fmt.Sprintf("alt%d", i)), w, 1, e0.ID, Other)
		if _, err := d.Add(alt); err != nil {
			t.Fatal(err)
		}
	}
	return d
}

func heapInUse() uint64 {
	var m runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

func TestForkLimit(t *testing.T) {
	const maxForks, n = 4, 5000
	d := floodForks(t, maxForks, n)
	if got := len(d.Ends()); got != 1+maxForks {
		t.Fatalf("%d segments after %d forks, want %d", got, n, 1+maxForks)
	}
	if !d.ForkFull(strandOf(1)) || d.ForkFull(strandOf(2)) {
		t.Fatal("ForkFull")
	}
	if got := len(d.TakeRefused()); got != n-maxForks {
		t.Fatalf("%d refused, want %d", got, n-maxForks)
	}
	if len(d.TakeRefused()) != 0 {
		t.Fatal("TakeRefused keeps what it returned")
	}
	if d.Len() != 2+maxForks {
		t.Fatalf("%d entries placed, want %d", d.Len(), 2+maxForks)
	}
	// the strand can still grow: an entry that extends a fork opens no segment
	next := node(idOf("alt0-next"), strandOf(1), 2, idOf("alt0"), Other)
	if p, err := d.Add(next); err != nil || len(p) != 1 {
		t.Fatalf("extending a fork: %v, %d placed", err, len(p))
	}
}

// TestForkLimitBoundsMemory: past the bound a flood of forks costs nothing, so memory stays flat where an unbounded DAG grows with
// the square of the flood (a clock is one slice of the segments, 8 bytes each).
func TestForkLimitBoundsMemory(t *testing.T) {
	const maxForks = 4
	small := heapInUse()
	d1 := floodForks(t, maxForks, 2000)
	mid := heapInUse()
	d2 := floodForks(t, maxForks, 4000)
	big := heapInUse()
	runtime.KeepAlive(d1)
	runtime.KeepAlive(d2)
	if grew := int64(big-mid) - int64(mid-small); grew > 1<<20 {
		t.Fatalf("doubling the flood grew the heap by %d bytes more than the first", grew)
	}
	if mid-small > 1<<20 || big-mid > 1<<20 {
		t.Fatalf("a bounded flood took %d and %d bytes", mid-small, big-mid)
	}
	for id := range d2.byID {
		c, _ := d2.Clock(id)
		if len(c) > 1+maxForks {
			t.Fatalf("clock of %d components", len(c))
		}
	}
}

// TestForkLimitQueued: an entry that waits for its link and is refused when the link arrives does not stop the entries behind
// it, and is reported once.
func TestForkLimitQueued(t *testing.T) {
	d, err := New(Config{MaxLinks: 2, MaxPendingBytes: 1 << 30, MaxForks: 1})
	if err != nil {
		t.Fatal(err)
	}
	w, c := strandOf(1), strandOf(2)
	e0 := node(idOf("e0"), w, 0, ID{}, Other)
	e1 := node(idOf("e1"), w, 1, e0.ID, Other)
	altA := node(idOf("altA"), w, 1, e0.ID, Other)
	altB := node(idOf("altB"), w, 1, e0.ID, Other)
	gate := node(idOf("gate"), c, 0, ID{}, Other)
	// altB waits for the gate through a link, then is refused on release; the entry after the gate must still be placed
	waiter := node(idOf("waiter"), c, 1, gate.ID, Other)
	altBL := altB
	altBL.Links = []Link{linkTo(gate)}
	addAll(t, d, []Node{e0, e1, altA, altBL, waiter})
	if len(d.TakeRefused()) != 0 {
		t.Fatal("refused before the gate")
	}
	placed, err := d.Add(gate)
	if err != nil {
		t.Fatal(err)
	}
	ids := map[ID]bool{}
	for _, p := range placed {
		ids[p.ID] = true
	}
	if !ids[gate.ID] || !ids[waiter.ID] || ids[altB.ID] {
		t.Fatalf("placed %d entries, want the gate and its waiter and not the refused fork", len(placed))
	}
	if r := d.TakeRefused(); len(r) != 1 || r[0] != altB.ID {
		t.Fatalf("refused %x", r)
	}
	if _, err := d.Add(altB); err != nil || len(d.TakeRefused()) != 1 {
		t.Fatalf("an entry refused once is refused again: %v", err)
	}
}

func TestForkLimitConfig(t *testing.T) {
	if _, err := New(Config{MaxLinks: 1, MaxPendingBytes: 1, MaxForks: -1}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("negative MaxForks: %v", err)
	}
}

// TestPendingPerStrand: one strand cannot take the whole pending budget, a strand with nothing pending may always park one entry,
// and the share comes back when the entries are placed.
func TestPendingPerStrand(t *testing.T) {
	const cost = pendingNodeCost
	d, err := New(Config{MaxLinks: 1, MaxPendingBytes: 100 * cost, MaxPendingPerStrand: 3 * cost})
	if err != nil {
		t.Fatal(err)
	}
	hostile, honest := strandOf(1), strandOf(2)
	gone := Link{Strand: strandOf(3), Index: 5, ID: idOf("gone")}
	parked := 0
	for i := range 50 {
		n := node(idOf(fmt.Sprintf("h%d", i)), hostile, uint64(i), ID{}, Other, gone)
		if i > 0 {
			n.Prev = idOf(fmt.Sprintf("h%d", i-1))
		}
		_, err := d.Add(n)
		if errors.Is(err, ErrPendingFull) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		parked++
	}
	if parked != 2 {
		t.Fatalf("%d entries of one strand parked, want 2 (a share of three costs, each entry costs more than one)", parked)
	}
	if n, b := d.Pending(); n != parked || b > 3*cost+3*pendingLinkCost {
		t.Fatalf("%d pending, %d bytes", n, b)
	}
	if _, err := d.Add(node(idOf("e0"), honest, 0, ID{}, Other, gone)); err != nil {
		t.Fatalf("another strand cannot park: %v", err)
	}
	big := node(idOf("big"), strandOf(4), 0, ID{}, Other, gone)
	big.Size = 10 * cost
	if _, err := d.Add(big); err != nil {
		t.Fatalf("an empty strand could not park one entry above its share: %v", err)
	}
	// the share comes back once what waits is placed
	d2, _ := New(Config{MaxLinks: 1, MaxPendingBytes: 100 * cost, MaxPendingPerStrand: 3 * cost})
	e0 := node(idOf("p0"), honest, 0, ID{}, Other)
	e1 := node(idOf("p1"), honest, 1, e0.ID, Other)
	if _, err := d2.Add(e1); err != nil {
		t.Fatal(err)
	}
	if _, err := d2.Add(e0); err != nil {
		t.Fatal(err)
	}
	if n, b := d2.Pending(); n != 0 || b != 0 || len(d2.strandBytes) != 0 {
		t.Fatalf("%d pending, %d bytes, %d strands counted after the entries were placed", n, b, len(d2.strandBytes))
	}
}
