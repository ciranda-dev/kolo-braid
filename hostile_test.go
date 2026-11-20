package kolobraid

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	kololog "github.com/ciranda-dev/kolo-log"
)

// rogueAppend writes raw bytes on the rogue's own strand, bypassing every check of the braid.
func rogueAppend(t testing.TB, p *peer, v []byte) EntryID {
	t.Helper()
	i, err := p.b.own.Append(v)
	if err != nil {
		t.Fatal(err)
	}
	rec, err := p.b.own.Record(i)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.b.strandGrew(p.b.meta.OwnStrand); err != nil {
		t.Fatal(err)
	}
	return entryIDOf(rec.Encode())
}

func rogueContent(t testing.TB, p *peer, c entryContent) []byte {
	t.Helper()
	b, err := encodeContent(c, p.b.meta.OwnStrand, p.b.own.Len(), p.b.bounds)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestHostileStrand: a writer fills its strand with garbage, entries of another group, a wrong prev and a link to an entry that
// never exists. Malformed entries take a position as void entries, the dangling one waits forever, and nothing else stops.
func TestHostileStrand(t *testing.T) {
	nt, ps := group(t, 3, GroupConfig{}, appApply, false)
	a, r := ps[0], ps[1]
	mustAppend(t, r.b, "hello=1")
	nt.settle(nil, 5*time.Second, func() bool { return quiet(ps...) })
	last := func() EntryID { return entryIDOf(mustRecord(t, r, r.b.own.Len()-1)) }
	rogueAppend(t, r, []byte("not an entry"))
	rogueAppend(t, r, rogueContent(t, r, entryContent{Group: GroupID{7}, Kind: KindData, Prev: last(), Body: []byte("x=other group")}))
	rogueAppend(t, r, rogueContent(t, r, entryContent{Group: a.b.meta.Group, Kind: KindData, Prev: EntryID{9}, Body: []byte("x=bad prev")}))
	nt.round(nil, 0)
	mustAppend(t, a.b, "after=1")
	nt.settle(nil, 10*time.Second, func() bool { return quiet(ps...) && settledGet(t, a.b, "after") == "1" })
	checkSame(t, ps)
	void := 0
	for o, err := range a.b.Entries(0) {
		if err != nil {
			t.Fatal(err)
		}
		if o.Strand == r.b.meta.OwnStrand && o.Void {
			void++
		}
	}
	if void != 3 || settledGet(t, a.b, "x") != "<none>" {
		t.Fatalf("%d void entries, view %s", void, dump(t, a.b))
	}
	// a link to an entry that never exists: that entry and the strand after it wait, the rest goes on
	ar := a.b.strandFor(a.b.meta.OwnStrand)
	_ = ar
	prev := entryIDOf(mustRecord(t, r, r.b.own.Len()-1))
	rogueAppend(t, r, rogueContent(t, r, entryContent{Group: a.b.meta.Group, Kind: KindData, Prev: prev,
		Links: []Link{{Strand: a.b.meta.OwnStrand, Index: 999, ID: EntryID{1}}}, Body: []byte("y=dangling")}))
	nt.round(nil, 0)
	mustAppend(t, a.b, "still=1")
	nt.settle(nil, 10*time.Second, func() bool { return quiet(ps...) && settledGet(t, ps[2].b, "still") == "1" })
	a.b.mu.Lock()
	n, _ := a.b.d.Pending()
	a.b.mu.Unlock()
	if n != 1 || settledGet(t, a.b, "y") != "<none>" {
		t.Fatalf("dangling entry: %d pending", n)
	}
}

func mustRecord(t testing.TB, p *peer, i uint64) []byte {
	t.Helper()
	r, err := p.b.own.Record(i)
	if err != nil {
		t.Fatal(err)
	}
	return r.Encode()
}

// TestPendingBound: a writer whose entries wait on a missing entry cannot hold more than MaxPendingBytes of another peer's memory.
func TestPendingBound(t *testing.T) {
	nt, ps := group(t, 2, GroupConfig{}, appApply, false)
	a, r := ps[0], ps[1]
	mustAppend(t, r.b, "hello=1")
	nt.settle(nil, 5*time.Second, func() bool { return quiet(ps...) })
	a.opts.Limits.MaxPendingBytes = 1 << 20
	a.reopen()
	nt.confirm(a)
	prev := entryIDOf(mustRecord(t, r, r.b.own.Len()-1))
	prev = rogueAppend(t, r, rogueContent(t, r, entryContent{Group: a.b.meta.Group, Kind: KindData, Prev: prev,
		Links: []Link{{Strand: a.b.meta.OwnStrand, Index: 999, ID: EntryID{1}}}}))
	body := bytes.Repeat([]byte{'p'}, 900)
	for i := 0; i < 1500; i++ {
		prev = rogueAppend(t, r, rogueContent(t, r, entryContent{Group: a.b.meta.Group, Kind: KindData, Prev: prev, Body: body}))
	}
	nt.round(nil, 0)
	a.b.mu.Lock()
	n, bytes := a.b.d.Pending()
	a.b.mu.Unlock()
	if bytes > a.b.lim.MaxPendingBytes || n >= 1500 {
		t.Fatalf("%d pending entries, %d bytes over the %d bound", n, bytes, a.b.lim.MaxPendingBytes)
	}
	a.b.mu.Lock()
	read, held := a.b.strands[r.b.meta.OwnStrand].next, len(a.b.nodes)
	a.b.mu.Unlock()
	if read >= r.b.own.Len() || held > n+64 {
		t.Fatalf("the strand was read on past the bound: %d of %d entries read, %d held", read, r.b.own.Len(), held)
	}
	mustAppend(t, a.b, "alive=1")
	waitUntil(t, 5*time.Second, func() bool { return settledGet(t, a.b, "alive") == "1" })
}

// TestEntryOfAnotherStrandKeyIsRefused: deliver checks the record against the strand's key.
func TestEntryOfAnotherStrandKeyIsRefused(t *testing.T) {
	_, ps := group(t, 2, GroupConfig{}, appApply, false)
	mustAppend(t, ps[1].b, "one")
	mustAppend(t, ps[1].b, "two")
	h := heldFrom(t, ps[1], 1)
	h[0].Strand = ps[0].b.meta.OwnStrand
	if err := ps[0].b.deliver(h[0]); err == nil {
		t.Fatal("an entry signed by another key was taken")
	}
	h[0].Strand = kololog.LogID{1}
	if err := ps[0].b.deliver(h[0]); err == nil {
		t.Fatal("an entry of a strand not followed was taken")
	}
	_ = fmt.Sprint
}

// TestPendingFullReadsAgain: a strand that fills its share of the pending bound with entries that wait forever is stopped and
// cannot keep another strand's entry from parking, and the entry of the other strand that arrives before what it links is read
// again once that entry is placed, and settles.
func TestPendingFullReadsAgain(t *testing.T) {
	nt, ps := group(t, 4, GroupConfig{MaxMembers: 4}, appApply, false)
	a, r, x, y := ps[0], ps[1], ps[2], ps[3]
	mustAppend(t, r.b, "hello=1")
	nt.settle(nil, 10*time.Second, func() bool { return quiet(ps...) })
	a.opts.Limits.MaxPendingBytes = 1 << 20
	a.reopen()
	nt.confirm(a)
	prev := entryIDOf(mustRecord(t, r, r.b.own.Len()-1))
	prev = rogueAppend(t, r, rogueContent(t, r, entryContent{Group: a.b.meta.Group, Kind: KindData, Prev: prev,
		Links: []Link{{Strand: a.b.meta.OwnStrand, Index: 999, ID: EntryID{1}}}}))
	body := bytes.Repeat([]byte{'p'}, 900)
	for range 1500 {
		prev = rogueAppend(t, r, rogueContent(t, r, entryContent{Group: a.b.meta.Group, Kind: KindData, Prev: prev, Body: body}))
	}
	pull(t, r, a, 0)
	a.b.mu.Lock()
	rFull := a.b.strands[r.b.meta.OwnStrand].full
	_, pendingBytes := a.b.d.Pending()
	a.b.mu.Unlock()
	if !rFull || pendingBytes > a.b.lim.MaxPendingBytes/4+2048 {
		t.Fatalf("the flooding strand is not held to its share: full %v, %d pending bytes", rFull, pendingBytes)
	}
	mustAppend(t, y.b, "y=1")
	pull(t, y, x, 0)
	mustAppend(t, x.b, "x=1")
	// x's strand alone reaches a, before y's
	one := func(p *peer) {
		id := p.b.meta.OwnStrand
		transfer(t, p.b.strandFor(id), a.b.replicaOf(id), ^uint64(0))
		if err := a.b.strandGrew(id); err != nil {
			t.Fatal(err)
		}
	}
	one(x)
	a.b.mu.Lock()
	xFull := a.b.strands[x.b.meta.OwnStrand].full
	a.b.mu.Unlock()
	if xFull {
		t.Fatal("x's entry was kept from parking by another strand's pending entries")
	}
	one(y)
	waitUntil(t, 10*time.Second, func() bool { return settledGet(t, a.b, "x") == "1" })
}
