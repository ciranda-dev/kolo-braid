package kolobraid

import (
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	kololog "github.com/ciranda-dev/kolo-log"
	kolostore "github.com/ciranda-dev/kolo-store"
)

func TestForkSurvivesRestart(t *testing.T) {
	nt, ps := group(t, 5, GroupConfig{}, appApply, false)
	seats := ps[:4]
	setSettlers(t, nt, ps, seats)
	w := ps[4]
	mustAppend(t, w.b, "base=1")
	nt.settle(nil, 20*time.Second, func() bool { return quiet(ps...) && settledGet(t, ps[0].b, "base") == "1" })
	h1, h2 := fork(t, w, seats[:2], seats[2:])
	rest := newNet(t, seats...)
	deliverAll(t, seats, h1, h2)
	rest.round(nil, 0)
	for _, p := range seats {
		p.reopen()
	}
	for _, p := range seats {
		rest.confirm(p)
	}
	mustAppend(t, seats[1].b, "after=1")
	rest.settle(nil, 30*time.Second, func() bool {
		return quiet(seats...) && settledGet(t, seats[2].b, "after") == "1" && !hasMember(seats[3].b, keyOf(w), true)
	})
	checkSame(t, seats)
	for _, p := range seats {
		p.b.mu.Lock()
		n := len(p.b.evidence)
		p.b.mu.Unlock()
		if n != 1 {
			t.Fatalf("%s holds %d evidence records after restart", p.name, n)
		}
	}
}

func TestEvidenceFromTheSyncLayerFencesTheWriter(t *testing.T) {
	nt, ps := group(t, 3, GroupConfig{}, appApply, false)
	w := ps[2]
	mustAppend(t, w.b, "base=1")
	nt.settle(nil, 10*time.Second, func() bool { return quiet(ps...) })
	h1, h2 := fork(t, w, ps[:1], nil)
	ev := evidenceBody{Key: keyOf(w), Strand: h1[0].Strand, Type: evidenceRecords, A: h1[0].Record.Encode(), B: h2[0].Record.Encode()}
	// from the moment it holds evidence, before any Evidence entry is ordered, a peer stops linking that strand
	p0 := ps[0].b
	p0.mu.Lock()
	before := slices.ContainsFunc(p0.links(), func(l Link) bool { return l.Strand == ev.Strand })
	p0.holdEvidence(ev, nil, false)
	after := slices.ContainsFunc(p0.links(), func(l Link) bool { return l.Strand == ev.Strand })
	p0.mu.Unlock()
	if !before || after {
		t.Fatalf("links to the forked strand: %v before the evidence, %v after", before, after)
	}
	if err := ps[1].b.addEvidence(ev); err != nil {
		t.Fatal(err)
	}
	waitEvent(t, ps[1].b, EventEquivocation, 5*time.Second)
	rest := newNet(t, ps[:2]...)
	rest.settle(nil, 20*time.Second, func() bool {
		return quiet(ps[:2]...) && !hasMember(ps[0].b, keyOf(w), true) && !hasMember(ps[1].b, keyOf(w), true)
	})
	checkSame(t, ps[:2])
	bad := ev
	bad.B = ev.A
	if err := ps[1].b.addEvidence(bad); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("records that do not conflict: %v", err)
	}
}

func TestClaimsSurviveRestart(t *testing.T) {
	nt, ps := group(t, 3, GroupConfig{CheckpointEvery: 64}, appApply, true)
	for i := 0; ps[0].b.Status().Settled < 70; i++ {
		mustAppend(t, ps[i%3].b, fmt.Sprintf("k=%d", i))
		nt.round(nil, 0)
	}
	for _, p := range ps {
		p.reopen()
	}
	for _, p := range ps {
		nt.confirm(p)
	}
	nt.settle(nil, 20*time.Second, func() bool {
		mustAppend(t, ps[0].b, "more")
		ok := true
		for _, p := range ps {
			ok = ok && hasEvent(p.b, EventCertified)()
		}
		return ok
	})
}

// TestViewBehindIsReplayedFromTheIndex closes a braid while settled batches still wait for a slow apply: Open replays them from
// the entry index, and the result equals a full rebuild.
func TestViewBehindIsReplayedFromTheIndex(t *testing.T) {
	gate := make(chan struct{})
	slow := func(tx *Tx, e *Entry) error {
		if e.Kind == KindData && string(e.Body) == "hold" {
			<-gate
		}
		return appApply(tx, e)
	}
	p := newFounder(t, "a", GroupConfig{}, slow)
	mustAppend(t, p.b, "hold")
	behind := false
	for i := 0; i < 400 && !behind; i++ {
		mustAppend(t, p.b, fmt.Sprintf("k%d=%d", i%9, i))
		time.Sleep(2 * time.Millisecond)
		p.b.mu.Lock()
		behind = p.b.pivots > p.b.view.Progress().Batches+2
		p.b.mu.Unlock()
	}
	close(gate)
	if !behind {
		t.Fatal("the view never fell behind settlement")
	}
	p.reopen()
	waitUntil(t, 20*time.Second, func() bool { return quiet(p) && p.b.view.Tail() <= 1 })
	got, prog := dump(t, p.b), p.b.view.Progress()
	p.close()
	p.opts.Rebuild = true
	p.reopen()
	p.opts.Rebuild = false
	waitUntil(t, 20*time.Second, func() bool { return quiet(p) && p.b.view.Progress().Next >= prog.Next })
	if dump(t, p.b) != got {
		t.Fatal("replay from the index differs from a rebuild")
	}
}

// TestErasedEntryIsRepaired: a writer deletes an entry of its own log while the braid is closed. Reading it gives ErrErased, a
// peer that pulls only the marker waits, and a repair copy from a peer that kept the entry lets it go on.
func TestErasedEntryIsRepaired(t *testing.T) {
	nt, ps := group(t, 2, GroupConfig{}, appApply, false)
	a, w := ps[0], ps[1]
	mustAppend(t, w.b, "kept=1")
	mustAppend(t, w.b, "next=2")
	nt.settle(nil, 10*time.Second, func() bool { return quiet(ps...) && settledGet(t, a.b, "next") == "2" })
	strand := w.b.meta.OwnStrand
	w.close()
	st := kolostore.New(PrefixBackend(w.db, append([]byte{storePrefix}, keyOf(w)...)))
	l, err := st.Open(w.id, strand)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Delete(0); err != nil {
		t.Fatal(err)
	}
	l.Close()
	st.Close()
	w.reopen()
	nt.confirm(w)
	var erased error
	for _, err := range w.b.Entries(0) {
		if err != nil {
			erased = err
			break
		}
	}
	if !errors.Is(erased, ErrErased) {
		t.Fatalf("reading an erased own entry: %v", erased)
	}
	j, nw := newJoiner(t, "j", a, appApply)
	if _, err := a.b.AddMember(ctx10(t), nw); err != nil {
		t.Fatal(err)
	}
	pull(t, a, a, 0)
	j.start()
	// j pulls w's strand from w only: the first entry comes as a marker
	src := w.b.strandFor(strand)
	if dst := j.b.replicaOf(strand); dst != nil && transfer(t, src, dst, ^uint64(0)) {
		if err := j.b.strandGrew(strand); err != nil {
			t.Fatal(err)
		}
	}
	js := j.b.strandFor(strand)
	j.b.mu.Lock()
	stopped := js.stop
	j.b.mu.Unlock()
	if !errors.Is(stopped, kololog.ErrDeleted) {
		t.Fatalf("the marker did not stop the strand: %v", stopped)
	}
	rep := a.b.strandFor(strand).rep
	rec, _ := rep.Record(0)
	v, err := rep.Get(0)
	if err != nil {
		t.Fatal(err)
	}
	if err := j.b.deliver(heldEntry{Strand: strand, Record: rec, Value: v}); err != nil {
		t.Fatal(err)
	}
	n := newNet(t, a, j)
	n.settle(nil, 10*time.Second, func() bool { return quiet(a, j) && settledGet(t, j.b, "kept") == "1" })
}
