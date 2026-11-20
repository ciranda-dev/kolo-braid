package kolobraid

import (
	"encoding/binary"
	"errors"
	"fmt"
	"testing"
	"time"

	kololog "github.com/ciranda-dev/kolo-log"
	kolostore "github.com/ciranda-dev/kolo-store"

	"github.com/ciranda-dev/kolo-braid/internal/dag"
)

func forkTargets(t testing.TB, b *Braid, strand kololog.LogID) int {
	t.Helper()
	v, ok := b.syncState()
	if !ok {
		t.Fatal("no sync state")
	}
	n := 0
	for _, l := range v.targets {
		if kololog.LogID(l.Strand) == strand {
			n++
		}
	}
	return n
}

// segmentsOf counts the chain segments of a strand: its histories.
func segmentsOf(b *Braid, strand kololog.LogID) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := 0
	for _, s := range b.d.Segments() {
		if kololog.LogID(s.Strand) == strand {
			n++
		}
	}
	return n
}

func storedKeys(t testing.TB, p *peer, prefix string) int {
	t.Helper()
	n := 0
	if err := scanPrefix(PrefixBackend(p.db, nil), []byte(prefix), func(k, v []byte) error { n++; return nil }); err != nil {
		t.Fatal(err)
	}
	return n
}

// TestForkFloodIsBounded: a writer W signs many histories of one index and a member C links each in an entry of its own, so an
// honest peer is asked to place every one. The peer keeps MaxForks of them, refuses the rest, stores and fetches no more, and
// keeps the same segments across a restart.
func TestForkFloodIsBounded(t *testing.T) {
	const maxForks, flood = 2, defaultMaxForks
	nt, ps := group(t, 3, GroupConfig{}, appApply, false)
	a, c, w := ps[0], ps[1], ps[2]
	a.opts.Limits.MaxForks = maxForks
	a.reopen()
	nt.confirm(a)
	mustAppend(t, w.b, "base=1")
	nt.settle(nil, 20*time.Second, func() bool { return quiet(ps...) && settledGet(t, a.b, "base") == "1" })
	at, strand := w.b.own.Len(), w.b.meta.OwnStrand
	backup := copyBackend(t, w.db)
	// the history that A holds
	mustAppend(t, w.b, "main=1")
	pull(t, w, a, 0)
	w.close()
	var alts [][]heldEntry
	for j := range flood {
		clone := &peer{t: t, name: fmt.Sprintf("w%d", j), id: w.id, db: copyBackend(t, backup)}
		clone.opts = testOptions(clone.db, appApply)
		clone.opts.AppendUnconfirmed = true
		clone.reopen()
		mustAppend(t, clone.b, fmt.Sprintf("alt=%d", j))
		alts = append(alts, heldFrom(t, clone, at))
		clone.close()
	}
	// C holds W only up to the fork point, so every alternative entry places there and the next entry of C links it
	for j := range flood {
		if err := c.b.deliver(alts[j][0]); err != nil {
			t.Fatalf("c: deliver %d: %v", j, err)
		}
		mustAppend(t, c.b, fmt.Sprintf("link=%d", j))
	}
	pull(t, c, a, 0)
	if forkTargets(t, a.b, strand) == 0 {
		t.Fatal("A does not ask for the entries C links")
	}
	before := segmentsOf(a.b, strand)
	refused := 0
	for j := range flood {
		err := a.b.deliver(alts[j][0])
		switch {
		case errors.Is(err, errForkLimit):
			refused++
		case err != nil:
			t.Fatalf("deliver %d: %v", j, err)
		}
	}
	if refused != flood-maxForks {
		t.Fatalf("%d refused, want %d", refused, flood-maxForks)
	}
	if got := segmentsOf(a.b, strand); got != before+maxForks {
		t.Fatalf("%d segments after the flood, want %d", got, before+maxForks)
	}
	if n := storedKeys(t, a, keyHistory); n != maxForks {
		t.Fatalf("%d second-history entries stored, want %d", n, maxForks)
	}
	if n := forkTargets(t, a.b, strand); n != 0 {
		t.Fatalf("A still asks for %d entries of a strand that holds its forks", n)
	}
	if !a.b.holdsEvidence(strand) {
		t.Fatal("no evidence against the writer")
	}
	a.reopen()
	nt.confirm(a)
	if got := segmentsOf(a.b, strand); got != before+maxForks {
		t.Fatalf("%d segments after a restart, want %d", got, before+maxForks)
	}
	mustAppend(t, a.b, "alive=1")
	nt.settle(nil, 20*time.Second, func() bool { return settledGet(t, a.b, "alive") == "1" })
}

func TestMaxForksRange(t *testing.T) {
	if got := (Limits{}).withDefaults(defaultMaxBody).MaxForks; got != defaultMaxForks {
		t.Fatalf("default %d", got)
	}
	for _, n := range []int{-1, 65} {
		if err := (Limits{MaxForks: n}).withDefaults(defaultMaxBody).validate(); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("MaxForks %d: %v", n, err)
		}
	}
	for _, n := range []int{1, 64} {
		if err := (Limits{MaxForks: n}).withDefaults(defaultMaxBody).validate(); err != nil {
			t.Fatalf("MaxForks %d: %v", n, err)
		}
	}
}

// TestLookupIgnoresAStaleRecord: the index record of an entry placed after the last stored segment table names a segment the
// restored table lacks; the entry was not released, so lookup does not find it and Open places it again.
func TestLookupIgnoresAStaleRecord(t *testing.T) {
	db := kolostore.NewMemory()
	var id EntryID
	id[0] = 7
	strand := kololog.LogID{1}
	rec := dag.Record{Seg: 5, Index: 1, Full: true, Comps: []dag.Comp{{Seg: 0, Value: 1}}}
	var seg [4]byte
	binary.BigEndian.PutUint32(seg[:], 5)
	if err := db.Put(entryKey(strand, 1, id), seg[:]); err != nil {
		t.Fatal(err)
	}
	if err := db.Put(segKey(5, 1), encodeSegEntry(id, dag.Other, rec.Encode())); err != nil {
		t.Fatal(err)
	}
	l := &lookup{db: db, segs: func() int { return 2 }, cache: map[dag.Link]lookupHit{}}
	_, _, ok, err := l.find(dag.Link{Strand: dag.StrandID(strand), Index: 1, ID: dag.ID(id)})
	if ok || err != nil {
		t.Fatalf("stale record: found %v, error %v", ok, err)
	}
	l.segs = func() int { return 6 }
	if _, _, ok, err := l.find(dag.Link{Strand: dag.StrandID(strand), Index: 1, ID: dag.ID(id)}); !ok || err != nil {
		t.Fatalf("record inside the table: found %v, error %v", ok, err)
	}
}
