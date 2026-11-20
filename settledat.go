package kolobraid

import (
	"bytes"
	"context"
	"errors"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"weak"

	kolostore "github.com/ciranda-dev/kolo-store"

	"github.com/ciranda-dev/kolo-braid/internal/prefixdb"
	"github.com/ciranda-dev/kolo-braid/internal/roster"
)

// settledAtState is what SettledAt keeps in a braid.
type settledAtState struct {
	// sem lets one SettledAt run at a time: they share the cursor.
	sem    chan struct{}
	seq    atomic.Uint64
	cursor *replayCursor // guarded by sem
	// open holds the snapshots not closed yet and dropping counts the deletions of their storage in flight, both guarded by
	// b.mu; calls counts the calls in flight, added to under b.mu while the braid is open.
	open     map[*scratchRec]struct{}
	dropping int
	calls    sync.WaitGroup
}

func (st *settledAtState) init() {
	st.sem = make(chan struct{}, 1)
	st.open = map[*scratchRec]struct{}{}
}

// scratchRec is the scratch view of one SettledAt snapshot; snap is weak, so a snapshot dropped without Close is collected.
type scratchRec struct {
	prefix []byte
	v      *view
	snap   weak.Pointer[Snapshot]
}

// replayCursor is a replay of the settled order kept at the end of a batch: SettledAt copies its view and replays only what
// follows it.
type replayCursor struct {
	prefix []byte
	v      *view
	r      *roster.Roster
	rv     *rosterView // the roster after position next - 1
	batch  uint64      // the next batch to replay
	next   uint64      // the first position of that batch
}

// SettledAt returns a snapshot of the settled view and roster as they were right after the entry at position was applied: what
// a member's roles, the settler seats (Snapshot.Settlers) and the records of the view were at that position. It replays the
// settled order into scratch storage under the braid's prefix, calling apply on this goroutine; one call runs at a time. A
// replay kept at the last batch end it reached makes a later position cost a copy of the view and the positions after that
// end; an earlier position replays from the braid's first one. Close the snapshot when done; Close of the braid closes it.
// A position not settled yet returns ErrNotFound, and one before the braid's first position ErrInvalidArgument.
func (b *Braid) SettledAt(ctx context.Context, position uint64) (*Snapshot, error) {
	b.mu.Lock()
	if err := b.usable(); err != nil {
		b.mu.Unlock()
		return nil, err
	}
	b.at.calls.Add(1)
	b.mu.Unlock()
	defer b.at.calls.Done()
	select {
	case b.at.sem <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-b.life.Done():
		return nil, ErrClosed
	}
	defer func() { <-b.at.sem }()
	prefix := be8(keySettledAt, b.at.seq.Add(1))
	v, err := b.replayAt(ctx, position, prefix)
	if err != nil {
		if cerr := clearPrefix(b.db, prefix); cerr != nil {
			return nil, errors.Join(err, cerr)
		}
		return nil, err
	}
	rec := &scratchRec{prefix: prefix, v: v}
	s := v.Settled()
	s.done = func() error { return b.dropScratch(rec) }
	rec.snap = weak.Make(s)
	// a Close that started meanwhile waits for this call, then closes the snapshot with the others
	b.mu.Lock()
	b.at.open[rec] = struct{}{}
	b.mu.Unlock()
	runtime.AddCleanup(s, func(a leakedScratch) { a.b.dropLeaked(a.rec) }, leakedScratch{b, rec})
	return s, nil
}

type leakedScratch struct {
	b   *Braid
	rec *scratchRec
}

// dropScratch deletes the scratch view of a closed snapshot; Close of the braid waits for it, so the backend is never closed
// under it.
func (b *Braid) dropScratch(rec *scratchRec) error {
	b.mu.Lock()
	delete(b.at.open, rec)
	b.at.dropping++
	b.mu.Unlock()
	defer func() {
		b.mu.Lock()
		b.at.dropping--
		b.cond.Broadcast()
		b.mu.Unlock()
	}()
	rec.v.Close()
	return clearPrefix(b.db, rec.prefix)
}

// dropLeaked deletes the scratch view of a snapshot that was collected without Close; after Close of the braid, which deleted
// it, it does nothing. It holds b.mu so Close never returns while it writes.
func (b *Braid) dropLeaked(rec *scratchRec) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.at.open[rec]; !ok {
		return
	}
	delete(b.at.open, rec)
	rec.v.Close()
	_ = clearPrefix(b.db, rec.prefix)
}

// closeSettledAt waits for the SettledAt calls in flight, closes the snapshots they returned, waits for every deletion of their
// storage, also one a Close of the caller started, and deletes the cursor; the braid is closed, so no call starts.
func (b *Braid) closeSettledAt() {
	if b.at.open == nil {
		return
	}
	b.at.calls.Wait()
	b.mu.Lock()
	recs := make([]*scratchRec, 0, len(b.at.open))
	for rec := range b.at.open {
		recs = append(recs, rec)
	}
	b.mu.Unlock()
	for _, rec := range recs {
		if s := rec.snap.Value(); s != nil {
			s.Close()
		} else {
			b.dropLeaked(rec)
		}
	}
	// a snapshot whose own Close won leaves the set once its deletion starts
	b.mu.Lock()
	for len(b.at.open) > 0 || b.at.dropping > 0 {
		b.cond.Wait()
	}
	b.mu.Unlock()
	b.dropCursor()
}

func (b *Braid) dropCursor() {
	if c := b.at.cursor; c != nil {
		c.v.Close()
		_ = clearPrefix(b.db, c.prefix)
		b.at.cursor = nil
	}
}

// replayAt builds the view at position p under prefix: the cursor moves on through the batches that end at p or before it,
// its view is copied, and the batch that holds p is replayed into the copy through p. The caller holds b.at.sem.
func (b *Braid) replayAt(ctx context.Context, p uint64, prefix []byte) (*view, error) {
	in, err := b.replayInputs(ctx, p)
	if err != nil {
		return nil, err
	}
	if c := b.at.cursor; c != nil && c.next > p+1 {
		b.dropCursor()
	}
	if b.at.cursor == nil {
		if b.at.cursor, err = b.newCursor(in); err != nil {
			return nil, err
		}
	}
	c := b.at.cursor
	for c.next <= p {
		rec, err := b.pivotAt(c.batch)
		if err != nil {
			b.dropCursor()
			return nil, err
		}
		if rec.Next > p+1 {
			break
		}
		rv, err := b.replayInto(in.src, c.v, c.r, c.batch, 0, noStop, true)
		if err != nil {
			b.dropCursor()
			return nil, err
		}
		c.rv, c.next, c.batch = rv, rec.Next, c.batch+1
	}
	if err := copyPrefix(ctx, b.db, c.prefix, prefix); err != nil {
		return nil, err
	}
	v, err := openView(PrefixBackend(b.db, prefix), in.vc, in.apply)
	if err != nil {
		return nil, err
	}
	if c.next == p+1 {
		v.setRoster(c.rv)
		return v, nil
	}
	if _, err := b.replayInto(in.src, v, c.r.Clone(), c.batch, 0, p+1, true); err != nil {
		v.Close()
		return nil, err
	}
	return v, nil
}

// newCursor makes a cursor at the braid's first position: a successor's seed pairs and the genesis roster.
func (b *Braid) newCursor(in replayInput) (*replayCursor, error) {
	prefix := []byte(keyReplayCursor)
	if err := clearPrefix(b.db, prefix); err != nil {
		return nil, err
	}
	v, err := openView(PrefixBackend(b.db, prefix), in.vc, in.apply)
	if err != nil {
		return nil, err
	}
	r, err := b.seedReplay(in, v)
	if err != nil {
		v.Close()
		return nil, errors.Join(err, clearPrefix(b.db, prefix))
	}
	return &replayCursor{prefix: prefix, v: v, r: r, next: in.vc.Next}, nil
}

// copyPrefix copies every pair under from to the same key under to, one page of at most pageCount pairs and pageBytes (plus the
// pair that passes it) per read and commit, so a copy holds no more of the view in memory; it stops when ctx ends, checked after
// every commit, the last one included.
func copyPrefix(ctx context.Context, db kolostore.Backend, from, to []byte) error {
	start, end := slices.Clone(from), prefixdb.Successor(from)
	for {
		bt, n, size := db.NewBatch(), 0, 0
		var last []byte
		if err := db.Scan(start, end, false, pageCount, func(k, v []byte) bool {
			bt.Put(append(slices.Clone(to), k[len(from):]...), v) // the batch keeps its own copy
			last, n, size = bytes.Clone(k), n+1, size+len(k)+len(v)
			return size < pageBytes
		}); err != nil {
			return err
		}
		if err := bt.Commit(); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if n < pageCount && size < pageBytes {
			return nil
		}
		start = append(last, 0)
	}
}
