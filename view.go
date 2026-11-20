package kolobraid

import (
	"bytes"
	"crypto/ed25519"
	"errors"
	"fmt"
	"iter"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"

	kololog "github.com/ciranda-dev/kolo-log"
	kolostore "github.com/ciranda-dev/kolo-store"

	"github.com/ciranda-dev/kolo-braid/internal/prefixdb"
)

const (
	// chunkEntries is how many settled entries one durable commit holds at most.
	chunkEntries = 256
	// Ranges of the local limits of the view, as in Limits.
	minMaxTail, maxMaxTail           = 64, 1 << 20
	minMaxTailBytes, maxMaxTailBytes = 1 << 20, 4 << 30
)

// viewConfig is what the view takes from the group config and the local limits.
type viewConfig struct {
	MaxBody         int   // sets the per-entry write budget, 16 x MaxBody
	CheckpointEvery int   // checkpoint spacing in settled positions
	MaxTail         int   // tentative entries kept
	MaxTailBytes    int64 // write budget of the tentative entries kept, in the units of the per-entry budget
	// Next and Initial give the position and the running digest a new view starts at: 0 and the group ID, or p+1 and a digest in a
	// successor braid. An existing view keeps its own.
	Next    uint64
	Initial Digest
	// chunk overrides chunkEntries in tests.
	chunk int
}

func (c viewConfig) validate() error {
	switch {
	case c.MaxBody < minMaxBody || c.MaxBody > maxMaxBody:
		return fmt.Errorf("%w: MaxBody out of range", ErrInvalidArgument)
	case c.CheckpointEvery < minCheckpoint || c.CheckpointEvery > maxCheckpoint:
		return fmt.Errorf("%w: CheckpointEvery out of range", ErrInvalidArgument)
	case c.MaxTail < minMaxTail || c.MaxTail > maxMaxTail:
		return fmt.Errorf("%w: MaxTail out of range", ErrInvalidArgument)
	case c.MaxTailBytes < minMaxTailBytes || c.MaxTailBytes > maxMaxTailBytes:
		return fmt.Errorf("%w: MaxTailBytes out of range", ErrInvalidArgument)
	}
	return nil
}

// applyStep is one entry to apply: the entry as apply sees it (the view sets Position), the roster at its position, the writes that
// the library made for it (roster changes, keys under "r"), and whether it is void. Pulses and void entries do not call apply.
type applyStep struct {
	Entry  Entry
	Roster rosterReader
	Lib    []viewWrite
	Void   bool
	// lazy is set when Entry.Body is still to be read from its strand.
	lazy bool
}

func appliesKind(k Kind) bool { return k <= KindEvidence && k != KindPulse }

func checkStep(s *applyStep) error {
	if s.Entry.Kind > KindPulse {
		return fmt.Errorf("%w: kind %d takes no position", ErrInvalidArgument, s.Entry.Kind)
	}
	if s.Void && len(s.Lib) > 0 {
		return fmt.Errorf("%w: a void entry has no library writes", ErrInvalidArgument)
	}
	for _, w := range s.Lib {
		if len(w.A) == 0 || w.A[0] != rosterPrefix {
			return fmt.Errorf("%w: library write outside the roster keys", ErrInvalidArgument)
		}
		switch w.Op {
		case opPut, opDelete:
		case opDeleteRange:
			if len(w.B) == 0 || bytes.Compare(w.B, []byte{rosterPrefix + 1}) > 0 {
				return fmt.Errorf("%w: library range outside the roster keys", ErrInvalidArgument)
			}
		default:
			return fmt.Errorf("%w: library write operation %d", ErrInvalidArgument, w.Op)
		}
	}
	return nil
}

type tailEntry struct {
	id     EntryID
	void   bool
	lib    []viewWrite
	status entryStatus
	writes []viewWrite
	digest Digest
	cost   int64
	roster rosterReader
}

func (t *tailEntry) matches(s *applyStep) bool {
	return t.id == s.Entry.ID && t.void == s.Void && slices.EqualFunc(t.lib, s.Lib, func(a, b viewWrite) bool {
		return a.Op == b.Op && bytes.Equal(a.A, b.A) && bytes.Equal(a.B, b.B)
	})
}

// overlay is the tentative state: the write-sets of the tail entries, newest on top of the durable view.
type overlay struct {
	l       *layer
	entries []tailEntry
	bytes   int64
}

func newOverlay() *overlay { return &overlay{l: newLayer()} }

func (o *overlay) push(t tailEntry) {
	o.l.replay(t.writes)
	o.bytes += t.cost
	o.entries = append(o.entries, t)
}

func (o *overlay) truncate(n int) {
	for len(o.entries) > n {
		t := o.entries[len(o.entries)-1]
		o.l.unwind(t.writes)
		o.bytes -= t.cost
		o.entries[len(o.entries)-1] = tailEntry{}
		o.entries = o.entries[:len(o.entries)-1]
	}
}

// dropFront removes the first n entries, which have become durable.
func (o *overlay) dropFront(n int) {
	rest := o.entries[n:]
	*o = overlay{l: newLayer()}
	for _, t := range rest {
		o.push(t)
	}
}

// tailHalt is a tentative entry whose apply failed: idx is the tail index it would take.
type tailHalt struct {
	idx int
	id  EntryID
	err error
}

// undo holds the values that one durable commit replaced, for snapshots opened before it.
type undo struct {
	epoch uint64
	l     *layer
}

// view is the transactional view over a kolostore.Backend: settled state on disk, tentative state in memory. One goroutine drives
// it (ApplyTail, DropTail, Settle, Verify, Reset); snapshots may read from any goroutine.
type view struct {
	cfg    viewConfig
	apply  ApplyFunc
	raw    kolostore.Backend
	db     kolostore.Backend // the view pairs, without their prefix
	budget int64
	chunk  int

	opMu sync.Mutex // held for the whole of every driving call; a second taker (re-entrancy, a second goroutine) fails

	mu   sync.RWMutex // guards what snapshots read: the overlay, the durable pairs, undos, snaps, halt, roster
	prog progress
	// roster is the roster after the last durable entry, published with prog so a snapshot pairs the two.
	roster   rosterReader
	tail     *overlay
	tailHalt *tailHalt
	halt     error
	closed   bool
	epoch    uint64
	snaps    map[*snapHandle]uint64
	undos    []undo
}

// openView opens the view stored in b, or creates it from cfg when b holds none.
func openView(b kolostore.Backend, cfg viewConfig, apply ApplyFunc) (*view, error) {
	if b == nil || apply == nil {
		return nil, fmt.Errorf("%w: backend and apply are required", ErrInvalidArgument)
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	v := &view{
		cfg: cfg, apply: apply, raw: b,
		db:     prefixdb.New(b, []byte{viewPrefix}),
		budget: int64(budgetFactor) * int64(cfg.MaxBody),
		chunk:  chunkEntries,
		tail:   newOverlay(),
		snaps:  make(map[*snapHandle]uint64),
	}
	if cfg.chunk > 0 {
		v.chunk = cfg.chunk
	}
	val, err := b.Get([]byte(progressKey))
	switch {
	case errors.Is(err, kolostore.ErrNotFound):
		v.prog = progress{Next: cfg.Next, Digest: cfg.Initial}
		if err := b.Put([]byte(progressKey), v.prog.encode()); err != nil {
			return nil, err
		}
	case err != nil:
		return nil, err
	default:
		if v.prog, err = decodeProgress(val); err != nil {
			return nil, err
		}
	}
	return v, nil
}

// Close ends the view; the backend stays open.
func (v *view) Close() error {
	v.mu.Lock()
	v.closed = true
	v.mu.Unlock()
	return nil
}

func (v *view) check() error {
	v.mu.RLock()
	defer v.mu.RUnlock()
	if v.closed {
		return ErrClosed
	}
	return v.halt
}

func (v *view) Progress() progress {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.prog
}

// Halted returns the error that halted settled application, or nil.
func (v *view) Halted() error {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.halt
}

func haltError(pos uint64, cause error) error {
	return fmt.Errorf("%w at position %d: %w", ErrHalted, pos, cause)
}

type outcome struct {
	status entryStatus
	writes []viewWrite
	cost   int64
	halt   error
}

func (v *view) evaluate(s *applyStep, pos uint64, base *layerReader) outcome {
	switch {
	case s.Void:
		return outcome{status: statusVoid}
	case !appliesKind(s.Entry.Kind):
		return outcome{status: statusApplied}
	}
	e := s.Entry.clone()
	e.Position = pos
	tx := newTx(base, s.Roster, v.budget)
	aerr, panicked := invoke(v.apply, tx, e)
	switch {
	case tx.fail != nil:
		return outcome{halt: haltError(pos, tx.fail)}
	case panicked != nil:
		return outcome{halt: haltError(pos, panicked)}
	case tx.limit, errors.Is(aerr, ErrRejected):
		return outcome{status: statusRejected}
	case aerr != nil:
		return outcome{halt: haltError(pos, aerr)}
	}
	return outcome{status: statusApplied, writes: tx.writes, cost: tx.spent}
}

// writeSetOf returns the writes the running digest covers: the library's, then apply's.
func writeSetOf(lib, writes []viewWrite) []viewWrite {
	if len(lib) == 0 {
		return writes
	}
	if len(writes) == 0 {
		return lib
	}
	return append(slices.Clone(lib), writes...)
}

type tailInfo struct {
	ID       EntryID
	Position uint64
	Status   entryStatus
	Digest   Digest
}

type tailResult struct {
	tailInfo
	// Paused is true when the tail is at MaxTail entries or MaxTailBytes of overlay: the entry was not applied.
	Paused bool
}

func (v *view) Tail() int {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return len(v.tail.entries)
}

func (v *view) TailEntry(i int) (tailInfo, bool) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	if i < 0 || i >= len(v.tail.entries) {
		return tailInfo{}, false
	}
	t := &v.tail.entries[i]
	return tailInfo{ID: t.id, Position: v.prog.Next + uint64(i), Status: t.status, Digest: t.digest}, true
}

// DropTail keeps the first n tentative entries and forgets the rest; a tentative halt ends when the entries before its position change.
func (v *view) DropTail(n int) error {
	if !v.opMu.TryLock() {
		return errReentered
	}
	defer v.opMu.Unlock()
	if n < 0 {
		return fmt.Errorf("%w: negative tail length", ErrInvalidArgument)
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed {
		return ErrClosed
	}
	v.tail.truncate(n)
	if v.tailHalt != nil && n < v.tailHalt.idx {
		v.tailHalt = nil
	}
	return nil
}

// ApplyTail applies s tentatively as the next entry of the tail. A failure of apply other than Reject is returned wrapped in
// ErrHalted, and the same entry fails again without running apply until the entries before it change (DropTail) or another
// entry is offered at that position; settled application is not affected.
func (v *view) ApplyTail(s applyStep) (tailResult, error) {
	if !v.opMu.TryLock() {
		return tailResult{}, errReentered
	}
	defer v.opMu.Unlock()
	if err := v.check(); err != nil {
		return tailResult{}, err
	}
	if err := checkStep(&s); err != nil {
		return tailResult{}, err
	}
	idx := len(v.tail.entries)
	if h := v.tailHalt; h != nil {
		if h.idx == idx && h.id == s.Entry.ID {
			return tailResult{}, h.err
		}
		v.tailHalt = nil
	}
	res := tailResult{tailInfo: tailInfo{ID: s.Entry.ID, Position: v.prog.Next + uint64(idx)}}
	if idx >= v.cfg.MaxTail || v.tail.bytes >= v.cfg.MaxTailBytes {
		res.Paused = true
		return res, nil
	}
	out := v.evaluate(&s, res.Position, &layerReader{layers: []*layer{v.tail.l}, db: v.db})
	if out.halt != nil {
		v.tailHalt = &tailHalt{idx: idx, id: s.Entry.ID, err: out.halt}
		return tailResult{}, out.halt
	}
	if v.tail.bytes+out.cost > v.cfg.MaxTailBytes {
		res.Paused = true
		return res, nil
	}
	prev := v.prog.Digest
	if idx > 0 {
		prev = v.tail.entries[idx-1].digest
	}
	t := tailEntry{
		id: s.Entry.ID, void: s.Void, lib: s.Lib, status: out.status, writes: out.writes, cost: out.cost, roster: s.Roster,
		digest: runningDigest(prev, res.Position, s.Entry.ID, out.status, writeSetOf(s.Lib, out.writes)),
	}
	v.mu.Lock()
	v.tail.push(t)
	v.mu.Unlock()
	res.Status, res.Digest = t.status, t.digest
	return res, nil
}

// chunk is the entries of one durable commit that are not committed yet.
type chunk struct {
	pending *layer
	entries []chunkEntry
	cost    int64
	reject  []uint64
}

type chunkEntry struct {
	lib    []viewWrite
	writes []viewWrite
	roster rosterReader
}

type settleResult struct {
	Entries  int // entries processed by this call
	Promoted int // of them, taken from the tail without calling apply
	Progress progress
	// Checkpoint is set when the batch ended at or past a multiple of CheckpointEvery.
	Checkpoint *checkpointRec
}

// Settle applies batch number batch of the settled order, which must be the next one, from the entry the stored progress names.
// Entries that equal the first tail entries are promoted without calling apply; at the first one that differs the tail is
// dropped and the rest applied. Every chunkEntries entries one atomic commit writes the view, the library's writes and the
// progress. A failure of apply other than Reject halts the view after the entries before it are committed.
func (v *view) Settle(batch uint64, steps iter.Seq2[applyStep, error]) (res settleResult, err error) {
	if !v.opMu.TryLock() {
		return res, errReentered
	}
	defer v.opMu.Unlock()
	if err := v.check(); err != nil {
		return res, err
	}
	if batch != v.prog.Batches {
		return res, fmt.Errorf("%w: next batch is %d", ErrInvalidArgument, v.prog.Batches)
	}
	startNext := v.prog.Next - v.prog.Offset
	p := v.prog
	ch := &chunk{pending: newLayer()}
	promoted, mismatch := 0, false
	flush := func(done bool) error {
		if len(ch.entries) == 0 && !done {
			return nil
		}
		q := p
		if done {
			q.Batches, q.Offset = p.Batches+1, 0
		}
		var cp *checkpointRec
		if done && checkpointDue(startNext, p.Next, v.cfg.CheckpointEvery) {
			cp = &checkpointRec{Position: p.Next - 1, Digest: p.Digest}
		}
		if err := v.commit(ch, q, cp, promoted, mismatch); err != nil {
			return err
		}
		if done {
			res.Checkpoint = cp
		}
		p, res.Progress = q, q
		ch, promoted, mismatch = &chunk{pending: newLayer()}, 0, false
		return nil
	}
	for s, serr := range steps {
		if serr != nil {
			if ferr := flush(false); ferr != nil {
				return res, ferr
			}
			return res, serr
		}
		if err := checkStep(&s); err != nil {
			if ferr := flush(false); ferr != nil {
				return res, ferr
			}
			return res, err
		}
		var ce chunkEntry
		var status entryStatus
		var cost int64
		var digest Digest
		if t := v.promotable(promoted, mismatch, &s); t != nil {
			ce, status, cost, digest = chunkEntry{lib: t.lib, writes: t.writes, roster: s.Roster}, t.status, t.cost, t.digest
			promoted++
			res.Promoted++
		} else {
			if promoted < v.tailLen() {
				mismatch = true
			}
			out := v.evaluate(&s, p.Next, &layerReader{layers: []*layer{ch.pending}, db: v.db})
			if out.halt != nil {
				if ferr := flush(false); ferr != nil {
					return res, ferr
				}
				v.mu.Lock()
				v.halt = out.halt
				v.mu.Unlock()
				return res, out.halt
			}
			ce, status, cost = chunkEntry{lib: s.Lib, writes: out.writes, roster: s.Roster}, out.status, out.cost
			digest = runningDigest(p.Digest, p.Next, s.Entry.ID, status, writeSetOf(s.Lib, out.writes))
		}
		ch.pending.replay(ce.writes)
		ch.entries = append(ch.entries, ce)
		ch.cost += cost
		if status == statusRejected {
			ch.reject = append(ch.reject, p.Next)
		}
		p.Next++
		p.Offset++
		p.Digest = digest
		res.Entries++
		if len(ch.entries) >= v.chunk || ch.cost >= v.budget {
			if err := flush(false); err != nil {
				return res, err
			}
		}
	}
	if err := flush(true); err != nil {
		return res, err
	}
	return res, nil
}

func (v *view) tailLen() int {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return len(v.tail.entries)
}

// promotable returns the next tail entry when it equals s and no earlier entry of the chunk broke the match.
func (v *view) promotable(promoted int, mismatch bool, s *applyStep) *tailEntry {
	if mismatch {
		return nil
	}
	v.mu.RLock()
	defer v.mu.RUnlock()
	if promoted >= len(v.tail.entries) {
		return nil
	}
	if t := &v.tail.entries[promoted]; t.matches(s) {
		return t
	}
	return nil
}

// commit writes one chunk and the progress in one batch, then moves the overlay and the snapshots' undo records to match.
func (v *view) commit(ch *chunk, to progress, cp *checkpointRec, promoted int, mismatch bool) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed {
		return ErrClosed
	}
	var applyWrites []viewWrite
	for _, e := range ch.entries {
		applyWrites = append(applyWrites, e.writes...)
	}
	var u *layer
	if len(v.snaps) > 0 {
		var err error
		if u, err = v.captureUndo(applyWrites); err != nil {
			return v.fail(to, err)
		}
	}
	b := v.raw.NewBatch()
	vb := prefixdb.WrapBatch(b, v.raw, []byte{viewPrefix})
	for _, e := range ch.entries {
		for _, w := range e.lib {
			writeTo(b, w)
		}
		for _, w := range e.writes {
			writeTo(vb, w)
		}
	}
	for _, pos := range ch.reject {
		b.Put(posKey(rejectedKey, pos), nil)
	}
	if cp != nil {
		b.Put(posKey(checkpoint, cp.Position), cp.Digest[:])
	}
	b.Put([]byte(progressKey), to.encode())
	if err := b.Commit(); err != nil {
		return v.fail(to, err)
	}
	v.prog = to
	if len(ch.entries) > 0 {
		if r := ch.entries[len(ch.entries)-1].roster; r != nil {
			v.roster = r
		}
		v.epoch++
		if u != nil {
			v.undos = append(v.undos, undo{epoch: v.epoch, l: u})
		}
	}
	if mismatch {
		v.tail = newOverlay()
		v.tailHalt = nil
	} else if promoted > 0 {
		v.tail.dropFront(promoted)
		if v.tailHalt != nil {
			if v.tailHalt.idx -= promoted; v.tailHalt.idx < 0 {
				v.tailHalt = nil
			}
		}
	}
	return nil
}

// fail halts the view after a storage error; the stored progress says what is durable, and the next open continues from it.
func (v *view) fail(at progress, err error) error {
	v.halt = haltError(at.Next, err)
	return v.halt
}

type writeTarget interface {
	Put(key, value []byte)
	Delete(key []byte)
	DeleteRange(start, end []byte)
}

func writeTo(t writeTarget, w viewWrite) {
	switch w.Op {
	case opPut:
		t.Put(w.A, w.B)
	case opDelete:
		t.Delete(w.A)
	case opDeleteRange:
		if len(w.B) == 0 {
			t.DeleteRange(w.A, nil)
			return
		}
		t.DeleteRange(w.A, w.B)
	}
}

// captureUndo records the durable values that ws are about to replace.
func (v *view) captureUndo(ws []viewWrite) (*layer, error) {
	u := newLayer()
	seen := make(map[string]bool)
	record := func(k, val []byte, has bool) {
		if seen[string(k)] {
			return
		}
		seen[string(k)] = true
		if has {
			u.put(string(k), val)
		} else {
			u.del(string(k))
		}
	}
	for _, w := range ws {
		if w.Op != opDeleteRange {
			val, err := v.db.Get(w.A)
			if err != nil && !errors.Is(err, kolostore.ErrNotFound) {
				return nil, err
			}
			record(w.A, val, err == nil)
			continue
		}
		var end []byte
		if len(w.B) > 0 {
			end = w.B
		}
		var ks, vs [][]byte
		if err := v.db.Scan(w.A, end, false, 0, func(k, val []byte) bool {
			ks, vs = append(ks, k), append(vs, val)
			return true
		}); err != nil {
			return nil, err
		}
		for i := range ks {
			record(ks[i], vs[i], true)
		}
	}
	return u, nil
}

// SeedPairs commits view pairs without calling apply, for the seed entries of a successor braid.
func (v *view) SeedPairs(pairs []seedPair) error {
	if !v.opMu.TryLock() {
		return errReentered
	}
	defer v.opMu.Unlock()
	if err := v.check(); err != nil {
		return err
	}
	for _, p := range pairs {
		if len(p.Key) > MaxViewKey || len(p.Value) > MaxViewValue {
			return fmt.Errorf("%w: seed pair too large", ErrInvalidArgument)
		}
	}
	ws := make([]viewWrite, len(pairs))
	for i, p := range pairs {
		ws[i] = viewWrite{Op: opPut, A: p.Key, B: p.Value}
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	var u *layer
	if len(v.snaps) > 0 {
		var err error
		if u, err = v.captureUndo(ws); err != nil {
			return err
		}
	}
	b := v.raw.NewBatch()
	vb := prefixdb.WrapBatch(b, v.raw, []byte{viewPrefix})
	for _, w := range ws {
		writeTo(vb, w)
	}
	if err := b.Commit(); err != nil {
		return err
	}
	v.epoch++
	if u != nil {
		v.undos = append(v.undos, undo{epoch: v.epoch, l: u})
	}
	return nil
}

// Reset empties the view, its checkpoints and rejection marks and starts it again from cfg.Next and cfg.Initial, in one commit
// with extra (library writes, such as the removal of the roster). It needs no open snapshot.
func (v *view) Reset(extra ...viewWrite) error {
	if !v.opMu.TryLock() {
		return errReentered
	}
	defer v.opMu.Unlock()
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed {
		return ErrClosed
	}
	if len(v.snaps) > 0 {
		return fmt.Errorf("%w: snapshots are open", ErrInvalidArgument)
	}
	for i := range extra {
		if err := checkStep(&applyStep{Lib: extra[i : i+1]}); err != nil {
			return err
		}
	}
	b := v.raw.NewBatch()
	vb := prefixdb.WrapBatch(b, v.raw, []byte{viewPrefix})
	vb.DeleteRange(nil, nil)
	b.DeleteRange([]byte(checkpoint), []byte(checkpoint[:len(checkpoint)-1]+"0"))
	b.DeleteRange([]byte(rejectedKey), []byte(rejectedKey[:len(rejectedKey)-1]+"0"))
	for _, w := range extra {
		writeTo(b, w)
	}
	p := progress{Next: v.cfg.Next, Digest: v.cfg.Initial}
	b.Put([]byte(progressKey), p.encode())
	if err := b.Commit(); err != nil {
		return err
	}
	v.prog, v.halt, v.tail, v.tailHalt, v.roster = p, nil, newOverlay(), nil, nil
	v.epoch++
	return nil
}

// Snapshot is a frozen read view of the braid's view: it keeps showing the state it was taken at while settlement goes on. Close it
// when done: an open snapshot makes every commit keep the values it replaces.
type Snapshot struct {
	v      *view
	h      *snapHandle
	epoch  uint64
	frozen *layer
	roster rosterReader
	pos    uint64
	closed atomic.Bool
	err    atomic.Pointer[error]
	// done runs once at Close, after the release; it deletes the scratch storage of a snapshot from SettledAt.
	done func() error
}

// snapHandle stands for an open snapshot in the view, so a snapshot that is dropped without Close can still be collected.
type snapHandle struct{ _ byte }

type cleanupArg struct {
	v *view
	h *snapHandle
}

func (v *view) snapshot(tentative bool) *Snapshot {
	v.mu.Lock()
	defer v.mu.Unlock()
	s := &Snapshot{v: v, h: &snapHandle{}, epoch: v.epoch, roster: v.roster}
	s.pos = v.prog.Next
	if tentative {
		s.frozen = v.tail.l.clone()
		s.pos += uint64(len(v.tail.entries))
		if n := len(v.tail.entries); n > 0 && v.tail.entries[n-1].roster != nil {
			s.roster = v.tail.entries[n-1].roster
		}
	}
	if s.pos > 0 {
		s.pos--
	}
	v.snaps[s.h] = s.epoch
	runtime.AddCleanup(s, func(a cleanupArg) { a.v.release(a.h) }, cleanupArg{v, s.h})
	return s
}

// Settled returns a snapshot of the settled view with the roster after its last entry.
func (v *view) Settled() *Snapshot { return v.snapshot(false) }

// View returns a snapshot of the settled view plus the tail with the roster after its last entry.
func (v *view) View() *Snapshot { return v.snapshot(true) }

// setRoster publishes r as the roster after the last durable entry, for a view whose entries were applied before it opened.
func (v *view) setRoster(r rosterReader) {
	v.mu.Lock()
	v.roster = r
	v.mu.Unlock()
}

// release removes a snapshot from the open ones and drops the undo records that no open snapshot needs.
func (v *view) release(h *snapHandle) {
	v.mu.Lock()
	defer v.mu.Unlock()
	delete(v.snaps, h)
	if len(v.snaps) == 0 {
		v.undos = nil
		return
	}
	oldest := ^uint64(0)
	for _, e := range v.snaps {
		oldest = min(oldest, e)
	}
	v.undos = slices.DeleteFunc(v.undos, func(u undo) bool { return u.epoch <= oldest })
}

// layersLocked returns the layers a snapshot reads, newest first; the caller holds v.mu.
func (s *Snapshot) layersLocked() []*layer {
	var ls []*layer
	if s.frozen != nil {
		ls = append(ls, s.frozen)
	}
	for _, u := range s.v.undos {
		if u.epoch > s.epoch {
			ls = append(ls, u.l)
		}
	}
	return ls
}

// Get returns a copy of the value of key at the snapshot, or ErrNotFound.
func (s *Snapshot) Get(key []byte) ([]byte, error) {
	if s.closed.Load() {
		return nil, ErrClosed
	}
	if len(key) > MaxViewKey {
		return nil, ErrNotFound
	}
	s.v.mu.RLock()
	defer s.v.mu.RUnlock()
	// a Close between the check above and the lock may have deleted the storage of a snapshot from SettledAt
	if s.closed.Load() {
		return nil, ErrClosed
	}
	val, ok, err := (&layerReader{layers: s.layersLocked(), db: s.v.db}).get(key)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrNotFound
	}
	return slices.Clone(val), nil
}

// Scan yields copies of the pairs with a key in [start, end) in ascending key order; an empty end means no upper bound. A storage
// error, or a Close, ends the iteration early, and Err then reports it.
func (s *Snapshot) Scan(start, end []byte) iter.Seq2[[]byte, []byte] {
	return s.scan(start, end, func(err error) { s.err.CompareAndSwap(nil, &err) })
}

// ScanErr is Scan with an error of its own: once the iteration ended, the function reports the error that ended it early, nil
// when none did, whatever other scans of the snapshot met.
func (s *Snapshot) ScanErr(start, end []byte) (iter.Seq2[[]byte, []byte], func() error) {
	var first atomic.Pointer[error]
	seq := s.scan(start, end, func(err error) {
		first.CompareAndSwap(nil, &err)
		s.err.CompareAndSwap(nil, &err)
	})
	return seq, func() error {
		if p := first.Load(); p != nil {
			return *p
		}
		return nil
	}
}

// Err returns the error that first ended a Scan of the snapshot early, nil when none did: check it after a Scan loop, since
// a scan cut short otherwise looks like a shorter range. It is ErrClosed for a Scan of a closed snapshot. It stays once set:
// ScanErr reports the error of one scan.
func (s *Snapshot) Err() error {
	if p := s.err.Load(); p != nil {
		return *p
	}
	return nil
}

// scan is Scan that hands a storage error, or ErrClosed, to onErr before it ends the iteration.
func (s *Snapshot) scan(start, end []byte, onErr func(error)) iter.Seq2[[]byte, []byte] {
	return func(yield func([]byte, []byte) bool) {
		sc := &scanner{
			layers: s.layersLocked,
			db:     s.v.db,
			start:  slices.Clone(start),
			end:    slices.Clone(end),
			open:   len(end) == 0,
			lock:   s.v.mu.RLock,
			unlock: s.v.mu.RUnlock,
			alive: func() bool {
				if s.closed.Load() {
					onErr(ErrClosed)
					return false
				}
				return true
			},
			onErr: onErr,
		}
		sc.run(yield)
	}
}

// Members returns the roster of the snapshot.
func (s *Snapshot) Members() []Member {
	if s.closed.Load() || s.roster == nil {
		return nil
	}
	ws := s.roster.Members()
	out := make([]Member, len(ws))
	for i, w := range ws {
		out[i] = w.clone()
	}
	return out
}

// Settlers is a settler config as a snapshot holds it.
type Settlers struct {
	// Config is the config number, as in Status.Config; Rule is its quorum rule and Quorum its q over Seats.
	Config uint64
	Rule   QuorumRule
	Quorum int
	// Seats are the config's seats in key order, each with the strand that counts for it and the roles the snapshot's roster
	// gives its key: none for a removed member, which keeps its seat until a config without it activates.
	Seats []Member
}

// Settlers returns the settler config whose seats Member.Seat marks in the snapshot, read with the roster in one step: the
// config of the batch that settled the snapshot's last entry, or for View, while it holds tentative entries, the config they are
// applied under. It is the zero Settlers for a closed snapshot and for one without a roster.
func (s *Snapshot) Settlers() Settlers {
	rv, ok := s.roster.(*rosterView)
	if s.closed.Load() || !ok || rv == nil || len(rv.cfg.Seats) == 0 {
		return Settlers{}
	}
	c := rv.cfg
	out := Settlers{Config: c.Number, Rule: QuorumRule(c.Rule), Quorum: c.Quorum(), Seats: make([]Member, len(c.Seats))}
	for i, st := range c.Seats {
		m := Member{Key: ed25519.PublicKey(slices.Clone(st.Key[:])), Strand: kololog.LogID(st.Strand), Seat: true}
		if w, ok := rv.Member(m.Key); ok {
			m.Roles = w.Roles
		}
		out.Seats[i] = m
	}
	return out
}

// Position returns the position of the last entry applied in the snapshot, or 0 when none is; 0 is also the position of a
// genesis applied alone, so a caller that must tell them apart compares with Status().Settled.
func (s *Snapshot) Position() uint64 { return s.pos }

// Close releases the snapshot's hold on the overlay and the backend, and deletes the scratch storage of a snapshot from
// SettledAt; a second Close returns nil.
func (s *Snapshot) Close() error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	s.v.release(s.h)
	if s.done != nil {
		return s.done()
	}
	return nil
}
