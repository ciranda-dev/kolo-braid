package kolobraid

import (
	"crypto/ed25519"
	"errors"
	"fmt"
	"iter"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"

	kololog "github.com/ciranda-dev/kolo-log"
)

// writeOpCost is what one view write costs in the per-entry write budget, on top of its key and value bytes: more than twice the
// measured 113-byte heap cost of one overlay write.
const writeOpCost = 256

// budgetFactor times MaxBody is the per-entry write budget: one entry fans out at most 16 times its own size into the view.
const budgetFactor = 16

// ApplyFunc turns one entry into writes on the view. It must be a pure deterministic function of the view before e, e and the
// roster at e's position: no clock, no randomness, no map iteration order, no I/O, no state of its own. It is called for every
// effective entry of kinds genesis, data, AddMember, RemoveMember, Settlers and Evidence. Nil commits tx's writes; an error
// wrapping ErrRejected (see Reject) discards them and records the entry as rejected on every peer; any other error and a panic
// discard them and halt the view on this peer. A Tx call that returned ErrViewLimit rejects the entry on every peer, whatever
// apply returns. tx and e are valid only during the call. apply can run on several goroutines at once and more than once for one
// position (tentatively, at settlement, after Open, in Braid.SettledAt), so it must never call a method of the Braid: a
// SettledAt from apply waits for itself until its context ends.
type ApplyFunc func(tx *Tx, e *Entry) error

// Entry is one entry as apply sees it. It carries no settled flag and no time.
type Entry struct {
	ID       EntryID
	Writer   ed25519.PublicKey
	Strand   kololog.LogID
	Index    uint64
	Position uint64
	Kind     Kind
	Body     []byte
	Links    []Link
}

func (e *Entry) clone() *Entry {
	c := *e
	c.Writer = slices.Clone(e.Writer)
	c.Body = slices.Clone(e.Body)
	c.Links = slices.Clone(e.Links)
	return &c
}

// Member is one member of the roster.
type Member struct {
	Key    ed25519.PublicKey
	Strand kololog.LogID
	Roles  Role
	// Seat is true for a seat of the settler config of the roster's position, the one Snapshot.Settlers returns.
	Seat bool
}

func (w Member) clone() Member {
	w.Key = slices.Clone(w.Key)
	return w
}

// rosterReader answers the roster questions of one position; the library supplies it for every entry.
type rosterReader interface {
	Member(key ed25519.PublicKey) (Member, bool)
	Members() []Member
}

type rejectError struct{ reason string }

func (e *rejectError) Error() string { return "kolobraid: entry rejected: " + e.reason }

func (e *rejectError) Is(target error) bool { return target == ErrRejected }

// Reject returns the error that apply returns to refuse an entry: the entry's writes are discarded and every peer records it as
// rejected. The reason is for the application's own errors and is never stored.
func Reject(reason string) error { return &rejectError{reason} }

// Tx is the handle apply works on: reads see the view before the entry plus the entry's own writes, writes are buffered and
// committed together when apply returns nil. One Tx serves one entry; after apply returns every method fails with ErrTxDone.
type Tx struct {
	mu     sync.Mutex
	done   atomic.Bool
	base   *layerReader
	buf    *layer
	writes []viewWrite
	spent  int64
	budget int64
	limit  bool
	fail   error
	roster rosterReader
}

func newTx(base *layerReader, roster rosterReader, budget int64) *Tx {
	return &Tx{base: base, buf: newLayer(), budget: budget, roster: roster}
}

func (tx *Tx) finish() {
	tx.mu.Lock()
	tx.done.Store(true)
	tx.mu.Unlock()
}

// Get returns a copy of the value of key, or ErrNotFound.
func (tx *Tx) Get(key []byte) ([]byte, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.done.Load() {
		return nil, ErrTxDone
	}
	if len(key) > MaxViewKey {
		return nil, ErrNotFound
	}
	if v, st := tx.buf.lookup(string(key)); st != absent {
		if st == deleted {
			return nil, ErrNotFound
		}
		return slices.Clone(v), nil
	}
	v, ok, err := tx.base.get(key)
	if err != nil {
		tx.fail = err
		return nil, err
	}
	if !ok {
		return nil, ErrNotFound
	}
	return slices.Clone(v), nil
}

// charge counts one write against the budget and the key and value bounds; it returns ErrViewLimit when one is exceeded, and
// then the entry is rejected whatever apply returns.
func (tx *Tx) charge(key, value int) error {
	if key > MaxViewKey || value > MaxViewValue {
		tx.limit = true
		return fmt.Errorf("%w: key or value too large", ErrViewLimit)
	}
	cost := int64(key) + int64(value) + writeOpCost
	if cost > tx.budget-tx.spent {
		tx.limit = true
		return fmt.Errorf("%w: write budget spent", ErrViewLimit)
	}
	tx.spent += cost
	return nil
}

// Put sets key to value. A key over 32 KiB, a value over 1 MiB, or a total over the entry's write budget returns ErrViewLimit,
// and the entry is then rejected on every peer, whatever apply returns.
func (tx *Tx) Put(key, value []byte) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.done.Load() {
		return ErrTxDone
	}
	if err := tx.charge(len(key), len(value)); err != nil {
		return err
	}
	k, v := slices.Clone(key), append([]byte{}, value...)
	tx.writes = append(tx.writes, viewWrite{Op: opPut, A: k, B: v})
	tx.buf.put(string(k), v)
	return nil
}

// Delete removes key; a key that does not exist is not an error. The limits of Put apply.
func (tx *Tx) Delete(key []byte) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.done.Load() {
		return ErrTxDone
	}
	if err := tx.charge(len(key), 0); err != nil {
		return err
	}
	k := slices.Clone(key)
	tx.writes = append(tx.writes, viewWrite{Op: opDelete, A: k})
	tx.buf.del(string(k))
	return nil
}

// DeleteRange removes every key in [start, end); an empty end means no upper bound. The limits of Put apply to both bounds.
func (tx *Tx) DeleteRange(start, end []byte) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.done.Load() {
		return ErrTxDone
	}
	if len(end) > MaxViewKey {
		tx.limit = true
		return fmt.Errorf("%w: key too large", ErrViewLimit)
	}
	if err := tx.charge(len(start), len(end)); err != nil {
		return err
	}
	s, e := slices.Clone(start), slices.Clone(end)
	tx.writes = append(tx.writes, viewWrite{Op: opDeleteRange, A: s, B: e})
	tx.buf.delRange(string(s), string(e), len(e) == 0)
	return nil
}

// Scan yields copies of the pairs with a key in [start, end) in ascending key order, the entry's own writes included; an empty end
// means no upper bound. The iteration sees the writes made before it started, not those made while it runs. After apply returns
// it yields nothing, and so it does after a storage error, which also halts the view.
func (tx *Tx) Scan(start, end []byte) iter.Seq2[[]byte, []byte] {
	return func(yield func([]byte, []byte) bool) {
		tx.mu.Lock()
		if tx.done.Load() {
			tx.mu.Unlock()
			return
		}
		frozen := tx.buf.freeze(string(start), string(end), len(end) == 0)
		tx.mu.Unlock()
		s := &scanner{
			layers: func() []*layer { return append([]*layer{frozen}, tx.base.layers...) },
			db:     tx.base.db,
			start:  slices.Clone(start),
			end:    slices.Clone(end),
			open:   len(end) == 0,
			alive:  func() bool { return !tx.done.Load() },
			onErr: func(err error) {
				tx.mu.Lock()
				tx.fail = err
				tx.mu.Unlock()
			},
		}
		s.run(yield)
	}
}

// Member returns the roster entry for key at the position of this entry.
func (tx *Tx) Member(key ed25519.PublicKey) (Member, bool) {
	if tx.done.Load() || tx.roster == nil {
		return Member{}, false
	}
	w, ok := tx.roster.Member(key)
	if !ok {
		return Member{}, false
	}
	return w.clone(), true
}

// Members returns the roster at the position of this entry.
func (tx *Tx) Members() []Member {
	if tx.done.Load() || tx.roster == nil {
		return nil
	}
	ws := tx.roster.Members()
	out := make([]Member, len(ws))
	for i, w := range ws {
		out[i] = w.clone()
	}
	return out
}

// panicError describes a recovered panic without its value, which could hold entry data; a runtime error has a fixed message.
func panicError(r any) error {
	if re, ok := r.(runtime.Error); ok {
		return fmt.Errorf("kolobraid: apply panicked: %s", re.Error())
	}
	return fmt.Errorf("kolobraid: apply panicked with a value of type %T", r)
}

// invoke calls apply and turns a panic into an error; tx is finished however apply ends.
func invoke(f ApplyFunc, tx *Tx, e *Entry) (err, panicked error) {
	defer tx.finish()
	defer func() {
		if r := recover(); r != nil {
			panicked = panicError(r)
		}
	}()
	return f(tx, e), nil
}

var errReentered = errors.New("kolobraid: view used from inside apply or from two goroutines")
