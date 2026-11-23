package kv

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	kolokv "github.com/ciranda-dev/kolo-kv"

	kolobraid "github.com/ciranda-dev/kolo-braid"
)

// Read is the view a DB reads.
type Read int

const (
	// Tentative reads settled and applied tentative entries: a write shows once applied, and a reorg can change a read.
	Tentative Read = iota
	// Settled reads settled entries only: no reorg undoes a value read, and a write shows only after settlement.
	Settled
)

// DB is a kolokv.Store over s in b's view. Put and Delete return once the entry is appended, and neither read shows it
// before the braid has applied it (Tentative) or settled it (Settled).
type DB struct {
	b      *kolobraid.Braid
	s      *Store
	read   Read
	closed atomic.Bool
}

var _ kolokv.Store = (*DB)(nil)

// Open returns a DB over b and s that reads the view read; s must be the Store that b's ApplyFunc routes kv entries to.
// Close does not close b.
func Open(b *kolobraid.Braid, s *Store, read Read) (*DB, error) {
	if b == nil || s == nil || (read != Tentative && read != Settled) {
		return nil, fmt.Errorf("%w: kv.Open needs a braid, a store and a read view", kolobraid.ErrInvalidArgument)
	}
	return &DB{b: b, s: s, read: read}, nil
}

// both is err that also matches target.
type both struct{ err, target error }

func (e both) Error() string { return e.err.Error() }

func (e both) Unwrap() []error { return []error{e.err, e.target} }

// mapErr makes the errors of the braid that kolokv has a twin for match the twin too.
func mapErr(err error) error {
	switch {
	case errors.Is(err, kolobraid.ErrNotFound):
		return both{err, kolokv.ErrNotFound}
	case errors.Is(err, kolobraid.ErrClosed):
		return both{err, kolokv.ErrClosed}
	}
	return err
}

// Put sets key to value; a ctx already done writes nothing and returns ctx.Err().
func (db *DB) Put(ctx context.Context, key, value []byte) error {
	return db.write(ctx, key, value, false)
}

// Delete removes key; a ctx already done writes nothing and returns ctx.Err().
func (db *DB) Delete(ctx context.Context, key []byte) error { return db.write(ctx, key, nil, true) }

func (db *DB) write(ctx context.Context, key, value []byte, del bool) error {
	if len(db.s.prefix)+len(key) > MaxKeySize {
		return both{fmt.Errorf("%w: kv key of %d bytes with a prefix of %d", kolobraid.ErrTooLarge, len(key), len(db.s.prefix)),
			kolokv.ErrKeyTooLarge}
	}
	if len(value) > MaxValueSize {
		return both{fmt.Errorf("%w: kv value of %d bytes", kolobraid.ErrTooLarge, len(value)), kolokv.ErrValueTooLarge}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if db.closed.Load() {
		return kolokv.ErrClosed
	}
	var err error
	if del {
		err = db.s.Delete(ctx, db.b, key)
	} else {
		err = db.s.Put(ctx, db.b, key, value)
	}
	if errors.Is(err, kolobraid.ErrTooLarge) {
		// the key and the value fit their bounds, so the body is over the braid's MaxBody
		return both{err, kolokv.ErrValueTooLarge}
	}
	return mapErr(err)
}

func (db *DB) snapshot() *kolobraid.Snapshot {
	if db.read == Settled {
		return db.b.Settled()
	}
	return db.b.View()
}

// Get returns a copy of the value of key; a missing key matches kolokv.ErrNotFound and kolobraid.ErrNotFound.
func (db *DB) Get(key []byte) ([]byte, error) {
	if db.closed.Load() {
		return nil, kolokv.ErrClosed
	}
	snap := db.snapshot()
	defer snap.Close()
	v, err := db.s.Get(snap, key)
	return v, mapErr(err)
}

// Range returns an iterator over the pairs of one snapshot with a key in [start, end); a nil end has no upper bound and
// an empty end yields nothing. Close it unless Next returned false: it holds the snapshot until then.
func (db *DB) Range(start, end []byte) *kolokv.Iterator {
	if db.closed.Load() {
		return failed(kolokv.ErrClosed)
	}
	if end != nil && len(end) == 0 {
		return new(kolokv.Iterator)
	}
	snap := db.snapshot()
	return kolokv.NewIterator(db.s.Range(snap, start, end), func() error {
		err := snap.Err()
		snap.Close()
		return mapErr(err)
	})
}

// Prefix returns an iterator over the pairs of one snapshot whose key starts with prefix, as Range does.
func (db *DB) Prefix(prefix []byte) *kolokv.Iterator { return db.Range(prefix, upper(prefix)) }

// failed returns an iterator that yields nothing and reports err.
func failed(err error) *kolokv.Iterator {
	return kolokv.NewIterator(func(func([]byte, []byte) bool) {}, func() error { return err })
}

// Close releases the DB; the braid stays open. Closing twice is not an error.
func (db *DB) Close() error {
	db.closed.Store(true)
	return nil
}
