// Package prefixdb wraps a kolostore.Backend so that every key lives under a fixed prefix.
package prefixdb

import (
	"bytes"
	"errors"
	"sync/atomic"

	kolostore "github.com/ciranda-dev/kolo-store"
)

// Successor returns the smallest key that is greater than every key starting with prefix, or nil when there is none (an empty
// prefix or one of 0xff bytes only).
func Successor(prefix []byte) []byte {
	i := len(prefix)
	for i > 0 && prefix[i-1] == 0xff {
		i--
	}
	if i == 0 {
		return nil
	}
	out := bytes.Clone(prefix[:i])
	out[i-1]++
	return out
}

type backend struct {
	b      kolostore.Backend
	prefix []byte
	closed atomic.Bool
}

// New returns a Backend whose keys are stored in b under prefix. Close marks only the wrapper closed and leaves b open, so
// several wrappers can share one backend. An empty prefix returns b itself.
func New(b kolostore.Backend, prefix []byte) kolostore.Backend {
	if len(prefix) == 0 {
		return b
	}
	return &backend{b: b, prefix: bytes.Clone(prefix)}
}

func (w *backend) key(k []byte) []byte {
	out := make([]byte, 0, len(w.prefix)+len(k))
	return append(append(out, w.prefix...), k...)
}

func (w *backend) Get(key []byte) ([]byte, error) {
	if w.closed.Load() {
		return nil, kolostore.ErrClosed
	}
	return w.b.Get(w.key(key))
}

func (w *backend) Put(key, value []byte) error {
	if w.closed.Load() {
		return kolostore.ErrClosed
	}
	return w.b.Put(w.key(key), value)
}

func (w *backend) Delete(key []byte) error {
	if w.closed.Load() {
		return kolostore.ErrClosed
	}
	return w.b.Delete(w.key(key))
}

// bounds maps the range [start, end) of the wrapper to the range of the wrapped backend; end nil means no upper bound.
func (w *backend) bounds(start, end []byte) (lo, hi []byte) {
	lo = w.key(start)
	if end == nil {
		return lo, Successor(w.prefix)
	}
	return lo, w.key(end)
}

func (w *backend) Scan(start, end []byte, reverse bool, limit int, fn func(key, value []byte) bool) error {
	if w.closed.Load() {
		return kolostore.ErrClosed
	}
	lo, hi := w.bounds(start, end)
	return w.b.Scan(lo, hi, reverse, limit, func(k, v []byte) bool {
		return fn(k[len(w.prefix):], v)
	})
}

func (w *backend) NewBatch() kolostore.Batch {
	return &batch{w: w, b: w.b.NewBatch()}
}

func (w *backend) Close() error {
	w.closed.Store(true)
	return nil
}

var errBatchUsed = errors.New("kolobraid: batch already committed")

// batch writes into an underlying batch under the prefix.
type batch struct {
	w    *backend
	b    kolostore.Batch
	used bool
}

// WrapBatch returns a Batch that adds its writes to b under prefix, so one commit can span several prefixes. A DeleteRange with a
// nil end, or in a prefix with no successor, removes every key of the prefix from the start on: it lists them in db at call time.
func WrapBatch(b kolostore.Batch, db kolostore.Backend, prefix []byte) kolostore.Batch {
	return &batch{w: &backend{b: db, prefix: bytes.Clone(prefix)}, b: b}
}

func (b *batch) Put(key, value []byte) { b.b.Put(b.w.key(key), value) }

func (b *batch) Delete(key []byte) { b.b.Delete(b.w.key(key)) }

func (b *batch) DeleteRange(start, end []byte) {
	lo, hi := b.w.bounds(start, end)
	if hi != nil {
		b.b.DeleteRange(lo, hi)
		return
	}
	_ = b.w.b.Scan(lo, nil, false, 0, func(k, _ []byte) bool {
		b.b.Delete(k)
		return true
	})
}

func (b *batch) Commit() error {
	if b.used {
		return errBatchUsed
	}
	b.used = true
	return b.b.Commit()
}
