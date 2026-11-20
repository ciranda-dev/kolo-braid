// Package kv is a key-value map kept in a braid's view. Puts and deletes are data entries, applied in the braid's order: the
// last effective operation on a key wins, on every peer alike, and no clock takes part. The order among concurrent writes is
// decided by the braid and is not fair, so a rule such as "the first write wins" must not rely on it. The map has no access
// control: any active member can put or delete any key. When keys belong to members, check
// Entry.Writer (and Tx.Member for its roles) in the apply function before Store.Apply.
//
// A kv entry's body is, in kolo-codec: string "kolo-braid/kv/v0" ‖ uvarint op (1 put, 2 delete) ‖ bytes(key) ‖ bytes(value),
// the value empty for a delete.
package kv

import (
	"bytes"
	"context"
	"fmt"
	"iter"
	"slices"

	kolocodec "github.com/ciranda-dev/kolo-codec"

	kolobraid "github.com/ciranda-dev/kolo-braid"
)

const (
	// MaxKeySize is the longest key in bytes, with the store's prefix: the bound of a view key.
	MaxKeySize = kolobraid.MaxViewKey
	// MaxValueSize is the longest value in bytes: the bound of a view value.
	MaxValueSize = kolobraid.MaxViewValue
)

// tag starts the body of every kv entry.
const tag = "kolo-braid/kv/v0"

const (
	opPut    = 1
	opDelete = 2
)

// op is one decoded kv entry body.
type op struct {
	Delete bool
	Key    []byte
	Value  []byte
}

// encode returns the entry body of o: the tag, the op (1 put, 2 delete), the key and the value, which is empty for a delete.
func (o op) encode() ([]byte, error) {
	switch {
	case len(o.Key) > MaxKeySize:
		return nil, fmt.Errorf("%w: kv key of %d bytes", kolobraid.ErrTooLarge, len(o.Key))
	case len(o.Value) > MaxValueSize:
		return nil, fmt.Errorf("%w: kv value of %d bytes", kolobraid.ErrTooLarge, len(o.Value))
	case o.Delete && len(o.Value) > 0:
		return nil, fmt.Errorf("%w: kv delete with a value", kolobraid.ErrInvalidArgument)
	}
	var e kolocodec.Encoder
	e.PutString(tag)
	if o.Delete {
		e.PutUvarint(opDelete)
	} else {
		e.PutUvarint(opPut)
	}
	e.PutBytes(o.Key)
	e.PutBytes(o.Value)
	return e.Bytes(), nil
}

// isOp reports whether body starts with the kv tag, so that Apply handles it.
func isOp(body []byte) bool {
	t, err := kolocodec.NewDecoder(body).String(len(tag))
	return err == nil && t == tag
}

// decodeOp decodes a kv entry body in its canonical form; anything else wraps kolobraid.ErrMalformed.
func decodeOp(body []byte) (op, error) {
	d := kolocodec.NewDecoder(body)
	if t, err := d.String(len(tag)); err != nil || t != tag {
		return op{}, fmt.Errorf("%w: not a kv entry", kolobraid.ErrMalformed)
	}
	n, err := d.Uvarint()
	if err != nil || (n != opPut && n != opDelete) {
		return op{}, fmt.Errorf("%w: kv op", kolobraid.ErrMalformed)
	}
	o := op{Delete: n == opDelete}
	if o.Key, err = d.Bytes(MaxKeySize); err != nil {
		return op{}, fmt.Errorf("%w: kv key: %w", kolobraid.ErrMalformed, err)
	}
	if o.Value, err = d.Bytes(MaxValueSize); err != nil {
		return op{}, fmt.Errorf("%w: kv value: %w", kolobraid.ErrMalformed, err)
	}
	if err := d.Done(); err != nil {
		return op{}, fmt.Errorf("%w: kv entry: %w", kolobraid.ErrMalformed, err)
	}
	if o.Delete && len(o.Value) > 0 {
		return op{}, fmt.Errorf("%w: kv delete with a value", kolobraid.ErrMalformed)
	}
	return o, nil
}

// Store is a kv map whose pairs live in the view under its prefix: key k is the view key prefix ‖ k. No other key of the
// application's view may start with the prefix.
type Store struct {
	prefix []byte
}

// New returns a store with the given view key prefix; prefix plus key is at most MaxKeySize bytes.
func New(prefix []byte) *Store { return &Store{prefix: slices.Clone(prefix)} }

func (s *Store) key(k []byte) []byte { return append(slices.Clip(s.prefix), k...) }

// Apply applies e when it is a data entry whose body starts with the kv tag, and reports false for every other entry, so that an
// application routes its own entries next to it. A kv body that does not decode, or whose key does not fit with the prefix,
// is rejected: the same on every peer.
func (s *Store) Apply(tx *kolobraid.Tx, e *kolobraid.Entry) (handled bool, err error) {
	if e.Kind != kolobraid.KindData || !isOp(e.Body) {
		return false, nil
	}
	o, err := decodeOp(e.Body)
	if err != nil {
		return true, kolobraid.Reject(err.Error())
	}
	// a key that does not fit with the prefix is over the view's key bound: the write fails and the entry is rejected
	if o.Delete {
		err = tx.Delete(s.key(o.Key))
	} else {
		err = tx.Put(s.key(o.Key), o.Value)
	}
	if err != nil {
		return true, kolobraid.Reject(err.Error())
	}
	return true, nil
}

func (s *Store) write(ctx context.Context, b *kolobraid.Braid, o op) error {
	if b == nil {
		return fmt.Errorf("%w: nil braid", kolobraid.ErrInvalidArgument)
	}
	if len(s.prefix)+len(o.Key) > MaxKeySize {
		return fmt.Errorf("%w: kv key of %d bytes with a prefix of %d", kolobraid.ErrTooLarge, len(o.Key), len(s.prefix))
	}
	body, err := o.encode()
	if err != nil {
		return err
	}
	_, err = b.Append(ctx, body)
	return err
}

// Put appends an entry that sets key to value. It takes effect at the entry's position in the order; a body over the braid's
// MaxBody returns kolobraid.ErrTooLarge.
func (s *Store) Put(ctx context.Context, b *kolobraid.Braid, key, value []byte) error {
	return s.write(ctx, b, op{Key: key, Value: value})
}

// Delete appends an entry that removes key; a key that does not exist is not an error.
func (s *Store) Delete(ctx context.Context, b *kolobraid.Braid, key []byte) error {
	return s.write(ctx, b, op{Delete: true, Key: key})
}

// Get returns the value of key in v, or kolobraid.ErrNotFound. Read from Braid.Settled for values that never change again,
// from Braid.View for settled plus tentative ones.
func (s *Store) Get(v *kolobraid.Snapshot, key []byte) ([]byte, error) {
	return v.Get(s.key(key))
}

// Range yields the pairs of v with a key in [start, end) in ascending key order; an empty end means no upper bound, as in
// Snapshot.Scan, and start >= end yields nothing. A storage error ends it early, and v.Err reports it.
func (s *Store) Range(v *kolobraid.Snapshot, start, end []byte) iter.Seq2[[]byte, []byte] {
	return func(yield func([]byte, []byte) bool) {
		if len(end) > 0 && bytes.Compare(start, end) >= 0 {
			return
		}
		hi := upper(s.prefix)
		if len(end) > 0 {
			hi = s.key(end)
		}
		for k, val := range v.Scan(s.key(start), hi) {
			if !yield(k[len(s.prefix):], val) {
				return
			}
		}
	}
}

// upper returns the smallest key above every key that starts with p, or nil when there is none.
func upper(p []byte) []byte {
	for i := len(p) - 1; i >= 0; i-- {
		if p[i] != 0xff {
			return append(slices.Clone(p[:i]), p[i]+1)
		}
	}
	return nil
}
