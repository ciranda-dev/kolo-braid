package kolobraid

import (
	"encoding/binary"
	"errors"
	"fmt"
	"iter"
	"slices"

	kolocodec "github.com/ciranda-dev/kolo-codec"
	kolostore "github.com/ciranda-dev/kolo-store"
)

const progressTag = "kolo-braid/view/v0"

// Keys of the view under the braid's prefix: view pairs under "v", the roster under "r" (written by the library with the entry that
// changes it), and the view's own records under "mview/".
const (
	viewPrefix   = 'v'
	rosterPrefix = 'r'
	progressKey  = "mview/progress"
	checkpoint   = "mview/cp/"
	rejectedKey  = "mview/rej/"
)

// progress is the settled state of the view, committed with every durable chunk: positions settled, the running digest after the
// last one, and where the settled batch list stands (batches finished, entries done of the next one).
type progress struct {
	Next    uint64
	Digest  Digest
	Batches uint64
	Offset  uint64
}

func (p progress) encode() []byte {
	var e kolocodec.Encoder
	e.PutString(progressTag)
	e.PutUvarint(p.Next)
	e.PutFixed(p.Digest[:])
	e.PutUvarint(p.Batches)
	e.PutUvarint(p.Offset)
	return e.Bytes()
}

// decodeProgress decodes the stored progress record; it errors for anything the encoder would not write.
func decodeProgress(b []byte) (progress, error) {
	r := newReader(b, ErrCorrupt)
	if tag := r.str(len(progressTag)); r.err == nil && tag != progressTag {
		r.failf("wrong progress tag")
	}
	var p progress
	p.Next = r.uvarint(^uint64(0))
	p.Digest = r.array()
	p.Batches = r.uvarint(^uint64(0))
	p.Offset = r.uvarint(^uint64(0))
	if err := r.end(); err != nil {
		return progress{}, err
	}
	if p.Batches > p.Next || p.Offset > p.Next {
		return progress{}, fmt.Errorf("%w: progress counts exceed the position", ErrCorrupt)
	}
	return p, nil
}

func posKey(prefix string, pos uint64) []byte {
	return binary.BigEndian.AppendUint64([]byte(prefix), pos)
}

// checkpointDue reports whether the batch that moves the settled count from before to after reaches a multiple of every.
func checkpointDue(before, after uint64, every int) bool {
	return after/uint64(every) > before/uint64(every)
}

// checkpointRec is a settled position that seats claim a digest for.
type checkpointRec struct {
	Position uint64
	Digest   Digest
}

// Checkpoint returns the digest stored for a checkpoint position, or false when none is stored.
func (v *view) Checkpoint(pos uint64) (Digest, bool, error) {
	val, err := v.raw.Get(posKey(checkpoint, pos))
	if errors.Is(err, kolostore.ErrNotFound) {
		return Digest{}, false, nil
	}
	if err != nil {
		return Digest{}, false, err
	}
	if len(val) != hashSize {
		return Digest{}, false, fmt.Errorf("%w: checkpoint record", ErrCorrupt)
	}
	return Digest(val), true, nil
}

// Verify compares a digest that a quorum of seats certified for pos with the one this peer settled. known is false while the position
// is not settled here. A mismatch halts the view and returns an error wrapping ErrDiverged.
func (v *view) Verify(pos uint64, certified Digest) (known bool, err error) {
	if !v.opMu.TryLock() {
		return false, errReentered
	}
	defer v.opMu.Unlock()
	mine, ok, err := v.Checkpoint(pos)
	if err != nil || !ok {
		return false, err
	}
	if mine != certified {
		err = fmt.Errorf("%w at position %d", ErrDiverged, pos)
		v.mu.Lock()
		v.halt = err
		v.mu.Unlock()
		return true, err
	}
	return true, nil
}

func (v *view) Rejected(pos uint64) (bool, error) {
	_, err := v.raw.Get(posKey(rejectedKey, pos))
	if errors.Is(err, kolostore.ErrNotFound) {
		return false, nil
	}
	return err == nil, err
}

// durablePairs yields the settled pairs in key order, a page at a time; a storage error ends the sequence and is left in *fail.
func (v *view) durablePairs(fail *error) iter.Seq2[[]byte, []byte] {
	return func(yield func([]byte, []byte) bool) {
		from := []byte{}
		for {
			var ks, vs [][]byte
			size := 0
			if err := v.db.Scan(from, nil, false, pageCount, func(k, val []byte) bool {
				ks, vs = append(ks, k), append(vs, val)
				size += len(k) + len(val)
				return size < pageBytes
			}); err != nil {
				*fail = err
				return
			}
			for i := range ks {
				if !yield(ks[i], vs[i]) {
					return
				}
			}
			if len(ks) == 0 || (len(ks) < pageCount && size < pageBytes) {
				return
			}
			from = append(slices.Clone(ks[len(ks)-1]), 0)
		}
	}
}

func (v *view) ContentDigest() (Digest, error) {
	if !v.opMu.TryLock() {
		return Digest{}, errReentered
	}
	defer v.opMu.Unlock()
	var fail error
	pos := v.prog.Next
	if pos > 0 {
		pos--
	}
	d := contentDigest(pos, v.durablePairs(&fail))
	return d, fail
}
