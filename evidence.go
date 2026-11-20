package kolobraid

import (
	"bytes"
	"errors"
	"fmt"
	"time"

	kololog "github.com/ciranda-dev/kolo-log"
)

// askRecord asks the peer that answered a pull with a second history for its record at index n-1, the index kolo-sync
// compared, to build evidence from; one RECORD_REQ outstanding per connection. The caller holds s.mu.
func (s *syncer) askRecord(r *remote, id kololog.LogID, n uint64) {
	if n == 0 || r.recordReq != nil || !r.alive {
		return
	}
	q := &recordReq{strand: id, index: n - 1}
	q.timer = time.AfterFunc(s.timeout, func() {
		s.mu.Lock()
		late := r.recordReq == q
		if late {
			r.recordReq = nil
		}
		s.mu.Unlock()
		if late {
			r.violate(fmt.Errorf("%w: no answer to a record request", ErrProtocol))
		}
	})
	r.recordReq = q
	select {
	case r.out <- recordReqMsg{Strand: id, Index: n - 1}:
	default:
	}
}

// recordAnswer answers a RECORD_REQ from the strand's log or replica, or else from the stored entries of other histories.
func (s *syncer) recordAnswer(r *remote, q recordReqMsg) recordMsg {
	if s.b.access(r.key) == accessNone {
		return recordMsg{}
	}
	src, _, ok := s.b.strandSrc(q.Strand)
	if !ok {
		return recordMsg{}
	}
	if rec, err := src.Record(q.Index); err == nil {
		return recordMsg{Record: &rec}
	}
	var out *kololog.Record
	_ = scanPrefix(s.b.db, be8(keyServed+string(q.Strand[:]), q.Index), func(k, v []byte) error {
		if e, err := decodeServed(k, v); err == nil {
			out = &e.Record
		}
		return errStop
	})
	return recordMsg{Record: out}
}

// onRecord checks a RECORD answer and turns a record that differs from the held one into evidence.
func (r *remote) onRecord(m recordMsg) error {
	s, b := r.s, r.s.b
	s.mu.Lock()
	q := r.recordReq
	r.recordReq = nil
	s.mu.Unlock()
	if q == nil {
		return fmt.Errorf("%w: a record answer without a request", ErrProtocol)
	}
	q.timer.Stop()
	if m.Record == nil {
		return nil
	}
	theirs := *m.Record
	key, _, ok := b.strandInfo(q.strand)
	if !ok {
		return nil
	}
	if theirs.Index != q.index || !kololog.VerifyRecord(key, q.strand, theirs) {
		return fmt.Errorf("%w: a record of another index, key or strand", ErrProtocol)
	}
	src, _, ok := b.strandSrc(q.strand)
	if !ok {
		return nil
	}
	mine, err := src.Record(q.index)
	if err != nil {
		return nil
	}
	if a, c := mine.Encode(), theirs.Encode(); !bytes.Equal(a, c) {
		if err := b.addEvidence(evidenceBody{Key: key, Strand: q.strand, Type: evidenceRecords, A: a, B: c}); errors.Is(err, ErrInvalidArgument) {
			return fmt.Errorf("%w: %w", ErrProtocol, err)
		}
	}
	return nil
}

// onEvidence takes evidence against a followed strand of its key; the decoder has verified both signed objects. A peer sends
// each piece once per connection.
func (r *remote) onEvidence(m evidenceMsg) error {
	b := r.s.b
	ev := m.Evidence
	key, _, ok := b.strandInfo(ev.Strand)
	if !ok || !bytes.Equal(key, ev.Key) {
		r.s.ignored.Add(1)
		return nil
	}
	if r.gotEv[ev.Strand] {
		return fmt.Errorf("%w: evidence against one strand sent twice", ErrProtocol)
	}
	r.gotEv[ev.Strand] = true
	if b.holdsEvidence(ev.Strand) {
		return nil
	}
	if err := b.addEvidence(ev); errors.Is(err, ErrInvalidArgument) {
		return fmt.Errorf("%w: %w", ErrProtocol, err)
	}
	return nil
}

func (b *Braid) holdsEvidence(id kololog.LogID) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.evidence[id] != nil
}
