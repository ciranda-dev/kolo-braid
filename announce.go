package kolobraid

import (
	"bytes"
	"fmt"
	"slices"

	kololog "github.com/ciranda-dev/kolo-log"
)

// announce sends the heads that changed since the last announcement on this connection, all of them the first time, in
// messages of at most announceHeads heads. It reports whether it sent any.
func (r *remote) announce() bool {
	b := r.s.b
	var changed []kololog.Head
	for _, h := range b.heldHeads() {
		enc := h.Encode()
		if !bytes.Equal(r.sent[h.LogID], enc) {
			r.sent[h.LogID] = enc
			changed = append(changed, h)
		}
	}
	for c := range slices.Chunk(changed, r.s.bounds().announceHeads()) {
		r.send(announceMsg{Group: b.meta.Group, Heads: c})
	}
	return len(changed) > 0
}

func (r *remote) sendEvidence() {
	b := r.s.b
	b.mu.Lock()
	var evs []evidenceBody
	for _, id := range b.sortedEvidence() {
		if !r.sentEv[id] {
			r.sentEv[id] = true
			evs = append(evs, b.evidence[id].ev)
		}
	}
	b.mu.Unlock()
	for _, ev := range evs {
		r.send(evidenceMsg{Group: b.meta.Group, Evidence: ev})
	}
}

// onAnnounce takes the peer's heads. Only a head larger than what this peer holds with keys and than the peer's last one is
// verified; a larger verified head lets the scheduler pull that strand from the peer. Heads of strands not followed are kept.
func (r *remote) onAnnounce(m announceMsg) error {
	s, b := r.s, r.s.b
	for _, h := range m.Heads {
		if h.LogID == b.meta.OwnStrand {
			enc := h.Encode()
			if bytes.Equal(enc, r.ownSeen) {
				continue
			}
			r.ownSeen = enc
			if err := s.ownHead(r, h); err != nil {
				return err
			}
			continue
		}
		key, held, ok := b.strandInfo(h.LogID)
		if !ok {
			r.keepUnknown(h)
			continue
		}
		if err := s.takeHead(r, key, held, h); err != nil {
			return err
		}
	}
	return nil
}

// takeHead verifies a head of a followed strand when it is larger than what is held and than the peer's last head; the caller
// does not hold s.mu.
func (s *syncer) takeHead(r *remote, key []byte, held uint64, h kololog.Head) error {
	id := h.LogID
	if err := s.sameSize(r, key, h); err != nil {
		return err
	}
	s.mu.Lock()
	floor := max(r.heads[id], min(held, s.cursorLocked(id, held)))
	s.mu.Unlock()
	if h.Size <= floor {
		return nil
	}
	if _, err := kololog.VerifyHead(key, id, h); err != nil {
		return fmt.Errorf("%w: a head that does not verify: %w", ErrProtocol, err)
	}
	s.mu.Lock()
	if h.Size > r.heads[id] {
		r.heads[id] = h.Size
		s.newHead(id, h.Size, held)
	}
	s.mu.Unlock()
	s.signal()
	return nil
}

// newHead lets the missing targets of a strand be asked again when a verified head is past what this peer holds and past every
// head of the strand seen before: one head change, however many peers announce it. The caller holds s.mu.
func (s *syncer) newHead(id kololog.LogID, size, held uint64) {
	if size > held && size > s.topHead[id] {
		s.topHead[id] = size
		s.forgetMissing(id)
	}
}

// sameSize compares a head of the size this peer holds but other bytes, once per connection and strand: two verified heads of
// one size with different roots are evidence, and the record at the last index is asked for to fetch the other history.
func (s *syncer) sameSize(r *remote, key []byte, h kololog.Head) error {
	mine, ok := s.b.heldHead(h.LogID)
	if !ok || h.Size == 0 || mine.Size != h.Size || r.sameSeen[h.LogID] || bytes.Equal(mine.Encode(), h.Encode()) {
		return nil
	}
	r.sameSeen[h.LogID] = true
	theirs, err := kololog.VerifyHead(key, h.LogID, h)
	if err != nil {
		return fmt.Errorf("%w: a head that does not verify: %w", ErrProtocol, err)
	}
	if mv, err := kololog.VerifyHead(key, h.LogID, mine); err != nil || mv.Root() == theirs.Root() {
		return nil
	}
	_ = s.b.addEvidence(evidenceBody{Key: key, Strand: h.LogID, Type: evidenceHeads, A: mine.Encode(), B: h.Encode()})
	s.mu.Lock()
	s.askRecord(r, h.LogID, h.Size)
	s.mu.Unlock()
	return nil
}

// keepUnknown keeps the newest head per strand that cannot be checked yet, at most Limits.MaxUnknownHeads per connection.
func (r *remote) keepUnknown(h kololog.Head) {
	limit := r.s.b.maxUnknownHeads()
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	old, ok := r.unknown[h.LogID]
	switch {
	case ok && old.Size >= h.Size:
	case !ok && len(r.unknown) >= limit:
	default:
		r.unknown[h.LogID] = h
	}
}

// recheck takes the kept heads of strands that are now followed, once the peer is known; the caller holds s.mu.
func (s *syncer) recheck(r *remote, v syncState) {
	for id, h := range r.unknown {
		ss, ok := v.strands[id]
		if !ok {
			continue
		}
		delete(r.unknown, id)
		if id == s.b.meta.OwnStrand {
			continue
		}
		floor := max(r.heads[id], min(ss.held, s.cursorLocked(id, ss.held)))
		if h.Size <= floor {
			continue
		}
		if _, err := kololog.VerifyHead(ss.key, id, h); err != nil {
			r.violate(fmt.Errorf("%w: a head that does not verify: %w", ErrProtocol, err))
			return
		}
		r.heads[id] = h.Size
		s.newHead(id, h.Size, ss.held)
	}
}

// pendingHead reports whether a connected peer announced a head of a strand this peer does not follow yet that key signed:
// any peer can name a strand, only its key can sign a head of it.
func (s *syncer) pendingHead(id kololog.LogID, key []byte) bool {
	s.mu.Lock()
	var hs []kololog.Head
	for r := range s.remotes {
		if h, ok := r.unknown[id]; ok {
			hs = append(hs, h)
		}
	}
	s.mu.Unlock()
	return slices.ContainsFunc(hs, func(h kololog.Head) bool {
		_, err := kololog.VerifyHead(key, id, h)
		return err == nil
	})
}
