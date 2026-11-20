package kolobraid

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"

	kolocodec "github.com/ciranda-dev/kolo-codec"
	kololog "github.com/ciranda-dev/kolo-log"
	kolostore "github.com/ciranda-dev/kolo-store"
	kolosync "github.com/ciranda-dev/kolo-sync"

	"github.com/ciranda-dev/kolo-braid/internal/dag"
)

// keyCursor is the KeysFrom cursor of each replica: the first index that may still lack its key.
const keyCursor = "mbf/"

// syncState is what the scheduler needs of the braid, read at once.
type syncState struct {
	waiting bool
	strands map[kololog.LogID]strandState
	// targets are entries some held entry needs that a strand holds another entry, or only a delete marker, in place of.
	targets []dag.Link
	repair  map[dag.Link]bool
	behind  bool
	ownLen  uint64
}

type strandState struct {
	key  ed25519.PublicKey
	rep  *kolostore.Replica
	held uint64
	// need is the length to fetch: for an active strand its settled length plus MaxPullAhead, for a seat's strand also
	// MaxPullAhead past its newest held pulse, and for any strand what a held entry links.
	need uint64
}

func (b *Braid) syncState() (syncState, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed || b.fatal != nil {
		return syncState{}, false
	}
	v := syncState{waiting: b.waiting, strands: map[kololog.LogID]strandState{}, repair: map[dag.Link]bool{},
		behind: b.conf.behind && !b.conf.lost, ownLen: b.own.Len()}
	// past MaxPendingBytes only what pending entries link is fetched, which shrinks the pending set
	full := false
	if !b.waiting {
		_, size := b.d.Pending()
		full = size >= b.lim.MaxPendingBytes
	}
	// a seat pulses at least every pulseSpan own entries, fewer than MaxPullAhead, so fetching its strand MaxPullAhead past
	// its newest pulse held always reaches the next one; what any held entry links is fetched whatever the bound
	window := map[kololog.LogID]uint64{}
	if !b.waiting {
		seats := b.st.Config().Seats
		if p, ok := b.st.Pending(); ok {
			seats = append(slices.Clone(seats), p.Seats...)
		}
		for _, st := range seats {
			id := kololog.LogID(st.Strand)
			if i, ok := b.newestPulse[id]; ok {
				window[id] = i + 1 + uint64(b.lim.MaxPullAhead)
			}
		}
	}
	for id, st := range b.strands {
		ss := strandState{key: st.key, rep: st.rep, held: st.length()}
		ss.need = ss.held
		switch {
		case b.waiting:
			ss.need = math.MaxUint64
		default:
			if _, active := b.tent.ByStrand(dag.StrandID(id)); active && !full {
				ss.need = max(ss.need, st.floor+uint64(b.lim.MaxPullAhead))
			}
			ss.need = max(ss.need, window[id])
			if w := b.welcome; w != nil && w.Strand == id {
				ss.need = max(ss.need, w.Index+1)
			}
			if errors.Is(st.stop, kololog.ErrDeleted) && st.next < ss.held {
				if rec, err := st.recordAt(st.next); err == nil {
					t := dag.Link{Strand: dag.StrandID(id), Index: st.next, ID: dag.ID(entryIDOf(rec.Encode()))}
					v.targets = append(v.targets, t)
					v.repair[t] = true
				}
			}
		}
		v.strands[id] = ss
	}
	if !b.waiting {
		// per strand only the lowest target is asked, so links to entries that never exist cost one request per strand and
		// cannot hide the targets of other strands; a strand at its MaxForks is not asked, its next history would be refused
		lowest := map[[2]dag.StrandID]bool{}
		for _, t := range b.d.Unresolved() {
			id := kololog.LogID(t.Strand)
			ss, ok := v.strands[id]
			if !ok || id == b.meta.OwnStrand {
				continue
			}
			if t.Index >= ss.held {
				ss.need = max(ss.need, t.Index+1)
				v.strands[id] = ss
				continue
			}
			first := false
			for _, w := range b.d.WaitingStrands(t) {
				if k := [2]dag.StrandID{t.Strand, w}; !lowest[k] {
					lowest[k], first = true, true
				}
			}
			if first && !v.repair[t] && !b.d.ForkFull(t.Strand) {
				v.targets = append(v.targets, t)
			}
		}
	}
	slices.SortFunc(v.targets, compareLinks)
	return v, true
}

func compareLinks(a, c dag.Link) int {
	if x := bytes.Compare(a.Strand[:], c.Strand[:]); x != 0 {
		return x
	}
	switch {
	case a.Index < c.Index:
		return -1
	case a.Index > c.Index:
		return 1
	}
	return bytes.Compare(a.ID[:], c.ID[:])
}

func (s *syncer) loop(ctx context.Context) {
	defer s.b.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.wake:
		}
		s.schedule()
	}
}

// schedule starts the pulls, the catch-up and the entry requests that are due, and wakes every connection's sender.
func (s *syncer) schedule() {
	v, ok := s.b.syncState()
	if !ok {
		return
	}
	s.mu.Lock()
	rs := s.sorted()
	s.mu.Unlock()
	for _, r := range rs {
		r.known.Store(s.known(r.key))
	}
	s.mu.Lock()
	for _, r := range rs {
		if r.alive && r.known.Load() {
			s.recheck(r, v)
		}
	}
	s.pulls(v, rs)
	s.catchUp(v, rs)
	s.requestEntries(v, rs)
	s.mu.Unlock()
	for _, r := range rs {
		r.signal()
	}
	s.runs.Add(1)
}

// sorted returns the live connections in a fixed order; the caller holds s.mu.
func (s *syncer) sorted() []*remote {
	rs := make([]*remote, 0, len(s.remotes))
	for r := range s.remotes {
		rs = append(rs, r)
	}
	slices.SortFunc(rs, func(a, c *remote) int { return bytes.Compare(a.key, c.key) })
	return rs
}

// pulls starts a pull of each strand that is below its need, or holds entries without their keys, from the connected peer with
// the largest verified head of it: one pull per strand, at most maxPulls at once. The caller holds s.mu.
func (s *syncer) pulls(v syncState, rs []*remote) {
	ids := make([]kololog.LogID, 0, len(v.strands))
	for id := range v.strands {
		ids = append(ids, id)
	}
	slices.SortFunc(ids, func(a, c kololog.LogID) int { return bytes.Compare(a[:], c[:]) })
	for _, id := range ids {
		ss := v.strands[id]
		if ss.rep == nil || s.pulling[id] {
			continue
		}
		if s.running >= maxPulls {
			return
		}
		from := s.cursorLocked(id, ss.held)
		var best *remote
		for _, r := range rs {
			h := r.heads[id]
			if !r.alive || r.marked[id] || h <= r.failed[id] {
				continue
			}
			if (ss.held < ss.need && h > ss.held) || from < min(h, ss.held) {
				if best == nil || h > best.heads[id] {
					best = r
				}
			}
		}
		if best == nil {
			continue
		}
		s.pulling[id] = true
		s.running++
		r, rep, need := best, ss.rep, ss.need
		if ss.held >= ss.need {
			// only keys are due: fetch them without entries, which a pull would bring past the need
			r.wg.Go(func() { s.pullKeys(r, id, rep, from) })
			continue
		}
		r.wg.Go(func() { s.pull(r, id, rep, need, from) })
	}
}

// pull fills rep from the peer with no delete markers, stopping once it holds need entries, and hands what grew to the braid.
func (s *syncer) pull(r *remote, id kololog.LogID, rep *kolostore.Replica, need, from uint64) {
	defer func() {
		s.mu.Lock()
		delete(s.pulling, id)
		s.running--
		s.mu.Unlock()
		s.signal()
	}()
	ctx, cancel := context.WithCancel(r.ctx)
	defer cancel()
	gap := s.gap()
	r.wg.Go(func() {
		t := time.NewTicker(gap)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if rep.Len() >= need {
					cancel()
					return
				}
			}
		}
	})
	before := rep.Len()
	from = min(from, before)
	_, err := kolosync.PullWith(ctx, r.conn, rep, &kolosync.PullOptions{NoMarkers: true, KeysFrom: from})
	capped := err != nil && ctx.Err() != nil && r.ctx.Err() == nil
	cancel()
	s.done(r, id, rep, before, from, err, capped)
}

// pullKeys fetches the keys of the entries the replica holds without them, from the peer, without new entries.
func (s *syncer) pullKeys(r *remote, id kololog.LogID, rep *kolostore.Replica, from uint64) {
	defer func() {
		s.mu.Lock()
		delete(s.pulling, id)
		s.running--
		s.mu.Unlock()
		s.signal()
	}()
	before := rep.Len()
	h, err := rep.Head()
	var vh kololog.VerifiedHead
	if err == nil {
		vh, err = kololog.VerifyHead(rep.Header().LogKey, id, h)
	}
	if err == nil && from < vh.Size() {
		var keys []kololog.EntryKey
		keys, err = kolosync.FetchKeys(r.ctx, r.conn, vh, from, vh.Size()-1)
		for i, k := range keys {
			info, ierr := rep.EntryInfo(from + uint64(i))
			if k == nil || ierr != nil || info.Keyed || info.Deleted {
				continue
			}
			if aerr := rep.AddKey(from+uint64(i), k); aerr != nil {
				err = fmt.Errorf("%w: %w", kolosync.ErrInvalid, aerr)
				break
			}
		}
	}
	s.done(r, id, rep, before, from, err, false)
}

// keyedFrom returns the first index from from on that the replica holds without its key, passing deleted entries.
func keyedFrom(rep *kolostore.Replica, from uint64) uint64 {
	for ; from < rep.Len(); from++ {
		info, err := rep.EntryInfo(from)
		if err != nil || (!info.Deleted && !info.Keyed) {
			break
		}
	}
	return from
}

// done stores the new cursor, hands what grew to the braid and records how the peer answered.
func (s *syncer) done(r *remote, id kololog.LogID, rep *kolostore.Replica, before, from uint64, err error, capped bool) {
	// the cursor follows what the replica holds, also after an error, since every stored chunk is verified
	c := keyedFrom(rep, from)
	if c > from {
		s.saveCursor(id, c)
	}
	grew := rep.Len() > before || c > from
	if grew {
		_ = s.b.strandGrew(id)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case err == nil:
		if !grew {
			r.failed[id] = r.heads[id]
		}
	case capped:
	case errors.Is(err, kolosync.ErrMarked):
		r.marked[id] = true
	case errors.Is(err, kolosync.ErrDiverged):
		r.failed[id] = r.heads[id]
		s.askRecord(r, id, rep.Len())
	case errors.Is(err, kolosync.ErrInvalid), errors.Is(err, kolosync.ErrProtocol):
		r.violate(err)
	default:
		r.failed[id] = r.heads[id]
	}
}

// cursorLocked returns a replica's KeysFrom cursor, at most held; the caller holds s.mu.
func (s *syncer) cursorLocked(id kololog.LogID, held uint64) uint64 {
	c, ok := s.cursors[id]
	if !ok {
		if v, err := getOpt(s.b.db, append([]byte(keyCursor), id[:]...)); err == nil && v != nil {
			c, _ = decodeCursor(v)
		}
		s.cursors[id] = c
	}
	return min(c, held)
}

func (s *syncer) saveCursor(id kololog.LogID, c uint64) {
	s.mu.Lock()
	s.cursors[id] = c
	s.mu.Unlock()
	var e kolocodec.Encoder
	e.PutUvarint(c)
	_ = s.b.db.Put(append([]byte(keyCursor), id[:]...), e.Bytes())
}

func decodeCursor(b []byte) (uint64, error) {
	r := newReader(b, ErrCorrupt)
	c := r.uvarint(math.MaxUint64)
	return c, r.end()
}

// catchUp rebuilds the own strand from the connected peer with the longest consistent head of it when the strand is behind;
// the caller holds s.mu.
func (s *syncer) catchUp(v syncState, rs []*remote) {
	if !v.behind || v.waiting || s.catch {
		return
	}
	var best *remote
	for _, r := range rs {
		if r.alive && r.ownSize > v.ownLen && (best == nil || r.ownSize > best.ownSize) {
			best = r
		}
	}
	if best == nil {
		return
	}
	s.catch = true
	r := best
	r.wg.Go(func() {
		defer func() {
			s.mu.Lock()
			s.catch = false
			s.mu.Unlock()
			s.signal()
		}()
		h, ok := s.b.catchUpFrom(r.ctx, r.conn)
		if !ok {
			s.mu.Lock()
			r.ownSize = 0
			s.mu.Unlock()
			return
		}
		// the head caught up to is the peer's: it now confirms the strand, and the other peers' heads, which were longer before,
		// are asked again, as a settler needs them
		_ = s.ownHead(r, h)
		s.mu.Lock()
		for o := range s.remotes {
			if o.alive && o != r {
				o.wg.Go(o.fetchOwnHead)
			}
		}
		s.mu.Unlock()
	})
}

// catchUpFrom pulls the own strand from conn into a temporary replica and adopts it; it returns the head adopted, and false
// when it adopted nothing.
func (b *Braid) catchUpFrom(ctx context.Context, conn Conn) (kololog.Head, bool) {
	rep, err := b.catchUpReplica()
	if err != nil {
		return kololog.Head{}, false
	}
	if _, err = kolosync.PullWith(ctx, conn, rep, &kolosync.PullOptions{NoMarkers: true}); err == nil {
		h, herr := rep.Head()
		if err = b.adoptOwn(rep); err == nil && herr == nil {
			return h, true
		}
		if err == nil {
			return kololog.Head{}, false
		}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if rep.Close() == nil && b.temp != nil {
		_ = b.temp.Delete(rep.ID())
	}
	return kololog.Head{}, false
}
