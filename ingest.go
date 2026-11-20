package kolobraid

import (
	"bytes"
	"crypto/ed25519"
	"encoding/binary"
	"errors"
	"fmt"
	"iter"
	"slices"

	kolocodec "github.com/ciranda-dev/kolo-codec"
	kololog "github.com/ciranda-dev/kolo-log"
	kolostore "github.com/ciranda-dev/kolo-store"

	"github.com/ciranda-dev/kolo-braid/internal/dag"
	"github.com/ciranda-dev/kolo-braid/internal/roster"
	"github.com/ciranda-dev/kolo-braid/internal/settle"
)

// strand is one followed strand: the own log or another member's replica, read in index order.
type strand struct {
	id     kololog.LogID
	key    ed25519.PublicKey
	header []byte
	log    *kolostore.Log
	rep    *kolostore.Replica
	// next is the next index to read; last is the ID of the entry at next-1.
	next uint64
	last EntryID
	// unrel lists the read entries not yet released, by index; floor is the first index not released.
	unrel []dag.Link
	floor uint64
	// top is the newest placed entry.
	top    dag.Link
	hasTop bool
	// stop is why reading stopped: an entry without its key or with a delete marker. full is set when the pending bound
	// stopped it: the entry waits for a predecessor and is read again after the next placement.
	stop error
	full bool
}

func (s *strand) length() uint64 {
	if s.log != nil {
		return s.log.Len()
	}
	return s.rep.Len()
}

func (s *strand) recordAt(i uint64) (kololog.Record, error) {
	if s.log != nil {
		return s.log.Record(i)
	}
	return s.rep.Record(i)
}

func (s *strand) value(i uint64) ([]byte, error) {
	if s.log != nil {
		return s.log.Get(i)
	}
	return s.rep.Get(i)
}

// histories holds entries that no strand log or replica holds, second histories and repair copies, so that Open places them
// again and replays read them. The braid stores the entries it was given; the sync layer adds the heads and proofs it serves.
type histories interface {
	put(bt kolostore.Batch, h heldEntry)
	held() iter.Seq2[heldEntry, error]
	// entry returns a stored entry, ok false when it is not stored.
	entry(strand kololog.LogID, index uint64, id EntryID) (heldEntry, bool, error)
}

// storedHistories keeps second-history entries under "ae" strand index ID: the record, the predecessor's ID and the plaintext.
type storedHistories struct{ db kolostore.Backend }

const keyHistory = "ae"

func histKey(strand kololog.LogID, index uint64, id EntryID) []byte {
	return append(be8(keyHistory+string(strand[:]), index), id[:]...)
}

func (h storedHistories) put(bt kolostore.Batch, e heldEntry) {
	var enc kolocodec.Encoder
	enc.PutBytes(e.Record.Encode())
	enc.PutFixed(e.Prev[:])
	enc.PutBytes(e.Value)
	bt.Put(histKey(e.Strand, e.Record.Index, entryIDOf(e.Record.Encode())), enc.Bytes())
}

func decodeHeld(k, v []byte) (heldEntry, error) {
	if len(k) != len(keyHistory)+32+8+32 {
		return heldEntry{}, fmt.Errorf("%w: second-history key", ErrCorrupt)
	}
	r := newReader(v, ErrCorrupt)
	rb := r.bytes(maxEvidenceObject)
	var e heldEntry
	e.Strand = kololog.LogID(k[len(keyHistory) : len(keyHistory)+32])
	e.Prev = r.array()
	e.Value = bytes.Clone(r.bytes(r.d.Len()))
	if err := r.end(); err != nil {
		return heldEntry{}, err
	}
	rec, err := kololog.DecodeRecord(rb)
	if err != nil || entryIDOf(rb) != EntryID(k[len(k)-32:]) || rec.Index != binary.BigEndian.Uint64(k[len(keyHistory)+32:]) {
		return heldEntry{}, fmt.Errorf("%w: second-history record", ErrCorrupt)
	}
	e.Record = rec
	return e, nil
}

func (h storedHistories) held() iter.Seq2[heldEntry, error] {
	return func(yield func(heldEntry, error) bool) {
		err := scanPrefix(h.db, []byte(keyHistory), func(k, v []byte) error {
			e, err := decodeHeld(k, v)
			if err != nil {
				return err
			}
			if !yield(e, nil) {
				return errStop
			}
			return nil
		})
		if err != nil && !errors.Is(err, errStop) {
			yield(heldEntry{}, err)
		}
	}
}

var errStop = errors.New("kolobraid: stop")

func (h storedHistories) entry(strand kololog.LogID, index uint64, id EntryID) (heldEntry, bool, error) {
	k := histKey(strand, index, id)
	v, err := getOpt(h.db, k)
	if err != nil || v == nil {
		return heldEntry{}, false, err
	}
	e, err := decodeHeld(k, v)
	return e, err == nil, err
}

// heldEntry is one entry of a second history: its verified record and plaintext, and the ID of its strand predecessor.
type heldEntry struct {
	Strand kololog.LogID
	Record kololog.Record
	Value  []byte
	Prev   EntryID
}

// heldEvidence is evidence against a strand that this peer holds, and whether it appended it as an Evidence entry.
type heldEvidence struct {
	ev       evidenceBody
	appended bool
}

// errStrandKey is a strand that is followed under another key than the one an entry names for it.
var errStrandKey = fmt.Errorf("%w: the strand is followed under another key", ErrKnownStrand)

// follow starts following a strand: it opens the own log or the replica of key and records the strand.
func (b *Braid) follow(id kololog.LogID, key ed25519.PublicKey, header []byte, bt kolostore.Batch) error {
	if s, ok := b.strands[id]; ok {
		if !bytes.Equal(s.key, key) {
			return errStrandKey
		}
		return nil
	}
	s := &strand{id: id, key: slices.Clone(key), header: slices.Clone(header)}
	if id == b.meta.OwnStrand {
		s.log = b.own
	} else {
		// another strand of the own key was written on another device, so it is a replica like any other
		r, err := b.storeFor(key).OpenReplica(key, id, header)
		if err != nil {
			return err
		}
		s.rep = r
	}
	b.strands[id] = s
	b.fresh = append(b.fresh, id)
	b.sync.offer(s)
	if l, ok := b.early[id]; ok {
		delete(b.early, id)
		for _, ev := range l {
			b.holdEvidence(ev, bt, true)
		}
	}
	if bt != nil {
		bt.Put(append([]byte(keyStrand), id[:]...), strandRec{Key: key, Header: header}.encode())
	}
	return nil
}

// strandGrew reads the new entries of a followed strand after its replica grew; the sync layer calls it.
func (b *Braid) strandGrew(id kololog.LogID) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.usable(); err != nil {
		return err
	}
	s, ok := b.strands[id]
	if !ok {
		return fmt.Errorf("%w: strand not followed", ErrNotFound)
	}
	if b.waiting {
		return b.beginWaiting()
	}
	s.stop = nil
	if err := b.ingest(s); err != nil {
		return b.fail(err)
	}
	if err := b.checkWelcome(); err != nil {
		return err
	}
	return b.progress()
}

// replicaOf returns the replica of a followed strand, nil for the own strand or one not followed.
func (b *Braid) replicaOf(id kololog.LogID) *kolostore.Replica {
	b.mu.Lock()
	defer b.mu.Unlock()
	if s, ok := b.strands[id]; ok {
		return s.rep
	}
	return nil
}

// ingestAll reads every followed strand, and the entries of second histories.
func (b *Braid) ingestAll() error {
	for {
		n := len(b.strands)
		ids := make([]kololog.LogID, 0, n)
		for id := range b.strands {
			ids = append(ids, id)
		}
		slices.SortFunc(ids, func(a, c kololog.LogID) int { return bytes.Compare(a[:], c[:]) })
		for _, id := range ids {
			if err := b.ingest(b.strands[id]); err != nil {
				return err
			}
		}
		if len(b.strands) == n {
			break
		}
	}
	for h, err := range b.hist.held() {
		if err != nil {
			return err
		}
		if err := b.deliverLocked(h); err != nil && !errors.Is(err, ErrNotFound) && !errors.Is(err, errForkLimit) {
			return err
		}
	}
	return nil
}

func (b *Braid) ingest(s *strand) error {
	bt := b.db.NewBatch()
	dirty := false
	for s.stop == nil && s.next < s.length() {
		i := s.next
		rec, err := s.recordAt(i)
		if err != nil {
			return err
		}
		val, err := s.value(i)
		if errors.Is(err, kololog.ErrKeyUnavailable) || errors.Is(err, kololog.ErrDeleted) {
			s.stop = err
			if s.id == b.meta.OwnStrand && errors.Is(err, kololog.ErrDeleted) {
				b.erased = fmt.Errorf("%w: own entry %d", ErrErased, i)
			}
			break
		}
		if err != nil {
			return err
		}
		enc := rec.Encode()
		n := b.decodeNode(s, i, entryIDOf(enc), s.last, enc, val)
		ok, err := b.addNode(n, bt)
		if errors.Is(err, errForkLimit) {
			break
		}
		if err != nil {
			return err
		}
		if s.full = !ok; s.full {
			break
		}
		dirty = true
		s.unrel = append(s.unrel, dag.Link{Strand: dag.StrandID(s.id), Index: i, ID: dag.ID(n.id)})
		s.next, s.last = i+1, n.id
	}
	if !dirty {
		return nil
	}
	return bt.Commit()
}

// decodeNode decodes the plaintext of entry i of a strand whose entry i-1 has the ID prev.
func (b *Braid) decodeNode(s *strand, i uint64, id, prev EntryID, record, val []byte) *node {
	n := &node{id: id, strand: s.id, index: i, prev: prev, writer: s.key, record: record, kind: KindData, bad: true}
	c, body, err := decodeContent(val, s.id, i, b.bounds)
	switch {
	case err != nil:
	case c.Kind != KindGenesis && c.Group != b.meta.Group:
	case i > 0 && c.Prev != prev:
	default:
		n.bad, n.kind, n.body, n.links, n.dec = false, c.Kind, c.Body, c.Links, body
	}
	return n
}

// addNode puts n into the DAG; false means it was not stored because the pending bound is reached, and errForkLimit that the fork
// bound refused it.
func (b *Braid) addNode(n *node, bt kolostore.Batch) (bool, error) {
	if _, ok := b.nodes[n.id]; ok {
		return true, nil
	}
	dn := dag.Node{ID: dag.ID(n.id), Strand: dag.StrandID(n.strand), Index: n.index, Prev: dag.ID(n.prev), Class: n.class(),
		Size: len(n.record) + len(n.body) + len(n.links)*linkMin}
	if n.index == 0 {
		dn.Prev = dag.ID{}
	}
	if !n.bad {
		for _, l := range n.links {
			dn.Links = append(dn.Links, dag.Link{Strand: dag.StrandID(l.Strand), Index: l.Index, ID: dag.ID(l.ID)})
		}
	}
	if !n.bad && (n.kind == KindData || n.kind == KindSeed) {
		n.body, n.dec, n.lazy = nil, nil, true
	}
	b.nodes[n.id] = n
	if !n.bad && n.kind == KindPulse {
		if i, ok := b.newestPulse[n.strand]; !ok || n.index > i {
			b.newestPulse[n.strand] = n.index
		}
	}
	placed, err := b.d.Add(dn)
	if errors.Is(err, dag.ErrPendingFull) {
		delete(b.nodes, n.id)
		return false, nil
	}
	if err != nil {
		delete(b.nodes, n.id)
		return false, fmt.Errorf("%w: %w", ErrCorrupt, err)
	}
	for _, p := range placed {
		if err := b.onPlaced(b.nodes[EntryID(p.ID)], p, bt); err != nil {
			return false, err
		}
	}
	refused := false
	for _, id := range b.d.TakeRefused() {
		delete(b.nodes, EntryID(id))
		refused = refused || EntryID(id) == n.id
	}
	if refused {
		return false, errForkLimit
	}
	return true, nil
}

// errForkLimit is an entry that needs another fork of its strand than Limits.MaxForks keeps.
var errForkLimit = errors.New("kolobraid: the strand holds the most forks")

// onPlaced records a placed entry in the index and feeds settlement.
func (b *Braid) onPlaced(n *node, p dag.Placed, bt kolostore.Batch) error {
	n.placed = true
	b.placedSince = append(b.placedSince, n.id)
	b.segCount = max(b.segCount, p.Seg+1)
	bt.Put(segKey(p.Seg, p.Index), encodeSegEntry(n.id, n.class(), p.Record))
	bt.Put(entryKey(n.strand, n.index, n.id), binary.BigEndian.AppendUint32(nil, uint32(p.Seg)))
	if s, ok := b.strands[n.strand]; ok && (!s.hasTop || n.index >= s.top.Index) {
		s.top, s.hasTop = dag.Link{Strand: dag.StrandID(n.strand), Index: n.index, ID: dag.ID(n.id)}, true
	}
	if n.bad || n.kind != KindPulse {
		b.work++
	}
	if n.strand != b.meta.OwnStrand {
		for _, l := range n.links {
			if l.Strand == b.meta.OwnStrand && l.Index >= b.own.Len() {
				b.longer()
			}
		}
	}
	if n.bad {
		return nil
	}
	switch body := n.dec.(type) {
	case pulseBody:
		if n.strand == b.meta.OwnStrand {
			b.notePulse(n, body)
		}
		sp := settle.Pulse{ID: dag.ID(n.id), Key: key32(n.writer), Strand: dag.StrandID(n.strand), Config: body.Config,
			Activating: dag.ID(body.Activating)}
		if body.Readiness != nil {
			r := [32]byte(*body.Readiness)
			sp.Ready = &r
		}
		if err := b.st.Add(sp); err != nil {
			return fmt.Errorf("%w: %w", ErrCorrupt, err)
		}
		if body.Claim != nil {
			b.noteClaim(n, *body.Claim, bt)
		}
	case evidenceBody:
		b.holdEvidence(body, bt, true)
	}
	return nil
}

// deliver places an entry of a strand that its log or replica does not hold: an entry of a second history, or a repair copy of
// one the replica holds only as a delete marker. The caller has verified h.Record and h.Value against a signed head of that
// history (kololog.OpenEntry) and h.Prev against the record before it.
func (b *Braid) deliver(h heldEntry) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.usable(); err != nil {
		return err
	}
	if b.waiting {
		return ErrUnconfirmed
	}
	if err := b.deliverLocked(h); err != nil {
		return err
	}
	return b.progress()
}

func (b *Braid) deliverLocked(h heldEntry) error {
	s, ok := b.strands[h.Strand]
	if !ok {
		return fmt.Errorf("%w: strand not followed", ErrNotFound)
	}
	if !kololog.VerifyRecord(s.key, s.id, h.Record) {
		return fmt.Errorf("%w: record of another key or strand", ErrInvalidArgument)
	}
	i := h.Record.Index
	enc := h.Record.Encode()
	id := entryIDOf(enc)
	if i < s.length() {
		held, err := s.recordAt(i)
		if err != nil {
			return err
		}
		if he := held.Encode(); !bytes.Equal(he, enc) {
			b.holdEvidence(evidenceBody{Key: s.key, Strand: s.id, Type: evidenceRecords, A: he, B: enc}, nil, false)
		} else if s.next == i && s.stop != nil {
			// a repair copy of the entry the replica holds only as a marker
			bt := b.db.NewBatch()
			n := b.decodeNode(s, i, id, s.last, enc, h.Value)
			ok, err := b.addNode(n, bt)
			if err != nil || !ok {
				return err
			}
			b.hist.put(bt, heldEntry{Strand: h.Strand, Record: h.Record, Value: h.Value, Prev: s.last})
			s.unrel = append(s.unrel, dag.Link{Strand: dag.StrandID(s.id), Index: i, ID: dag.ID(id)})
			s.next, s.last, s.stop = i+1, id, nil
			if err := bt.Commit(); err != nil {
				return err
			}
			return b.ingest(s)
		} else {
			return nil
		}
	}
	bt := b.db.NewBatch()
	n := b.decodeNode(s, i, id, h.Prev, enc, h.Value)
	if _, err := b.addNode(n, bt); err != nil {
		return err
	}
	b.hist.put(bt, h)
	return bt.Commit()
}

// holdEvidence keeps verified evidence against a strand: settlement learns of it, and unless an Evidence entry carried it the
// writer loop appends it as one.
func (b *Braid) holdEvidence(ev evidenceBody, bt kolostore.Batch, fromEntry bool) {
	if _, ok := b.strands[ev.Strand]; !ok && fromEntry {
		b.keepEarly(ev)
		return
	}
	if !b.evidenceFits(ev) {
		return
	}
	h, ok := b.evidence[ev.Strand]
	if ok && (h.appended || !fromEntry) {
		return
	}
	if !ok {
		h = &heldEvidence{ev: ev}
		b.evidence[ev.Strand] = h
		b.st.Equivocated(dag.StrandID(ev.Strand))
		b.ev.push(Event{Kind: EventEquivocation, Member: ev.Key})
	}
	h.appended = fromEntry
	if enc, err := encodeBody(h.ev, b.bounds); err == nil {
		flag := byte(0)
		if h.appended {
			flag = 1
		}
		k, v := append([]byte(keyEvidence), ev.Strand[:]...), append([]byte{flag}, enc...)
		if bt != nil {
			bt.Put(k, v)
		} else {
			_ = b.db.Put(k, v)
		}
	}
	b.signal()
}

// maxEarlyKeys is how many keys an unfollowed strand keeps early evidence for: any member can accuse any strand under a key
// of its own, so one slot would let it push out the evidence against the strand's real key.
const maxEarlyKeys = 4

// keepEarly keeps an Evidence entry against a strand not followed yet, which the order may follow from an AddMember entry
// placed in the same read: one per key and at most maxEarlyKeys per strand, for at most 2 x MaxMembers strands, until a settled
// batch leaves no AddMember entry that names the strand. The caller holds b.mu.
func (b *Braid) keepEarly(ev evidenceBody) {
	if checkEvidence(ev) != nil {
		return
	}
	l := b.early[ev.Strand]
	if (l == nil && len(b.early) >= 2*b.bounds.MaxMembers) || len(l) >= maxEarlyKeys ||
		slices.ContainsFunc(l, func(e evidenceBody) bool { return bytes.Equal(e.Key, ev.Key) }) {
		return
	}
	b.early[ev.Strand] = append(l, ev)
}

// evidenceFits reports whether ev is valid against the strand it names: a log ID does not bind a key, so any key can sign
// objects that name any strand, and only the followed strand's own key counts.
func (b *Braid) evidenceFits(ev evidenceBody) bool {
	s, ok := b.strands[ev.Strand]
	return ok && bytes.Equal(s.key, ev.Key) && checkEvidence(ev) == nil
}

// addEvidence takes evidence that the sync layer received or built: two signed objects of one strand that cannot both be true.
func (b *Braid) addEvidence(ev evidenceBody) error {
	if err := checkEvidence(ev); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidArgument, err)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.usable(); err != nil {
		return err
	}
	if b.waiting {
		return ErrUnconfirmed
	}
	b.holdEvidence(ev, nil, false)
	return b.progress()
}

func (b *Braid) markAppended(strand kololog.LogID) {
	h := b.evidence[strand]
	if h == nil {
		return
	}
	h.appended = true
	if enc, err := encodeBody(h.ev, b.bounds); err == nil {
		_ = b.db.Put(append([]byte(keyEvidence), strand[:]...), append([]byte{1}, enc...))
	}
}

func (b *Braid) loadEvidence() error {
	return scanPrefix(b.db, []byte(keyEvidence), func(k, v []byte) error {
		if len(v) < 1 {
			return fmt.Errorf("%w: evidence record", ErrCorrupt)
		}
		body, err := decodeBody(KindEvidence, v[1:], b.bounds)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrCorrupt, err)
		}
		ev := body.(evidenceBody)
		if !b.evidenceFits(ev) {
			return nil
		}
		b.evidence[ev.Strand] = &heldEvidence{ev: ev, appended: v[0] == 1}
		b.st.Equivocated(dag.StrandID(ev.Strand))
		return nil
	})
}

// readEntry reads a placed or released entry from its strand or from the second histories.
func (b *Braid) readEntry(strandID kololog.LogID, index uint64, id EntryID) (*node, error) {
	return b.readFrom(b.strands, b.nodes, strandID, index, id)
}

// readFrom is readEntry over the given strands and in-memory nodes; nodes may be nil for a settled entry.
func (b *Braid) readFrom(strands map[kololog.LogID]*strand, nodes map[EntryID]*node, strandID kololog.LogID, index uint64,
	id EntryID) (*node, error) {
	s, ok := strands[strandID]
	if !ok {
		return nil, fmt.Errorf("%w: entry of a strand not followed", ErrCorrupt)
	}
	if index < s.length() {
		rec, err := s.recordAt(index)
		if err != nil {
			return nil, err
		}
		enc := rec.Encode()
		if entryIDOf(enc) == id {
			val, err := s.value(index)
			if err != nil {
				h, ok, herr := b.hist.entry(strandID, index, id)
				if herr != nil || !ok {
					if errors.Is(err, kololog.ErrDeleted) {
						return nil, fmt.Errorf("%w: entry %d: %w", ErrErased, index, err)
					}
					return nil, err
				}
				val = h.Value
			}
			var prev EntryID
			if index > 0 {
				pr, err := s.recordAt(index - 1)
				if err != nil {
					return nil, err
				}
				prev = entryIDOf(pr.Encode())
			}
			return b.decodeNode(s, index, id, prev, enc, val), nil
		}
	}
	if n, ok := nodes[id]; ok && !n.lazy {
		return n, nil
	}
	h, ok, err := b.hist.entry(strandID, index, id)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("%w: entry %d of a strand", errNotHeld, index)
	}
	return b.decodeNode(s, index, id, h.Prev, h.Record.Encode(), h.Value), nil
}

// release forgets the entries of a settled batch and moves each strand's floor past its released prefix.
func (b *Braid) release(ids []EntryID) {
	for _, id := range ids {
		delete(b.nodes, id)
	}
	// early evidence waits only for strands that an AddMember entry still to settle names
	if len(b.early) > 0 {
		named := map[kololog.LogID]bool{}
		for _, n := range b.nodes {
			if nm, ok := n.dec.(NewMember); ok {
				named[nm.Strand] = true
			}
		}
		for st := range b.early {
			if !named[st] {
				delete(b.early, st)
			}
		}
	}
	for _, s := range b.strands {
		k := 0
		for k < len(s.unrel) {
			if _, held := b.nodes[EntryID(s.unrel[k].ID)]; held {
				break
			}
			k++
		}
		if k > 0 {
			s.floor = s.unrel[k-1].Index + 1
			s.unrel = slices.Delete(s.unrel, 0, k)
		}
	}
}

func rosterKey(k ed25519.PublicKey) roster.Key { return roster.Key(key32(k)) }
