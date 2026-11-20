package kolobraid

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"time"

	kolocodec "github.com/ciranda-dev/kolo-codec"
	kololog "github.com/ciranda-dev/kolo-log"
	kolosync "github.com/ciranda-dev/kolo-sync"

	"github.com/ciranda-dev/kolo-braid/internal/dag"
)

// keyServed keeps, under "ap" strand index ID, every entry fetched with ENTRY_REQ as an ENTRY answer serves it on: the record,
// its predecessor's ID, the ciphertext, the key, the inclusion proof and the signed head it was verified against.
const keyServed = "ap"

// entryBudget is what the entries of one ENTRY answer may take: the message bound less its head and counts.
const entryBudget = maxMessageSize - maxHeadSize - 64

type servedEntry struct {
	Record     kololog.Record
	Prev       EntryID
	Ciphertext []byte
	Key        []byte
	Proof      kololog.InclusionProof
	Head       kololog.Head
}

func servedKey(strand kololog.LogID, index uint64, id EntryID) []byte {
	return append(be8(keyServed+string(strand[:]), index), id[:]...)
}

func (e servedEntry) encode() []byte {
	var enc kolocodec.Encoder
	enc.PutBytes(e.Record.Encode())
	enc.PutFixed(e.Prev[:])
	enc.PutBytes(e.Ciphertext)
	enc.PutBytes(e.Key)
	enc.PutBytes(e.Proof.Encode())
	enc.PutBytes(e.Head.Encode())
	return enc.Bytes()
}

func decodeServed(k, v []byte) (servedEntry, error) {
	if len(k) != len(keyServed)+32+8+32 {
		return servedEntry{}, fmt.Errorf("%w: served entry key", ErrCorrupt)
	}
	r := newReader(v, ErrCorrupt)
	rb := r.bytes(maxEvidenceObject)
	var e servedEntry
	e.Prev = r.array()
	e.Ciphertext = bytes.Clone(r.bytes(maxMessageSize))
	e.Key = bytes.Clone(r.bytes(entryKeySize))
	pb := r.bytes(maxEvidenceObject)
	hb := r.bytes(maxHeadSize)
	if err := r.end(); err != nil {
		return servedEntry{}, err
	}
	rec, err := kololog.DecodeRecord(rb)
	if err != nil || entryIDOf(rb) != EntryID(k[len(k)-32:]) || rec.Index != beUint(k[len(keyServed)+32:len(k)-32]) ||
		rec.LogID != kololog.LogID(k[len(keyServed):len(keyServed)+32]) {
		return servedEntry{}, fmt.Errorf("%w: served entry record", ErrCorrupt)
	}
	if len(e.Key) != 0 && len(e.Key) != entryKeySize {
		return servedEntry{}, fmt.Errorf("%w: served entry key of %d bytes", ErrCorrupt, len(e.Key))
	}
	if e.Proof, err = kololog.DecodeInclusionProof(pb); err != nil || e.Proof.Index != rec.Index {
		return servedEntry{}, fmt.Errorf("%w: served entry proof", ErrCorrupt)
	}
	if e.Head, err = kololog.DecodeHead(hb); err != nil {
		return servedEntry{}, fmt.Errorf("%w: served entry head", ErrCorrupt)
	}
	e.Record = rec
	return e, nil
}

// requestEntries asks, for each strand, for its lowest target that is not missing, from one connected peer at a time and one
// ENTRY_REQ per connection. A target every peer answered without is missing until a new head of its strand or a new peer arrives.
// The caller holds s.mu.
func (s *syncer) requestEntries(v syncState, rs []*remote) {
	live := map[dag.Link]bool{}
	for _, t := range v.targets {
		live[t] = true
	}
	for t := range s.asked {
		if !live[t] {
			delete(s.asked, t)
		}
	}
	for t := range s.missing {
		if !live[t] {
			delete(s.missing, t)
		}
	}
	busy := map[kololog.LogID]bool{}
	for _, r := range rs {
		if r.entryReq != nil {
			busy[kololog.LogID(r.entryReq.target.Strand)] = true
		}
	}
	for _, t := range v.targets {
		id := kololog.LogID(t.Strand)
		if busy[id] || s.missing[t] {
			continue
		}
		var cand *remote
		open := false
		for _, r := range rs {
			if !r.alive || s.asked[t][r] {
				continue
			}
			open = true
			if cand == nil && r.entryReq == nil && r.known.Load() {
				cand = r
			}
		}
		if !open {
			s.missing[t] = true
			continue
		}
		busy[id] = true
		if cand == nil {
			continue
		}
		start := t.Index
		if !v.repair[t] {
			start -= min(t.Index, max(s.window[id], 2)-1)
		}
		q := &entryReq{target: t, start: start}
		r := cand
		q.timer = time.AfterFunc(s.timeout, func() { s.expired(r, q) })
		r.entryReq = q
		select {
		case r.out <- entryReqMsg{Strand: id, Start: start, Index: t.Index, ID: EntryID(t.ID)}:
		default:
		}
	}
}

// answerRead, when a test sets it, runs once an answer's request is taken and before the answer is checked.
var answerRead func(r *remote, q *entryReq)

func (s *syncer) expired(r *remote, q *entryReq) {
	s.mu.Lock()
	late := r.entryReq == q && !q.answering
	if late {
		r.entryReq = nil
	}
	s.mu.Unlock()
	if late {
		r.violate(fmt.Errorf("%w: no answer to an entry request", ErrProtocol))
	}
}

// answered records that a peer answered without a target; the caller holds s.mu.
func (s *syncer) answered(r *remote, t dag.Link) {
	if s.asked[t] == nil {
		s.asked[t] = map[*remote]bool{}
	}
	s.asked[t][r] = true
}

// forgetMissing lets the targets of a strand be asked again after a new head of it; the caller holds s.mu.
func (s *syncer) forgetMissing(id kololog.LogID) {
	for t := range s.missing {
		if kololog.LogID(t.Strand) == id {
			delete(s.missing, t)
		}
	}
	for t := range s.asked {
		if kololog.LogID(t.Strand) == id {
			delete(s.asked, t)
		}
	}
}

// onEntry checks an ENTRY answer: the head against the strand's key, every entry against its inclusion proof, consecutive
// indexes, the requested ID at the requested index, and a first entry this peer already holds (or index 0), so the history's
// tree links the new entries to what is held. An unheld first entry widens the next request. Then it stores and places them.
func (r *remote) onEntry(m entryMsg) error {
	s, b := r.s, r.s.b
	s.mu.Lock()
	q := r.entryReq
	if q != nil {
		q.answering = true
	}
	s.mu.Unlock()
	if q == nil {
		return fmt.Errorf("%w: an entry answer without a request", ErrProtocol)
	}
	if answerRead != nil {
		answerRead(r, q)
	}
	q.timer.Stop()
	defer s.signal()
	// the request stays outstanding until its answer is recorded, so the scheduler cannot ask the peer again in between
	defer func() {
		s.mu.Lock()
		if r.entryReq == q {
			r.entryReq = nil
		}
		s.mu.Unlock()
	}()
	t := q.target
	id := kololog.LogID(t.Strand)
	key, _, ok := b.strandInfo(id)
	if !ok || m.Head == nil || len(m.Entries) == 0 {
		s.mu.Lock()
		s.answered(r, t)
		s.mu.Unlock()
		return nil
	}
	vh, err := kololog.VerifyHead(key, id, *m.Head)
	if err != nil {
		return fmt.Errorf("%w: an entry answer's head: %w", ErrProtocol, err)
	}
	first := m.Entries[0].Record.Index
	if first < q.start || first > t.Index || uint64(len(m.Entries))-1 > t.Index-first {
		return fmt.Errorf("%w: entries outside the range asked", ErrProtocol)
	}
	vals := make([][]byte, 0, len(m.Entries))
	for i, it := range m.Entries {
		if it.Record.Index != first+uint64(i) || it.Proof.Index != it.Record.Index || it.Proof.Size != vh.Size() {
			return fmt.Errorf("%w: entries out of order", ErrProtocol)
		}
		val, err := kololog.OpenEntry(vh, it.Proof, kololog.Entry{Record: it.Record.Encode(), Ciphertext: it.Ciphertext, Key: it.Key})
		if errors.Is(err, kololog.ErrKeyUnavailable) {
			break
		}
		if err != nil {
			return fmt.Errorf("%w: an entry that does not verify: %w", ErrProtocol, err)
		}
		vals = append(vals, val)
	}
	items := m.Entries[:len(vals)]
	if len(items) == 0 {
		s.mu.Lock()
		s.answered(r, t)
		s.mu.Unlock()
		return nil
	}
	if last := items[len(items)-1]; last.Record.Index == t.Index && entryIDOf(last.Record.Encode()) != EntryID(t.ID) {
		return fmt.Errorf("%w: entries of another history than the one asked", ErrProtocol)
	}
	held, repair := b.holdsRecord(id, first, items[0].Record.Encode())
	if first > 0 && !held {
		s.mu.Lock()
		if first > q.start || q.start == 0 {
			s.answered(r, t)
		} else {
			s.window[id] = min(max(s.window[id], 2)*2, t.Index+1)
		}
		s.mu.Unlock()
		return nil
	}
	s.storeServed(id, vh.Head(), items)
	var hs []heldEntry
	for i, it := range items {
		if i == 0 && held && !repair {
			continue
		}
		var prev EntryID
		if i > 0 {
			prev = entryIDOf(items[i-1].Record.Encode())
		}
		hs = append(hs, heldEntry{Strand: id, Record: it.Record, Value: vals[i], Prev: prev})
	}
	_ = b.deliverMany(hs)
	s.mu.Lock()
	delete(s.window, id)
	s.mu.Unlock()
	return nil
}

// deliverMany places the verified entries of one answer in order, then settles once.
func (b *Braid) deliverMany(hs []heldEntry) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.usable(); err != nil {
		return err
	}
	if b.waiting {
		return ErrUnconfirmed
	}
	for _, h := range hs {
		if err := b.deliverLocked(h); err != nil {
			break
		}
	}
	return b.progress()
}

// holdsRecord reports whether this peer holds the record at an index of a strand, in its log or replica or as a second-history
// entry, and whether the replica holds it only without its value, so that the entry is a repair copy.
func (b *Braid) holdsRecord(id kololog.LogID, i uint64, enc []byte) (held, repair bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	st := b.strands[id]
	if st == nil {
		return false, false
	}
	if i < st.length() {
		if rec, err := st.recordAt(i); err == nil && bytes.Equal(rec.Encode(), enc) {
			return true, st.next == i && st.stop != nil
		}
	}
	eid := entryIDOf(enc)
	if n, ok := b.nodes[eid]; ok && n.strand == id && n.index == i {
		return true, false
	}
	if _, ok, err := b.hist.entry(id, i, eid); err == nil && ok {
		return true, false
	}
	return false, false
}

// prevOf returns the ID of the strand predecessor of a held entry at index i > 0: from the DAG or the stored second histories
// for an entry of another history, from the log or replica otherwise.
func (b *Braid) prevOf(id kololog.LogID, i uint64, enc []byte) EntryID {
	b.mu.Lock()
	defer b.mu.Unlock()
	eid := entryIDOf(enc)
	if n, ok := b.nodes[eid]; ok {
		return n.prev
	}
	if h, ok, err := b.hist.entry(id, i, eid); err == nil && ok {
		return h.Prev
	}
	if st := b.strands[id]; st != nil && i <= st.length() {
		if rec, err := st.recordAt(i - 1); err == nil {
			return entryIDOf(rec.Encode())
		}
	}
	return EntryID{}
}

// storeServed keeps the entries of a verified answer with its head and their proofs, so this peer can serve them on.
func (s *syncer) storeServed(id kololog.LogID, head kololog.Head, items []entryItem) {
	bt := s.b.db.NewBatch()
	for i, it := range items {
		e := servedEntry{Record: it.Record, Ciphertext: it.Ciphertext, Key: it.Key, Proof: it.Proof, Head: head}
		switch {
		case i > 0:
			e.Prev = entryIDOf(items[i-1].Record.Encode())
		case it.Record.Index > 0:
			e.Prev = s.b.prevOf(id, it.Record.Index, it.Record.Encode())
		}
		bt.Put(servedKey(id, it.Record.Index, entryIDOf(it.Record.Encode())), e.encode())
	}
	_ = bt.Commit()
}

// entryAnswer answers an ENTRY_REQ from the strand's log or replica, with its current head and fresh proofs, or else from the
// stored entries of other histories, with their stored head and proofs; keys only to a peer with key access.
func (s *syncer) entryAnswer(r *remote, q entryReqMsg) entryMsg {
	acc := s.b.access(r.key)
	if acc == accessNone {
		return entryMsg{}
	}
	keys := acc == accessKeys
	src, _, ok := s.b.strandSrc(q.Strand)
	if !ok {
		return entryMsg{}
	}
	if m, ok := answerFrom(src, q, keys); ok {
		return m
	}
	return s.servedAnswer(q, keys)
}

// itemSize is what an entry takes in an ENTRY answer.
func itemSize(it entryItem) int {
	return len(it.Record.Encode()) + len(it.Ciphertext) + len(it.Key) + len(it.Proof.Encode()) + 4*5
}

func answerFrom(src kolosync.Source, q entryReqMsg, keys bool) (entryMsg, bool) {
	h, err := src.Head()
	if err != nil || h.Size <= q.Index {
		return entryMsg{}, false
	}
	rec, err := src.Record(q.Index)
	if err != nil || entryIDOf(rec.Encode()) != q.ID {
		return entryMsg{}, false
	}
	var items []entryItem
	size := 0
	for i := q.Start; i <= q.Index; i++ {
		e, err := src.Entry(i)
		if err != nil || len(e.Marker) != 0 || e.Ciphertext == nil {
			break
		}
		p, err := src.InclusionProof(i, h.Size)
		if err != nil {
			break
		}
		r, err := kololog.DecodeRecord(e.Record)
		if err != nil {
			break
		}
		it := entryItem{Record: r, Ciphertext: e.Ciphertext, Proof: p}
		if keys {
			it.Key = e.Key
		}
		if size += itemSize(it); size > entryBudget && len(items) > 0 {
			break
		}
		items = append(items, it)
	}
	if len(items) == 0 {
		return entryMsg{}, false
	}
	return entryMsg{Head: &h, Entries: items}, true
}

// servedAnswer answers from the stored entries of other histories: the chain that ends at the requested entry, down to the
// start asked, from its lowest stored entry up while the entries share that entry's head.
func (s *syncer) servedAnswer(q entryReqMsg, keys bool) entryMsg {
	var chain []servedEntry
	size := 0
	idx, id := q.Index, q.ID
	for {
		k := servedKey(q.Strand, idx, id)
		v, err := getOpt(s.b.db, k)
		if err != nil || v == nil {
			break
		}
		e, err := decodeServed(k, v)
		if err != nil || (len(chain) > 0 && size+len(v) > entryBudget) {
			break
		}
		chain = append(chain, e)
		if size += len(v); idx == q.Start || idx == 0 {
			break
		}
		idx, id = idx-1, e.Prev
	}
	if len(chain) == 0 {
		return entryMsg{}
	}
	slices.Reverse(chain)
	head := chain[0].Head
	he := head.Encode()
	var items []entryItem
	for _, e := range chain {
		if !bytes.Equal(e.Head.Encode(), he) {
			break
		}
		it := entryItem{Record: e.Record, Ciphertext: e.Ciphertext, Proof: e.Proof}
		if keys {
			it.Key = e.Key
		}
		items = append(items, it)
	}
	return entryMsg{Head: &head, Entries: items}
}
