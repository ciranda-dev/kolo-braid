package kolobraid

import (
	"bytes"
	"crypto/ed25519"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"slices"

	kolocodec "github.com/ciranda-dev/kolo-codec"
	kololog "github.com/ciranda-dev/kolo-log"
	kolostore "github.com/ciranda-dev/kolo-store"

	"github.com/ciranda-dev/kolo-braid/internal/dag"
	"github.com/ciranda-dev/kolo-braid/internal/prefixdb"
	"github.com/ciranda-dev/kolo-braid/internal/settle"
)

// Keys under the braid's prefix besides the view's: "l" key one kolo-store Store per member key, "a" second histories, "t"
// temporary stores, "y" the views the settled order is replayed into (to check a successor; for SettledAt "yc" its cursor and
// "ys" ‖ sequence each snapshot), "x" the entry index and "mb" the braid's own records. Open clears "t" and "y": one Braid value
// per prefix at a time.
const (
	storePrefix     = 'l'
	tempPrefix      = 't'
	scratchPrefix   = 'y'
	keySettledAt    = "ys"
	keyReplayCursor = "yc"
	metaTag         = "kolo-braid/meta/v0"
	keyMeta         = "mbg"
	keyStrand       = "mbs/"
	keyPivot        = "mbp/"
	keyConfig       = "mbc/"
	keyReleased     = "mbr"
	keyEvidence     = "mbe/"
	keyHead         = "mbh"
	keyClaim        = "mbq/"
	keyMyClaim      = "mbk"
	keySeeded       = "mbd"
	keyMark         = "mbm/"
	keySeg          = "xs"
	keyEntry        = "xe"
	keyPos          = "xp"
)

// meta is what Open needs first: the group, the founder and the own strand.
type meta struct {
	Group         GroupID
	FounderKey    ed25519.PublicKey
	FounderStrand kololog.LogID
	FounderHeader []byte
	OwnStrand     kololog.LogID
	Anchor        *anchorRec
}

func (m meta) encode() []byte {
	var e kolocodec.Encoder
	e.PutString(metaTag)
	e.PutFixed(m.Group[:])
	e.PutFixed(m.FounderKey)
	e.PutFixed(m.FounderStrand[:])
	e.PutBytes(m.FounderHeader)
	e.PutFixed(m.OwnStrand[:])
	e.PutBool(m.Anchor != nil)
	if m.Anchor != nil {
		e.PutFixed(m.Anchor.Pivot[:])
		e.PutUvarint(m.Anchor.Config)
	}
	return e.Bytes()
}

func decodeMeta(b []byte) (meta, error) {
	r := newReader(b, ErrCorrupt)
	if tag := r.str(len(metaTag)); r.err == nil && tag != metaTag {
		r.failf("wrong meta tag")
	}
	var m meta
	m.Group = r.array()
	m.FounderKey = r.fixed(ed25519.PublicKeySize)
	m.FounderStrand = r.array()
	m.FounderHeader = r.bytes(maxHeader)
	m.OwnStrand = r.array()
	if r.flag() {
		m.Anchor = &anchorRec{Pivot: r.array()}
		m.Anchor.Config = r.uvarint(math.MaxUint64)
	}
	return m, r.end()
}

// strandRec is a strand the braid follows: its key and signed log header.
type strandRec struct {
	Key    ed25519.PublicKey
	Header []byte
}

func (s strandRec) encode() []byte {
	var e kolocodec.Encoder
	e.PutFixed(s.Key)
	e.PutBytes(s.Header)
	return e.Bytes()
}

func decodeStrandRec(b []byte) (strandRec, error) {
	r := newReader(b, ErrCorrupt)
	s := strandRec{Key: r.fixed(ed25519.PublicKeySize), Header: r.bytes(maxHeader)}
	return s, r.end()
}

// pivotRec is one committed pivot as stored: enough to replay its batch from the entry index.
type pivotRec struct {
	ID        EntryID
	Config    uint64
	Round     uint64
	Activates bool
	Clock     dag.Clock
	// Next is the first position after the pivot's batch.
	Next uint64
}

func putClock(e *kolocodec.Encoder, c dag.Clock) {
	e.PutUvarint(uint64(len(c)))
	for _, v := range c {
		e.PutUvarint(v)
	}
}

func (r *reader) clock() dag.Clock {
	n := r.uvarint(uint64(r.d.Len()))
	if r.err != nil {
		return nil
	}
	c := make(dag.Clock, 0, n)
	for i := uint64(0); i < n && r.err == nil; i++ {
		c = append(c, r.uvarint(math.MaxUint64))
	}
	return c
}

func (p pivotRec) encode() []byte {
	var e kolocodec.Encoder
	e.PutFixed(p.ID[:])
	e.PutUvarint(p.Config)
	e.PutUvarint(p.Round)
	e.PutBool(p.Activates)
	putClock(&e, p.Clock)
	e.PutUvarint(p.Next)
	return e.Bytes()
}

func decodePivotRec(b []byte) (pivotRec, error) {
	r := newReader(b, ErrCorrupt)
	var p pivotRec
	p.ID = r.array()
	p.Config = r.uvarint(math.MaxUint64)
	p.Round = r.uvarint(math.MaxUint64)
	p.Activates = r.flag()
	p.Clock = r.clock()
	p.Next = r.uvarint(math.MaxUint64)
	return p, r.end()
}

// configRec is a settler config as stored, for replaying the Seat flags of its batches.
type configRec struct {
	Rule       QuorumRule
	Seats      []seatRec
	Activating EntryID
}

func (c configRec) encode() []byte {
	var e kolocodec.Encoder
	e.PutUvarint(uint64(c.Rule))
	e.PutUvarint(uint64(len(c.Seats)))
	for _, s := range c.Seats {
		e.PutFixed(s.Key)
		e.PutFixed(s.Strand[:])
	}
	e.PutFixed(c.Activating[:])
	return e.Bytes()
}

func decodeConfigRec(b []byte) (configRec, error) {
	r := newReader(b, ErrCorrupt)
	var c configRec
	c.Rule = QuorumRule(r.uvarint(uint64(TwoThirds)))
	n := r.uvarint(math.MaxInt32)
	if r.err == nil && n > uint64(r.d.Len()/(ed25519.PublicKeySize+len(kololog.LogID{}))) {
		r.fail(kolocodec.ErrShortBuffer)
	}
	for i := uint64(0); i < n && r.err == nil; i++ {
		c.Seats = append(c.Seats, seatRec{Key: r.fixed(ed25519.PublicKeySize), Strand: r.array()})
	}
	c.Activating = r.array()
	return c, r.end()
}

// released is the settlement state saved with each settled batch: everything a restart needs to continue from it.
type released struct {
	Pivots   uint64
	Next     uint64
	Last     EntryID
	Frontier dag.Clock
	Segments []dag.Segment
	Floors   map[kololog.LogID]uint64
	Settle   []byte
}

func (s released) encode() []byte {
	var e kolocodec.Encoder
	e.PutUvarint(s.Pivots)
	e.PutUvarint(s.Next)
	e.PutFixed(s.Last[:])
	putClock(&e, s.Frontier)
	e.PutUvarint(uint64(len(s.Segments)))
	for _, g := range s.Segments {
		e.PutFixed(g.Strand[:])
		e.PutUvarint(g.Start)
		e.PutUvarint(uint64(g.Parent + 1))
		e.PutUvarint(g.Released)
		e.PutBool(g.Base != nil)
		if g.Base != nil {
			putClock(&e, g.Base)
		}
		e.PutFixed(g.First[:])
	}
	ids := make([]kololog.LogID, 0, len(s.Floors))
	for id := range s.Floors {
		ids = append(ids, id)
	}
	slices.SortFunc(ids, func(a, b kololog.LogID) int { return bytes.Compare(a[:], b[:]) })
	e.PutUvarint(uint64(len(ids)))
	for _, id := range ids {
		e.PutFixed(id[:])
		e.PutUvarint(s.Floors[id])
	}
	e.PutBytes(s.Settle)
	return e.Bytes()
}

func decodeReleased(b []byte) (released, error) {
	r := newReader(b, ErrCorrupt)
	var s released
	s.Pivots = r.uvarint(math.MaxUint64)
	s.Next = r.uvarint(math.MaxUint64)
	s.Last = r.array()
	s.Frontier = r.clock()
	n := r.uvarint(uint64(r.d.Len() / 36))
	for i := uint64(0); i < n && r.err == nil; i++ {
		var g dag.Segment
		g.Strand = r.array()
		g.Start = r.uvarint(math.MaxUint64)
		g.Parent = int(r.uvarint(math.MaxInt32)) - 1
		g.Released = r.uvarint(math.MaxUint64)
		if r.flag() {
			g.Base = r.clock()
			if g.Base == nil && r.err == nil {
				g.Base = dag.Clock{}
			}
		}
		g.First = r.array()
		s.Segments = append(s.Segments, g)
	}
	nf := r.uvarint(uint64(r.d.Len() / 33))
	s.Floors = make(map[kololog.LogID]uint64, nf)
	var prev *kololog.LogID
	for i := uint64(0); i < nf && r.err == nil; i++ {
		id := kololog.LogID(r.array())
		if prev != nil && bytes.Compare(prev[:], id[:]) >= 0 {
			r.failf("strand floors not ascending")
		}
		prev = &id
		s.Floors[id] = r.uvarint(math.MaxUint64)
	}
	s.Settle = bytes.Clone(r.bytes(r.d.Len()))
	return s, r.end()
}

func be8(prefix string, v uint64) []byte { return binary.BigEndian.AppendUint64([]byte(prefix), v) }

// markKey is the key of a settlement round mark: its config, seat, segment and index, so a config's marks scan in order.
func markKey(m settle.Mark) []byte {
	k := be8(keyMark, m.Config)
	k = binary.BigEndian.AppendUint32(k, uint32(m.Seat))
	k = binary.BigEndian.AppendUint32(k, uint32(m.Seg))
	return binary.BigEndian.AppendUint64(k, m.Index)
}

func storedMarks(db kolostore.Backend, config uint64) ([]settle.Mark, error) {
	var out []settle.Mark
	err := scanPrefix(db, be8(keyMark, config), func(k, v []byte) error {
		if len(k) != len(keyMark)+24 {
			return fmt.Errorf("%w: round mark key", ErrCorrupt)
		}
		round, n := binary.Uvarint(v)
		if n <= 0 || n != len(v) {
			return fmt.Errorf("%w: round mark", ErrCorrupt)
		}
		k = k[len(keyMark)+8:]
		out = append(out, settle.Mark{Config: config, Seat: int(binary.BigEndian.Uint32(k)), Seg: int(binary.BigEndian.Uint32(k[4:])),
			Index: binary.BigEndian.Uint64(k[8:]), Round: round})
		return nil
	})
	return out, err
}

func segKey(seg int, index uint64) []byte {
	k := binary.BigEndian.AppendUint32([]byte(keySeg), uint32(seg))
	return binary.BigEndian.AppendUint64(k, index)
}

func entryKey(strand kololog.LogID, index uint64, id EntryID) []byte {
	k := append([]byte(keyEntry), strand[:]...)
	k = binary.BigEndian.AppendUint64(k, index)
	return append(k, id[:]...)
}

// segEntry is the index record of a placed entry, stored under its segment and index.
type segEntry struct {
	ID     EntryID
	Class  dag.Class
	Record dag.Record
}

func encodeSegEntry(id EntryID, class dag.Class, record []byte) []byte {
	out := make([]byte, 0, 33+len(record))
	out = append(out, id[:]...)
	out = append(out, byte(class))
	return append(out, record...)
}

func decodeSegEntry(b []byte) (segEntry, error) {
	if len(b) < 33 || b[32] > byte(dag.Other) {
		return segEntry{}, fmt.Errorf("%w: entry index record", ErrCorrupt)
	}
	rec, err := dag.DecodeRecord(b[33:])
	if err != nil {
		return segEntry{}, fmt.Errorf("%w: %w", ErrCorrupt, err)
	}
	return segEntry{ID: EntryID(b[:32]), Class: dag.Class(b[32]), Record: rec}, nil
}

// posRec is one settled position: the entry and whether it was void.
type posRec struct {
	Strand kololog.LogID
	Index  uint64
	ID     EntryID
	Void   bool
}

func (p posRec) encode() []byte {
	var e kolocodec.Encoder
	e.PutFixed(p.Strand[:])
	e.PutUvarint(p.Index)
	e.PutFixed(p.ID[:])
	e.PutBool(p.Void)
	return e.Bytes()
}

func decodePosRec(b []byte) (posRec, error) {
	r := newReader(b, ErrCorrupt)
	var p posRec
	p.Strand = r.array()
	p.Index = r.uvarint(math.MaxUint64)
	p.ID = r.array()
	p.Void = r.flag()
	return p, r.end()
}

// lookup answers dag.Lookup from the entry index, rebuilding a clock from the nearest full one; recent answers are cached.
type lookup struct {
	db    kolostore.Backend
	segs  func() int
	cache map[dag.Link]lookupHit
	bytes int
}

type lookupHit struct {
	seg   int
	clock dag.Clock
}

// maxLookupBytes bounds the clocks kept by lookup, at 8 bytes per segment: what 4,096 clocks of 1,024 segments take.
const maxLookupBytes = 32 << 20

func (l *lookup) find(link dag.Link) (int, dag.Clock, bool, error) {
	if h, ok := l.cache[link]; ok {
		return h.seg, slices.Clone(h.clock), true, nil
	}
	v, err := l.db.Get(entryKey(kololog.LogID(link.Strand), link.Index, EntryID(link.ID)))
	if errors.Is(err, kolostore.ErrNotFound) {
		return 0, nil, false, nil
	}
	if err != nil {
		return 0, nil, false, err
	}
	if len(v) != 4 {
		return 0, nil, false, fmt.Errorf("%w: entry index key", ErrCorrupt)
	}
	seg := int(binary.BigEndian.Uint32(v))
	c, ok, err := l.clockAt(seg, link.Index, EntryID(link.ID))
	if errors.Is(err, dag.ErrOutsideTable) {
		// the record of an entry placed after the last stored table: it was not released, so it is placed again
		return 0, nil, false, nil
	}
	if err != nil || !ok {
		return 0, nil, ok, err
	}
	if l.bytes+8*len(c) > maxLookupBytes {
		clear(l.cache)
		l.bytes = 0
	}
	l.cache[link] = lookupHit{seg: seg, clock: slices.Clone(c)}
	l.bytes += 8 * len(c)
	return seg, c, true, nil
}

// clockAt rebuilds the clock of the entry with id at index of segment seg.
func (l *lookup) clockAt(seg int, index uint64, id EntryID) (dag.Clock, bool, error) {
	var chain []dag.Record
	for i := index; ; i-- {
		v, err := l.db.Get(segKey(seg, i))
		if errors.Is(err, kolostore.ErrNotFound) {
			return nil, false, nil
		}
		if err != nil {
			return nil, false, err
		}
		se, err := decodeSegEntry(v)
		if err != nil {
			return nil, false, err
		}
		if i == index && se.ID != id {
			return nil, false, nil
		}
		if se.Record.Seg != seg || se.Record.Index != i {
			return nil, false, fmt.Errorf("%w: entry index record out of place", ErrCorrupt)
		}
		chain = append(chain, se.Record)
		if se.Record.Full {
			break
		}
		if i == 0 || len(chain) > 256 {
			return nil, false, fmt.Errorf("%w: entry index without a full clock", ErrCorrupt)
		}
	}
	var c dag.Clock
	n := l.segs()
	for i := len(chain) - 1; i >= 0; i-- {
		next, err := chain[i].Clock(c, n)
		if err != nil {
			return nil, false, fmt.Errorf("%w: %w", ErrCorrupt, err)
		}
		c = next
	}
	return c, true, nil
}

// storeFor returns the kolo-store Store that holds the strands of key.
func (b *Braid) storeFor(key ed25519.PublicKey) *kolostore.Store {
	k := string(key)
	if s, ok := b.stores[k]; ok {
		return s
	}
	s := kolostore.New(prefixdb.New(b.db, append([]byte{storePrefix}, key...)))
	b.stores[k] = s
	return s
}

// getOpt reads key, returning nil and no error when it is not stored.
func getOpt(db kolostore.Backend, key []byte) ([]byte, error) {
	v, err := db.Get(key)
	if errors.Is(err, kolostore.ErrNotFound) {
		return nil, nil
	}
	return v, err
}

// scanPrefix calls fn for every key starting with prefix, a page at a time, so fn may use the backend.
func scanPrefix(db kolostore.Backend, prefix []byte, fn func(k, v []byte) error) error {
	from := slices.Clone(prefix)
	end := prefixdb.Successor(prefix)
	for {
		var ks, vs [][]byte
		if err := db.Scan(from, end, false, pageCount, func(k, v []byte) bool {
			ks, vs = append(ks, bytes.Clone(k)), append(vs, bytes.Clone(v))
			return true
		}); err != nil {
			return err
		}
		for i := range ks {
			if err := fn(ks[i], vs[i]); err != nil {
				return err
			}
		}
		if len(ks) < pageCount {
			return nil
		}
		from = append(ks[len(ks)-1], 0)
	}
}
