package kolobraid

import (
	"bytes"
	"crypto/ed25519"
	"encoding/hex"
	"iter"
	"slices"

	kolocodec "github.com/ciranda-dev/kolo-codec"
	kololog "github.com/ciranda-dev/kolo-log"
	"golang.org/x/crypto/blake2b"
)

// Hash domain tags. Every hash starts with its own tag, so a value hashed for one use is never valid for another.
const (
	entryIDTag = "kolo-braid/entry-id/v0"
	orderTag   = "kolo-braid/order/v0"
	configTag  = "kolo-braid/config/v0"
	digestTag  = "kolo-braid/digest/v0"
	contentTag = "kolo-braid/content/v0"
	topicTag   = "kolo-braid/topic/v0"
	joinTag    = "kolo-braid/join/v0"
	welcomeTag = "kolo-braid/welcome/v0"
	hashSize   = blake2b.Size256
)

// EntryID names one strand entry: the BLAKE2b-256 hash of its signed kolo-log record.
type EntryID [32]byte

// GroupID is the entry ID of the genesis entry.
type GroupID [32]byte

type configID [hashSize]byte

// Digest is a running digest, a content digest or a claim.
type Digest [32]byte

// String returns the ID as lowercase hex.
func (id EntryID) String() string { return hex.EncodeToString(id[:]) }

// String returns the ID as lowercase hex.
func (id GroupID) String() string { return hex.EncodeToString(id[:]) }

func (id configID) String() string { return hex.EncodeToString(id[:]) }

// String returns the digest as lowercase hex.
func (d Digest) String() string { return hex.EncodeToString(d[:]) }

// entryIDOf returns the entry ID of the entry whose kolo-log record has the given encoding (kololog.Record.Encode).
func entryIDOf(record []byte) EntryID {
	var e kolocodec.Encoder
	e.PutString(entryIDTag)
	e.PutBytes(record)
	return blake2b.Sum256(e.Bytes())
}

// orderKey returns the tie-break key of an entry in a batch ordered under seed: the pivot's entry ID, or the group ID for the
// tail that no predicted pivot covers.
func orderKey(seed [hashSize]byte, id EntryID) [hashSize]byte {
	var e kolocodec.Encoder
	e.PutString(orderTag)
	e.PutFixed(seed[:])
	e.PutFixed(id[:])
	return blake2b.Sum256(e.Bytes())
}

type seatRec struct {
	Key    ed25519.PublicKey
	Strand kololog.LogID
}

// configIDOf returns the ID of config number n with the given quorum rule and seats, which it sorts by key.
func configIDOf(n uint64, rule QuorumRule, seats []seatRec) configID {
	seats = slices.Clone(seats)
	slices.SortFunc(seats, func(a, b seatRec) int { return bytes.Compare(a.Key, b.Key) })
	var e kolocodec.Encoder
	e.PutString(configTag)
	e.PutUvarint(n)
	e.PutUvarint(uint64(rule))
	for _, s := range seats {
		e.PutFixed(s.Key)
		e.PutFixed(s.Strand[:])
	}
	return blake2b.Sum256(e.Bytes())
}

// entryStatus is the outcome of an entry at its position, as the running digest records it.
type entryStatus uint8

const (
	// statusApplied is an entry that apply accepted.
	statusApplied entryStatus = iota
	// statusRejected is an entry that apply refused with Reject.
	statusRejected
	// statusVoid is an entry that takes no effect at its position.
	statusVoid
)

type writeOp uint8

// View write operations. Their numbers are part of the digest.
const (
	opPut         writeOp = 1
	opDelete      writeOp = 2
	opDeleteRange writeOp = 3
)

// viewWrite is one view write of an entry: A is the key (the range start for OpDeleteRange), B the value (the range end for
// OpDeleteRange, empty for OpDelete).
type viewWrite struct {
	Op   writeOp
	A, B []byte
}

// initialDigest returns the running digest before position 0: the group ID.
func initialDigest(g GroupID) Digest { return Digest(g) }

// runningDigest returns the running digest after the entry with the given ID at position, which made the writes in order.
func runningDigest(prev Digest, position uint64, id EntryID, status entryStatus, writes []viewWrite) Digest {
	h, _ := blake2b.New256(nil)
	var e kolocodec.Encoder
	e.PutString(digestTag)
	e.PutFixed(prev[:])
	e.PutUvarint(position)
	e.PutFixed(id[:])
	e.PutUvarint(uint64(status))
	h.Write(e.Bytes())
	for _, w := range writes {
		var we kolocodec.Encoder
		we.PutUvarint(uint64(w.Op))
		we.PutBytes(w.A)
		we.PutBytes(w.B)
		h.Write(we.Bytes())
	}
	var d Digest
	h.Sum(d[:0])
	return d
}

// contentDigest returns the digest of a view at position: every pair, which the caller yields in ascending key order.
func contentDigest(position uint64, pairs iter.Seq2[[]byte, []byte]) Digest {
	h, _ := blake2b.New256(nil)
	var e kolocodec.Encoder
	e.PutString(contentTag)
	e.PutUvarint(position)
	h.Write(e.Bytes())
	for k, v := range pairs {
		var pe kolocodec.Encoder
		pe.PutBytes(k)
		pe.PutBytes(v)
		h.Write(pe.Bytes())
	}
	var d Digest
	h.Sum(d[:0])
	return d
}

func topicOf(g GroupID) [hashSize]byte {
	var e kolocodec.Encoder
	e.PutString(topicTag)
	e.PutFixed(g[:])
	return blake2b.Sum256(e.Bytes())
}
