package kolobraid

import (
	"crypto/ed25519"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"

	kolocodec "github.com/ciranda-dev/kolo-codec"
	kololog "github.com/ciranda-dev/kolo-log"
)

const (
	// MaxJoinRequest is the longest join request, in bytes: the join data bound of kolo-invite.
	MaxJoinRequest = 64 << 10
	// MaxWelcome is the longest welcome, in bytes: the default payload bound of kolo-invite.
	MaxWelcome = 1 << 20
)

const (
	// maxMessageSize is the longest stream message, length prefix excluded: the sync message bound of kolo-sync.
	maxMessageSize = 4 << 20
	// maxHeadSize is the longest encoded kolo-log head: a 10-byte size is its largest field.
	maxHeadSize = 192
	// maxAnnounceExtra is what an announce may carry besides its heads' worth of bytes.
	maxAnnounceExtra = 64
	// A request is its version, type and fields at their longest uvarints.
	maxRecordReq = 2 + 32 + 10
	maxEntryReq  = 2 + 32 + 10 + 10 + 32
	// maxRecordMsg is the version, the type, the length prefix and a record at the object bound.
	maxRecordMsg   = 2 + 2 + maxEvidenceObject
	maxEvidenceMsg = 8300
)

// JoinRequest is what a joiner sends in a join request: its new strand and app data.
type JoinRequest struct {
	Strand  kololog.LogID
	Header  []byte
	AppData []byte
}

// encodeJoinRequest returns the canonical encoding of r, or an error wrapping ErrInvalidArgument or ErrTooLarge when
// ParseJoinRequest would refuse it.
func encodeJoinRequest(r JoinRequest) ([]byte, error) {
	var e kolocodec.Encoder
	e.PutString(joinTag)
	e.PutFixed(r.Strand[:])
	e.PutBytes(r.Header)
	e.PutBytes(r.AppData)
	out := e.Bytes()
	if _, err := ParseJoinRequest(out); err != nil {
		return nil, encodeErr(err)
	}
	return out, nil
}

// ParseJoinRequest parses a join request. The header must be signed by the key it names, name the strand and use the Bytes
// encoding; whether that key is the joiner's is for the caller to check. It returns an error wrapping ErrMalformed.
func ParseJoinRequest(b []byte) (JoinRequest, error) {
	if len(b) > MaxJoinRequest {
		return JoinRequest{}, fmt.Errorf("%w: %w: join request of %d bytes", ErrMalformed, ErrTooLarge, len(b))
	}
	r := newReader(b, ErrMalformed)
	if tag := r.str(len(joinTag)); r.err == nil && tag != joinTag {
		r.failf("wrong tag")
	}
	var j JoinRequest
	j.Strand = kololog.LogID(r.array())
	j.Header = r.bytes(maxHeader)
	j.AppData = r.bytes(r.d.Len())
	if err := r.end(); err != nil {
		return JoinRequest{}, err
	}
	if err := checkSelfHeader(j.Header, j.Strand); err != nil {
		return JoinRequest{}, fmt.Errorf("%w: %w", ErrMalformed, err)
	}
	return j, nil
}

func checkSelfHeader(header []byte, strand kololog.LogID) error {
	h, err := kololog.DecodeLogHeader(header)
	if err != nil {
		return err
	}
	return checkHeader(header, h.LogKey, strand)
}

// anchorRec is the inviter's last committed pivot, the trusted pivot a joiner settles inside.
type anchorRec struct {
	Pivot  EntryID
	Config uint64
}

// welcomeMsg is the payload an inviter seals to a joiner.
type welcomeMsg struct {
	Group         GroupID
	FounderKey    ed25519.PublicKey
	FounderStrand kololog.LogID
	FounderHeader []byte
	InviterKey    ed25519.PublicKey
	InviterStrand kololog.LogID
	InviterHeader []byte
	// AddIndex and AddID name the AddMember entry on the inviter's strand.
	AddIndex uint64
	AddID    EntryID
	// Anchor is nil when the inviter has no committed pivot.
	Anchor *anchorRec
}

// encodeWelcome returns the canonical encoding of w, or an error wrapping ErrInvalidArgument or ErrTooLarge when ParseWelcome
// would refuse it.
func encodeWelcome(w welcomeMsg) ([]byte, error) {
	var e kolocodec.Encoder
	e.PutString(welcomeTag)
	e.PutFixed(w.Group[:])
	e.PutFixed(w.FounderKey)
	e.PutFixed(w.FounderStrand[:])
	e.PutBytes(w.FounderHeader)
	e.PutFixed(w.InviterKey)
	e.PutFixed(w.InviterStrand[:])
	e.PutBytes(w.InviterHeader)
	e.PutUvarint(w.AddIndex)
	e.PutFixed(w.AddID[:])
	e.PutBool(w.Anchor != nil)
	if a := w.Anchor; a != nil {
		e.PutFixed(a.Pivot[:])
		e.PutUvarint(a.Config)
	}
	out := e.Bytes()
	if _, err := parseWelcome(out); err != nil {
		return nil, encodeErr(err)
	}
	return out, nil
}

// parseWelcome parses a welcome. Each header must be signed by its key, name its strand and use the Bytes encoding. It returns
// an error wrapping ErrMalformed.
func parseWelcome(b []byte) (welcomeMsg, error) {
	if len(b) > MaxWelcome {
		return welcomeMsg{}, fmt.Errorf("%w: %w: welcome of %d bytes", ErrMalformed, ErrTooLarge, len(b))
	}
	r := newReader(b, ErrMalformed)
	if tag := r.str(len(welcomeTag)); r.err == nil && tag != welcomeTag {
		r.failf("wrong tag")
	}
	var w welcomeMsg
	w.Group = GroupID(r.array())
	w.FounderKey = r.fixed(ed25519.PublicKeySize)
	w.FounderStrand = kololog.LogID(r.array())
	w.FounderHeader = r.bytes(maxHeader)
	w.InviterKey = r.fixed(ed25519.PublicKeySize)
	w.InviterStrand = kololog.LogID(r.array())
	w.InviterHeader = r.bytes(maxHeader)
	w.AddIndex = r.uvarint(math.MaxUint64)
	w.AddID = EntryID(r.array())
	if r.flag() {
		w.Anchor = &anchorRec{Pivot: EntryID(r.array())}
		w.Anchor.Config = r.uvarint(math.MaxUint64)
	}
	if err := r.end(); err != nil {
		return welcomeMsg{}, err
	}
	if err := checkHeader(w.FounderHeader, w.FounderKey, w.FounderStrand); err != nil {
		return welcomeMsg{}, fmt.Errorf("%w: founder %w", ErrMalformed, err)
	}
	if err := checkHeader(w.InviterHeader, w.InviterKey, w.InviterStrand); err != nil {
		return welcomeMsg{}, fmt.Errorf("%w: inviter %w", ErrMalformed, err)
	}
	return w, nil
}

type messageType uint64

// Stream message types. Their numbers are part of the format.
const (
	msgAnnounce messageType = 1 + iota
	msgEvidence
	msgRecordReq
	msgRecord
	msgEntryReq
	msgEntry
)

// message is a stream message: AnnounceMsg, EvidenceMsg, RecordReqMsg, RecordMsg, EntryReqMsg or EntryMsg.
type message interface {
	Type() messageType
	encode(e *kolocodec.Encoder)
}

// announceMsg lists signed heads, one per strand, of a group.
type announceMsg struct {
	Group GroupID
	Heads []kololog.Head
}

// evidenceMsg carries one Evidence body of a group.
type evidenceMsg struct {
	Group    GroupID
	Evidence evidenceBody
}

// recordReqMsg asks for the record at an index of a strand.
type recordReqMsg struct {
	Strand kololog.LogID
	Index  uint64
}

// recordMsg answers RecordReqMsg; Record is nil when the peer does not hold it.
type recordMsg struct {
	Record *kololog.Record
}

// entryReqMsg asks for the entries Start to Index of the history of a strand in which the entry at Index has the given ID.
type entryReqMsg struct {
	Strand kololog.LogID
	Start  uint64
	Index  uint64
	ID     EntryID
}

// entryItem is one entry of an EntryMsg; Key is empty when the peer does not give the key.
type entryItem struct {
	Record     kololog.Record
	Ciphertext []byte
	Key        []byte
	Proof      kololog.InclusionProof
}

// entryMsg answers EntryReqMsg. Head is nil and Entries empty when the peer does not hold the entry. The receiver verifies the
// head, the proofs and the entries.
type entryMsg struct {
	Head    *kololog.Head
	Entries []entryItem
}

func (announceMsg) Type() messageType { return msgAnnounce }

func (evidenceMsg) Type() messageType { return msgEvidence }

func (recordReqMsg) Type() messageType { return msgRecordReq }

func (recordMsg) Type() messageType { return msgRecord }

func (entryReqMsg) Type() messageType { return msgEntryReq }

func (entryMsg) Type() messageType { return msgEntry }

func (m announceMsg) encode(e *kolocodec.Encoder) {
	e.PutFixed(m.Group[:])
	e.PutUvarint(uint64(len(m.Heads)))
	for _, h := range m.Heads {
		e.PutBytes(h.Encode())
	}
}

func (m evidenceMsg) encode(e *kolocodec.Encoder) {
	e.PutFixed(m.Group[:])
	var be kolocodec.Encoder
	m.Evidence.encode(&be)
	e.PutBytes(be.Bytes())
}

func (m recordReqMsg) encode(e *kolocodec.Encoder) {
	e.PutFixed(m.Strand[:])
	e.PutUvarint(m.Index)
}

func (m recordMsg) encode(e *kolocodec.Encoder) {
	if m.Record == nil {
		e.PutBytes(nil)
		return
	}
	e.PutBytes(m.Record.Encode())
}

func (m entryReqMsg) encode(e *kolocodec.Encoder) {
	e.PutFixed(m.Strand[:])
	e.PutUvarint(m.Start)
	e.PutUvarint(m.Index)
	e.PutFixed(m.ID[:])
}

func (m entryMsg) encode(e *kolocodec.Encoder) {
	if m.Head == nil {
		e.PutBytes(nil)
	} else {
		e.PutBytes(m.Head.Encode())
	}
	e.PutUvarint(uint64(len(m.Entries)))
	for _, it := range m.Entries {
		e.PutBytes(it.Record.Encode())
		e.PutBytes(it.Ciphertext)
		e.PutBytes(it.Key)
		e.PutBytes(it.Proof.Encode())
	}
}

// encodeMessage returns m as one frame: a 4-byte little-endian length, the version 0, the type and the fields. It returns an
// error wrapping ErrInvalidArgument or ErrTooLarge when DecodeMessage would refuse the frame.
func encodeMessage(m message, bd formatBounds) ([]byte, error) {
	if m == nil {
		return nil, fmt.Errorf("%w: nil message", ErrInvalidArgument)
	}
	var e kolocodec.Encoder
	e.PutUvarint(0)
	e.PutUvarint(uint64(m.Type()))
	m.encode(&e)
	p := e.Bytes()
	if len(p) > maxMessageSize {
		return nil, fmt.Errorf("%w: %w: message of %d bytes", ErrInvalidArgument, ErrTooLarge, len(p))
	}
	frame := binary.LittleEndian.AppendUint32(make([]byte, 0, 4+len(p)), uint32(len(p)))
	frame = append(frame, p...)
	if _, err := decodeMessage(frame, bd); err != nil {
		return nil, encodeErr(err)
	}
	return frame, nil
}

// decodeMessage parses one whole frame made by EncodeMessage. It returns an error wrapping ErrProtocol for a length that does
// not match, another version, an unknown type, a field over its bound, trailing bytes or a non-canonical encoding.
func decodeMessage(frame []byte, bd formatBounds) (message, error) {
	if !bd.valid() {
		return nil, fmt.Errorf("%w: bounds", ErrInvalidArgument)
	}
	if len(frame) < 4 || uint64(binary.LittleEndian.Uint32(frame)) != uint64(len(frame)-4) {
		return nil, fmt.Errorf("%w: frame length does not match", ErrProtocol)
	}
	return decodePayload(frame[4:], bd)
}

// readMessage reads one frame from r and parses it. The length and the type are checked before the rest is read, so a message
// never allocates more than its type allows, and admit, when it is set, decides from the prefix whether a message of that type
// may be read at all: its error is returned as is. It returns io.EOF when r ends before a frame starts, the reader's own error
// for a failed read, and an error wrapping ErrProtocol for a frame that breaks the format.
func readMessage(r io.Reader, bd formatBounds, admit func(messageType) error) (message, error) {
	if !bd.valid() {
		return nil, fmt.Errorf("%w: bounds", ErrInvalidArgument)
	}
	var pre [6]byte
	if _, err := io.ReadFull(r, pre[:4]); err != nil {
		return nil, readErr(err)
	}
	n := binary.LittleEndian.Uint32(pre[:4])
	if n < 2 || n > maxMessageSize {
		return nil, fmt.Errorf("%w: frame length %d", ErrProtocol, n)
	}
	if _, err := io.ReadFull(r, pre[4:]); err != nil {
		return nil, readErr(midFrame(err))
	}
	t := messageType(pre[5])
	if pre[4] != 0 || t < msgAnnounce || t > msgEntry {
		return nil, fmt.Errorf("%w: version %d type %d", ErrProtocol, pre[4], pre[5])
	}
	if int(n) > maxMessage(t, bd) {
		return nil, fmt.Errorf("%w: %w: %d bytes for type %d", ErrProtocol, ErrTooLarge, n, pre[5])
	}
	if admit != nil {
		if err := admit(t); err != nil {
			return nil, err
		}
	}
	p := make([]byte, n)
	copy(p, pre[4:])
	if _, err := io.ReadFull(r, p[2:]); err != nil {
		return nil, readErr(midFrame(err))
	}
	return decodePayload(p, bd)
}

// midFrame turns a clean end of input after the frame started into an unexpected one.
func midFrame(err error) error {
	if err == io.EOF {
		return io.ErrUnexpectedEOF
	}
	return err
}

// readErr maps an end of input inside a frame to a protocol error and keeps every other error, so a timeout stays a timeout.
func readErr(err error) error {
	if errors.Is(err, io.ErrUnexpectedEOF) {
		return fmt.Errorf("%w: truncated message", ErrProtocol)
	}
	return err
}

func maxMessage(t messageType, bd formatBounds) int {
	switch t {
	case msgAnnounce:
		return bd.MaxMembers*maxHeadSize + maxAnnounceExtra
	case msgEvidence:
		return maxEvidenceMsg
	case msgRecordReq:
		return maxRecordReq
	case msgRecord:
		return maxRecordMsg
	case msgEntryReq:
		return maxEntryReq
	}
	return maxMessageSize
}

func decodePayload(p []byte, bd formatBounds) (message, error) {
	r := newReader(p, ErrProtocol)
	r.uvarint(0)
	t := messageType(r.uvarint(uint64(msgEntry)))
	if r.err == nil && t < msgAnnounce {
		r.failf("unknown type")
	}
	if r.err == nil && len(p) > maxMessage(t, bd) {
		r.failf("%w: %d bytes for type %d", ErrTooLarge, len(p), t)
	}
	var m message
	switch t {
	case msgAnnounce:
		m = decodeAnnounce(r, bd)
	case msgEvidence:
		m = decodeEvidenceMsg(r, bd)
	case msgRecordReq:
		m = recordReqMsg{Strand: kololog.LogID(r.array()), Index: r.uvarint(math.MaxUint64)}
	case msgRecord:
		m = recordMsg{Record: decodeRecord(r, true)}
	case msgEntryReq:
		m = decodeEntryReq(r)
	case msgEntry:
		m = decodeEntry(r)
	}
	if err := r.end(); err != nil {
		return nil, err
	}
	return m, nil
}

func decodeAnnounce(r *reader, bd formatBounds) announceMsg {
	m := announceMsg{Group: GroupID(r.array())}
	n := r.uvarint(uint64(bd.MaxMembers))
	if r.err == nil && n > uint64(r.d.Len()/2) {
		r.fail(kolocodec.ErrShortBuffer)
	}
	for i := uint64(0); i < n && r.err == nil; i++ {
		if h := decodeHead(r, false); h != nil {
			m.Heads = append(m.Heads, *h)
		}
	}
	return m
}

// decodeHead reads one length-prefixed head; an empty one is nil when optional.
func decodeHead(r *reader, optional bool) *kololog.Head {
	b := r.bytes(maxHeadSize)
	if r.err != nil || (optional && len(b) == 0) {
		return nil
	}
	h, err := kololog.DecodeHead(b)
	if err != nil {
		r.fail(err)
		return nil
	}
	return &h
}

// decodeRecord reads one length-prefixed record; an empty one is nil when optional.
func decodeRecord(r *reader, optional bool) *kololog.Record {
	b := r.bytes(maxEvidenceObject)
	if r.err != nil || (optional && len(b) == 0) {
		return nil
	}
	rec, err := kololog.DecodeRecord(b)
	if err != nil {
		r.fail(err)
		return nil
	}
	return &rec
}

func decodeEvidenceMsg(r *reader, bd formatBounds) evidenceMsg {
	m := evidenceMsg{Group: GroupID(r.array())}
	b := r.bytes(maxEvidenceMsg)
	if r.err != nil {
		return m
	}
	body, err := decodeBody(KindEvidence, b, bd)
	if err != nil {
		r.fail(err)
		return m
	}
	m.Evidence = body.(evidenceBody)
	return m
}

func decodeEntryReq(r *reader) entryReqMsg {
	m := entryReqMsg{Strand: kololog.LogID(r.array())}
	m.Start = r.uvarint(math.MaxUint64)
	m.Index = r.uvarint(math.MaxUint64)
	m.ID = EntryID(r.array())
	if r.err == nil && m.Start > m.Index {
		r.failf("start after index")
	}
	return m
}

func decodeEntry(r *reader) entryMsg {
	m := entryMsg{Head: decodeHead(r, true)}
	n := r.uvarint(math.MaxUint64)
	if r.err == nil && n > uint64(r.d.Len()/4) {
		r.fail(kolocodec.ErrShortBuffer)
	}
	for i := uint64(0); i < n && r.err == nil; i++ {
		var it entryItem
		if rec := decodeRecord(r, false); rec != nil {
			it.Record = *rec
		}
		it.Ciphertext = r.bytes(r.d.Len())
		it.Key = r.bytes(entryKeySize)
		if r.err == nil && len(it.Key) != 0 && len(it.Key) != entryKeySize {
			r.failf("key of %d bytes", len(it.Key))
		}
		pb := r.bytes(maxEvidenceObject)
		if r.err == nil {
			p, err := kololog.DecodeInclusionProof(pb)
			if err != nil {
				r.fail(err)
			}
			it.Proof = p
		}
		m.Entries = append(m.Entries, it)
	}
	return m
}
