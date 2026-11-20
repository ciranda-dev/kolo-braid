package kolobraid

import (
	"bytes"
	"crypto/ed25519"
	"errors"
	"fmt"
	"math"
	"unicode/utf8"

	kolocodec "github.com/ciranda-dev/kolo-codec"
	kololog "github.com/ciranda-dev/kolo-log"
)

const entryTag = "kolo-braid/entry/v0"

const (
	// MaxAppTag is the longest application tag in a genesis entry, in bytes.
	MaxAppTag = 64
	// maxHeader is the longest signed kolo-log header an entry or message carries, in bytes.
	maxHeader = 512
	// maxEvidenceObject is the longest signed kolo-log record or head in an Evidence body, in bytes: kolo-log's input bound.
	maxEvidenceObject = 4 << 10
	// MaxViewKey is the longest key of a view pair, in bytes.
	MaxViewKey = 32 << 10
	// MaxViewValue is the longest value of a view pair, in bytes.
	MaxViewValue = 1 << 20

	// Ranges of GroupConfig. The reasons are in the field docs.
	minMaxMembers, maxMaxMembers = 2, 1024
	maxMaxSettlers               = 64
	minMaxBody, maxMaxBody       = 1 << 10, 3 << 20
	minCheckpoint, maxCheckpoint = 64, 1 << 20

	defaultMaxMembers  = 64
	defaultMaxSettlers = 7
	defaultMinSettlers = 4
	defaultMaxBody     = 1 << 20
	defaultCheckpoint  = 1024

	// maxFixedBody bounds the body of the kinds whose size follows from their fields: the largest is Evidence.
	maxFixedBody = 8300
	// seedPairMax is what one seed pair of the largest view key and value needs: the pair count and two length prefixes.
	seedPairMax = MaxViewKey + MaxViewValue + 16
	// entryKeySize is the size of a kolo-log per-entry key.
	entryKeySize = 32
)

// QuorumRule says how many settlers must agree.
type QuorumRule uint8

const (
	// Majority needs floor(n/2) + 1 seats and tolerates crashes.
	Majority QuorumRule = 1
	// TwoThirds needs floor(2n/3) + 1 seats and tolerates equivocating seats as well.
	TwoThirds QuorumRule = 2
)

// Role is a bit set of the rights of a member.
type Role uint8

// RoleAdmin may change the membership.
const RoleAdmin Role = 1

// Kind is the kind of an entry.
type Kind uint8

// Entry kinds. Their numbers are part of the format.
const (
	KindGenesis Kind = iota
	KindData
	KindAddMember
	KindRemoveMember
	KindSettlers
	KindEvidence
	KindPulse
	KindSeed
)

// GroupConfig is fixed at Create and agreed by every peer through the genesis entry.
type GroupConfig struct {
	// App is the application tag, 1 to 64 bytes of UTF-8.
	App string
	// Quorum is the quorum rule; the default is Majority.
	Quorum QuorumRule
	// MaxMembers is the most active members; the default 64 is one node's default peer budget, the range is 2 to 1024.
	MaxMembers int
	// MaxSettlers is the most settlers; the default is 7 (Majority then tolerates 3 offline) or MaxMembers if smaller, the range is 1 to 64.
	MaxSettlers int
	// MinSettlers is the seat count Admit grows to; the default is 4 (Majority has q 3, so one offline and one equivocating seat are tolerated) or MaxSettlers if smaller, the range is 1 to MaxSettlers.
	MinSettlers int
	// MaxBody is the longest data body; the default 1 MiB is the view's value bound, the range 1 KiB to 3 MiB keeps an entry within one 4 MiB sync message.
	MaxBody int
	// CheckpointEvery is the settled positions between digest claims; the default is 1024, the range 64 to 2^20.
	CheckpointEvery int
}

// WithDefaults returns c with every zero field except App set to its default.
func (c GroupConfig) WithDefaults() GroupConfig {
	if c.Quorum == 0 {
		c.Quorum = Majority
	}
	if c.MaxMembers == 0 {
		c.MaxMembers = defaultMaxMembers
	}
	if c.MaxSettlers == 0 {
		c.MaxSettlers = min(defaultMaxSettlers, c.MaxMembers)
	}
	if c.MinSettlers == 0 {
		c.MinSettlers = min(defaultMinSettlers, c.MaxSettlers)
	}
	if c.MaxBody == 0 {
		c.MaxBody = defaultMaxBody
	}
	if c.CheckpointEvery == 0 {
		c.CheckpointEvery = defaultCheckpoint
	}
	return c
}

// Validate returns an error wrapping ErrInvalidConfig unless every field is inside its range.
func (c GroupConfig) Validate() error {
	var why string
	switch {
	case len(c.App) < 1 || len(c.App) > MaxAppTag:
		why = "app tag must be 1 to 64 bytes"
	case !utf8.ValidString(c.App):
		why = "app tag is not valid UTF-8"
	case c.Quorum != Majority && c.Quorum != TwoThirds:
		why = "unknown quorum rule"
	case c.MaxMembers < minMaxMembers || c.MaxMembers > maxMaxMembers:
		why = "MaxMembers must be 2 to 1024"
	case c.MaxSettlers < 1 || c.MaxSettlers > maxMaxSettlers || c.MaxSettlers > c.MaxMembers:
		why = "MaxSettlers must be 1 to 64 and at most MaxMembers"
	case c.MinSettlers < 1 || c.MinSettlers > c.MaxSettlers:
		why = "MinSettlers must be 1 to MaxSettlers"
	case c.MaxBody < minMaxBody || c.MaxBody > maxMaxBody:
		why = "MaxBody must be 1 KiB to 3 MiB"
	case c.CheckpointEvery < minCheckpoint || c.CheckpointEvery > maxCheckpoint:
		why = "CheckpointEvery must be 64 to 2^20"
	default:
		return nil
	}
	return fmt.Errorf("%w: %s", ErrInvalidConfig, why)
}

// bounds returns the bounds that entries of the group are checked against.
func (c GroupConfig) bounds() formatBounds {
	return formatBounds{MaxMembers: c.MaxMembers, MaxSettlers: c.MaxSettlers, MaxBody: c.MaxBody}
}

// formatBounds are the group limits that decoders check an entry or message against.
type formatBounds struct {
	MaxMembers  int
	MaxSettlers int
	MaxBody     int
}

// genesisBounds returns the loosest bounds a group can have, for decoding a genesis entry before its own values are known.
func genesisBounds() formatBounds {
	return formatBounds{MaxMembers: maxMaxMembers, MaxSettlers: maxMaxSettlers, MaxBody: maxMaxBody}
}

func (b formatBounds) valid() bool {
	return b.MaxMembers >= minMaxMembers && b.MaxMembers <= maxMaxMembers &&
		b.MaxSettlers >= 1 && b.MaxSettlers <= min(maxMaxSettlers, b.MaxMembers) &&
		b.MaxBody >= minMaxBody && b.MaxBody <= maxMaxBody
}

func (b formatBounds) bodyMax(k Kind) int {
	switch k {
	case KindData:
		return b.MaxBody
	case KindSeed:
		return max(b.MaxBody, seedPairMax)
	}
	return maxFixedBody
}

// Link names one entry of another strand in the causal past of the entry that holds it.
type Link struct {
	Strand kololog.LogID
	Index  uint64
	ID     EntryID
}

// entryContent is the plaintext of one strand entry, the value of one kolo-log entry.
type entryContent struct {
	// Group is the group ID; all zero only in the genesis entry.
	Group GroupID
	Kind  Kind
	// Prev is the ID of the previous entry of the strand; all zero at index 0.
	Prev EntryID
	// Links name at most one entry per other strand, strictly ascending by strand log ID.
	Links []Link
	// Body is the encoded body of the kind; see EncodeBody and DecodeBody.
	Body []byte
}

// encodeContent returns the canonical encoding of c as entry index of the strand, or an error wrapping ErrInvalidArgument or
// ErrTooLarge when DecodeContent would refuse it.
func encodeContent(c entryContent, strand kololog.LogID, index uint64, bd formatBounds) ([]byte, error) {
	var e kolocodec.Encoder
	e.PutString(entryTag)
	e.PutFixed(c.Group[:])
	e.PutUvarint(uint64(c.Kind))
	e.PutFixed(c.Prev[:])
	e.PutUvarint(uint64(len(c.Links)))
	for _, l := range c.Links {
		e.PutFixed(l.Strand[:])
		e.PutUvarint(l.Index)
		e.PutFixed(l.ID[:])
	}
	e.PutBytes(c.Body)
	out := e.Bytes()
	if _, err := decodeEntryContent(out, strand, index, bd); err != nil {
		return nil, encodeErr(err)
	}
	return out, nil
}

// decodeEntryContent parses the entry at index of the strand with the given log ID. It returns an error wrapping ErrMalformed for
// anything that breaks the format: tag, kind, body, canonical form, links, bounds, or fields that do not fit the index.
func decodeEntryContent(b []byte, strand kololog.LogID, index uint64, bd formatBounds) (entryContent, error) {
	c, _, err := decodeContent(b, strand, index, bd)
	return c, err
}

// decodeContent is DecodeContent that also returns the decoded body.
func decodeContent(b []byte, strand kololog.LogID, index uint64, bd formatBounds) (entryContent, entryBody, error) {
	if !bd.valid() {
		return entryContent{}, nil, fmt.Errorf("%w: bounds", ErrInvalidArgument)
	}
	r := newReader(b, ErrMalformed)
	if tag := r.str(len(entryTag)); r.err == nil && tag != entryTag {
		r.failf("wrong tag")
	}
	var c entryContent
	c.Group = r.array()
	c.Kind = Kind(r.uvarint(uint64(KindSeed)))
	c.Prev = EntryID(r.array())
	switch {
	case r.err != nil:
	case (c.Kind == KindGenesis) != (c.Group == GroupID{}):
		r.failf("group field is zero only in the genesis entry")
	case c.Kind == KindGenesis && index != 0:
		r.failf("genesis entry not at index 0")
	case (index == 0) != (c.Prev == EntryID{}):
		r.failf("prev field is zero only at index 0")
	}
	n := r.uvarint(uint64(bd.MaxMembers - 1))
	switch {
	case r.err != nil:
	case c.Kind == KindGenesis && n > 0:
		r.failf("genesis entry with links")
	case n > uint64(r.d.Len()/linkMin):
		r.fail(kolocodec.ErrShortBuffer)
	}
	if n > 0 && r.err == nil {
		c.Links = make([]Link, 0, n)
	}
	for i := uint64(0); i < n && r.err == nil; i++ {
		var l Link
		l.Strand = kololog.LogID(r.array())
		l.Index = r.uvarint(math.MaxUint64)
		l.ID = EntryID(r.array())
		switch {
		case r.err != nil:
		case l.Strand == strand:
			r.failf("link to the entry's own strand")
		case len(c.Links) > 0 && bytes.Compare(c.Links[len(c.Links)-1].Strand[:], l.Strand[:]) >= 0:
			r.failf("links not strictly ascending by strand")
		}
		c.Links = append(c.Links, l)
	}
	c.Body = r.bytes(r.d.Len())
	if err := r.end(); err != nil {
		return entryContent{}, nil, err
	}
	body, err := decodeBody(c.Kind, c.Body, bd)
	if err != nil {
		return entryContent{}, nil, err
	}
	return c, body, nil
}

// linkMin is the size of the smallest encoded link.
const linkMin = 32 + 1 + 32

// entryBody is the decoded body of an entry: Data, Genesis, NewMember, RemoveMember, Settlers, Evidence, Pulse or Seed.
type entryBody interface {
	Kind() Kind
	encode(e *kolocodec.Encoder)
}

// dataBody is the body of a data entry: application bytes.
type dataBody []byte

// continuation says that a braid continues another, in its genesis entry.
type continuation struct {
	// Group is the group ID of the old braid.
	Group GroupID
	// Position is the old settled position the seed copies.
	Position uint64
	// Content is the content digest of the old view at Position.
	Content Digest
	// Seeds is the number of seed entries that follow the genesis entry.
	Seeds uint64
}

type genesisBody struct {
	Config GroupConfig
	// Continues is nil unless this braid is the successor of another.
	Continues *continuation
}

// NewMember is the body of an AddMember entry, and what AddMember takes.
type NewMember struct {
	Key    ed25519.PublicKey
	Strand kololog.LogID
	// Header is the strand's signed log header: at most MaxHeader bytes, signed by Key, naming Strand and the Bytes encoding.
	Header []byte
	Roles  Role
}

type removeBody struct {
	Key ed25519.PublicKey
}

// settlersBody is the body of a settlersBody entry: the settler keys, strictly ascending, and the quorum rule.
type settlersBody struct {
	Rule QuorumRule
	Keys []ed25519.PublicKey
}

// evidenceType says what the two objects of an Evidence body are.
type evidenceType uint8

const (
	// evidenceRecords is two kolo-log records of one index with different bytes.
	evidenceRecords evidenceType = 1
	// evidenceHeads is two kolo-log heads of one size with different roots.
	evidenceHeads evidenceType = 2
)

// evidenceBody is the body of an evidenceBody entry: two signed kolo-log objects of one strand that cannot both be true.
type evidenceBody struct {
	Key    ed25519.PublicKey
	Strand kololog.LogID
	Type   evidenceType
	// A and B are the encoded records or heads, each at most MaxEvidenceObject bytes, signed by Key for Strand.
	A, B []byte
}

// pulseClaim is a seat's claim of the running digest at a settled position.
type pulseClaim struct {
	Position uint64
	Digest   Digest
}

// pulseBody is the body of a pulse entry. Only pulses take part in settlement.
type pulseBody struct {
	// Config is the config number the pulse claims.
	Config uint64
	// Activating is the pivot that activated the config; set only when Config is at least 1.
	Activating EntryID
	// Claim is nil when the pulse claims no digest.
	Claim *pulseClaim
	// Readiness is the config ID the seat is ready for; nil when the pulse is not a readiness pulse.
	Readiness *configID
}

type seedPair struct {
	Key, Value []byte
}

// seedBody is the body of a seed entry: view pairs of a successor braid, keys strictly ascending.
type seedBody struct {
	Pairs []seedPair
}

func (dataBody) Kind() Kind { return KindData }

func (genesisBody) Kind() Kind { return KindGenesis }

// Kind returns KindAddMember.
func (NewMember) Kind() Kind { return KindAddMember }

func (removeBody) Kind() Kind { return KindRemoveMember }

func (settlersBody) Kind() Kind { return KindSettlers }

func (evidenceBody) Kind() Kind { return KindEvidence }

func (pulseBody) Kind() Kind { return KindPulse }

func (seedBody) Kind() Kind { return KindSeed }

const (
	flagClaim = 1 << iota
	flagReady
)

func (d dataBody) encode(e *kolocodec.Encoder) { e.PutFixed(d) }

func (g genesisBody) encode(e *kolocodec.Encoder) {
	c := g.Config
	e.PutString(c.App)
	e.PutUvarint(uint64(c.Quorum))
	e.PutUvarint(uint64(c.MaxMembers))
	e.PutUvarint(uint64(c.MaxSettlers))
	e.PutUvarint(uint64(c.MinSettlers))
	e.PutUvarint(uint64(c.MaxBody))
	e.PutUvarint(uint64(c.CheckpointEvery))
	e.PutBool(g.Continues != nil)
	if k := g.Continues; k != nil {
		e.PutFixed(k.Group[:])
		e.PutUvarint(k.Position)
		e.PutFixed(k.Content[:])
		e.PutUvarint(k.Seeds)
	}
}

func (w NewMember) encode(e *kolocodec.Encoder) {
	e.PutFixed(w.Key)
	e.PutFixed(w.Strand[:])
	e.PutUvarint(uint64(w.Roles))
	e.PutBytes(w.Header)
}

func (w removeBody) encode(e *kolocodec.Encoder) { e.PutFixed(w.Key) }

func (s settlersBody) encode(e *kolocodec.Encoder) {
	e.PutUvarint(uint64(s.Rule))
	e.PutUvarint(uint64(len(s.Keys)))
	for _, k := range s.Keys {
		e.PutFixed(k)
	}
}

func (v evidenceBody) encode(e *kolocodec.Encoder) {
	e.PutFixed(v.Key)
	e.PutFixed(v.Strand[:])
	e.PutUvarint(uint64(v.Type))
	e.PutBytes(v.A)
	e.PutBytes(v.B)
}

func (p pulseBody) encode(e *kolocodec.Encoder) {
	e.PutUvarint(p.Config)
	if p.Config >= 1 {
		e.PutFixed(p.Activating[:])
	}
	var flags uint64
	if p.Claim != nil {
		flags |= flagClaim
	}
	if p.Readiness != nil {
		flags |= flagReady
	}
	e.PutUvarint(flags)
	if c := p.Claim; c != nil {
		e.PutUvarint(c.Position)
		e.PutFixed(c.Digest[:])
	}
	if p.Readiness != nil {
		e.PutFixed(p.Readiness[:])
	}
}

func (s seedBody) encode(e *kolocodec.Encoder) {
	e.PutUvarint(uint64(len(s.Pairs)))
	for _, p := range s.Pairs {
		e.PutBytes(p.Key)
		e.PutBytes(p.Value)
	}
}

// encodeBody returns the canonical encoding of b, or an error wrapping ErrInvalidArgument or ErrTooLarge when DecodeBody would
// refuse it.
func encodeBody(b entryBody, bd formatBounds) ([]byte, error) {
	if b == nil {
		return nil, fmt.Errorf("%w: nil body", ErrInvalidArgument)
	}
	if p, ok := b.(pulseBody); ok && p.Config == 0 && p.Activating != (EntryID{}) {
		return nil, fmt.Errorf("%w: a config 0 pulse names no activating pivot", ErrInvalidArgument)
	}
	var e kolocodec.Encoder
	b.encode(&e)
	out := e.Bytes()
	if _, err := decodeBody(b.Kind(), out, bd); err != nil {
		return nil, encodeErr(err)
	}
	return out, nil
}

// decodeBody parses the body of an entry of the given kind. It returns an error wrapping ErrMalformed for a body that breaks
// the rules of its kind or is not canonical, and one wrapping ErrTooLarge as well when a bound is the reason. A NewMember or
// Evidence body is verified against its signatures; see the type docs.
func decodeBody(k Kind, b []byte, bd formatBounds) (entryBody, error) {
	if !bd.valid() {
		return nil, fmt.Errorf("%w: bounds", ErrInvalidArgument)
	}
	if k > KindSeed {
		return nil, fmt.Errorf("%w: unknown kind %d", ErrMalformed, k)
	}
	if len(b) > bd.bodyMax(k) {
		return nil, fmt.Errorf("%w: %w: %s body of %d bytes", ErrMalformed, ErrTooLarge, kindName(k), len(b))
	}
	r := newReader(b, ErrMalformed)
	var body entryBody
	switch k {
	case KindData:
		return dataBody(bytes.Clone(b)), nil
	case KindGenesis:
		body = decodeGenesis(r)
	case KindAddMember:
		body = decodeNewMember(r)
	case KindRemoveMember:
		body = removeBody{Key: r.fixed(ed25519.PublicKeySize)}
	case KindSettlers:
		body = decodeSettlers(r, bd)
	case KindEvidence:
		body = decodeEvidence(r)
	case KindPulse:
		body = decodePulse(r)
	case KindSeed:
		body = decodeSeed(r)
	}
	if err := r.end(); err != nil {
		return nil, err
	}
	return body, nil
}

func kindName(k Kind) string {
	return [...]string{"genesis", "data", "add member", "remove member", "settlers", "evidence", "pulse", "seed"}[k]
}

func decodeGenesis(r *reader) genesisBody {
	var g genesisBody
	c := &g.Config
	c.App = r.str(MaxAppTag)
	c.Quorum = QuorumRule(r.uvarint(uint64(TwoThirds)))
	c.MaxMembers = int(r.uvarint(maxMaxMembers))
	c.MaxSettlers = int(r.uvarint(maxMaxSettlers))
	c.MinSettlers = int(r.uvarint(maxMaxSettlers))
	c.MaxBody = int(r.uvarint(maxMaxBody))
	c.CheckpointEvery = int(r.uvarint(maxCheckpoint))
	if r.flag() {
		g.Continues = &continuation{Group: GroupID(r.array())}
		g.Continues.Position = r.uvarint(math.MaxUint64)
		g.Continues.Content = Digest(r.array())
		g.Continues.Seeds = r.uvarint(math.MaxUint64)
	}
	if r.err == nil {
		if err := c.Validate(); err != nil {
			r.fail(err)
		}
	}
	return g
}

func decodeNewMember(r *reader) NewMember {
	var w NewMember
	w.Key = r.fixed(ed25519.PublicKeySize)
	w.Strand = kololog.LogID(r.array())
	w.Roles = Role(r.uvarint(uint64(RoleAdmin)))
	w.Header = r.bytes(maxHeader)
	if r.err == nil {
		if err := checkHeader(w.Header, w.Key, w.Strand); err != nil {
			r.fail(err)
		}
	}
	return w
}

// checkHeader parses a log header and checks that key signed it and that it names the strand and the Bytes encoding.
func checkHeader(header []byte, key ed25519.PublicKey, strand kololog.LogID) error {
	h, err := kololog.DecodeLogHeader(header)
	switch {
	case err != nil:
		return err
	case h.LogID != strand:
		return fmt.Errorf("header of another strand")
	case h.Encoding != kololog.Bytes:
		return fmt.Errorf("header without the Bytes encoding")
	}
	return kololog.VerifyLogHeader(key, h)
}

func decodeSettlers(r *reader, bd formatBounds) settlersBody {
	var s settlersBody
	s.Rule = QuorumRule(r.uvarint(uint64(TwoThirds)))
	if r.err == nil && s.Rule == 0 {
		r.failf("unknown quorum rule")
	}
	n := r.uvarint(uint64(bd.MaxSettlers))
	switch {
	case r.err != nil:
	case n == 0:
		r.failf("no settler")
	case n > uint64(r.d.Len()/ed25519.PublicKeySize):
		r.fail(kolocodec.ErrShortBuffer)
	}
	for i := uint64(0); i < n && r.err == nil; i++ {
		k := ed25519.PublicKey(r.fixed(ed25519.PublicKeySize))
		if r.err == nil && len(s.Keys) > 0 && bytes.Compare(s.Keys[len(s.Keys)-1], k) >= 0 {
			r.failf("settler keys not strictly ascending")
		}
		s.Keys = append(s.Keys, k)
	}
	return s
}

func decodeEvidence(r *reader) evidenceBody {
	var v evidenceBody
	v.Key = r.fixed(ed25519.PublicKeySize)
	v.Strand = kololog.LogID(r.array())
	v.Type = evidenceType(r.uvarint(uint64(evidenceHeads)))
	v.A = r.bytes(maxEvidenceObject)
	v.B = r.bytes(maxEvidenceObject)
	if r.err == nil {
		if err := checkEvidence(v); err != nil {
			r.fail(err)
		}
	}
	return v
}

// checkEvidence parses both objects and checks that they conflict and that the key signed both for the strand: kolo-log's
// verifiers refuse an object of another key or log.
func checkEvidence(v evidenceBody) error {
	switch v.Type {
	case evidenceRecords:
		a, err := kololog.DecodeRecord(v.A)
		if err != nil {
			return err
		}
		b, err := kololog.DecodeRecord(v.B)
		if err != nil {
			return err
		}
		switch {
		case a.Index != b.Index || bytes.Equal(v.A, v.B):
			return fmt.Errorf("records do not conflict")
		case !kololog.VerifyRecord(v.Key, v.Strand, a) || !kololog.VerifyRecord(v.Key, v.Strand, b):
			return fmt.Errorf("record of another key or strand, or with a bad signature")
		}
	case evidenceHeads:
		a, err := kololog.DecodeHead(v.A)
		if err != nil {
			return err
		}
		b, err := kololog.DecodeHead(v.B)
		if err != nil {
			return err
		}
		if a.Size != b.Size || a.Root == b.Root {
			return fmt.Errorf("heads do not conflict")
		}
		if _, err := kololog.VerifyHead(v.Key, v.Strand, a); err != nil {
			return err
		}
		if _, err := kololog.VerifyHead(v.Key, v.Strand, b); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown evidence type")
	}
	return nil
}

func decodePulse(r *reader) pulseBody {
	var p pulseBody
	p.Config = r.uvarint(math.MaxUint64)
	if p.Config >= 1 {
		p.Activating = EntryID(r.array())
	}
	flags := r.uvarint(flagClaim | flagReady)
	if flags&flagClaim != 0 {
		p.Claim = &pulseClaim{Position: r.uvarint(math.MaxUint64)}
		p.Claim.Digest = Digest(r.array())
	}
	if flags&flagReady != 0 {
		id := configID(r.array())
		p.Readiness = &id
	}
	return p
}

func decodeSeed(r *reader) seedBody {
	var s seedBody
	n := r.uvarint(math.MaxUint64)
	if r.err == nil && n > uint64(r.d.Len()/2) {
		r.fail(kolocodec.ErrShortBuffer)
	}
	for i := uint64(0); i < n && r.err == nil; i++ {
		p := seedPair{Key: r.bytes(MaxViewKey), Value: r.bytes(MaxViewValue)}
		if r.err == nil && len(s.Pairs) > 0 && bytes.Compare(s.Pairs[len(s.Pairs)-1].Key, p.Key) >= 0 {
			r.failf("seed keys not strictly ascending")
		}
		s.Pairs = append(s.Pairs, p)
	}
	return s
}

// encodeErr turns a decode error of an encoder's own output into the error the caller gets.
func encodeErr(err error) error {
	if errors.Is(err, ErrTooLarge) || errors.Is(err, kolocodec.ErrTooLarge) {
		return fmt.Errorf("%w: %w: %w", ErrInvalidArgument, ErrTooLarge, err)
	}
	return fmt.Errorf("%w: %w", ErrInvalidArgument, err)
}

// reader decodes with a sticky error, so a decoder checks it once at the end.
type reader struct {
	d    *kolocodec.Decoder
	base error
	err  error
}

func newReader(b []byte, base error) *reader {
	return &reader{d: kolocodec.NewDecoder(b), base: base}
}

func (r *reader) fail(err error) {
	if r.err == nil {
		r.err = fmt.Errorf("%w: %w", r.base, err)
	}
}

func (r *reader) failf(format string, args ...any) {
	r.fail(fmt.Errorf(format, args...))
}

func (r *reader) uvarint(max uint64) uint64 {
	if r.err != nil {
		return 0
	}
	v, err := r.d.UvarintMax(max)
	if err != nil {
		r.fail(err)
	}
	return v
}

func (r *reader) fixed(n int) []byte {
	if r.err != nil {
		return make([]byte, n)
	}
	b, err := r.d.Fixed(n)
	if err != nil {
		r.fail(err)
		return make([]byte, n)
	}
	return b
}

func (r *reader) array() (a [32]byte) {
	copy(a[:], r.fixed(len(a)))
	return a
}

func (r *reader) bytes(max int) []byte {
	if r.err != nil {
		return nil
	}
	b, err := r.d.Bytes(max)
	if err != nil {
		r.fail(err)
	}
	return b
}

func (r *reader) str(max int) string {
	if r.err != nil {
		return ""
	}
	s, err := r.d.String(max)
	if err != nil {
		r.fail(err)
	}
	return s
}

func (r *reader) flag() bool {
	if r.err != nil {
		return false
	}
	v, err := r.d.Bool()
	if err != nil {
		r.fail(err)
	}
	return v
}

// end returns the first error, or one for bytes left over.
func (r *reader) end() error {
	if r.err == nil {
		if err := r.d.Done(); err != nil {
			r.fail(err)
		}
	}
	return r.err
}
