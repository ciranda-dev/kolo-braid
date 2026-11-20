package kolobraid

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
	"testing/iotest"

	kololog "github.com/ciranda-dev/kolo-log"
)

func testHead(seed byte, size uint64) kololog.Head {
	return kololog.Head{LogKey: pat(32, seed), LogID: lid(seed + 1), Size: size, Root: arr(pat(32, seed+2)), Signature: pat(64, seed+3)}
}

func testRecord(seed byte, index, size uint64) kololog.Record {
	return kololog.Record{LogKey: pat(32, seed), LogID: lid(seed + 1), Index: index, Size: size,
		ContentHash: arr(pat(32, seed+2)), KeyCommit: arr(pat(32, seed+3)), Signature: pat(64, seed+4)}
}

func testProof(index, size uint64, hashes int, seed byte) kololog.InclusionProof {
	p := kololog.InclusionProof{Index: index, Size: size}
	for i := range hashes {
		p.Path = append(p.Path, arr(pat(32, seed+byte(i))))
	}
	return p
}

func testMessages() map[string]message {
	rec := testRecord(2, 7, 256)
	head := testHead(4, 20)
	return map[string]message{
		"msg-announce":       announceMsg{Group: GroupID(arr(pat(32, 0x70))), Heads: []kololog.Head{testHead(1, 5), testHead(9, 100000)}},
		"msg-announce-empty": announceMsg{Group: GroupID(arr(pat(32, 0x70)))},
		"msg-recordreq":      recordReqMsg{Strand: lid(0x31), Index: 129},
		"msg-record":         recordMsg{Record: &rec},
		"msg-record-empty":   recordMsg{},
		"msg-entryreq":       entryReqMsg{Strand: lid(0x31), Start: 3, Index: 300, ID: eid(0x55)},
		"msg-entry": entryMsg{Head: &head, Entries: []entryItem{
			{Record: testRecord(3, 0, 256), Ciphertext: pat(256, 5), Key: pat(32, 6), Proof: testProof(0, 20, 3, 7)},
			{Record: testRecord(5, 1, 336), Ciphertext: pat(336, 8), Key: []byte{}, Proof: testProof(1, 20, 0, 0)},
		}},
		"msg-entry-empty": entryMsg{},
	}
}

func TestMessageVectors(t *testing.T) {
	bd := defaultBounds()
	for name, m := range testMessages() {
		want := vec(t, name)
		got, err := encodeMessage(m, bd)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("%s: encoded\n got %x\nwant %x", name, got, want)
		}
		dec, err := decodeMessage(want, bd)
		if err != nil {
			t.Fatalf("%s: decode: %v", name, err)
		}
		if dec.Type() != m.Type() {
			t.Errorf("%s: type %d want %d", name, dec.Type(), m.Type())
		}
		if !reflect.DeepEqual(dec, m) {
			t.Errorf("%s: decoded %+v\nwant %+v", name, dec, m)
		}
	}
	if msgAnnounce != 1 || msgEvidence != 2 || msgRecordReq != 3 || msgRecord != 4 || msgEntryReq != 5 || msgEntry != 6 {
		t.Error("message type numbers are part of the format")
	}
}

func TestEvidenceMessage(t *testing.T) {
	bd := defaultBounds()
	a := newSigner(t)
	a.append(t, "one")
	headA := a.head(t)
	b := a.fork(t)
	b.append(t, "two")
	ev := evidenceBody{Key: a.id.PublicKey(), Strand: a.log.ID(), Type: evidenceHeads, A: headA.Encode(), B: b.head(t).Encode()}
	m := evidenceMsg{Group: GroupID(eid(1)), Evidence: ev}
	frame, err := encodeMessage(m, bd)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeMessage(frame, bd)
	if err != nil || !reflect.DeepEqual(got, m) {
		t.Fatalf("round trip: %v", err)
	}
	if n := len(frame) - 4; n > maxEvidenceMsg {
		t.Errorf("an evidence message of %d bytes is over its bound", n)
	}
	bad := evidenceMsg{Group: m.Group, Evidence: evidenceBody{Key: ev.Key, Strand: ev.Strand, Type: evidenceHeads, A: ev.A, B: ev.A}}
	if _, err := encodeMessage(bad, bd); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("evidence of two equal heads: %v", err)
	}
	// A hostile frame with the same bad evidence is a protocol error.
	var e = bytes.NewBuffer(nil)
	e.Write([]byte{0, byte(msgEvidence)})
	e.Write(m.Group[:])
	body := rawBody(bad.Evidence)
	e.WriteByte(byte(len(body)&0x7f | 0x80))
	e.WriteByte(byte(len(body) >> 7))
	e.Write(body)
	if _, err := decodeMessage(withLen(e.Bytes()), bd); !errors.Is(err, ErrProtocol) {
		t.Errorf("hostile evidence message: %v", err)
	}
}

// withLen prefixes a payload with its 4-byte little-endian length.
func withLen(p []byte) []byte {
	return append(binary.LittleEndian.AppendUint32(nil, uint32(len(p))), p...)
}

func TestMessageRejects(t *testing.T) {
	bd := defaultBounds()
	announce := vec(t, "msg-announce")
	entry := vec(t, "msg-entry")
	cases := map[string][]byte{
		"length too long":  append(binary.LittleEndian.AppendUint32(nil, uint32(len(announce)-3)), announce[4:]...),
		"length too short": append(binary.LittleEndian.AppendUint32(nil, uint32(len(announce)-5)), announce[4:]...),
		"trailing byte":    withLen(append(bytes.Clone(announce[4:]), 0)),
		"version 1":        withLen(append([]byte{1}, announce[5:]...)),
		"version 0x80 00":  withLen(append([]byte{0x80, 0x00}, announce[5:]...)),
		"type 0":           withLen(append([]byte{0, 0}, announce[6:]...)),
		"type 7":           withLen(append([]byte{0, 7}, announce[6:]...)),
		"type 0x81 00":     withLen(append([]byte{0, 0x81, 0x00}, announce[6:]...)),
		"type 7 alone":     withLen([]byte{0, 7}),
		"type 0 alone":     withLen([]byte{0, 0}),
		"no payload":       withLen(nil),
		"version only":     withLen([]byte{0}),
		"short frame":      {1, 0},
		"empty":            nil,
	}
	for name, b := range cases {
		if _, err := decodeMessage(b, bd); !errors.Is(err, ErrProtocol) {
			t.Errorf("%s: err = %v, want ErrProtocol", name, err)
		}
	}
	for _, name := range []string{"msg-announce", "msg-recordreq", "msg-record", "msg-entryreq", "msg-entry"} {
		v := vec(t, name)
		for i := range v {
			if _, err := decodeMessage(v[:i], bd); err == nil {
				t.Fatalf("%s truncated to %d bytes decodes", name, i)
			}
			// The same payload cut short but with a matching length prefix.
			if i > 4 {
				if _, err := decodeMessage(withLen(v[4:i]), bd); err == nil {
					t.Fatalf("%s payload cut to %d bytes decodes", name, i-4)
				}
			}
		}
	}
	// Announce: MaxMembers heads are the most.
	heads := func(n int) announceMsg {
		m := announceMsg{Group: GroupID(eid(1))}
		for range n {
			m.Heads = append(m.Heads, testHead(1, 5))
		}
		return m
	}
	small := formatBounds{MaxMembers: 2, MaxSettlers: 1, MaxBody: 1 << 10}
	if _, err := encodeMessage(heads(2), small); err != nil {
		t.Errorf("2 heads at MaxMembers 2: %v", err)
	}
	if _, err := encodeMessage(heads(3), small); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("3 heads at MaxMembers 2: %v", err)
	}
	if _, err := encodeMessage(heads(64), bd); err != nil {
		t.Errorf("64 heads at MaxMembers 64: %v", err)
	}
	if _, err := encodeMessage(heads(65), bd); err == nil {
		t.Error("65 heads at MaxMembers 64")
	}
	// A head of 193 bytes, one over the bound, and a head that does not parse.
	for name, h := range map[string][]byte{"193 bytes": append(testHead(1, 5).Encode(), make([]byte, 193-len(testHead(1, 5).Encode()))...), "junk": []byte("junk"), "empty": nil} {
		p := append([]byte{0, byte(msgAnnounce)}, eid(1).slice()...)
		p = append(p, 1, byte(len(h)))
		p = append(p, h...)
		if _, err := decodeMessage(withLen(p), bd); !errors.Is(err, ErrProtocol) {
			t.Errorf("announce with a head of %s: %v", name, err)
		}
	}
	// Entry: a key of 31, 1 or 33 bytes, a bad record, proof or head.
	good := testMessages()["msg-entry"].(entryMsg)
	mod := func(f func(m *entryMsg)) entryMsg {
		m := entryMsg{Head: good.Head, Entries: append([]entryItem(nil), good.Entries...)}
		f(&m)
		return m
	}
	for name, m := range map[string]entryMsg{
		"key of 31":  mod(func(m *entryMsg) { m.Entries[0].Key = pat(31, 1) }),
		"key of 1":   mod(func(m *entryMsg) { m.Entries[0].Key = []byte{1} }),
		"key of 33":  mod(func(m *entryMsg) { m.Entries[0].Key = pat(33, 1) }),
		"path of 66": mod(func(m *entryMsg) { m.Entries[0].Proof = testProof(0, 20, 66, 1) }),
	} {
		if _, err := encodeMessage(m, bd); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("entry with %s: %v", name, err)
		}
	}
	if _, err := encodeMessage(entryReqMsg{Strand: lid(1), Start: 5, Index: 4}, bd); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("start after index: %v", err)
	}
	if _, err := encodeMessage(entryReqMsg{Strand: lid(1), Start: 4, Index: 4}, bd); err != nil {
		t.Errorf("start equal to index: %v", err)
	}
	if _, err := encodeMessage(nil, bd); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("nil message: %v", err)
	}
	if _, err := decodeMessage(entry, formatBounds{}); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("zero bounds: %v", err)
	}
	// A record over 4 KiB.
	p := append([]byte{0, byte(msgRecord)}, 0x81, 0x20)
	p = append(p, make([]byte, 4096+1)...)
	if _, err := decodeMessage(withLen(p), bd); !errors.Is(err, ErrProtocol) {
		t.Errorf("record of 4097 bytes: %v", err)
	}
	// A claim of a huge entry count is refused before anything is allocated.
	p = append([]byte{0, byte(msgEntry), 0}, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01)
	if _, err := decodeMessage(withLen(p), bd); !errors.Is(err, ErrProtocol) {
		t.Errorf("entry count 2^64-1: %v", err)
	}
}

func TestMessageSizeBounds(t *testing.T) {
	bd := defaultBounds()
	if maxMessage(msgAnnounce, bd) != 12352 {
		t.Errorf("announce bound at 64 members = %d, the record says 12,352", maxMessage(msgAnnounce, bd))
	}
	// The largest honest announce (heads of 5-byte sizes) fits.
	h := testHead(1, 1<<34)
	m := announceMsg{Group: GroupID(eid(1))}
	for range 64 {
		m.Heads = append(m.Heads, h)
	}
	if _, err := encodeMessage(m, bd); err != nil {
		t.Errorf("64 honest heads: %v", err)
	}
	// A head with a 10-byte size is 192 bytes, the most a head can be: 63 of them fit the announce bound, 64 do not (the bound is
	// MaxMembers x 192 + 64, and each head also has a length prefix).
	full := testHead(1, 1<<63)
	if n := len(full.Encode()); n != maxHeadSize {
		t.Fatalf("a head with a 10-byte size is %d bytes", n)
	}
	for count, ok := range map[int]bool{63: true, 64: false} {
		m := announceMsg{Group: GroupID(eid(1))}
		for range count {
			m.Heads = append(m.Heads, full)
		}
		if _, err := encodeMessage(m, bd); ok != (err == nil) {
			t.Errorf("%d announce heads of 192 bytes: %v", count, err)
		}
	}
	// An entry message of exactly MaxMessageSize is accepted and one byte more is refused.
	mk := func(n int) entryMsg {
		return entryMsg{Entries: []entryItem{{Record: testRecord(3, 0, 1), Ciphertext: make([]byte, n)}}}
	}
	probe, err := encodeMessage(mk(maxMessageSize/2), bd)
	if err != nil {
		t.Fatal(err)
	}
	overhead := len(probe) - 4 - maxMessageSize/2
	fits, err := encodeMessage(mk(maxMessageSize-overhead), bd)
	if err != nil || len(fits)-4 != maxMessageSize {
		t.Fatalf("a message of exactly MaxMessageSize: %v", err)
	}
	if _, err := readMessage(bytes.NewReader(fits), bd, nil); err != nil {
		t.Errorf("ReadMessage of exactly MaxMessageSize: %v", err)
	}
	if _, err := encodeMessage(mk(maxMessageSize-overhead+1), bd); !errors.Is(err, ErrTooLarge) {
		t.Errorf("a message of MaxMessageSize+1: %v", err)
	}
}

func TestReadMessage(t *testing.T) {
	bd := defaultBounds()
	names := []string{"msg-announce", "msg-recordreq", "msg-record", "msg-entryreq", "msg-entry", "msg-record-empty", "msg-entry-empty", "msg-announce-empty"}
	var stream bytes.Buffer
	for _, n := range names {
		stream.Write(vec(t, n))
	}
	r := iotest.OneByteReader(&stream)
	for _, n := range names {
		m, err := readMessage(r, bd, nil)
		if err != nil {
			t.Fatalf("%s: %v", n, err)
		}
		again, err := encodeMessage(m, bd)
		if err != nil || !bytes.Equal(again, vec(t, n)) {
			t.Fatalf("%s does not read back: %v", n, err)
		}
	}
	if _, err := readMessage(r, bd, nil); err != io.EOF {
		t.Errorf("end of stream: %v, want io.EOF", err)
	}
	for _, n := range names {
		v := vec(t, n)
		for i := 1; i < len(v); i++ {
			if _, err := readMessage(bytes.NewReader(v[:i]), bd, nil); !errors.Is(err, ErrProtocol) {
				t.Fatalf("%s cut to %d bytes: %v", n, i, err)
			}
		}
	}
	// A failed read is the reader's own error, not a protocol error.
	boom := errors.New("boom")
	if _, err := readMessage(io.MultiReader(bytes.NewReader(vec(t, "msg-entry")[:10]), iotest.ErrReader(boom)), bd, nil); !errors.Is(err, boom) || errors.Is(err, ErrProtocol) {
		t.Errorf("failed read: %v", err)
	}
	if _, err := readMessage(bytes.NewReader(nil), formatBounds{}, nil); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("zero bounds: %v", err)
	}
}

// guard fails the test if more than limit bytes are read.
type guard struct {
	r     io.Reader
	limit int
	t     *testing.T
}

func (g *guard) Read(p []byte) (int, error) {
	n, err := g.r.Read(p[:min(len(p), g.limit+1)])
	g.limit -= n
	if g.limit < 0 {
		g.t.Error("read past the bytes a decision needs")
		return n, io.ErrClosedPipe
	}
	return n, err
}

func TestReadMessageChecksBeforeAllocating(t *testing.T) {
	bd := defaultBounds()
	huge := func(n uint32, typ byte, version byte) io.Reader {
		return io.MultiReader(bytes.NewReader(binary.LittleEndian.AppendUint32(nil, n)), bytes.NewReader([]byte{version, typ}), zeros{})
	}
	cases := map[string]struct {
		r     io.Reader
		limit int // bytes a correct reader consumes before refusing
	}{
		"zero length":            {huge(0, 1, 0), 4},
		"length 1":               {huge(1, 1, 0), 4},
		"over MaxMessageSize":    {huge(maxMessageSize+1, 6, 0), 4},
		"all ones":               {huge(0xffffffff, 6, 0), 4},
		"version 1":              {huge(100, 1, 1), 6},
		"type 0":                 {huge(100, 0, 0), 6},
		"type 7":                 {huge(100, 7, 0), 6},
		"announce over its cap":  {huge(uint32(maxMessage(msgAnnounce, bd))+1, byte(msgAnnounce), 0), 6},
		"evidence over its cap":  {huge(maxEvidenceMsg+1, byte(msgEvidence), 0), 6},
		"record request over":    {huge(maxRecordReq+1, byte(msgRecordReq), 0), 6},
		"record over its cap":    {huge(maxRecordMsg+1, byte(msgRecord), 0), 6},
		"entry request over cap": {huge(maxEntryReq+1, byte(msgEntryReq), 0), 6},
	}
	for name, c := range cases {
		if _, err := readMessage(&guard{r: c.r, limit: c.limit, t: t}, bd, nil); !errors.Is(err, ErrProtocol) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

type zeros struct{}

func (zeros) Read(p []byte) (int, error) { clear(p); return len(p), nil }

func TestJoinRequest(t *testing.T) {
	s := newSigner(t)
	r := JoinRequest{Strand: s.log.ID(), Header: s.header(), AppData: []byte("hello")}
	b, err := encodeJoinRequest(r)
	if err != nil {
		t.Fatal(err)
	}
	got, err := ParseJoinRequest(b)
	if err != nil || got.Strand != r.Strand || !bytes.Equal(got.Header, r.Header) || string(got.AppData) != "hello" {
		t.Fatalf("round trip: %+v %v", got, err)
	}
	if !bytes.HasPrefix(b, append([]byte{byte(len(joinTag))}, joinTag...)) || joinTag != "kolo-braid/join/v0" {
		t.Error("a join request starts with its tag")
	}
	// Empty app data is fine; the total is bounded by MaxJoinRequest.
	if _, err := encodeJoinRequest(JoinRequest{Strand: r.Strand, Header: r.Header}); err != nil {
		t.Errorf("no app data: %v", err)
	}
	fixed := len(b) - len(r.AppData) - 1
	room := MaxJoinRequest - fixed - 3 // the app data length prefix is 3 bytes at this size
	if _, err := encodeJoinRequest(JoinRequest{Strand: r.Strand, Header: r.Header, AppData: make([]byte, room)}); err != nil {
		t.Errorf("a request of exactly MaxJoinRequest: %v", err)
	}
	if _, err := encodeJoinRequest(JoinRequest{Strand: r.Strand, Header: r.Header, AppData: make([]byte, room+1)}); !errors.Is(err, ErrTooLarge) {
		t.Errorf("a request over MaxJoinRequest: %v", err)
	}
	other := newSigner(t)
	utf8 := newSigner(t, kololog.WithValueEncoding(kololog.UTF8))
	tampered := bytes.Clone(r.Header)
	tampered[len(tampered)-1] ^= 1
	for name, j := range map[string]JoinRequest{
		"another strand":  {Strand: other.log.ID(), Header: r.Header},
		"bad signature":   {Strand: r.Strand, Header: tampered},
		"not Bytes":       {Strand: utf8.log.ID(), Header: utf8.header()},
		"junk header":     {Strand: r.Strand, Header: []byte("junk")},
		"header over max": {Strand: r.Strand, Header: append(bytes.Clone(r.Header), make([]byte, maxHeader)...)},
	} {
		if _, err := encodeJoinRequest(j); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("%s: %v", name, err)
		}
	}
	for name, mod := range map[string][]byte{
		"trailing byte": append(bytes.Clone(b), 0),
		"wrong tag":     append([]byte{byte(len(joinTag))}, append([]byte(strings.Replace(joinTag, "v0", "v1", 1)), b[1+len(joinTag):]...)...),
		"empty":         nil,
	} {
		if _, err := ParseJoinRequest(mod); !errors.Is(err, ErrMalformed) {
			t.Errorf("%s: %v", name, err)
		}
	}
	for i := range b {
		if _, err := ParseJoinRequest(b[:i]); err == nil {
			t.Fatalf("a join request cut to %d bytes parses", i)
		}
	}
}

func TestWelcome(t *testing.T) {
	founder, inviter := newSigner(t), newSigner(t)
	w := welcomeMsg{
		Group: GroupID(eid(1)), FounderKey: founder.id.PublicKey(), FounderStrand: founder.log.ID(), FounderHeader: founder.header(),
		InviterKey: inviter.id.PublicKey(), InviterStrand: inviter.log.ID(), InviterHeader: inviter.header(),
		AddIndex: 300, AddID: eid(2),
	}
	for name, anchor := range map[string]*anchorRec{"no anchor": nil, "anchor": {Pivot: eid(3), Config: 4}} {
		w.Anchor = anchor
		b, err := encodeWelcome(w)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		got, err := parseWelcome(b)
		if err != nil || !reflect.DeepEqual(got, w) {
			t.Fatalf("%s: round trip %v", name, err)
		}
		if len(b) > 700 {
			t.Errorf("%s: a welcome is %d bytes; the record says about 590", name, len(b))
		}
		for i := range b {
			if _, err := parseWelcome(b[:i]); err == nil {
				t.Fatalf("%s: a welcome cut to %d bytes parses", name, i)
			}
		}
		if _, err := parseWelcome(append(bytes.Clone(b), 0)); !errors.Is(err, ErrMalformed) {
			t.Errorf("%s: trailing byte: %v", name, err)
		}
	}
	w.Anchor = nil
	b, _ := encodeWelcome(w)
	flag := bytes.Clone(b)
	flag[len(flag)-1] = 2
	if _, err := parseWelcome(flag); !errors.Is(err, ErrMalformed) {
		t.Errorf("anchor flag 2: %v", err)
	}
	swapped := w
	swapped.FounderHeader, swapped.InviterHeader = w.InviterHeader, w.FounderHeader
	if _, err := encodeWelcome(swapped); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("headers of the wrong strands: %v", err)
	}
	for name, mod := range map[string]func(w *welcomeMsg){
		"founder header of the inviter": func(w *welcomeMsg) { w.FounderHeader = inviter.header() },
		"inviter header of the founder": func(w *welcomeMsg) { w.InviterHeader = founder.header() },
	} {
		bad := w
		mod(&bad)
		if _, err := encodeWelcome(bad); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("%s: %v", name, err)
		}
	}
	wrongKey := w
	wrongKey.InviterKey = founder.id.PublicKey()
	if _, err := encodeWelcome(wrongKey); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("inviter key that did not sign its header: %v", err)
	}
	if _, err := parseWelcome(make([]byte, MaxWelcome+1)); !errors.Is(err, ErrTooLarge) {
		t.Errorf("welcome over MaxWelcome: %v", err)
	}
	if joinTag == welcomeTag || welcomeTag != "kolo-braid/welcome/v0" {
		t.Error("welcome tag")
	}
}

// TestReadMessageAdmits: a message that admit refuses is refused from its prefix, before any byte of its payload is read.
func TestReadMessageAdmits(t *testing.T) {
	bd := defaultBounds()
	stop := errors.New("not now")
	f := vec(t, "msg-entry")
	_, err := readMessage(&guard{r: bytes.NewReader(f), limit: 6, t: t}, bd, func(messageType) error { return stop })
	if !errors.Is(err, stop) {
		t.Fatalf("refused type: %v", err)
	}
	if _, err := readMessage(bytes.NewReader(f), bd, func(messageType) error { return nil }); err != nil {
		t.Fatalf("admitted type: %v", err)
	}
}
