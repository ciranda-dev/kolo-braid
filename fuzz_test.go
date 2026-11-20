package kolobraid

import (
	"bytes"
	"errors"
	"testing"
)

// Every fuzz target keeps its seeds here, in code: no testdata/fuzz directory is committed.

func FuzzDecodeEntry(f *testing.F) {
	for _, n := range []string{"entry-data", "entry-data-empty", "entry-remove", "entry-settlers", "entry-pulse-0", "entry-genesis"} {
		v := vec(f, n)
		f.Add(v, uint64(0), false)
		f.Add(v, uint64(5), false)
		f.Add(v, uint64(5), true)
	}
	f.Add([]byte(nil), uint64(0), false)
	f.Add(nonMinimal(vec(f, "entry-data"), 52), uint64(5), false)
	bd := defaultBounds()
	f.Fuzz(func(t *testing.T, b []byte, index uint64, self bool) {
		strand := testStrand
		if self {
			strand = lid(0x10)
		}
		c, err := decodeEntryContent(b, strand, index, bd)
		if err != nil {
			return
		}
		again, err := encodeContent(c, strand, index, bd)
		if err != nil || !bytes.Equal(again, b) {
			t.Fatalf("a decoded entry does not encode back to its bytes: %v", err)
		}
		if _, err := decodeBody(c.Kind, c.Body, bd); err != nil {
			t.Fatalf("the body of a decoded entry does not decode: %v", err)
		}
		for i := 1; i < len(c.Links); i++ {
			if bytes.Compare(c.Links[i-1].Strand[:], c.Links[i].Strand[:]) >= 0 {
				t.Fatal("decoded links are not ascending")
			}
		}
	})
}

func FuzzDecodeBody(f *testing.F) {
	for _, s := range []struct {
		kind Kind
		name string
	}{
		{KindPulse, "body-pulse-1"}, {KindPulse, "body-pulse-2"}, {KindPulse, "body-pulse-3"}, {KindSeed, "body-seed"},
		{KindGenesis, "body-genesis"}, {KindGenesis, "body-genesis-continues"},
	} {
		f.Add(byte(s.kind), vec(f, s.name))
	}
	f.Add(byte(KindData), []byte("data"))
	f.Add(byte(KindRemoveMember), pat(32, 1))
	f.Add(byte(KindSettlers), mustBody(f, settlersBody{Rule: Majority, Keys: manyKeys(3)}))
	s := newSigner(f)
	s.append(f, "one")
	member, recA, headA := s.member(), s.record(f, 0).Encode(), s.head(f).Encode()
	other := s.fork(f)
	other.append(f, "two")
	f.Add(byte(KindAddMember), mustBody(f, member))
	f.Add(byte(KindEvidence), mustBody(f, evidenceBody{Key: member.Key, Strand: member.Strand, Type: evidenceRecords, A: recA, B: other.record(f, 0).Encode()}))
	f.Add(byte(KindEvidence), mustBody(f, evidenceBody{Key: member.Key, Strand: member.Strand, Type: evidenceHeads, A: headA, B: other.head(f).Encode()}))
	f.Add(byte(KindSeed), []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01})
	bd := genesisBounds()
	f.Fuzz(func(t *testing.T, kind byte, b []byte) {
		body, err := decodeBody(Kind(kind), b, bd)
		if err != nil {
			return
		}
		if body.Kind() != Kind(kind) {
			t.Fatalf("kind %d decoded as %T", kind, body)
		}
		again, err := encodeBody(body, bd)
		if err != nil || !bytes.Equal(again, b) {
			t.Fatalf("a decoded %T does not encode back to its bytes: %v", body, err)
		}
	})
}

func FuzzDecodeMessage(f *testing.F) {
	for name := range testMessages() {
		f.Add(vec(f, name))
	}
	a := newSigner(f)
	a.append(f, "one")
	key, strand, headA := a.id.PublicKey(), a.log.ID(), a.head(f).Encode()
	b := a.fork(f)
	b.append(f, "two")
	m, err := encodeMessage(evidenceMsg{Group: GroupID(eid(1)), Evidence: evidenceBody{Key: key, Strand: strand, Type: evidenceHeads,
		A: headA, B: b.head(f).Encode()}}, defaultBounds())
	if err != nil {
		f.Fatal(err)
	}
	f.Add(m)
	f.Add([]byte{})
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0, 1})
	f.Add(withLen([]byte{0, byte(msgEntry), 0, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01}))
	bd := defaultBounds()
	f.Fuzz(func(t *testing.T, frame []byte) {
		got, err := decodeMessage(frame, bd)
		streamed, serr := readMessage(bytes.NewReader(frame), bd, nil)
		if err != nil {
			return
		}
		// A frame that decodes whole also reads from a stream, and both give one message.
		if serr != nil || streamed.Type() != got.Type() {
			t.Fatalf("DecodeMessage accepts a frame that ReadMessage refuses: %v", serr)
		}
		again, err := encodeMessage(got, bd)
		if err != nil || !bytes.Equal(again, frame) {
			t.Fatalf("a decoded %T does not encode back to its bytes: %v", got, err)
		}
		// A refused type stops the read at the prefix; any other type reads as before.
		stop := errors.New("refused")
		_, aerr := readMessage(bytes.NewReader(frame), bd, func(mt messageType) error {
			if mt == got.Type() {
				return stop
			}
			return nil
		})
		if !errors.Is(aerr, stop) {
			t.Fatalf("a refused %T was read: %v", got, aerr)
		}
		if m, err := readMessage(bytes.NewReader(frame), bd, func(messageType) error { return nil }); err != nil || m.Type() != got.Type() {
			t.Fatalf("an admitted %T was not read: %v", got, err)
		}
	})
}

func FuzzDecodeJoinRequest(f *testing.F) {
	s := newSigner(f)
	b, err := encodeJoinRequest(JoinRequest{Strand: s.log.ID(), Header: s.header(), AppData: []byte("app")})
	if err != nil {
		f.Fatal(err)
	}
	f.Add(b)
	f.Add([]byte{})
	f.Add([]byte(joinTag))
	f.Fuzz(func(t *testing.T, in []byte) {
		j, err := ParseJoinRequest(in)
		if err != nil {
			return
		}
		again, err := encodeJoinRequest(j)
		if err != nil || !bytes.Equal(again, in) {
			t.Fatalf("a parsed join request does not encode back to its bytes: %v", err)
		}
	})
}

func FuzzDecodeWelcome(f *testing.F) {
	founder, inviter := newSigner(f), newSigner(f)
	w := welcomeMsg{
		Group: GroupID(eid(1)), FounderKey: founder.id.PublicKey(), FounderStrand: founder.log.ID(), FounderHeader: founder.header(),
		InviterKey: inviter.id.PublicKey(), InviterStrand: inviter.log.ID(), InviterHeader: inviter.header(), AddIndex: 3, AddID: eid(2),
	}
	for _, a := range []*anchorRec{nil, {Pivot: eid(3), Config: 1}} {
		w.Anchor = a
		b, err := encodeWelcome(w)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(b)
	}
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, in []byte) {
		got, err := parseWelcome(in)
		if err != nil {
			return
		}
		again, err := encodeWelcome(got)
		if err != nil || !bytes.Equal(again, in) {
			t.Fatalf("a parsed welcome does not encode back to its bytes: %v", err)
		}
	})
}
