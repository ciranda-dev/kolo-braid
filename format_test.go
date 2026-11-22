package kolobraid

import (
	"bytes"
	"crypto/ed25519"
	"encoding/binary"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"

	kolocodec "github.com/ciranda-dev/kolo-codec"
	kololog "github.com/ciranda-dev/kolo-log"
)

func lid(seed byte) kololog.LogID { return kololog.LogID(arr(pat(32, seed))) }
func eid(seed byte) EntryID       { return EntryID(arr(pat(32, seed))) }

func mustBody(t testing.TB, b entryBody) []byte {
	t.Helper()
	out, err := encodeBody(b, defaultBounds())
	if err != nil {
		t.Fatalf("EncodeBody(%T): %v", b, err)
	}
	return out
}

func rawBody(b entryBody) []byte {
	var e kolocodec.Encoder
	b.encode(&e)
	return e.Bytes()
}

func TestContentVectors(t *testing.T) {
	group, prev := GroupID(arr(pat(32, 0x70))), eid(0x80)
	genesis := genesisBody{Config: GroupConfig{App: "app/1", Quorum: Majority, MaxMembers: 64, MaxSettlers: 7, MinSettlers: 4, MaxBody: 1 << 20, CheckpointEvery: 1024}}
	cases := []struct {
		name  string
		index uint64
		c     entryContent
	}{
		{"entry-data", 5, entryContent{Group: group, Kind: KindData, Prev: prev,
			Links: []Link{{lid(0x10), 0, eid(0x11)}, {lid(0x20), 300, eid(0x21)}}, Body: []byte("hello braid")}},
		{"entry-data-empty", 0, entryContent{Group: group, Kind: KindData, Body: []byte{}}},
		{"entry-remove", 5, entryContent{Group: group, Kind: KindRemoveMember, Prev: prev, Body: mustBody(t, removeBody{Key: pat(32, 0xd0)})}},
		{"entry-settlers", 5, entryContent{Group: group, Kind: KindSettlers, Prev: prev, Links: []Link{{lid(0x10), 1, eid(0x11)}},
			Body: mustBody(t, settlersBody{Rule: TwoThirds, Keys: []ed25519.PublicKey{pat(32, 0x01), pat(32, 0x41), pat(32, 0x81)}})}},
		{"entry-pulse-0", 5, entryContent{Group: group, Kind: KindPulse, Prev: prev, Links: []Link{{lid(0x10), 5, eid(0x11)}, {lid(0x30), 6, eid(0x31)}},
			Body: mustBody(t, pulseBody{})}},
		{"entry-genesis", 0, entryContent{Kind: KindGenesis, Body: mustBody(t, genesis)}},
	}
	for _, tc := range cases {
		want := vec(t, tc.name)
		got, err := encodeContent(tc.c, testStrand, tc.index, defaultBounds())
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("%s: encoded\n got %x\nwant %x", tc.name, got, want)
		}
		dec, err := decodeEntryContent(want, testStrand, tc.index, defaultBounds())
		if err != nil {
			t.Fatalf("%s: decode: %v", tc.name, err)
		}
		if !reflect.DeepEqual(dec.Links, tc.c.Links) && len(dec.Links)+len(tc.c.Links) > 0 || dec.Group != tc.c.Group || dec.Kind != tc.c.Kind ||
			dec.Prev != tc.c.Prev || !bytes.Equal(dec.Body, tc.c.Body) {
			t.Errorf("%s: decoded %+v want %+v", tc.name, dec, tc.c)
		}
	}
}

func TestBodyVectors(t *testing.T) {
	cases := []struct {
		name string
		kind Kind
		body entryBody
	}{
		{"body-pulse-1", KindPulse, pulseBody{Config: 3, Activating: eid(0xe0), Claim: &pulseClaim{2048, Digest(eid(0xe1))}}},
		{"body-pulse-2", KindPulse, pulseBody{Config: 300, Activating: eid(0xe0), Claim: &pulseClaim{1024, Digest(eid(0xe2))}, Readiness: ptr(configID(eid(0xe3)))}},
		{"body-pulse-3", KindPulse, pulseBody{Config: 1, Activating: eid(0xe0), Readiness: ptr(configID(eid(0xe4)))}},
		{"body-seed", KindSeed, seedBody{Pairs: []seedPair{{[]byte{}, []byte("empty key")}, {[]byte("a"), []byte{}}, {[]byte("b"), pat(200, 9)}}}},
		{"body-genesis", KindGenesis, genesisBody{Config: GroupConfig{App: "app/1", Quorum: Majority, MaxMembers: 64, MaxSettlers: 7, MinSettlers: 4, MaxBody: 1 << 20, CheckpointEvery: 1024}}},
		{"body-genesis-continues", KindGenesis, genesisBody{
			Config:    GroupConfig{App: "app/2", Quorum: TwoThirds, MaxMembers: 1024, MaxSettlers: 64, MinSettlers: 64, MaxBody: 3 << 20, CheckpointEvery: 1 << 20},
			Continues: &continuation{Group: GroupID(eid(0xa0)), Position: 123456, Content: Digest(eid(0xa1)), Seeds: 2}}},
	}
	for _, tc := range cases {
		want := vec(t, tc.name)
		got, err := encodeBody(tc.body, genesisBounds())
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("%s: encoded\n got %x\nwant %x", tc.name, got, want)
		}
		dec, err := decodeBody(tc.kind, want, genesisBounds())
		if err != nil {
			t.Fatalf("%s: decode: %v", tc.name, err)
		}
		if !reflect.DeepEqual(dec, tc.body) {
			t.Errorf("%s: decoded %+v want %+v", tc.name, dec, tc.body)
		}
	}
}

func ptr[T any](v T) *T { return &v }

// nonMinimal rewrites the one-byte uvarint at off as the two bytes b|0x80, 0x00, which carries the same value.
func nonMinimal(b []byte, off int) []byte {
	out := append([]byte(nil), b[:off]...)
	out = append(out, b[off]|0x80, 0x00)
	return append(out, b[off+1:]...)
}

func TestContentNonCanonicalVarints(t *testing.T) {
	valid := vec(t, "entry-data")
	// Offsets in entry-data: tag 20, group 32 -> kind at 52, prev 33..84, link count at 85.
	const kindOff, countOff = 52, 85
	linkIndexOff := countOff + 1 + 32 // index of the first link (value 0)
	bodyLenOff := len(valid) - len("hello braid") - 1
	for name, off := range map[string]int{"kind": kindOff, "link count": countOff, "link index": linkIndexOff, "body length": bodyLenOff} {
		if _, err := decodeEntryContent(valid, testStrand, 5, defaultBounds()); err != nil {
			t.Fatal(err)
		}
		_, err := decodeEntryContent(nonMinimal(valid, off), testStrand, 5, defaultBounds())
		if !errors.Is(err, ErrMalformed) || !errors.Is(err, kolocodec.ErrNonCanonical) {
			t.Errorf("%s: err = %v, want a non-canonical ErrMalformed", name, err)
		}
	}
}

func TestContentRejects(t *testing.T) {
	valid := vec(t, "entry-data")
	bd := defaultBounds()
	mut := func(f func(b []byte) []byte) []byte { return f(append([]byte(nil), valid...)) }
	cases := map[string][]byte{
		"trailing byte":   append(append([]byte(nil), valid...), 0),
		"wrong tag":       mut(func(b []byte) []byte { b[5] ^= 1; return b }),
		"longer tag":      mut(func(b []byte) []byte { b[0]++; return b }),
		"kind 8":          mut(func(b []byte) []byte { b[52] = 8; return b }),
		"kind 255":        mut(func(b []byte) []byte { b[52] = 0xff; return b }),
		"zero group":      mut(func(b []byte) []byte { copy(b[20:52], make([]byte, 32)); return b }),
		"zero prev":       mut(func(b []byte) []byte { copy(b[53:85], make([]byte, 32)); return b }),
		"empty":           nil,
		"tag only":        valid[:20],
		"body over input": mut(func(b []byte) []byte { b[len(b)-12] = 0x7f; return b }),
	}
	for name, b := range cases {
		if _, err := decodeEntryContent(b, testStrand, 5, bd); !errors.Is(err, ErrMalformed) {
			t.Errorf("%s: err = %v, want ErrMalformed", name, err)
		}
	}
	for i := range valid {
		if _, err := decodeEntryContent(valid[:i], testStrand, 5, bd); err == nil {
			t.Fatalf("a truncation to %d bytes decodes", i)
		}
	}
	if _, err := decodeEntryContent(valid, testStrand, 0, bd); !errors.Is(err, ErrMalformed) {
		t.Errorf("a non-zero prev at index 0: err = %v", err)
	}
	if _, err := decodeEntryContent(valid, lid(0x10), 5, bd); !errors.Is(err, ErrMalformed) {
		t.Errorf("a link to the entry's own strand: err = %v", err)
	}
	if _, err := decodeEntryContent(valid, testStrand, 5, formatBounds{}); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("zero bounds: err = %v", err)
	}
}

func TestContentGenesisRules(t *testing.T) {
	gen := vec(t, "entry-genesis")
	if _, err := decodeEntryContent(gen, testStrand, 0, defaultBounds()); err != nil {
		t.Fatal(err)
	}
	if _, err := decodeEntryContent(gen, testStrand, 1, defaultBounds()); !errors.Is(err, ErrMalformed) {
		t.Errorf("genesis at index 1: err = %v", err)
	}
	// A genesis entry anywhere but index 0 is refused even when its prev field is set, as a later entry's is.
	var e kolocodec.Encoder
	e.PutString(entryTag)
	e.PutFixed(make([]byte, 32))
	e.PutUvarint(uint64(KindGenesis))
	e.PutFixed(eid(9).slice())
	e.PutUvarint(0)
	e.PutBytes(mustBody(t, genesisBody{Config: GroupConfig{App: "a"}.WithDefaults()}))
	if _, err := decodeEntryContent(e.Bytes(), testStrand, 1, defaultBounds()); !errors.Is(err, ErrMalformed) {
		t.Errorf("genesis at index 1 with a prev: %v", err)
	}
	// A genesis kind with a group set, and a data kind with the group zero.
	body := mustBody(t, genesisBody{Config: GroupConfig{App: "a"}.WithDefaults()})
	for name, c := range map[string]entryContent{
		"genesis with a group": {Group: GroupID(eid(1)), Kind: KindGenesis, Body: body},
		"data without a group": {Kind: KindData, Body: []byte("x")},
		"genesis with a link":  {Kind: KindGenesis, Links: []Link{{lid(1), 0, eid(2)}}, Body: body},
		"genesis with a prev":  {Kind: KindGenesis, Prev: eid(3), Body: body},
	} {
		if _, err := encodeContent(c, testStrand, 0, defaultBounds()); err == nil {
			t.Errorf("%s: encoded", name)
		}
	}
}

func TestContentLinks(t *testing.T) {
	bd := formatBounds{MaxMembers: 64, MaxSettlers: 7, MaxBody: 1 << 10}
	mk := func(n int, f func(i int) Link) entryContent {
		c := entryContent{Group: GroupID(eid(1)), Kind: KindData, Prev: eid(2), Body: []byte("x")}
		for i := range n {
			c.Links = append(c.Links, f(i))
		}
		return c
	}
	asc := func(i int) Link { return Link{Strand: kololog.LogID{byte(i + 1)}, ID: eid(byte(i))} }
	// MaxMembers - 1 = 63 links is the most.
	for n, ok := range map[int]bool{0: true, 1: true, 63: true, 64: false} {
		b, err := encodeContent(mk(n, asc), testStrand, 3, bd)
		if ok != (err == nil) {
			t.Fatalf("%d links: encode err = %v", n, err)
		}
		if !ok {
			// A hostile encoding of 64 links is refused by the decoder as well.
			var e kolocodec.Encoder
			e.PutString(entryTag)
			e.PutFixed(eid(1).slice())
			e.PutUvarint(uint64(KindData))
			e.PutFixed(eid(2).slice())
			e.PutUvarint(uint64(n))
			for i := range n {
				l := asc(i)
				e.PutFixed(l.Strand[:])
				e.PutUvarint(0)
				e.PutFixed(l.ID[:])
			}
			e.PutBytes([]byte("x"))
			if _, err := decodeEntryContent(e.Bytes(), testStrand, 3, bd); !errors.Is(err, ErrMalformed) || !errors.Is(err, kolocodec.ErrTooLarge) {
				t.Fatalf("64 links decoded: %v", err)
			}
			continue
		}
		dec, err := decodeEntryContent(b, testStrand, 3, bd)
		if err != nil || len(dec.Links) != n {
			t.Fatalf("%d links: %v", n, err)
		}
	}
	bad := map[string]entryContent{
		"descending": mk(2, func(i int) Link { return Link{Strand: kololog.LogID{byte(9 - i)}, ID: eid(1)} }),
		"duplicate":  mk(2, func(i int) Link { return Link{Strand: kololog.LogID{5}, Index: uint64(i), ID: eid(1)} }),
		"own strand": mk(1, func(int) Link { return Link{Strand: testStrand, ID: eid(1)} }),
	}
	for name, c := range bad {
		if _, err := encodeContent(c, testStrand, 3, bd); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("%s: err = %v", name, err)
		}
	}
	// A link may name an all-zero entry ID: the record leaves that to the placement rules (the link never resolves).
	zero := mk(1, func(int) Link { return Link{Strand: kololog.LogID{5}} })
	if _, err := encodeContent(zero, testStrand, 3, bd); err != nil {
		t.Errorf("zero link ID: %v", err)
	}
}

func (id EntryID) slice() []byte { return id[:] }

func TestBodyBounds(t *testing.T) {
	bd := formatBounds{MaxMembers: 64, MaxSettlers: 7, MaxBody: 1 << 10}
	if _, err := decodeBody(KindData, make([]byte, 1<<10), bd); err != nil {
		t.Errorf("data at MaxBody: %v", err)
	}
	for _, k := range []Kind{KindData, KindPulse, KindRemoveMember, KindSettlers, KindEvidence, KindGenesis, KindAddMember, KindSeed} {
		over := bd.bodyMax(k) + 1
		if _, err := decodeBody(k, make([]byte, over), bd); !errors.Is(err, ErrTooLarge) || !errors.Is(err, ErrMalformed) {
			t.Errorf("%s body of MaxBody+1: err = %v", kindName(k), err)
		}
	}
	// Data bodies follow MaxBody; the seed body fits one pair of the largest key and value whatever MaxBody is.
	if bd.bodyMax(KindData) != 1<<10 || bd.bodyMax(KindSeed) != MaxViewKey+MaxViewValue+16 || bd.bodyMax(KindPulse) != 8300 {
		t.Error("body bounds")
	}
	if g := (formatBounds{MaxMembers: 64, MaxSettlers: 7, MaxBody: 3 << 20}); g.bodyMax(KindSeed) != 3<<20 {
		t.Error("the seed bound is the larger of MaxBody and one maximal pair")
	}
	big := seedBody{Pairs: []seedPair{{Key: bytes.Repeat([]byte{1}, MaxViewKey), Value: make([]byte, MaxViewValue)}}}
	b, err := encodeBody(big, bd)
	if err != nil {
		t.Fatalf("one maximal seed pair: %v", err)
	}
	if len(b) > bd.bodyMax(KindSeed) {
		t.Fatalf("a maximal pair is %d bytes, over the seed bound %d", len(b), bd.bodyMax(KindSeed))
	}
	for name, s := range map[string]seedBody{
		"key over bound":   {Pairs: []seedPair{{Key: make([]byte, MaxViewKey+1)}}},
		"value over bound": {Pairs: []seedPair{{Value: make([]byte, MaxViewValue+1)}}},
	} {
		if _, err := encodeBody(s, bd); err == nil {
			t.Errorf("seed with a %s encoded", name)
		}
	}
	if _, err := encodeContent(entryContent{Group: GroupID(eid(1)), Kind: KindData, Prev: eid(2), Body: make([]byte, 1<<10+1)}, testStrand, 1, bd); !errors.Is(err, ErrTooLarge) {
		t.Errorf("EncodeContent over MaxBody: %v", err)
	}
}

func TestGroupConfig(t *testing.T) {
	d := GroupConfig{App: "x"}.WithDefaults()
	want := GroupConfig{App: "x", Quorum: Majority, MaxSettlers: 7, MinSettlers: 4, MaxBody: 1 << 20, CheckpointEvery: 1024}
	if d != want {
		t.Errorf("defaults = %+v", d)
	}
	if err := d.Validate(); err != nil {
		t.Fatal(err)
	}
	if small := (GroupConfig{App: "x", MaxMembers: 2}).WithDefaults(); small.MaxSettlers != 2 || small.MinSettlers != 2 || small.Validate() != nil {
		t.Errorf("small group defaults = %+v", small)
	}
	for name, f := range map[string]func(c *GroupConfig){
		"empty app":             func(c *GroupConfig) { c.App = "" },
		"app of 65":             func(c *GroupConfig) { c.App = strings.Repeat("a", 65) },
		"app not UTF-8":         func(c *GroupConfig) { c.App = "\xff" },
		"quorum 0":              func(c *GroupConfig) { c.Quorum = 0 },
		"quorum 3":              func(c *GroupConfig) { c.Quorum = 3 },
		"members 1":             func(c *GroupConfig) { c.MaxMembers = 1 },
		"members -1":            func(c *GroupConfig) { c.MaxMembers = -1 },
		"settlers 0":            func(c *GroupConfig) { c.MaxSettlers = 0 },
		"settlers over links":   func(c *GroupConfig) { c.MaxSettlers = 40000 },
		"settlers over members": func(c *GroupConfig) { c.MaxMembers, c.MaxSettlers, c.MinSettlers = 4, 5, 4 },
		"min settlers 0":        func(c *GroupConfig) { c.MinSettlers = 0 },
		"min over max":          func(c *GroupConfig) { c.MinSettlers = 8 },
		"body 1023":             func(c *GroupConfig) { c.MaxBody = 1023 },
		"body over 3 MiB":       func(c *GroupConfig) { c.MaxBody = 3<<20 + 1 },
		"checkpoint 63":         func(c *GroupConfig) { c.CheckpointEvery = 63 },
		"checkpoint over 2^20":  func(c *GroupConfig) { c.CheckpointEvery = 1<<20 + 1 },
	} {
		c := d
		f(&c)
		if err := c.Validate(); !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("%s: Validate = %v", name, err)
		}
		// The same values in a genesis body are refused by the decoder.
		if _, err := decodeBody(KindGenesis, rawBody(genesisBody{Config: c}), genesisBounds()); !errors.Is(err, ErrMalformed) {
			t.Errorf("%s: DecodeBody = %v", name, err)
		}
	}
	// The ends of every range are accepted.
	for name, c := range map[string]GroupConfig{
		"low":                        {App: "a", Quorum: Majority, MaxMembers: 2, MaxSettlers: 1, MinSettlers: 1, MaxBody: 1 << 10, CheckpointEvery: 64},
		"high":                       {App: strings.Repeat("a", 64), Quorum: TwoThirds, MaxSettlers: 1000, MinSettlers: 1000, MaxBody: 3 << 20, CheckpointEvery: 1 << 20},
		"1,100 members, 70 settlers": {App: "a", Quorum: Majority, MaxMembers: 1100, MaxSettlers: 70, MinSettlers: 4, MaxBody: 1 << 20, CheckpointEvery: 1024},
	} {
		if _, err := decodeBody(KindGenesis, rawBody(genesisBody{Config: c}), genesisBounds()); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
	if d.bounds() != (formatBounds{0, 7, 1 << 20}) || !genesisBounds().valid() || (formatBounds{}).valid() {
		t.Error("bounds")
	}
}

func TestGroupConfigNoMemberCeiling(t *testing.T) {
	if d := (GroupConfig{App: "x"}).WithDefaults(); d.MaxMembers != 0 || d.MaxSettlers != 7 {
		t.Errorf("defaults = %+v", d)
	}
	for _, n := range [][2]int{{0, 7}, {1025, 65}, {30000, 300}, {1100, 70}, {0, 300}} {
		c := GroupConfig{App: "x", MaxMembers: n[0], MaxSettlers: n[1]}.WithDefaults()
		if err := c.Validate(); err != nil {
			t.Errorf("members %d, settlers %d: %v", n[0], n[1], err)
			continue
		}
		got, err := decodeBody(KindGenesis, rawBody(genesisBody{Config: c}), genesisBounds())
		if err != nil || got.(genesisBody).Config != c {
			t.Errorf("members %d, settlers %d: genesis round trip %v", n[0], n[1], err)
		}
	}
}

func TestSettlersBodyFollowsMaxSettlers(t *testing.T) {
	keys := func(n int) []ed25519.PublicKey {
		out := make([]ed25519.PublicKey, n)
		for i := range out {
			out[i] = make(ed25519.PublicKey, 32)
			binary.BigEndian.PutUint32(out[i], uint32(i)+1)
		}
		return out
	}
	bd := formatBounds{MaxSettlers: 300, MaxBody: 1 << 20}
	b, err := encodeBody(settlersBody{Rule: Majority, Keys: keys(300)}, bd)
	if err != nil {
		t.Fatalf("300 keys under MaxSettlers 300: %v", err)
	}
	if got, err := decodeBody(KindSettlers, b, bd); err != nil || len(got.(settlersBody).Keys) != 300 {
		t.Fatalf("300 keys decoded: %v", err)
	}
	if _, err := encodeBody(settlersBody{Rule: Majority, Keys: keys(301)}, bd); err == nil {
		t.Error("301 keys under MaxSettlers 300")
	}
}

func TestCreateBeyondOldCeilings(t *testing.T) {
	f := newFounder(t, "f", GroupConfig{MaxMembers: 1100, MaxSettlers: 70}, appApply)
	if f.b.cfg.MaxMembers != 1100 || f.b.cfg.MaxSettlers != 70 {
		t.Fatalf("config %+v", f.b.cfg)
	}
	g := newFounder(t, "g", GroupConfig{}, appApply)
	if g.b.cfg.MaxMembers != 0 {
		t.Fatalf("default config %+v", g.b.cfg)
	}
}

func TestGenesisContinuation(t *testing.T) {
	cfg := GroupConfig{App: "a"}.WithDefaults()
	b := rawBody(genesisBody{Config: cfg})
	// A continues flag of 2, and a flag with no continuation fields, are refused.
	flagAt := len(b) - 1
	for name, mod := range map[string][]byte{
		"flag 2":        append(append([]byte(nil), b[:flagAt]...), 2),
		"flag 1 only":   append(append([]byte(nil), b[:flagAt]...), 1),
		"extra fields":  append(append([]byte(nil), b...), make([]byte, 32)...),
		"trailing byte": append(append([]byte(nil), b...), 0),
	} {
		if _, err := decodeBody(KindGenesis, mod, genesisBounds()); !errors.Is(err, ErrMalformed) {
			t.Errorf("%s: err = %v", name, err)
		}
	}
}

func TestSimpleBodies(t *testing.T) {
	bd := defaultBounds()
	k := func(b byte) ed25519.PublicKey { return pat(32, b) }
	for _, n := range []int{0, 31, 33, 64} {
		if _, err := decodeBody(KindRemoveMember, make([]byte, n), bd); !errors.Is(err, ErrMalformed) {
			t.Errorf("remove member body of %d bytes: %v", n, err)
		}
	}
	for name, s := range map[string]settlersBody{
		"none":       {Rule: Majority},
		"rule 0":     {Rule: 0, Keys: []ed25519.PublicKey{k(1)}},
		"rule 3":     {Rule: 3, Keys: []ed25519.PublicKey{k(1)}},
		"descending": {Rule: Majority, Keys: []ed25519.PublicKey{k(2), k(1)}},
		"duplicate":  {Rule: Majority, Keys: []ed25519.PublicKey{k(1), k(1)}},
		"too many":   {Rule: Majority, Keys: manyKeys(8)},
	} {
		if _, err := encodeBody(s, bd); err == nil {
			t.Errorf("settlers %s encoded", name)
		}
		if _, err := decodeBody(KindSettlers, rawBody(s), bd); !errors.Is(err, ErrMalformed) {
			t.Errorf("settlers %s decoded: %v", name, err)
		}
	}
	if _, err := encodeBody(settlersBody{Rule: Majority, Keys: manyKeys(7)}, bd); err != nil {
		t.Errorf("MaxSettlers keys: %v", err)
	}
	pulse := mustBody(t, pulseBody{Config: 2, Activating: eid(1), Claim: &pulseClaim{1, Digest(eid(2))}})
	for name, b := range map[string][]byte{
		"flags 4":       rawBody(pulseBody{}),
		"claim cut":     pulse[:len(pulse)-1],
		"trailing byte": append(append([]byte(nil), pulse...), 0),
	} {
		if name == "flags 4" {
			b = append(b[:len(b)-1:len(b)-1], 4)
		}
		if _, err := decodeBody(KindPulse, b, bd); !errors.Is(err, ErrMalformed) {
			t.Errorf("pulse %s: %v", name, err)
		}
	}
	if _, err := encodeBody(pulseBody{Config: 0, Activating: eid(1)}, bd); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("a config 0 pulse with an activating pivot: %v", err)
	}
	for name, s := range map[string]seedBody{
		"descending": {Pairs: []seedPair{{Key: []byte("b")}, {Key: []byte("a")}}},
		"duplicate":  {Pairs: []seedPair{{Key: []byte("a")}, {Key: []byte("a")}}},
	} {
		if _, err := decodeBody(KindSeed, rawBody(s), bd); !errors.Is(err, ErrMalformed) {
			t.Errorf("seed %s: %v", name, err)
		}
	}
	// A pair count that the input cannot hold is refused before anything is allocated.
	for _, n := range []uint64{1 << 40, math.MaxUint64} {
		var e kolocodec.Encoder
		e.PutUvarint(n)
		if _, err := decodeBody(KindSeed, e.Bytes(), bd); !errors.Is(err, ErrMalformed) {
			t.Errorf("seed of %d pairs: %v", n, err)
		}
		var s kolocodec.Encoder
		s.PutUvarint(uint64(Majority))
		s.PutUvarint(n)
		if _, err := decodeBody(KindSettlers, s.Bytes(), bd); !errors.Is(err, ErrMalformed) {
			t.Errorf("settlers of %d keys: %v", n, err)
		}
	}
	if _, err := decodeBody(Kind(8), nil, bd); !errors.Is(err, ErrMalformed) {
		t.Errorf("kind 8: %v", err)
	}
	if _, err := encodeBody(nil, bd); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("nil body: %v", err)
	}
	if d, err := decodeBody(KindData, nil, bd); err != nil || len(d.(dataBody)) != 0 {
		t.Errorf("empty data body: %v", err)
	}
}

func manyKeys(n int) []ed25519.PublicKey {
	out := make([]ed25519.PublicKey, n)
	for i := range out {
		out[i] = pat(32, byte(i*3))
	}
	return out
}

func TestKinds(t *testing.T) {
	for k, b := range []entryBody{genesisBody{}, dataBody{}, NewMember{}, removeBody{}, settlersBody{}, evidenceBody{}, pulseBody{}, seedBody{}} {
		if b.Kind() != Kind(k) {
			t.Errorf("%T.Kind() = %d, want %d", b, b.Kind(), k)
		}
	}
	if KindSeed != 7 || KindGenesis != 0 {
		t.Error("kind numbers are part of the format")
	}
}

func TestNewMemberBody(t *testing.T) {
	bd := defaultBounds()
	s := newSigner(t)
	s.append(t, "x")
	w := s.member()
	w.Roles = RoleAdmin
	b := mustBody(t, w)
	got, err := decodeBody(KindAddMember, b, bd)
	if err != nil {
		t.Fatal(err)
	}
	if g := got.(NewMember); !bytes.Equal(g.Key, w.Key) || g.Strand != w.Strand || !bytes.Equal(g.Header, w.Header) || g.Roles != RoleAdmin {
		t.Errorf("decoded %+v", g)
	}
	other := newSigner(t)
	utf8 := newSigner(t, kololog.WithValueEncoding(kololog.UTF8))
	tampered := bytes.Clone(w.Header)
	tampered[len(tampered)-1] ^= 1
	for name, mod := range map[string]NewMember{
		"roles 2":         {Key: w.Key, Strand: w.Strand, Header: w.Header, Roles: 2},
		"another key":     {Key: other.id.PublicKey(), Strand: w.Strand, Header: w.Header},
		"another strand":  {Key: w.Key, Strand: lid(7), Header: w.Header},
		"bad signature":   {Key: w.Key, Strand: w.Strand, Header: tampered},
		"not Bytes":       utf8.member(),
		"header of junk":  {Key: w.Key, Strand: w.Strand, Header: []byte("junk")},
		"no header":       {Key: w.Key, Strand: w.Strand},
		"header too long": {Key: w.Key, Strand: w.Strand, Header: append(bytes.Clone(w.Header), make([]byte, maxHeader)...)},
	} {
		if _, err := decodeBody(KindAddMember, rawBody(mod), bd); !errors.Is(err, ErrMalformed) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if _, err := decodeBody(KindAddMember, rawBody(NewMember{Key: w.Key, Strand: w.Strand, Header: append(bytes.Clone(w.Header), make([]byte, maxHeader)...)}), bd); !errors.Is(err, kolocodec.ErrTooLarge) {
		t.Error("a header over MaxHeader is a bound error")
	}
	// The bound is exact: 512 bytes get as far as the header's own trailing bytes check, 513 are over the bound.
	pad := func(n int) []byte { return append(bytes.Clone(w.Header), make([]byte, n-len(w.Header))...) }
	if _, err := decodeBody(KindAddMember, rawBody(NewMember{Key: w.Key, Strand: w.Strand, Header: pad(maxHeader)}), bd); errors.Is(err, kolocodec.ErrTooLarge) || err == nil {
		t.Errorf("a header of MaxHeader bytes: %v", err)
	}
	if _, err := decodeBody(KindAddMember, rawBody(NewMember{Key: w.Key, Strand: w.Strand, Header: pad(maxHeader + 1)}), bd); !errors.Is(err, kolocodec.ErrTooLarge) {
		t.Errorf("a header of MaxHeader+1 bytes: %v", err)
	}
	// MaxHeader holds a real header with room to spare.
	if len(w.Header) > maxHeader/2 {
		t.Errorf("a header is %d bytes", len(w.Header))
	}
}

func TestEvidenceBody(t *testing.T) {
	bd := defaultBounds()
	a := newSigner(t)
	a.append(t, "one")
	headA, recA := a.head(t), a.record(t, 0)
	b := a.fork(t)
	b.append(t, "two")
	headB, recB := b.head(t), b.record(t, 0)
	key, strand := a.id.PublicKey(), a.log.ID()

	recs := evidenceBody{Key: key, Strand: strand, Type: evidenceRecords, A: recA.Encode(), B: recB.Encode()}
	heads := evidenceBody{Key: key, Strand: strand, Type: evidenceHeads, A: headA.Encode(), B: headB.Encode()}
	for name, ev := range map[string]evidenceBody{"records": recs, "heads": heads} {
		enc := mustBody(t, ev)
		got, err := decodeBody(KindEvidence, enc, bd)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if !reflect.DeepEqual(got, ev) {
			t.Errorf("%s: decoded %+v", name, got)
		}
	}
	other := newSigner(t)
	other.append(t, "x")
	tamperedRec := bytes.Clone(recB.Encode())
	tamperedRec[len(tamperedRec)-1] ^= 1
	tamperedHead := bytes.Clone(headB.Encode())
	tamperedHead[len(tamperedHead)-1] ^= 1
	c := b.fork(t)
	c.append(t, "one")
	c.append(t, "three")
	for name, ev := range map[string]evidenceBody{
		"type 0":                 {Key: key, Strand: strand, Type: 0, A: recs.A, B: recs.B},
		"type 3":                 {Key: key, Strand: strand, Type: 3, A: recs.A, B: recs.B},
		"records swapped type":   {Key: key, Strand: strand, Type: evidenceHeads, A: recs.A, B: recs.B},
		"same record twice":      {Key: key, Strand: strand, Type: evidenceRecords, A: recs.A, B: recs.A},
		"same head twice":        {Key: key, Strand: strand, Type: evidenceHeads, A: heads.A, B: heads.A},
		"another key":            {Key: other.id.PublicKey(), Strand: strand, Type: evidenceRecords, A: recs.A, B: recs.B},
		"another strand":         {Key: key, Strand: lid(5), Type: evidenceRecords, A: recs.A, B: recs.B},
		"another key's record":   {Key: key, Strand: strand, Type: evidenceRecords, A: recs.A, B: other.record(t, 0).Encode()},
		"bad record signature":   {Key: key, Strand: strand, Type: evidenceRecords, A: recs.A, B: tamperedRec},
		"bad head signature":     {Key: key, Strand: strand, Type: evidenceHeads, A: heads.A, B: tamperedHead},
		"heads of two sizes":     {Key: key, Strand: strand, Type: evidenceHeads, A: heads.A, B: c.head(t).Encode()},
		"records of two indexes": {Key: key, Strand: strand, Type: evidenceRecords, A: recs.A, B: c.record(t, 1).Encode()},
		"junk":                   {Key: key, Strand: strand, Type: evidenceRecords, A: []byte("a"), B: []byte("b")},
		"object over 4 KiB":      {Key: key, Strand: strand, Type: evidenceRecords, A: recs.A, B: make([]byte, maxEvidenceObject+1)},
		"first over 4 KiB":       {Key: key, Strand: strand, Type: evidenceRecords, A: make([]byte, maxEvidenceObject+1), B: recs.B},
	} {
		_, err := decodeBody(KindEvidence, rawBody(ev), bd)
		if !errors.Is(err, ErrMalformed) {
			t.Errorf("%s: %v", name, err)
		}
		// An object over 4 KiB is refused for its size, before it is parsed.
		if strings.HasSuffix(name, "over 4 KiB") && !errors.Is(err, kolocodec.ErrTooLarge) {
			t.Errorf("%s: %v is not a bound error", name, err)
		}
	}
	// The largest evidence body fits the fixed body bound.
	if n := 32 + 32 + 1 + 2*(2+maxEvidenceObject); n > maxFixedBody {
		t.Errorf("an evidence body of %d bytes is over %d", n, maxFixedBody)
	}
}
