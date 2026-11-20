package kolobraid

import (
	"bytes"
	"encoding/hex"
	"errors"
	"strings"
	"testing"

	kolostore "github.com/ciranda-dev/kolo-store"
)

func vtPat(n int, seed int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(seed + i)
	}
	return b
}

// Known answers computed independently of the Go code: a hand-written Python script that writes the encodings out byte by byte
// and hashes them with hashlib.
const (
	vecDigest0     = "405d5fd98975caa661462c28051c0b82e0943836ff0894844497778bffb106f6"
	vecDigest1     = "67af0b751dc8f7691fe86dedba84268b74f42a73a61ef720881f1a1fc119c5d4"
	vecDigest2     = "0a6eec6b82c61e491f349bea53e8d47d8eed656230157fa3db78b4efca9314e6"
	vecDigest3     = "ec0d081e4cc420dd29e395bf4be73db8f5956537525f0e58e130eab710110165"
	vecDigest4     = "4da9b4f7e4847b4456a996bc440a117db8449cb3005ca3d445cd9f9d7303d821"
	vecContent     = "d1936d0e0d0b4c02074d79b65cec4ecf17ae0795b800f1f70a117617f33e0c84"
	vecContentSeed = "b0cef3cc7f9f8e99d81fb6e79589b5e5ac503d13ada3b072b3164717bd1afc15"
)

// vecScript is the history of the vectors: the body of every entry is a list of the writes of vtApply's operations.
func vecScript() []Entry {
	big := strings.Repeat("z", 300)
	ops := [][]vtOp{
		{{kind: 'p', a: "alpha", b: "1"}, {kind: 'p', a: "beta", b: "2"}},
		{{kind: 'p', a: "alpha", b: "3"}, {kind: 'd', a: "beta"}},
		{{kind: 'p', a: "ignored", b: "x"}, {kind: 'x', a: "no"}},
		{{kind: 'p', a: "", b: "empty"}, {kind: 'r', a: "a", b: "b"}},
		{{kind: 'p', a: "zeta", b: big}},
	}
	es := make([]Entry, len(ops))
	for i := range es {
		es[i] = vtEntry(i, ops[i]...)
		copy(es[i].ID[:], vtPat(32, i+1))
	}
	return es
}

func TestDigestVectors(t *testing.T) {
	want := []string{vecDigest0, vecDigest1, vecDigest2, vecDigest3, vecDigest4}
	cfg := vtConfig()
	cfg.Initial = Digest(vtPat(32, 0x40))
	es := vecScript()
	check := func(what string, digestAt func(i int) Digest) {
		for i, w := range want {
			if got := digestAt(i).String(); got != w {
				t.Errorf("%s: digest after position %d = %s, want %s", what, i, got, w)
			}
		}
	}
	// settled in single batches, so every digest is a checkpoint-free progress
	v := vtOpen(t, kolostore.NewMemory(), cfg, vtApply)
	got := make([]Digest, len(es))
	for i, e := range es {
		res, err := settleOne(v, uint64(i), e)
		if err != nil {
			t.Fatal(err)
		}
		got[i] = res.Progress.Digest
	}
	check("settled", func(i int) Digest { return got[i] })
	// tentatively, the same chain
	tv := vtOpen(t, kolostore.NewMemory(), cfg, vtApply)
	applyAllTail(t, tv, es)
	check("tail", func(i int) Digest { ti, _ := tv.TailEntry(i); return ti.Digest })
	// the status of the third entry
	if rej, _ := v.Rejected(2); !rej {
		t.Error("entry 2 must be rejected")
	}
	cd, err := v.ContentDigest()
	if err != nil || cd.String() != vecContent {
		t.Errorf("content digest %s %v, want %s", cd, err, vecContent)
	}
	// a successor view that starts at position 100 holds seed pairs
	sc := vtConfig()
	sc.Next = 100
	sv := vtOpen(t, kolostore.NewMemory(), sc, vtApply)
	if err := sv.SeedPairs([]seedPair{{Key: nil, Value: []byte("empty key")}, {Key: []byte("a"), Value: []byte("1")}, {Key: []byte("b")}}); err != nil {
		t.Fatal(err)
	}
	if cd, err := sv.ContentDigest(); err != nil || cd.String() != vecContentSeed {
		t.Errorf("seed content digest %s %v, want %s", cd, err, vecContentSeed)
	}
	if _, err := hex.DecodeString(vecContent); err != nil {
		t.Fatal(err)
	}
}

func TestProgressRoundTrip(t *testing.T) {
	for _, p := range []progress{
		{},
		{Next: 1, Digest: Digest(vtPat(32, 1)), Batches: 1},
		{Next: 1 << 40, Digest: Digest(vtPat(32, 7)), Batches: 1<<40 - 3, Offset: 77},
		{Next: ^uint64(0), Digest: Digest(vtPat(32, 9)), Batches: ^uint64(0), Offset: ^uint64(0)},
	} {
		got, err := decodeProgress(p.encode())
		if err != nil || got != p {
			t.Errorf("%+v: %+v %v", p, got, err)
		}
	}
}

func TestDecodeProgressRefuses(t *testing.T) {
	good := progress{Next: 300, Digest: Digest(vtPat(32, 3)), Batches: 2, Offset: 5}.encode()
	var wrongTag []byte
	wrongTag = append(wrongTag, 0x12)
	wrongTag = append(wrongTag, "kolo-braid/view/v1"...)
	wrongTag = append(wrongTag, good[19:]...)
	cases := map[string][]byte{
		"empty":              nil,
		"truncated":          good[:len(good)-1],
		"trailing":           append(bytes.Clone(good), 0),
		"wrong tag":          wrongTag,
		"offset > next":      progress{Next: 3, Batches: 1, Offset: 4}.encode(),
		"batches > next":     progress{Next: 3, Batches: 4}.encode(),
		"non-minimal varint": append(append(bytes.Clone(good[:19]), 0xac, 0x82, 0x00), good[21:]...),
		"junk":               bytes.Repeat([]byte{0xff}, 70),
	}
	for name, b := range cases {
		if _, err := decodeProgress(b); !errors.Is(err, ErrCorrupt) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestOpenRefusesCorruptStoredState(t *testing.T) {
	b := kolostore.NewMemory()
	b.Put([]byte(progressKey), []byte("garbage"))
	if _, err := openView(b, vtConfig(), vtApply); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
	b = kolostore.NewMemory()
	v := vtOpen(t, b, vtConfig(), vtApply)
	b.Put(posKey(checkpoint, 5), []byte("short"))
	if _, _, err := v.Checkpoint(5); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
}

func TestCheckpointDue(t *testing.T) {
	for _, c := range []struct {
		before, after uint64
		every         int
		want          bool
	}{
		{0, 63, 64, false}, {0, 64, 64, true}, {63, 64, 64, true}, {64, 65, 64, false}, {64, 128, 64, true},
		{0, 1000, 64, true}, {100, 100, 64, false}, {127, 128, 64, true}, {128, 191, 64, false},
		{99, 100, 100, true}, {1 << 30, 1<<30 + 1, 1 << 20, false},
	} {
		if got := checkpointDue(c.before, c.after, c.every); got != c.want {
			t.Errorf("checkpointDue(%d, %d, %d) = %v", c.before, c.after, c.every, got)
		}
	}
}

func TestKeyspacesDoNotOverlap(t *testing.T) {
	// the view's own keys must not start with the view or the roster prefix
	for _, k := range []string{progressKey, checkpoint, rejectedKey} {
		if k[0] == viewPrefix || k[0] == rosterPrefix {
			t.Errorf("%q starts in another keyspace", k)
		}
	}
	if !bytes.HasPrefix(posKey(checkpoint, 1), []byte(checkpoint)) || len(posKey(rejectedKey, 1)) != len(rejectedKey)+8 {
		t.Error("position keys")
	}
	if bytes.Compare(posKey(checkpoint, 255), posKey(checkpoint, 256)) >= 0 {
		t.Error("position keys sort by position")
	}
}
