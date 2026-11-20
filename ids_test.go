package kolobraid

import (
	"bytes"
	"iter"
	"testing"

	kololog "github.com/ciranda-dev/kolo-log"
)

func TestEntryIDVectors(t *testing.T) {
	for i, want := range []string{"entryid-0", "entryid-1"} {
		rec := vec(t, "record-"+string(rune('0'+i)))
		// The record vectors are parsed and rebuilt by kolo-log, so they are valid records.
		r, err := kololog.DecodeRecord(rec)
		if err != nil || !bytes.Equal(r.Encode(), rec) {
			t.Fatalf("record-%d is not a canonical kolo-log record: %v", i, err)
		}
		if got := entryIDOf(rec); !bytes.Equal(got[:], vec(t, want)) {
			t.Errorf("%s: got %x want %x", want, got, vec(t, want))
		}
	}
	if entryIDOf(vec(t, "record-0")) == entryIDOf(vec(t, "record-1")) {
		t.Error("two records share an ID")
	}
}

func TestOrderKeyVectors(t *testing.T) {
	a, b := arr(pat(32, 0x10)), arr(pat(32, 0x20))
	if got := orderKey(a, b); !bytes.Equal(got[:], vec(t, "order-0")) {
		t.Errorf("order-0: got %x", got)
	}
	if got := orderKey(b, a); !bytes.Equal(got[:], vec(t, "order-1")) {
		t.Errorf("order-1: got %x", got)
	}
}

func TestConfigIDVectors(t *testing.T) {
	seat := func(key, strand byte) seatRec {
		return seatRec{Key: pat(32, key), Strand: kololog.LogID(arr(pat(32, strand)))}
	}
	cases := []struct {
		name  string
		n     uint64
		rule  QuorumRule
		seats []seatRec
	}{
		{"config-0", 0, Majority, []seatRec{seat(0x40, 0x50)}},
		// Given out of key order: the ID sorts them.
		{"config-1", 200, TwoThirds, []seatRec{seat(0x90, 0xa0), seat(0x10, 0x20), seat(0x50, 0x60)}},
		{"config-2", 1, Majority, nil},
	}
	for _, c := range cases {
		before := append([]seatRec(nil), c.seats...)
		if got := configIDOf(c.n, c.rule, c.seats); !bytes.Equal(got[:], vec(t, c.name)) {
			t.Errorf("%s: got %x want %x", c.name, got, vec(t, c.name))
		}
		for i := range before {
			if !bytes.Equal(before[i].Key, c.seats[i].Key) {
				t.Errorf("%s: ConfigIDOf reordered its argument", c.name)
			}
		}
	}
}

func TestRunningDigestVectors(t *testing.T) {
	cases := []struct {
		name   string
		prev   byte
		pos    uint64
		id     byte
		status entryStatus
		writes []viewWrite
	}{
		{"digest-0", 1, 0, 2, statusApplied, nil},
		{"digest-1", 3, 1000, 4, statusRejected, []viewWrite{{opPut, []byte("k"), []byte("v")}}},
		{"digest-2", 5, 1 << 40, 6, statusVoid, nil},
		{"digest-3", 7, 5, 8, statusApplied, []viewWrite{
			{opPut, []byte("alpha"), []byte("1")},
			{opDelete, []byte("beta"), nil},
			{opDeleteRange, []byte("a"), []byte("z")},
			{opPut, nil, nil},
		}},
	}
	for _, c := range cases {
		got := runningDigest(Digest(arr(pat(32, c.prev))), c.pos, EntryID(arr(pat(32, c.id))), c.status, c.writes)
		if !bytes.Equal(got[:], vec(t, c.name)) {
			t.Errorf("%s: got %x want %x", c.name, got, vec(t, c.name))
		}
	}
}

func TestRunningDigestDependsOnEveryInput(t *testing.T) {
	prev, id := Digest(arr(pat(32, 1))), EntryID(arr(pat(32, 2)))
	w := []viewWrite{{opPut, []byte("a"), []byte("b")}}
	base := runningDigest(prev, 3, id, statusApplied, w)
	others := map[string]Digest{
		"prev":     runningDigest(Digest(arr(pat(32, 9))), 3, id, statusApplied, w),
		"position": runningDigest(prev, 4, id, statusApplied, w),
		"id":       runningDigest(prev, 3, EntryID(arr(pat(32, 9))), statusApplied, w),
		"status":   runningDigest(prev, 3, id, statusVoid, w),
		"writes":   runningDigest(prev, 3, id, statusApplied, nil),
		"op":       runningDigest(prev, 3, id, statusApplied, []viewWrite{{opDelete, []byte("a"), []byte("b")}}),
		"split":    runningDigest(prev, 3, id, statusApplied, []viewWrite{{opPut, []byte("ab"), nil}}),
	}
	for name, d := range others {
		if d == base {
			t.Errorf("changing %s does not change the digest", name)
		}
	}
}

func TestInitialDigestIsGroupID(t *testing.T) {
	g := GroupID(arr(pat(32, 0x33)))
	if initialDigest(g) != Digest(g) {
		t.Error("the digest before position 0 is the group ID")
	}
}

func seq2(pairs [][2][]byte) iter.Seq2[[]byte, []byte] {
	return func(yield func([]byte, []byte) bool) {
		for _, p := range pairs {
			if !yield(p[0], p[1]) {
				return
			}
		}
	}
}

func TestContentDigestVectors(t *testing.T) {
	if got := contentDigest(0, seq2(nil)); !bytes.Equal(got[:], vec(t, "content-0")) {
		t.Errorf("content-0: got %x", got)
	}
	pairs := [][2][]byte{{nil, []byte("empty key")}, {[]byte("a"), nil}, {[]byte("b"), pat(300, 3)}}
	if got := contentDigest(77, seq2(pairs)); !bytes.Equal(got[:], vec(t, "content-1")) {
		t.Errorf("content-1: got %x", got)
	}
}

func TestTopicVector(t *testing.T) {
	if got := topicOf(GroupID(arr(pat(32, 0x30)))); !bytes.Equal(got[:], vec(t, "topic-0")) {
		t.Errorf("topic-0: got %x", got)
	}
}

func TestTagsAreDistinct(t *testing.T) {
	tags := []string{entryIDTag, orderTag, configTag, digestTag, contentTag, topicTag, joinTag, welcomeTag, entryTag}
	seen := map[string]bool{}
	for _, tag := range tags {
		if seen[tag] {
			t.Errorf("tag %q repeats", tag)
		}
		seen[tag] = true
		if len(tag) < len("kolo-braid/x/v0") || tag[:len("kolo-braid/")] != "kolo-braid/" || tag[len(tag)-3:] != "/v0" {
			t.Errorf("tag %q is not kolo-braid/<name>/v0", tag)
		}
	}
}

func TestIDStrings(t *testing.T) {
	id := EntryID(arr(pat(32, 0)))
	const want = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
	if id.String() != want || GroupID(id).String() != want || configID(id).String() != want || Digest(id).String() != want {
		t.Error("ID strings are lowercase hex")
	}
}
