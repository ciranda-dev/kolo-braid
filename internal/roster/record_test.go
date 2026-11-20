package roster

import (
	"errors"
	"iter"
	"slices"
	"testing"

	kolocodec "github.com/ciranda-dev/kolo-codec"
)

func pairs(ws []Write) iter.Seq2[[]byte, []byte] {
	return func(yield func([]byte, []byte) bool) {
		for _, w := range ws {
			if !yield(w.Key, w.Value) {
				return
			}
		}
	}
}

func sampleRoster(t *testing.T) *seq {
	t.Helper()
	s := founded(t, addW(kB, sB, Admin), addW(kC, sC, 0), addW(kD, sD, 0), settlersE(TwoThirds, kA, kB, kC))
	s.must(sA, addW(kC, sE, Admin))
	s.must(sB, removeW(kD))
	return s
}

func TestRecordsRoundTrip(t *testing.T) {
	s := sampleRoster(t)
	recs := s.r.Records()
	l, err := Load(s.r.Genesis(), pairs(recs))
	if err != nil {
		t.Fatal(err)
	}
	if !slices.EqualFunc(l.Records(), recs, sameWrite) || !slices.Equal(l.Members(), s.r.Members()) ||
		l.Admins() != s.r.Admins() || l.UsedCount() != s.r.UsedCount() || !l.Founded() {
		t.Fatal("reload differs")
	}
	rule, st := l.Settlers()
	if rule != TwoThirds || !slices.Equal(st, []Key{kA, kB, kC}) {
		t.Fatal("settlers")
	}
	// the reloaded roster decides the same
	e := Entry{Kind: KindAddMember, Strand: sB, Index: 9, Add: Add{Key: kD, Strand: sD}}
	if l.Check(&e) != UsedStrand || s.r.Check(&e) != UsedStrand {
		t.Fatal("reloaded roster forgot a used strand")
	}
	for i := 1; i < len(recs); i++ {
		if string(recs[i-1].Key) >= string(recs[i].Key) {
			t.Fatal("records not ascending")
		}
	}
	empty, err := Load(s.r.Genesis(), pairs(nil))
	if err != nil || empty.Founded() || empty.Records() != nil || empty.Len() != 0 {
		t.Fatal("empty load")
	}
}

func TestLoadCorrupt(t *testing.T) {
	s := sampleRoster(t)
	g := s.r.Genesis()
	recs := s.r.Records()
	find := func(k string) int {
		return slices.IndexFunc(recs, func(w Write) bool { return string(w.Key) == k })
	}
	edit := func(f func([]Write) []Write) []Write {
		c := make([]Write, len(recs))
		for i, w := range recs {
			c[i] = Write{Key: slices.Clone(w.Key), Value: slices.Clone(w.Value)}
		}
		return f(c)
	}
	wB := find(string(memberKey(kB)))
	settlersAt := find("s")
	paramsAt := find("p")
	usedB := find("u" + string(sB[:]))
	settlers := func(rule, n uint64, keys ...Key) []byte {
		var e kolocodec.Encoder
		e.PutUvarint(rule)
		e.PutUvarint(n)
		for _, k := range keys {
			e.PutFixed(k[:])
		}
		return e.Bytes()
	}
	cases := map[string][]Write{
		"empty key":          edit(func(c []Write) []Write { return append(c, Write{Key: nil}) }),
		"unknown prefix":     edit(func(c []Write) []Write { return append(c, Write{Key: []byte("x")}) }),
		"short member key":   edit(func(c []Write) []Write { return append(c, Write{Key: []byte("w1")}) }),
		"params twice":       edit(func(c []Write) []Write { return append(c, c[paramsAt]) }),
		"settlers twice":     edit(func(c []Write) []Write { return append(c, c[settlersAt]) }),
		"member twice":       edit(func(c []Write) []Write { return append(c, c[wB]) }),
		"used twice":         edit(func(c []Write) []Write { return append(c, c[usedB]) }),
		"no params":          edit(func(c []Write) []Write { return slices.Delete(c, paramsAt, paramsAt+1) }),
		"no settlers":        edit(func(c []Write) []Write { return slices.Delete(c, settlersAt, settlersAt+1) }),
		"active not used":    edit(func(c []Write) []Write { return slices.Delete(c, usedB, usedB+1) }),
		"other params":       edit(func(c []Write) []Write { c[paramsAt].Value[0] ^= 1; return c }),
		"params trailing":    edit(func(c []Write) []Write { c[paramsAt].Value = append(c[paramsAt].Value, 0); return c }),
		"used short":         edit(func(c []Write) []Write { c[usedB].Value = c[usedB].Value[:31]; return c }),
		"used by other key":  edit(func(c []Write) []Write { c[usedB].Value = kA[:]; return c }),
		"member short":       edit(func(c []Write) []Write { c[wB].Value = c[wB].Value[:20]; return c }),
		"member roles":       edit(func(c []Write) []Write { c[wB].Value[32] = 2; return c }),
		"member no position": edit(func(c []Write) []Write { c[wB].Value = c[wB].Value[:33]; return c }),
		"member trailing":    edit(func(c []Write) []Write { c[wB].Value = append(c[wB].Value, 0); return c }),
		"member non-canonical": edit(func(c []Write) []Write {
			c[wB].Value = append(c[wB].Value[:32], 0x81, 0x00, 0x01)
			return c
		}),
		"two keys one strand": edit(func(c []Write) []Write {
			v := slices.Clone(c[wB].Value)
			return append(c, Write{Key: memberKey(kD), Value: v})
		}),
		"settler not active":    edit(func(c []Write) []Write { c[settlersAt].Value = settlers(1, 1, kD); return c }),
		"settlers unsorted":     edit(func(c []Write) []Write { c[settlersAt].Value = settlers(1, 2, kB, kA); return c }),
		"settlers duplicate":    edit(func(c []Write) []Write { c[settlersAt].Value = settlers(1, 2, kA, kA); return c }),
		"settlers rule 0":       edit(func(c []Write) []Write { c[settlersAt].Value = settlers(0, 1, kA); return c }),
		"settlers rule 3":       edit(func(c []Write) []Write { c[settlersAt].Value = settlers(3, 1, kA); return c }),
		"settlers short":        edit(func(c []Write) []Write { c[settlersAt].Value = settlers(1, 2, kA); return c }),
		"settlers over max":     edit(func(c []Write) []Write { c[settlersAt].Value = settlers(1, 8); return c }),
		"settlers trailing":     edit(func(c []Write) []Write { c[settlersAt].Value = append(settlers(1, 1, kA), 0); return c }),
		"settlers empty value":  edit(func(c []Write) []Write { c[settlersAt].Value = nil; return c }),
		"params empty value":    edit(func(c []Write) []Write { c[paramsAt].Value = nil; return c }),
		"only a used record":    {recs[usedB]},
		"only a settler record": {recs[settlersAt]},
	}
	for name, c := range cases {
		if _, err := Load(g, pairs(c)); !errors.Is(err, ErrCorrupt) {
			t.Errorf("%s: %v", name, err)
		}
	}
	// a store of more members than MaxMembers
	full := newSeq(t, genesisOf(3, 3, 0))
	full.must(sA, addW(kB, sB, 0))
	full.must(sA, addW(kC, sC, 0))
	over := full.r.Records()
	w := full.r.memberWrite(Member{Key: kD, Strand: sD})
	over = append(over, w, Write{Key: append([]byte{recUsed}, sD[:]...), Value: kD[:]})
	if _, err := Load(full.r.Genesis(), pairs(over)); !errors.Is(err, ErrCorrupt) {
		t.Errorf("too many members: %v", err)
	}
	if _, err := Load(Genesis{}, pairs(recs)); !errors.Is(err, ErrInvalid) {
		t.Error("bad genesis")
	}
	// zero settlers after evidence are a valid state
	z := founded(t, addW(kB, sB, 0))
	z.must(sB, evidenceE(kA, sA))
	if _, err := Load(z.r.Genesis(), pairs(z.r.Records())); err != nil {
		t.Fatal(err)
	}
}

func TestWritesOfEachChange(t *testing.T) {
	s := founded(t)
	res := s.must(sA, addW(kB, sB, Admin))
	if len(res.Writes) != 2 || string(res.Writes[0].Key) != string(memberKey(kB)) || res.Writes[1].Key[0] != recUsed {
		t.Fatal("add writes")
	}
	res = s.must(sA, addW(kB, sB, 0))
	if len(res.Writes) != 1 || string(res.Writes[0].Key) != string(memberKey(kB)) {
		t.Fatal("roles writes")
	}
	res = s.must(sA, addW(kB, sC, 0))
	if len(res.Writes) != 2 || res.Writes[1].Key[0] != recUsed || StrandID(res.Writes[1].Key[1:]) != sC {
		t.Fatal("replace writes")
	}
	res = s.must(sA, settlersE(Majority, kA, kB))
	if len(res.Writes) != 1 || res.Writes[0].Key[0] != recSettlers {
		t.Fatal("settlers writes")
	}
	res = s.must(sA, removeW(kB))
	if len(res.Writes) != 2 || !res.Writes[0].Delete || res.Writes[1].Key[0] != recSettlers || res.Writes[1].Delete {
		t.Fatal("remove writes")
	}
	if res := s.must(sA, dataE()); res.Writes != nil {
		t.Fatal("data writes")
	}
}
