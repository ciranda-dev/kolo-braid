package roster

import (
	"slices"
	"testing"
)

// fuzzHistory turns bytes into a genesis and a sequence of entries over 6 keys and 8 strands: every kind, malformed
// entries, wrong genesis entries, forged authors, reused strands and arbitrary settler sets.
func fuzzHistory(data []byte) (Genesis, []Entry) {
	next := func() byte {
		if len(data) == 0 {
			return 0
		}
		b := data[0]
		data = data[1:]
		return b
	}
	h := next()
	mw := 2 + int(h&3)
	g := Genesis{Group: eid("fuzz"), Founder: kid(0), Strand: sid(0), Rule: Rule(1 + h>>2&1), MaxMembers: mw,
		MaxSettlers: 1 + int(h>>3&3)%mw, Seeds: uint64(h >> 5 & 3)}
	index := map[StrandID]uint64{}
	var out []Entry
	for len(data) > 0 && len(out) < 400 {
		op, a, b, c := next(), next(), next(), next()
		strand := sid(int(op & 7))
		e := Entry{Strand: strand, Index: index[strand], Kind: Kind(op >> 3 & 7), Malformed: op&0x80 != 0}
		index[strand]++
		switch e.Kind {
		case KindGenesis:
			e.ID = g.Group
			if a&1 != 0 {
				e.ID = eid("not genesis")
			}
		case KindAddMember:
			e.Add = Add{Key: kid(int(a % 6)), Strand: sid(int(b % 8)), Roles: Role(c % 3)}
		case KindRemoveMember:
			e.Target = kid(int(a % 6))
		case KindSettlers:
			e.Settlers.Rule = Rule(c % 4)
			for k := range 6 {
				if a&(1<<k) != 0 {
					e.Settlers.Keys = append(e.Settlers.Keys, kid(k))
				}
			}
			if b&1 != 0 && len(e.Settlers.Keys) > 1 {
				e.Settlers.Keys[0], e.Settlers.Keys[1] = e.Settlers.Keys[1], e.Settlers.Keys[0]
			}
		case KindEvidence:
			e.Accused = Seat{Key: kid(int(a % 6)), Strand: sid(int(b % 8))}
		case KindSeed:
			e.Index = uint64(a % 4)
		}
		out = append(out, e)
	}
	return g, out
}

func FuzzApply(f *testing.F) {
	f.Add([]byte{0x03, 0x00, 0, 0, 0, 0x10, 1, 1, 1, 0x11, 0, 0, 0, 0x21, 0, 0, 0})
	f.Add([]byte{0x1f, 0x00, 0, 0, 0, 0x10, 1, 1, 1, 0x10, 2, 2, 0, 0x20, 0x07, 0, 1, 0x19, 1, 0, 0, 0x28, 1, 1, 0})
	f.Add([]byte{0xff, 0x00, 1, 0, 0, 0x00, 0, 0, 0, 0x38, 0, 0, 0, 0x90, 1, 1, 1, 0x10, 1, 0, 2})
	f.Add([]byte{0x02, 0x00, 0, 0, 0, 0x10, 1, 1, 1, 0x10, 1, 2, 1, 0x11, 0, 0, 0, 0x29, 0, 0, 0, 0x18, 1, 0, 0})
	f.Fuzz(func(t *testing.T, data []byte) {
		g, entries := fuzzHistory(data)
		r, err := New(g)
		if err != nil {
			t.Fatal(err)
		}
		rf := &ref{g: g}
		store := map[string][]byte{}
		for p := range entries {
			e := &entries[p]
			c := r.Clone()
			check := r.Check(e)
			res := r.Apply(uint64(p), e)
			if res.Reason != check || res.Effective() != rf.step(uint64(p), e) {
				t.Fatalf("position %d: %v, check %v, reference disagrees", p, res.Reason, check)
			}
			if !slices.Equal(r.Members(), rf.members()) {
				t.Fatalf("position %d: members differ from the reference", p)
			}
			if res.Effective() != c.Apply(uint64(p), e).Effective() {
				t.Fatal("clone decides differently")
			}
			for _, w := range res.Writes {
				if w.Delete {
					delete(store, string(w.Key))
				} else {
					store[string(w.Key)] = w.Value
				}
			}
			if r.Len() > g.MaxMembers {
				t.Fatal("over MaxMembers")
			}
			_, st := r.Settlers()
			if len(st) > g.MaxSettlers {
				t.Fatal("over MaxSettlers")
			}
			for _, k := range st {
				if _, ok := r.Member(k); !ok {
					t.Fatal("inactive settler")
				}
			}
		}
		recs := r.Records()
		if len(recs) != len(store) {
			t.Fatalf("%d records, %d left by the writes", len(recs), len(store))
		}
		for _, w := range recs {
			if string(store[string(w.Key)]) != string(w.Value) {
				t.Fatal("record differs from the writes")
			}
		}
		l, err := Load(g, pairs(recs))
		if err != nil {
			t.Fatal(err)
		}
		if !slices.EqualFunc(l.Records(), recs, sameWrite) || l.Admins() != r.Admins() {
			t.Fatal("reload differs")
		}
	})
}

func FuzzLoad(f *testing.F) {
	g := genesisOf(64, 7, 0)
	r, err := New(g)
	if err != nil {
		f.Fatal(err)
	}
	for i, e := range []Entry{{Kind: KindGenesis, ID: g.Group}, addW(kB, sB, Admin), addW(kC, sC, 0), settlersE(Majority, kA, kB)} {
		e.Strand, e.Index = sA, uint64(i)
		if !r.Apply(uint64(i), &e).Effective() {
			f.Fatal("seed history")
		}
	}
	var seed []byte
	for _, w := range r.Records() {
		seed = append(seed, byte(len(w.Key)))
		seed = append(seed, w.Key...)
		seed = append(seed, byte(len(w.Value)))
		seed = append(seed, w.Value...)
	}
	f.Add(seed)
	f.Add(seed[:len(seed)/2])
	f.Add([]byte{1, 'p', 0})
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		var recs []Write
		for len(data) > 0 {
			n := int(data[0])
			data = data[1:]
			k := data[:min(n, len(data))]
			data = data[len(k):]
			var v []byte
			if len(data) > 0 {
				m := int(data[0])
				data = data[1:]
				v = data[:min(m, len(data))]
				data = data[len(v):]
			}
			recs = append(recs, Write{Key: k, Value: v})
		}
		r, err := Load(g, pairs(recs))
		if err != nil {
			return
		}
		again, err := Load(g, pairs(r.Records()))
		if err != nil || !slices.EqualFunc(again.Records(), r.Records(), sameWrite) || again.Admins() != r.Admins() {
			t.Fatalf("a loaded roster does not reload: %v", err)
		}
		for i := range 8 {
			r.Apply(uint64(i), &Entry{Kind: Kind(2 + i%4), Strand: sid(i % 3), Index: 1, Target: kid(i % 3),
				Add: Add{Key: kid(i), Strand: sid(i + 3)}, Accused: Seat{Key: kid(i % 3), Strand: sid(i % 3)},
				Settlers: Settlers{Rule: Majority, Keys: []Key{kid(i % 3)}}})
		}
	})
}
