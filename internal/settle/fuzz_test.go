package settle

import (
	"bytes"
	"os"
	"slices"
	"testing"

	"github.com/ciranda-dev/kolo-braid/internal/dag"
)

func stateSeeds(t testing.TB) [][]byte {
	var out [][]byte
	w := newWorld()
	w.lockstep(seq(4), map[int]*ent{}, 9, 0, nil)
	p := newPeer(t, w, cfg0(w, Majority, seq(4)...), nil, rules{})
	p.feed(w.ents...)
	p.s.Equivocated(strandID(30))
	out = append(out, p.s.Save())
	gw, gcfg := dCase{true, 1, 1, 0, 4, -1, -1, dNone}.build(t)
	g := newPeer(t, gw, gcfg, nil, rules{})
	g.feed(gw.ents[:len(gw.ents)/2]...)
	out = append(out, g.s.Save())
	cw, _, _, real10 := closureWorld(scenario{3, Majority}, 2)
	c := newPeer(t, cw, cfg0(cw, Majority, seq(3)...), &Anchor{ID: real10.id}, rules{})
	c.feed(cw.ents[:20]...)
	out = append(out, c.s.Save())
	b := newPeer(t, cw, cfg0(cw, Majority, seq(3)...), &Anchor{ID: cw.byStrand[0][1].id}, rules{})
	b.feed(cw.ents...)
	out = append(out, b.s.Save())
	return out
}

// FuzzLoad: Load never panics, and what loads saves to bytes that load back to the same state and marks.
func FuzzLoad(f *testing.F) {
	for _, s := range stateSeeds(f) {
		f.Add(s)
	}
	f.Add([]byte{})
	f.Add([]byte("\x14kolo-braid/settle/v1"))
	if old, err := os.ReadFile("testdata/state-v1-inline-marks.bin"); err == nil {
		f.Add(old)
	}
	d, err := dag.New(dag.Config{MaxLinks: 63, MaxPendingBytes: 1 << 20})
	if err != nil {
		f.Fatal(err)
	}
	roster := func(_ uint64, ps []Pivot) ([]Proposal, error) { return make([]Proposal, len(ps)), nil }
	f.Fuzz(func(t *testing.T, b []byte) {
		s, err := Load(d, roster, b)
		if err != nil {
			return
		}
		saved := s.Save()
		s2, err := Load(d, roster, saved)
		if err != nil {
			t.Fatalf("a saved state does not load: %v", err)
		}
		if err := s2.RestoreMarks(s.Marks()); err != nil || !bytes.Equal(s2.Save(), saved) || !slices.Equal(s2.Marks(), s.Marks()) {
			t.Fatalf("loaded state saves to other bytes or marks: %v", err)
		}
		if len(s.TakeMarks()) != len(s.Marks()) || len(s2.TakeMarks()) != 0 {
			t.Fatal("marks of a state saved with them inline are not handed out once")
		}
		s.Halted()
		s.Predicted()
		s.Awaiting()
		_, _ = s.Advance()
	})
}

// fuzzWorld turns bytes into a braid of 3 to 7 seats plus two other writers, with hostile pulses: wrong config numbers,
// wrong activating pivots, readiness pulses (junk IDs and the ID of the last Settlers entry), pulses of non-settlers and
// second histories, and Settlers entries of an admin (key 70).
func fuzzWorld(data []byte) (*world, int, Rule) {
	if len(data) < 2 {
		return nil, 0, 0
	}
	n := 3 + int(data[0])%5
	rule := []Rule{Majority, TwoThirds}[data[1]%2]
	data = data[2:]
	w := newWorld()
	writers := append(seq(n), 20, 21)
	heads := map[int]*ent{}
	var set []int
	for len(data) >= 3 && len(w.ents) < 160 {
		op, a, b := data[0], data[1], data[2]
		data = data[3:]
		k := writers[int(a)%len(writers)]
		var links []*ent
		for i, o := range append(slices.Clone(writers), 70) {
			es := w.byStrand[o]
			if o == k || len(es) == 0 || b&(1<<(i%8)) == 0 {
				continue
			}
			links = append(links, es[len(es)-1-int(op>>4)%min(len(es), 4)])
		}
		prev := heads[k]
		if op&0x0c == 0x0c && prev != nil && prev.prev != nil {
			prev = prev.prev
		}
		if op&0x33 == 0x30 {
			set = nil
			for i, o := range writers {
				if b&(1<<(i%8)) != 0 {
					set = append(set, o)
				}
			}
			heads[70] = w.settlersEntry(70, 70, heads[70], Majority, set, links...)
			continue
		}
		var e *ent
		switch op & 3 {
		case 0:
			e = w.add(k, k, prev, links...)
		case 1:
			e = w.pulse(k, prev, 0, nil, links...)
		case 2:
			var act *ent
			if len(w.ents) > 0 {
				act = w.ents[int(b)%len(w.ents)]
			}
			e = w.pulse(k, prev, uint64(b%3), act, links...)
			if b%3 == 0 {
				e.act = nil
			}
		case 3:
			id := [32]byte{b}
			if b&0x80 != 0 && set != nil {
				id = configID(1, Majority, w.seats(set...))
			}
			e = w.readiness(k, prev, 0, nil, id, links...)
		}
		if prev == heads[k] {
			heads[k] = e
		}
	}
	return w, n, rule
}

// FuzzSettle: settling never panics; with no seat strand forked, every delivery order, and a peer that settles everything
// in one walk, commits the same list and ends in the same config awaiting the same seats; with forked seats within the
// tolerated f, the lists are prefixes of one another.
func FuzzSettle(f *testing.F) {
	f.Add([]byte{0, 0, 1, 0, 0xff, 1, 1, 0xff, 1, 2, 0xff, 1, 0, 0xff, 1, 1, 0xff, 1, 2, 0xff})
	f.Add([]byte{1, 1, 0x11, 3, 0x0f, 0x21, 1, 0x0f, 0x31, 2, 0x0f, 0x0d, 0, 0xff, 0x02, 1, 0x05, 0x03, 2, 0x01})
	f.Add(slices.Repeat([]byte{4, 0, 1, 0, 0xff, 1, 1, 0xff, 1, 2, 0xff, 1, 3, 0xff, 0x0d, 1, 0x7f}, 6))
	f.Add(readinessSeed())
	f.Fuzz(func(t *testing.T, data []byte) {
		w, n, rule := fuzzWorld(data)
		if w == nil {
			return
		}
		cfg := cfg0(w, rule, seq(n)...)
		var lists [][]dag.ID
		var peers []*peer
		forked := 0
		for o, order := range [][]*ent{w.ents, reversed(w.ents), shuffled(w.ents, uint64(len(data)))} {
			p, err := newPeerE(w, cfg, nil, rules{})
			if err != nil {
				t.Fatal(err)
			}
			p.feed(order...)
			if p.err != nil {
				t.Fatalf("order %d: %v", o, p.err)
			}
			if err := checkValid(p); err != nil {
				t.Fatal(err)
			}
			lists = append(lists, p.ids())
			peers = append(peers, p)
			if o == 0 {
				for k := range n {
					if p.d.Forked(strandID(k)) {
						forked++
					}
				}
			}
		}
		var y *peer
		for _, mark := range []int{len(w.ents), 2 * len(w.ents) / 3, len(w.ents) / 2, len(w.ents) / 3} {
			var err error
			if y, err = newPeerE(w, cfg, nil, rules{}); err != nil {
				t.Fatal(err)
			}
			y.place(w.ents[:mark]...)
			y.advance()
			y.feed(w.ents[mark:]...)
			if y.err != nil {
				t.Fatalf("one walk through %d: %v", mark, y.err)
			}
			lists = append(lists, y.ids())
			peers = append(peers, y)
		}
		if forked == 0 {
			for i, p := range peers {
				if p.s.Config().Number != y.s.Config().Number || !slices.Equal(p.s.Awaiting(), y.s.Awaiting()) {
					t.Fatalf("peer %d: config %d awaiting %v; peer %d: config %d awaiting %v", i, p.s.Config().Number,
						p.s.Awaiting(), len(peers)-1, y.s.Config().Number, y.s.Awaiting())
				}
			}
		}
		q := Quorum(rule, n)
		for i := range lists {
			for j := range lists {
				switch {
				case forked == 0 && !slices.Equal(lists[i], lists[j]):
					t.Fatalf("orders %d and %d differ", i, j)
				case forked > 0 && 2*q-n > forked && !isPrefix(lists[i], lists[j]):
					t.Fatalf("orders %d and %d conflict with %d forked seats", i, j, forked)
				}
			}
		}
	})
}

// readinessSeed is a FuzzSettle input with three seats: key 20 writes a junk readiness pulse, the admin adds keys 20 and 21,
// 20 is ready, and 21 is ready much later; peers that settle part of it in one walk must agree with the others.
func readinessSeed() []byte {
	out := []byte{0, 0}
	round := func(extra byte) {
		for k, others := range []byte{0x06, 0x05, 0x03} {
			out = append(out, 0x01, byte(k), others|extra)
		}
	}
	out = append(out, 0x03, 3, 0x00)
	round(0x08)
	round(0)
	round(0)
	out = append(out, 0x30, 0, 0x1f)
	round(0x20)
	for range 4 {
		round(0)
	}
	out = append(out, 0x03, 3, 0x81)
	round(0x08)
	for range 6 {
		round(0)
	}
	out = append(out, 0x03, 4, 0x81)
	round(0x10)
	for range 4 {
		round(0)
	}
	return out
}
