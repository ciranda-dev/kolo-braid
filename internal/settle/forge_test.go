package settle

import (
	"errors"
	"slices"
	"testing"
)

// currentSeatForgery builds the case of a seat of the current config, an admin, that signs a second history from its own
// round-10 pivot index: a Settlers entry naming keys 60 and 61 it owns, their readiness pulses, its fake pivot fc and their
// config-1 pulses naming fc. No real config change exists. It returns the world, the honest entries, the real pivot and fc.
func currentSeatForgery(n int) (w *world, honest bitset, real10, fc *ent) {
	w = newWorld()
	keys := seq(n)
	attacker := (10 / 2) % n
	heads := w.lockstep(keys, map[int]*ent{}, 10, 0, nil)
	prev := map[int]*ent{}
	for k, v := range heads {
		prev[k] = v
	}
	var fakes []*ent
	for _, k := range keys {
		var links []*ent
		for _, o := range keys {
			if o != k {
				links = append(links, prev[o])
			}
		}
		if k == attacker {
			f1 := w.settlersEntry(k, k, prev[k], Majority, []int{60, 61})
			fcid := configID(1, Majority, w.seats(60, 61))
			r60 := w.readiness(60, nil, 0, nil, fcid, prev[0])
			r61 := w.readiness(61, nil, 0, nil, fcid, prev[0])
			fc = w.pulse(k, f1, 0, nil, append(slices.Clone(links), r60, r61)...)
			c60 := w.pulse(60, r60, 1, fc, fc)
			c61 := w.pulse(61, r61, 1, fc, fc)
			fakes = append(fakes, f1, r60, r61, fc, c60, c61)
		}
		heads[k] = w.pulse(k, prev[k], 0, nil, links...)
		if k == attacker {
			real10 = heads[k]
		}
	}
	// the attacker's own vote for fc on its second history: one vote, under q for n >= 3
	var others []*ent
	for _, k := range keys {
		if k != attacker {
			others = append(others, heads[k])
		}
	}
	fakes = append(fakes, w.pulse(attacker, fc, 0, nil, others...))
	w.lockstep(keys, heads, 4, 0, nil)
	for _, f := range fakes {
		f.fake = true
	}
	for _, e := range w.ents {
		if !e.fake {
			honest.set(e.n)
		}
	}
	return w, honest, real10, fc
}

// TestCurrentSeatForgedCertificate: a closure certificate that no q seats of the config voted for is never used, so a seat
// of the current config that forges one on a second history cannot split settled prefixes (n = 4, f = 1).
func TestCurrentSeatForgedCertificate(t *testing.T) {
	for _, rule := range []Rule{Majority, TwoThirds} {
		for _, dev := range []rules{{}, {certNoVotes: true}} {
			w, honest, real10, fc := currentSeatForgery(4)
			cfg := cfg0(w, rule, seq(4)...)
			old := newPeer(t, w, cfg, nil, dev)
			old.feed(w.in(honest)...)
			if !slices.Contains(old.ids(), real10.id) {
				t.Fatalf("%v: the honest peer did not commit the real round-10 pivot: %v", rule, old.rounds())
			}
			v := newPeer(t, w, cfg, nil, dev)
			for _, e := range w.in(honest) {
				if e.n < real10.n {
					v.feed(e)
				}
			}
			v.feed(w.ents...)
			conflict := !isPrefix(v.ids(), old.ids())
			if dev.certNoVotes {
				// the negative control: without the votes the forged certificate is used and the victim settles a fake prefix
				if !conflict || v.s.Config().Activating != fc.id {
					t.Fatalf("%v without votes: victim %v config %d, want the forged certificate used", rule, v.rounds(), v.s.Config().Number)
				}
				continue
			}
			var he *HaltError
			if conflict || v.s.Config().Number != 0 || !errors.As(v.s.Halted(), &he) || he.Reason != SeatFork {
				t.Fatalf("%v: honest %v, victim %v in config %d, halted %v", rule, old.rounds(), v.rounds(), v.s.Config().Number, v.s.Halted())
			}
		}
	}
}

// TestCertificateWaitsForVotes: the real closure certificate of a late second history is used only once q seats of the
// closed config are seen voting for its pivot, and it is used as soon as they are.
func TestCertificateWaitsForVotes(t *testing.T) {
	for _, s := range bPrimeCases {
		w, honest, fake, _ := closureWorld(s, 3)
		var votes, rest []*ent
		for _, e := range w.ents {
			if e.pulse && e.key < s.n && e.index == 11 {
				votes = append(votes, e)
			} else if honest.has(e.n) || e == fake {
				rest = append(rest, e)
			}
		}
		j := newPeer(t, w, cfg0(w, s.rule, seq(s.n)...), nil, rules{})
		j.place(rest...)
		j.advance()
		if j.s.Config().Number != 0 || j.s.Halted() == nil {
			t.Fatalf("n=%d: a certificate without votes was used: config %d %v", s.n, j.s.Config().Number, roundsOf(j, 0))
		}
		j.feed(votes...)
		if got := roundsOf(j, 0); !slices.Equal(got, oldList) || j.s.Config().Number != 1 {
			t.Fatalf("n=%d: with the votes the joiner got %v in config %d, halted %v", s.n, got, j.s.Config().Number, j.s.Halted())
		}
	}
}

// TestAnchoredJoinerNotHalted: a joiner that settles inside its welcome anchor is never reported halted for a former seat's
// second history, since the anchor settles that config.
func TestAnchoredJoinerNotHalted(t *testing.T) {
	for _, s := range bPrimeCases {
		w, _, fake, real10 := closureWorld(s, 1)
		order := append([]*ent{}, w.ents[:fake.prev.n+1]...)
		order = append(order, fake)
		order = append(order, w.ents[fake.prev.n+1:fake.n]...)
		j := newPeer(t, w, cfg0(w, s.rule, seq(s.n)...), &Anchor{ID: real10.id, Config: 0}, rules{})
		for _, e := range order {
			j.feed(e)
			if err := j.s.Halted(); err != nil {
				t.Fatalf("n=%d: the anchored joiner reports %v at entry %d", s.n, err, e.n)
			}
		}
		if got := roundsOf(j, 0); !slices.Equal(got, oldList) {
			t.Fatalf("n=%d: anchored joiner %v", s.n, got)
		}
	}
}
