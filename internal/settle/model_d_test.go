package settle

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ciranda-dev/kolo-braid/internal/dag"
)

// dCase places the membership entries of one scope d world: the Settlers entry linked by setSeat at setStep, the new
// settler's readiness pulse linked by readySeat at readyStep (-1: it never comes online), an undo linked by undoSeat at
// undoStep (-1: none), and for a growing config one extra of dExtra.
type dCase struct {
	grow                 bool // 3 to 4 seats; else 4 to 3
	setSeat, setStep     int
	readySeat, readyStep int
	undoSeat, undoStep   int
	extra                dExtra
}

// dExtra makes the change also add key 4, ready four steps after key 3, and for dJunk and dReplaced gives key 3 a readiness
// pulse that cannot count, settled before its valid one or with it.
type dExtra uint8

const (
	dNone dExtra = iota
	// dTwo: only the second added seat.
	dTwo
	// dJunk: the new settler first writes a readiness pulse for an ID no config has.
	dJunk
	// dReplaced: an earlier Settlers entry adds keys 3, 4 and 5; the new settler is ready for it first, then for the change
	// that replaces it.
	dReplaced
)

const dSteps0, dSteps1 = 14, 6

func (c dCase) build(t testing.TB) (*world, Config) {
	w := newWorld()
	old, next := seq(3), seq(4)
	if !c.grow {
		old, next = seq(4), seq(3)
	}
	if c.grow && c.extra != dNone {
		next = seq(5)
	}
	cfg := cfg0(w, Majority, old...)
	var first, early *ent
	if c.grow && c.extra == dReplaced {
		first = w.settlersEntry(70, 70, nil, Majority, seq(6))
	}
	set := w.settlersEntry(70, 70, first, Majority, next)
	var undo, ready, ready4 *ent
	if c.undoStep >= 0 {
		undo = w.settlersEntry(70, 70, set, Majority, old)
	}
	cid := configID(1, Majority, w.seats(next...))
	heads := map[int]*ent{}
	for step := range dSteps0 {
		prev := map[int]*ent{}
		for k, v := range heads {
			prev[k] = v
		}
		if c.grow && c.readyStep >= 0 && step == max(0, c.readyStep-2) && c.extra != dNone && c.extra != dTwo {
			id := configID(99, Majority, nil)
			if c.extra == dReplaced {
				id = configID(1, Majority, w.seats(seq(6)...))
			}
			early = w.readiness(3, nil, 0, nil, id, prev[0])
		}
		if c.grow && step == c.readyStep {
			// the new settler writes its readiness pulse holding what seat 0 held
			ready = w.readiness(3, early, 0, nil, cid, prev[0])
		}
		if c.grow && c.extra != dNone && c.readyStep >= 0 && step == c.readyStep+4 {
			ready4 = w.readiness(4, nil, 0, nil, cid, prev[0])
		}
		for _, k := range old {
			var links []*ent
			for _, o := range old {
				if o != k {
					links = append(links, prev[o])
				}
			}
			if first != nil && k == c.setSeat && step == max(0, c.setStep-1) {
				links = append(links, first)
			}
			if k == c.setSeat && step == c.setStep {
				links = append(links, set)
			}
			if k == c.undoSeat && step == c.undoStep {
				links = append(links, undo)
			}
			if early != nil && k == c.readySeat && step == max(0, c.readyStep-2)+1 {
				links = append(links, early)
			}
			if k == c.readySeat && step == c.readyStep+1 {
				links = append(links, ready)
			}
			if ready4 != nil && k == (c.readySeat+1)%len(old) && step == c.readyStep+5 {
				links = append(links, ready4)
			}
			heads[k] = w.pulse(k, prev[k], 0, nil, links...)
		}
	}
	// the activation the reference finds; config 1 then runs in lockstep with its seats
	rf := newRef(w, w.all(), Majority, old, dagRoster(t, w, w.all(), Proposal{Rule: Majority, Seats: cfg.Seats}))
	list := rf.committed()
	if rf.cfgN == 1 {
		act := list[len(list)-1].e
		h1 := map[int]*ent{}
		for _, k := range keysOf(rf.cfgSeats()) {
			h1[k] = heads[k]
			if es := w.byStrand[k]; !slices.Contains(old, k) && len(es) > 0 {
				h1[k] = es[len(es)-1]
			}
		}
		keys := keysOf(rf.cfgSeats())
		first := true
		for range dSteps1 {
			prev := map[int]*ent{}
			for k, v := range h1 {
				prev[k] = v
			}
			for _, k := range keys {
				var links []*ent
				for _, o := range keys {
					if o != k && prev[o] != nil {
						links = append(links, prev[o])
					}
				}
				if first {
					links = append(links, act)
				}
				h1[k] = w.pulse(k, prev[k], 1, act, links...)
			}
			first = false
		}
	}
	return w, cfg
}

func (r *ref) cfgSeats() []Seat {
	var out []Seat
	for _, s := range r.seats {
		out = append(out, Seat{Key: keyOf(s.key), Strand: strandID(s.strand)})
	}
	return out
}

func dCases(full bool) []dCase {
	var out []dCase
	// negative offsets: a readiness pulse written before the change, which counts only when no pivot before the one that
	// settles the change holds it
	setSteps, readyOff, undoOff := []int{1, 2}, []int{-2, 2, 5}, []int{3}
	if full {
		setSteps, readyOff, undoOff = []int{0, 1, 2, 3}, []int{-3, -2, -1, 1, 2, 3, 4, 5, 6}, []int{1, 2, 3, 4, 5}
	}
	for _, grow := range []bool{true, false} {
		old := 3
		if !grow {
			old = 4
		}
		for setSeat := range old {
			for _, setStep := range setSteps {
				type rd struct{ seat, step int }
				readies := []rd{{-1, -1}}
				if grow {
					for _, o := range readyOff {
						if setStep+o < 0 {
							continue
						}
						for s := range old {
							if full || s == 0 {
								readies = append(readies, rd{s, setStep + o})
							}
						}
					}
				}
				undos := []rd{{-1, -1}}
				for _, o := range undoOff {
					for s := range old {
						if full || s == setSeat {
							undos = append(undos, rd{s, setStep + o})
						}
					}
				}
				extras := []dExtra{dNone}
				if grow {
					extras = []dExtra{dNone, dJunk, dReplaced, dTwo}
				}
				for _, r := range readies {
					for _, u := range undos {
						for _, x := range extras {
							if x != dNone && r.step < 0 {
								continue
							}
							out = append(out, dCase{grow, setSeat, setStep, r.seat, r.step, u.seat, u.step, x})
						}
					}
				}
			}
		}
	}
	return out
}

var dActivated atomic.Int64

func checkD(t testing.TB, c dCase, seed uint64) (int, error) {
	w, cfg := c.build(t)
	base := Proposal{Rule: Majority, Seats: cfg.Seats}
	all := w.all()
	rf := newRef(w, all, Majority, keysOf(cfg.Seats), dagRoster(t, w, all, base))
	rl := rf.committed()
	full := refIDs(rl)
	roster := func(D bitset) func([]dag.ID) Proposal { return dagRoster(t, w, D, base) }
	r := rand.New(rand.NewPCG(seed, 4))
	n := 0
	for _, order := range [][]*ent{nil, shuffled(w.ents, seed)} {
		n++
		if err := checkSet(w, all, cfg, full, roster, order); err != nil {
			return n, fmt.Errorf("%+v full DAG: %w", c, err)
		}
	}
	// a peer that settles the first mark entries in one walk, then entry by entry, agrees with the reference
	for mark := len(w.ents); mark > 0; mark -= 5 {
		y, err := newPeerE(w, cfg, nil, rules{})
		if err != nil {
			return n, err
		}
		y.place(w.ents[:mark]...)
		y.advance()
		y.feed(w.ents[mark:]...)
		if y.err != nil {
			return n, y.err
		}
		n++
		if !slices.Equal(y.ids(), full) || y.s.Config().Number != rf.cfgN {
			return n, fmt.Errorf("%+v: one walk through %d of %d entries: %d pivots in config %d, reference %d in config %d",
				c, mark, len(w.ents), len(y.pivots), y.s.Config().Number, len(full), rf.cfgN)
		}
	}
	// every prefix of the creation order is an ideal: the incremental list stays a prefix of the full one
	p, err := newPeerE(w, cfg, nil, rules{})
	if err != nil {
		return n, err
	}
	for i, e := range w.ents {
		p.feed(e)
		if p.err != nil {
			return n, p.err
		}
		if !isPrefix(p.ids(), full) {
			return n, fmt.Errorf("%+v: after %d entries the list conflicts with the full DAG's", c, i+1)
		}
		n++
		if i%17 == 0 {
			var D bitset
			for _, x := range w.ents[:i+1] {
				D.set(x.n)
			}
			if err := checkSet(w, D, cfg, full, roster, nil); err != nil {
				return n, fmt.Errorf("%+v prefix %d: %w", c, i+1, err)
			}
		}
	}
	for range 4 {
		var tips []*ent
		for _, es := range w.byStrand {
			if k := r.IntN(len(es) + 1); k > 0 {
				tips = append(tips, es[k-1])
			}
		}
		n++
		if err := checkSet(w, closure(tips...), cfg, full, roster, nil); err != nil {
			return n, fmt.Errorf("%+v random ideal: %w", c, err)
		}
	}
	// an offline new settler never stops config 0
	if c.grow && c.readyStep < 0 {
		if p.s.Config().Number != 0 || len(p.pivots) < dSteps0/2-1 {
			return n, fmt.Errorf("%+v: offline new settler: config %d, %d pivots", c, p.s.Config().Number, len(p.pivots))
		}
		if _, pending := p.s.Pending(); pending != (c.undoStep < 0) {
			return n, fmt.Errorf("%+v: pending %v", c, pending)
		}
	}
	if !c.grow && c.undoStep < 0 && p.s.Config().Number != 1 {
		return n, fmt.Errorf("%+v: a removal did not activate", c)
	}
	if p.s.Config().Number == 1 {
		dActivated.Add(1)
		if c.grow && !slices.Contains(keysOf(p.s.Config().Seats), 3) {
			return n, fmt.Errorf("%+v: config 1 lacks the new seat", c)
		}
	}
	return n, nil
}

// TestModelScopeD is scope d: configs 3 to 4 and 4 to 3, the Settlers entry linked by every seat at several steps
// (concurrent with pivots), the new settler's readiness at several points or never, and an undo replacing the change.
func TestModelScopeD(t *testing.T) {
	limit := time.Minute
	if *fullModel {
		limit = time.Hour
	}
	sc := newScope(t, "d", limit)
	cases := dCases(*fullModel)
	if raceEnabled && !*fullModel {
		cases = cases[:len(cases)/8]
	}
	jobs, ideals, err := parallel(sc, func(yield func(int) bool) {
		for i := range cases {
			if !yield(i) {
				return
			}
		}
	}, func(i int) (int, error) { return checkD(t, cases[i], uint64(i)) })
	if err != nil {
		t.Fatalf("scope d after %d worlds: %v", jobs, err)
	}
	sc.done("%d worlds (%d end in config 1), %d ideals and orders checked", jobs, dActivated.Load(), ideals)
	dActivated.Store(0)
}
