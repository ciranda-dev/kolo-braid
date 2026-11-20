package settle

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/ciranda-dev/kolo-braid/internal/dag"
)

// randomWorld draws an unstructured braid: n seats and two data writers append entries that link a recent entry of random
// strands; a non-settler also writes pulses and a data writer sometimes signs a second history.
func randomWorld(r *rand.Rand, n, steps int) *world {
	w := newWorld()
	writers := append(seq(n), 20, 21, 22)
	heads := map[int]*ent{}
	for range steps {
		k := writers[r.IntN(len(writers))]
		var links []*ent
		for _, o := range writers {
			es := w.byStrand[o]
			if o == k || len(es) == 0 || r.IntN(3) == 0 {
				continue
			}
			links = append(links, es[len(es)-1-r.IntN(min(len(es), 3))])
		}
		prev := heads[k]
		if k == 21 && prev != nil && prev.prev != nil && r.IntN(8) == 0 {
			prev = prev.prev
		}
		var e *ent
		switch {
		case k < n || k == 22:
			e = w.pulse(k, prev, 0, nil, links...)
		default:
			e = w.add(k, k, prev, links...)
		}
		if prev == heads[k] {
			heads[k] = e
		}
	}
	return w
}

// checkValid checks that every committed pivot is a counting pulse at an even round on its pivot seat, the seat's vertex
// of that round, with rounds increasing and each past holding the previous pivot.
func checkValid(p *peer) error {
	for i, x := range p.pivots {
		e := p.w.byID[x.ID]
		if !e.pulse || e.ready != nil || e.strand != e.key || x.Round%2 != 0 {
			return fmt.Errorf("pivot %d is not a counting pulse at an even round", i)
		}
		// config 0's seats are keys 0 to n-1; a later config's seats come from Settlers entries
		if n := len(p.r.base.Seats); x.Config == 0 && (e.key >= n || int((x.Round/2)%uint64(n)) != e.key) {
			return fmt.Errorf("pivot %d is not on its round's pivot seat", i)
		}
		if i > 0 {
			prev := p.w.byID[p.pivots[i-1].ID]
			if x.Round <= p.pivots[i-1].Round || !e.past.has(prev.n) {
				return fmt.Errorf("pivot %d does not follow pivot %d", i, i-1)
			}
		}
	}
	return nil
}

// TestPropertyAgreement: random DAGs with 3 to 7 settlers; every delivery order gives the same list, every list along the
// way is a prefix of it, the list equals the reference and every pivot is valid.
func TestPropertyAgreement(t *testing.T) {
	dags, orders := 300, 20
	if raceEnabled {
		dags, orders = 40, 5
	}
	commits := 0
	for i := range dags {
		r := rand.New(rand.NewPCG(uint64(i), 21))
		n := 3 + i%5
		rule := []Rule{Majority, TwoThirds}[i%2]
		w := randomWorld(r, n, 40+r.IntN(80))
		cfg := cfg0(w, rule, seq(n)...)
		rf := newRef(w, w.all(), rule, seq(n), staticRoster(Proposal{Rule: rule, Seats: cfg.Seats}))
		want := refIDs(rf.committed())
		commits += len(want)
		for o := range orders {
			order := shuffled(w.ents, uint64(i*1000+o))
			p := newPeer(t, w, cfg, nil, rules{})
			for _, e := range order {
				p.feed(e)
				if !isPrefix(p.ids(), want) {
					t.Fatalf("dag %d order %d: list %v conflicts with the reference", i, o, p.rounds())
				}
			}
			if !slices.Equal(p.ids(), want) {
				t.Fatalf("dag %d order %d: %d pivots, reference %d", i, o, len(p.ids()), len(want))
			}
			if err := checkValid(p); err != nil {
				t.Fatalf("dag %d: %v", i, err)
			}
		}
	}
	if commits < dags {
		t.Fatalf("only %d pivots over %d DAGs: the generator settles too little", commits, dags)
	}
	t.Logf("%d DAGs x %d orders, %d pivots", dags, orders, commits)
}

// TestPropertyAllOrders: every one of the 40,320 delivery orders of an 8-pulse DAG commits the same pivot.
func TestPropertyAllOrders(t *testing.T) {
	w := newWorld()
	w.lockstep(seq(4), map[int]*ent{}, 2, 0, nil)
	cfg := cfg0(w, Majority, seq(4)...)
	idx := seq(8)
	var want []dag.ID
	count := 0
	every := 1
	if raceEnabled {
		every = 41
	}
	var perm func(k int)
	perm = func(k int) {
		if k == len(idx) {
			count++
			if count%every != 0 {
				return
			}
			order := make([]*ent, len(idx))
			for i, j := range idx {
				order[i] = w.ents[j]
			}
			p := newPeer(t, w, cfg, nil, rules{})
			p.feed(order...)
			if want == nil {
				want = p.ids()
			}
			if !slices.Equal(p.ids(), want) || len(want) != 1 {
				t.Fatalf("order %v: %v", idx, p.rounds())
			}
			return
		}
		for i := k; i < len(idx); i++ {
			idx[k], idx[i] = idx[i], idx[k]
			perm(k + 1)
			idx[k], idx[i] = idx[i], idx[k]
		}
	}
	perm(0)
	if count != 40320 {
		t.Fatalf("%d orders", count)
	}
}

// TestPropertyTermination: under a fair schedule (every seat links every other seat's newest pulse each round) every pivot
// round commits; with seats silent but q online, every round whose pivot seat is online commits.
func TestPropertyTermination(t *testing.T) {
	for n := 3; n <= 7; n++ {
		for _, rule := range []Rule{Majority, TwoThirds} {
			w := newWorld()
			const steps = 30
			w.lockstep(seq(n), map[int]*ent{}, steps, 0, nil)
			p := newPeer(t, w, cfg0(w, rule, seq(n)...), nil, rules{})
			p.feed(w.ents...)
			if len(p.pivots) != steps/2 {
				t.Fatalf("n=%d rule %d: %d pivots after %d fair rounds, want %d", n, rule, len(p.pivots), steps, steps/2)
			}
			q := Quorum(rule, n)
			online := seq(q)
			w2 := newWorld()
			w2.lockstep(online, map[int]*ent{}, steps, 0, nil)
			p2 := newPeer(t, w2, cfg0(w2, rule, seq(n)...), nil, rules{})
			p2.feed(w2.ents...)
			want := 0
			for r := 0; r+1 < steps; r += 2 {
				if (r/2)%n < q {
					want++
				}
			}
			if len(p2.pivots) != want {
				t.Fatalf("n=%d rule %d with %d online: %d pivots, want %d", n, rule, q, len(p2.pivots), want)
			}
		}
	}
}
