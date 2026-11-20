package kolobraid

import (
	"crypto/ed25519"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/ciranda-dev/kolo-braid/internal/dag"
)

// group builds a braid of n members on separate backends: the founder adds every joiner, and with settlers set the settler
// set becomes every member.
func group(t testing.TB, n int, cfg GroupConfig, apply ApplyFunc, settlers bool) (*net, []*peer) {
	t.Helper()
	a := newFounder(t, "p0", cfg, apply)
	peers := []*peer{a}
	for i := 1; i < n; i++ {
		j, nw := newJoiner(t, fmt.Sprintf("p%d", i), a, apply)
		if i == 1 {
			nw.Roles = RoleAdmin
		}
		if _, err := a.b.AddMember(ctx10(t), nw); err != nil {
			t.Fatal(err)
		}
		j.start()
		peers = append(peers, j)
	}
	nt := newNet(t, peers...)
	nt.settle(nil, 20*time.Second, func() bool { return quiet(peers...) && allActive(peers, n) })
	if settlers && n > 1 {
		keys := make([]ed25519.PublicKey, n)
		for i, p := range peers {
			keys[i] = keyOf(p)
		}
		if _, err := a.b.SetSettlers(ctx10(t), keys, cfg.WithDefaults().Quorum); err != nil {
			t.Fatal(err)
		}
		// a config whose added seats are ready activates without another entry
		nt.settle(nil, 30*time.Second, func() bool {
			s := a.b.Status()
			return quiet(peers...) && ((s.PendingConfig != nil && len(s.AwaitingReadiness) == 0) || s.Config > 0)
		})
		mustAppend(t, a.b, "settlers=set")
		nt.settle(nil, 30*time.Second, func() bool {
			if !quiet(peers...) {
				return false
			}
			for _, p := range peers {
				if s := p.b.Status(); s.Seats != n {
					return false
				}
			}
			return true
		})
	}
	return nt, peers
}

func allActive(peers []*peer, n int) bool {
	for _, p := range peers {
		s := p.b.Settled()
		ok := len(s.Members()) == n
		s.Close()
		if !ok {
			return false
		}
	}
	return true
}

// settledIDs returns the IDs of a peer's settled entries in order.
func settledIDs(t testing.TB, b *Braid) []EntryID {
	t.Helper()
	var out []EntryID
	for o, err := range b.Entries(0) {
		if err != nil {
			t.Fatal(err)
		}
		if !o.Settled {
			break
		}
		out = append(out, o.ID)
	}
	return out
}

// checkSame fails unless every peer has the same settled order, running digest and view, byte for byte.
func checkSame(t testing.TB, peers []*peer) {
	t.Helper()
	ref := peers[0]
	rids, rview, rprog := settledIDs(t, ref.b), dump(t, ref.b), ref.b.view.Progress()
	for _, p := range peers[1:] {
		ids, view, prog := settledIDs(t, p.b), dump(t, p.b), p.b.view.Progress()
		switch {
		case !slices.Equal(ids, rids):
			t.Fatalf("%s: settled order differs from %s at %d", p.name, ref.name, firstDiff(ids, rids))
		case view != rview:
			t.Fatalf("%s: view differs from %s:\n%s\n%s", p.name, ref.name, view, rview)
		case prog.Digest != rprog.Digest:
			t.Fatalf("%s: running digest differs", p.name)
		}
	}
}

func firstDiff(a, b []EntryID) int {
	for i := range min(len(a), len(b)) {
		if a[i] != b[i] {
			return i
		}
	}
	return min(len(a), len(b))
}

// TestPropertyConvergence is the central acceptance: peers fed the same entries in random orders, in pieces and across
// partitions end with byte-identical views and the same settled prefix.
func TestPropertyConvergence(t *testing.T) {
	seeds := 6
	if testing.Short() {
		seeds = 2
	}
	for seed := range seeds {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(uint64(seed), 99))
			n := 3 + seed%3
			nt, peers := group(t, n, GroupConfig{}, appApply, true)
			appended := 1
			for step := 0; step < 60; step++ {
				switch x := rng.IntN(10); {
				case x < 6:
					p := peers[rng.IntN(n)]
					mustAppend(t, p.b, fmt.Sprintf("k%d=%d", rng.IntN(8), step))
					appended++
				case x < 8:
					nt.round(rng, uint64(1+rng.IntN(3)))
				case x == 8:
					cut := 1 + rng.IntN(n-1)
					perm := rng.Perm(n)
					nt.partition(perm[:cut], perm[cut:])
				default:
					nt.heal()
				}
				if rng.IntN(4) == 0 {
					time.Sleep(time.Duration(rng.IntN(20)) * time.Millisecond)
				}
			}
			nt.heal()
			nt.settle(rng, 60*time.Second, func() bool { return quiet(peers...) })
			checkSame(t, peers)
			if got := settledGet(t, peers[0].b, "#"); got != fmt.Sprint(appended) {
				t.Fatalf("%s data entries settled, want %d", got, appended)
			}
			if s := peers[0].b.Status(); s.Seats != n || s.Config == 0 {
				t.Fatalf("settled by config %d with %d seats", s.Config, s.Seats)
			}
		})
	}
}

// seatApply records, for every data entry, whether its author is a seat of the config at the entry's position.
func seatApply(tx *Tx, e *Entry) error {
	if e.Kind != KindData {
		return nil
	}
	w, _ := tx.Member(e.Writer)
	v := "0"
	if w.Seat {
		v = "1"
	}
	return tx.Put(append([]byte("seat/"), e.ID[:]...), []byte(v))
}

// TestSeatFlagsAcrossAConfigChange: entries applied tentatively before a new config activates, and settled after it, must see the
// seats of the new config, exactly as a rebuild from the stored pivots does.
func TestSeatFlagsAcrossAConfigChange(t *testing.T) {
	for seed := range 3 {
		nt, ps := group(t, 2, GroupConfig{}, seatApply, false)
		a, w := ps[0], ps[1]
		if _, err := a.b.SetSettlers(ctx10(t), []ed25519.PublicKey{keyOf(a), keyOf(w)}, Majority); err != nil {
			t.Fatal(err)
		}
		nt.settle(nil, 20*time.Second, func() bool {
			s := a.b.Status()
			return quiet(ps...) && ((s.PendingConfig != nil && len(s.AwaitingReadiness) == 0) || s.Config > 0)
		})
		mustAppend(t, a.b, "trigger")
		for i := 0; i < 5+seed; i++ {
			mustAppend(t, w.b, fmt.Sprint("w", i))
			nt.round(nil, 1)
		}
		nt.settle(nil, 20*time.Second, func() bool { return quiet(ps...) && a.b.Status().Seats == 2 })
		want := dump(t, w.b)
		w.close()
		w.opts.Rebuild = true
		w.reopen()
		w.opts.Rebuild = false
		nt.confirm(w)
		nt.settle(nil, 20*time.Second, func() bool { return quiet(ps...) })
		if got := dump(t, w.b); got != want {
			t.Fatalf("seed %d: the live view and a rebuild disagree on seat flags", seed)
		}
		checkSame(t, ps)
		for _, p := range ps {
			p.close()
		}
	}
}

// TestNoPromotionAcrossConfigs: a settled batch of another config than the one the tail was applied under applies its entries
// again instead of taking their tentative write-sets, which saw other seats.
func TestNoPromotionAcrossConfigs(t *testing.T) {
	var mu sync.Mutex
	calls := map[EntryID]int{}
	counting := func(tx *Tx, e *Entry) error {
		mu.Lock()
		calls[e.ID]++
		mu.Unlock()
		return seatApply(tx, e)
	}
	_, ps := group(t, 2, GroupConfig{}, counting, true)
	a := ps[0]
	ps[1].close()
	ref := mustAppend(t, a.b, "x")
	// park the applier once its tail holds the entry, applied and idle, all under one lock: a replan may apply the tail
	// again before that, so the count is taken there
	var steps []applyStep
	var before int
	waitUntil(t, 10*time.Second, func() bool {
		a.b.mu.Lock()
		defer a.b.mu.Unlock()
		if a.b.busy || len(a.b.jobs) > 0 || a.b.plan == nil || a.b.plan.gen != a.b.appliedGen {
			return false
		}
		steps = steps[:0]
		for _, s := range a.b.plan.steps {
			steps = append(steps, s)
			if s.Entry.ID == ref.ID {
				break
			}
		}
		if len(steps) == 0 || steps[len(steps)-1].Entry.ID != ref.ID || a.b.view.Tail() < len(steps) {
			return false
		}
		mu.Lock()
		before = calls[ref.ID]
		mu.Unlock()
		a.b.viewHalt = errors.New("applier parked by the test") // the applier goroutine stays idle from here on
		return before > 0
	})
	// the tail starts with these entries: a batch of the next config holding them must call apply again
	a.b.runBatch(&batchJob{index: a.b.view.Progress().Batches, steps: steps, config: a.b.st.Config().Number + 1})
	mu.Lock()
	n := calls[ref.ID]
	mu.Unlock()
	if n != before+1 {
		t.Fatalf("apply called %d times after %d: the tentative write-set of another config was promoted", n, before)
	}
}

// fullOrder is the tail order computed from scratch, as buildPlan does without the last plan; the caller holds b.mu.
func (b *Braid) fullOrder(t testing.TB) []EntryID {
	t.Helper()
	var ids []EntryID
	add := func(id dag.ID) error {
		if n := b.nodes[EntryID(id)]; n != nil && n.positioned() {
			ids = append(ids, EntryID(id))
		}
		return nil
	}
	front := b.frontier
	for _, p := range b.st.Predicted() {
		pc, ok := b.d.Clock(p)
		if !ok {
			break
		}
		if err := b.d.Order(front, pc, [32]byte(p), add); err != nil {
			t.Fatal(err)
		}
		front = pc
	}
	if err := b.d.Order(front, nil, [32]byte(b.meta.Group), add); err != nil {
		t.Fatal(err)
	}
	return ids
}

// TestIncrementalPlanEqualsFull: a plan extended with the entries placed since the last one is the plan computed from
// scratch, with the same tentative roster, through random appends, partial deliveries and partitions that stall settlement.
func TestIncrementalPlanEqualsFull(t *testing.T) {
	for seed := range 3 {
		rng := rand.New(rand.NewPCG(uint64(seed), 7))
		nt, peers := group(t, 4, GroupConfig{}, appApply, true)
		check := func() {
			for _, p := range peers {
				p.b.mu.Lock()
				got, want := slices.Clone(p.b.prevTail), p.b.fullOrder(t)
				members := p.b.tent.Members()
				r := p.b.settled.Clone()
				pos := p.b.settledNext
				for _, id := range want {
					if n := p.b.nodes[id]; n.membership() {
						e := n.rosterEntry()
						r.Apply(pos, &e)
					}
					pos++
				}
				p.b.mu.Unlock()
				if !slices.Equal(got, want) || !slices.Equal(members, r.Members()) {
					t.Fatalf("seed %d %s: the plan differs from the full one at %d", seed, p.name, firstDiff(got, want))
				}
			}
		}
		for step := range 80 {
			switch x := rng.IntN(10); {
			case x < 6:
				mustAppend(t, peers[rng.IntN(4)].b, fmt.Sprintf("k%d=%d", rng.IntN(8), step))
			case x < 8:
				nt.round(rng, uint64(1+rng.IntN(3)))
			case x == 8:
				// a 2/2 split of the four seats stalls settlement on both sides
				perm := rng.Perm(4)
				nt.heal()
				nt.partition(perm[:2], perm[2:])
			default:
				nt.heal()
			}
			check()
		}
	}
}
