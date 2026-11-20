package roster

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
)

// hgen builds random membership histories: members with one strand each to start, admins adding, removing and replacing
// members, settler changes, evidence, data, members that were never added, replays of earlier bodies and malformed
// entries. Each entry links a random earlier entry of some other strands, so many entries are concurrent.
type hgen struct {
	r       *rand.Rand
	b       *braid
	keys    []Key
	strands []StrandID
	owner   []int
	started []bool
	bodies  []Entry
	addedBy map[StrandID]*item
	// view is the roster in generation order: what an honest writer roughly believes when it writes
	view *ref
}

func newHGen(seed uint64, members int, maxMembers int) *hgen {
	g := &hgen{r: rand.New(rand.NewPCG(seed, 11)), addedBy: map[StrandID]*item{}}
	for i := range members {
		g.keys = append(g.keys, kid(100+i))
	}
	g.b = newBraid(g.keys[0], sid(100), 0)
	g.b.g.MaxMembers = maxMembers
	g.b.g.MaxSettlers = min(4, maxMembers)
	g.b.g.Group = eid(fmt.Sprint("genesis ", seed))
	g.b.items[0].e.ID, g.b.items[0].node.ID = g.b.g.Group, g.b.g.Group
	g.b.byID = map[ID]*item{g.b.g.Group: g.b.items[0]}
	g.strands, g.owner, g.started = []StrandID{sid(100)}, []int{0}, []bool{true}
	g.view = &ref{g: g.b.g}
	g.view.step(0, &g.b.items[0].e)
	return g
}

func (g *hgen) newStrand(owner int) StrandID {
	s := sid(100 + len(g.strands))
	g.strands, g.owner, g.started = append(g.strands, s), append(g.owner, owner), append(g.started, false)
	return s
}

func (g *hgen) body() Entry {
	k := g.keys[g.r.IntN(len(g.keys))]
	switch x := g.r.IntN(100); {
	case x < 30:
		return dataE()
	case x < 50:
		owner := slices.Index(g.keys, k)
		s := g.strands[g.r.IntN(len(g.strands))]
		if g.r.IntN(4) > 0 {
			s = g.newStrand(owner)
		}
		return addW(k, s, Role(g.r.IntN(2)))
	case x < 64:
		return removeW(k)
	case x < 72:
		var keys []Key
		for _, kk := range g.keys {
			if g.r.IntN(3) == 0 {
				keys = append(keys, kk)
			}
		}
		if len(keys) == 0 {
			keys = []Key{k}
		}
		return settlersE(Rule(1+g.r.IntN(2)), keys...)
	case x < 76:
		i := g.r.IntN(len(g.strands))
		return evidenceE(g.keys[g.owner[i]], g.strands[i])
	case x < 84:
		return Entry{Kind: KindPulse}
	case x < 88 && len(g.bodies) > 0:
		return g.bodies[g.r.IntN(len(g.bodies))]
	case x < 92:
		return Entry{Kind: Kind(g.r.IntN(8)), Malformed: true}
	}
	return dataE()
}

func (g *hgen) build(n int) *braid {
	for i := 0; len(g.b.items) < n; i++ {
		si := g.r.IntN(len(g.strands))
		s := g.strands[si]
		start := !g.started[si]
		author := g.view.author(s)
		if author < 0 && g.r.IntN(8) > 0 {
			continue
		}
		g.started[si] = true
		e := g.body()
		if k := e.Kind; (k == KindAddMember || k == KindRemoveMember || k == KindSettlers) && !e.Malformed &&
			(author < 0 || g.view.ws[author].Roles != Admin) && g.r.IntN(4) > 0 {
			e = dataE()
		}
		if e.Kind != KindData && e.Kind != KindPulse && !e.Malformed {
			g.bodies = append(g.bodies, e)
		}
		var links []*item
		for _, other := range g.strands {
			if other == s || g.r.IntN(3) == 0 {
				continue
			}
			var cands []*item
			for _, it := range g.b.items {
				if it.e.Strand == other {
					cands = append(cands, it)
				}
			}
			if len(cands) > 0 {
				// mostly the newest, sometimes an older one
				j := len(cands) - 1 - min(len(cands)-1, g.r.IntN(3))
				links = append(links, cands[j])
			}
		}
		if a := g.addedBy[s]; start && a != nil {
			links = append(links, a)
		}
		it := g.b.add(fmt.Sprint("e", i), s, e, links...)
		g.view.step(uint64(len(g.b.items)), &it.e)
		if e.Kind == KindAddMember && g.addedBy[e.Add.Strand] == nil {
			g.addedBy[e.Add.Strand] = it
		}
	}
	return g.b
}

// checkInvariants checks what must hold at every position: an effective entry's author strand was active just before it,
// a membership change was made by an admin, members stay within MaxMembers, settler keys are active members, and no
// removal or role change leaves the braid without an admin or a settler (only evidence may).
func checkInvariants(t *testing.T, b *braid, steps []step) {
	t.Helper()
	var before []Member
	fenced, isFounded := false, false
	for p, s := range steps {
		it := b.byID[s.id]
		author := slices.IndexFunc(before, func(w Member) bool { return w.Strand == it.e.Strand })
		if s.effective && it.e.Kind != KindGenesis {
			if author < 0 {
				t.Fatalf("position %d: %s effective with an inactive author", p, it.name)
			}
			switch it.e.Kind {
			case KindAddMember, KindRemoveMember, KindSettlers:
				if before[author].Roles&Admin == 0 {
					t.Fatalf("position %d: %s effective with a non-admin author", p, it.name)
				}
			}
		}
		if s.effective && it.e.Kind == KindEvidence {
			fenced = true
		}
		if s.change == Founded {
			isFounded = true
		}
		if len(s.members) > b.g.MaxMembers {
			t.Fatalf("position %d: %d members", p, len(s.members))
		}
		admins := 0
		for _, w := range s.members {
			if w.Roles&Admin != 0 {
				admins++
			}
		}
		for _, k := range s.settlers {
			if !slices.ContainsFunc(s.members, func(w Member) bool { return w.Key == k }) {
				t.Fatalf("position %d: settler not active", p)
			}
		}
		if isFounded && !fenced && (admins == 0 || len(s.settlers) == 0) {
			t.Fatalf("position %d: %s left %d admins and %d settlers", p, it.name, admins, len(s.settlers))
		}
		before = s.members
	}
}

// TestPropertyDeliveryOrders: for random histories of 3 to 8 members, the roster at every position is the same for every
// delivery order, equals the reference roster over the reference Kahn order, and keeps the invariants.
func TestPropertyDeliveryOrders(t *testing.T) {
	shuffles := 1000
	if testing.Short() || raceEnabled {
		shuffles = 50
	}
	effective := map[Reason]int{}
	for c := range 30 {
		members := 3 + c%6
		g := newHGen(uint64(c), members, []int{3, 4, 8, 64}[c%4])
		b := g.build(40 + 4*c)
		seed := seedOf(c)
		order := naiveOrder(b.items, seed)
		want := refFold(b.g, order, b.byID)
		first := b.run(t, seed)
		if i, ok := sameAsRef(first, want); !ok {
			t.Fatalf("history %d: production differs from the reference at position %d: %v against %v", c, i, first[i], want[i])
		}
		checkInvariants(t, b, first)
		for _, s := range first {
			effective[s.reason]++
		}
		rng := rand.New(rand.NewPCG(uint64(c), 3))
		for range shuffles {
			perm := rng.Perm(len(b.items))
			if err := sameSteps(first, fold(t, b.roster(t), orderOf(t, b.nodes(), perm, seed), b.byID)); err != nil {
				t.Fatalf("history %d: %v", c, err)
			}
		}
	}
	for r := OK; r <= TooManySettlers; r++ {
		if r != NotGenesis && r != NotSeed && effective[r] == 0 {
			t.Errorf("no entry with outcome %v: the generator misses a case", r)
		}
	}
	t.Logf("outcomes: %v", effective)
}

// TestPropertyAllOrdersSmall checks every delivery order of small histories.
func TestPropertyAllOrdersSmall(t *testing.T) {
	n := 3
	if testing.Short() || raceEnabled {
		n = 1
	}
	all := perms(8)
	for c := range n {
		g := newHGen(uint64(1000+c), 3+c, 64)
		b := g.build(8)
		seed := seedOf(c)
		first := b.run(t, seed)
		if _, ok := sameAsRef(first, refFold(b.g, naiveOrder(b.items, seed), b.byID)); !ok {
			t.Fatalf("history %d differs from the reference", c)
		}
		for _, perm := range all {
			if err := sameSteps(first, fold(t, b.roster(t), orderOf(t, b.nodes(), perm, seed), b.byID)); err != nil {
				t.Fatalf("history %d: %v", c, err)
			}
		}
	}
}

// TestPropertyNoDivergenceAfterRevoke: two peers receive the same entries in different orders. The rule the record rejects,
// checking each entry against the roster when it arrives, makes them keep different entries for some histories; at the
// entry's position in the order, peers that hold the same entries always agree, at every point of the delivery.
func TestPropertyNoDivergenceAfterRevoke(t *testing.T) {
	arrivalDiverged := 0
	cases := 40
	if testing.Short() || raceEnabled {
		cases = 10
	}
	for c := range cases {
		g := newHGen(uint64(2000+c), 3+c%6, 64)
		b := g.build(30)
		seed := seedOf(c)
		results := map[string]string{}
		arrival := map[string]bool{}
		rng := rand.New(rand.NewPCG(uint64(c), 5))
		for range 8 {
			perm := rng.Perm(len(b.items))
			placed := deliver(t, b, perm, seed, results)
			arrival[fmt.Sprint(arrivalOutcome(t, b, placed))] = true
		}
		if len(arrival) > 1 {
			arrivalDiverged++
		}
	}
	if arrivalDiverged == 0 {
		t.Fatal("the arrival-time rule never diverged: the histories miss the counterexample class")
	}
	t.Logf("arrival-time rule diverged in %d of %d histories; the position rule in none", arrivalDiverged, cases)
}

// deliver feeds the entries one at a time in perm order to one DAG and, after each, computes the roster over everything
// placed; the outcome for a placed set must match what any other delivery computed for that set. It returns the entries in
// the order they became placed, which is what the arrival-time rule sees.
func deliver(t *testing.T, b *braid, perm []int, seed [32]byte, results map[string]string) []*item {
	t.Helper()
	nodes := b.nodes()
	d := newTestDAG(t)
	var placed []*item
	for _, i := range perm {
		ps, err := d.Add(nodes[i])
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range ps {
			placed = append(placed, b.byID[p.ID])
		}
		var order []ID
		if err := d.Order(nil, nil, seed, func(id ID) error { order = append(order, id); return nil }); err != nil {
			t.Fatal(err)
		}
		ids := slices.Clone(order)
		slices.SortFunc(ids, func(x, y ID) int { return cmpKey(Key(x), Key(y)) })
		key := fmt.Sprint(ids)
		got := fmt.Sprint(fold(t, b.roster(t), order, b.byID))
		if prev, ok := results[key]; ok && prev != got {
			t.Fatalf("two deliveries of one entry set disagree")
		}
		results[key] = got
	}
	return placed
}

func arrivalOutcome(t *testing.T, b *braid, placed []*item) []bool {
	t.Helper()
	out := arrivalFold(t, b, placed)
	// report per entry ID, so two arrival orders are compared entry by entry
	byID := map[ID]bool{}
	for i, it := range placed {
		byID[it.e.ID] = out[i]
	}
	res := make([]bool, len(b.items))
	for i, it := range b.items {
		res[i] = byID[it.e.ID]
	}
	return res
}
