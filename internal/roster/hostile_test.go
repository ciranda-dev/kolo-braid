package roster

import (
	"math/rand/v2"
	"slices"
	"testing"
)

// TestForgedMembership: membership entries of every kind on a strand that was never added, or added without the admin
// role, are void whatever they claim, and so are the entries of strands they name.
func TestForgedMembership(t *testing.T) {
	s := founded(t, addW(kB, sB, 0))
	for _, strand := range []StrandID{sD, sB} {
		want := Inactive
		if strand == sB {
			want = NotAdmin
		}
		s.void(strand, addW(kE, sE, Admin), want)
		s.void(strand, addW(kB, sB, Admin), want)
		s.void(strand, removeW(kA), want)
		s.void(strand, settlersE(TwoThirds, kB), want)
	}
	s.void(sD, evidenceE(kA, sA), Inactive)
	s.void(sE, dataE(), Inactive)
	if w, _ := s.r.Member(kB); w.Roles != 0 || s.r.Len() != 2 || !s.active(kA) {
		t.Fatal("a forged entry changed the roster")
	}
}

// TestReplays: replaying an earlier membership body changes nothing it should not.
func TestReplays(t *testing.T) {
	s := founded(t, addW(kB, sB, Admin), addW(kC, sC, 0), settlersE(Majority, kA, kB))
	s.must(sA, removeW(kC))
	s.void(sB, addW(kC, sC, 0), UsedStrand)
	s.void(sB, removeW(kC), NotActive)
	s.must(sB, evidenceE(kB, sB))
	s.void(sA, evidenceE(kB, sB), NotActive)
	s.void(sB, dataE(), Inactive)
	// the same Settlers body again is effective and idempotent
	s.must(sA, settlersE(Majority, kA))
	before := s.r.Records()
	s.must(sA, settlersE(Majority, kA))
	if !slices.EqualFunc(before, s.r.Records(), sameWrite) {
		t.Fatal("repeated Settlers changed the roster")
	}
}

// TestRemoveAfterSettlersFenced: once evidence took the last settler, a removal is void until a Settlers entry names one.
func TestRemoveAfterSettlersFenced(t *testing.T) {
	s := founded(t, addW(kB, sB, Admin), addW(kC, sC, 0))
	s.must(sB, evidenceE(kA, sA))
	s.void(sB, removeW(kC), LastSettler)
	s.must(sB, settlersE(Majority, kB))
	s.must(sB, removeW(kC))
}

// TestAuthorizationCycle: B and C, never added, add each other as admins; neither entry is effective in either order. In the
// DAG, entries that link each other stay pending forever.
func TestAuthorizationCycle(t *testing.T) {
	for _, bFirst := range []bool{true, false} {
		s := founded(t)
		x, y := addW(kC, sC, Admin), addW(kB, sB, Admin)
		if bFirst {
			s.void(sB, x, Inactive)
			s.void(sC, y, Inactive)
		} else {
			s.void(sC, y, Inactive)
			s.void(sB, x, Inactive)
		}
		s.void(sB, removeW(kA), Inactive)
	}
	b := newBraid(kA, sA, 0)
	cB, cC := eid("cycle b"), eid("cycle c")
	b.addID("cb", cB, sB, addW(kC, sC, Admin))
	b.addID("cc", cC, sC, addW(kB, sB, Admin))
	b.items[1].node.Links = append(b.items[1].node.Links, link(b.items[2]))
	b.items[2].node.Links = append(b.items[2].node.Links, link(b.items[1]))
	d := newTestDAG(t)
	for _, n := range b.nodes() {
		if _, err := d.Add(n); err != nil {
			t.Fatal(err)
		}
	}
	if d.Len() != 1 {
		t.Fatalf("%d entries placed", d.Len())
	}
	if n, _ := d.Pending(); n != 2 {
		t.Fatalf("%d pending", n)
	}
}

// TestDelegationRevoked: A makes B an admin, B adds C, then a demotion of B is ordered before B's next add: that add is
// void, and C, added before it, stays.
func TestDelegationRevoked(t *testing.T) {
	s := founded(t, addW(kB, sB, Admin))
	s.must(sB, addW(kC, sC, Admin))
	s.must(sA, addW(kB, sB, 0))
	s.void(sB, addW(kD, sD, 0), NotAdmin)
	s.must(sC, addW(kD, sD, 0))
	if s.r.Len() != 4 || s.r.Admins() != 2 {
		t.Fatal(s.r.Members())
	}
}

// TestHugeHistory folds a long random history at MaxMembers 1024 and checks it against the reference, the writes against
// the records, and a reload against the roster.
func TestHugeHistory(t *testing.T) {
	n := 100000
	if testing.Short() || raceEnabled {
		n = 5000
	}
	g := Genesis{Group: eid("huge"), Founder: kid(0), Strand: sid(0), Rule: Majority, MaxMembers: 1024, MaxSettlers: 64}
	r, _ := New(g)
	rf := &ref{g: g}
	rng := rand.New(rand.NewPCG(4, 4))
	store := map[string][]byte{}
	apply := func(p int, e *Entry) {
		res := r.Apply(uint64(p), e)
		if res.Effective() != rf.step(uint64(p), e) {
			t.Fatalf("position %d: %v, reference disagrees", p, res.Reason)
		}
		for _, w := range res.Writes {
			if w.Delete {
				delete(store, string(w.Key))
			} else {
				store[string(w.Key)] = w.Value
			}
		}
	}
	apply(0, &Entry{Kind: KindGenesis, ID: g.Group, Strand: g.Strand})
	next := 1
	for p := 1; p < n; p++ {
		e := Entry{Strand: sid(0)}
		if ws := rf.ws; len(ws) > 0 {
			e.Strand = ws[rng.IntN(len(ws))].Strand
		}
		k := kid(rng.IntN(1500))
		switch x := rng.IntN(10); {
		case x < 5:
			e.Kind, e.Add = KindAddMember, Add{Key: k, Strand: sid(next), Roles: Role(rng.IntN(2))}
			next++
		case x < 8:
			e.Kind, e.Target = KindRemoveMember, k
		case x < 9:
			keys := []Key{k}
			if len(rf.ws) > 0 {
				keys = append(keys, rf.ws[rng.IntN(len(rf.ws))].Key)
			}
			e.Kind, e.Settlers = KindSettlers, settlersE(Majority, keys...).Settlers
		default:
			e.Kind = KindData
		}
		apply(p, &e)
		if p%5000 == 0 && !slices.Equal(r.Members(), rf.members()) {
			t.Fatalf("position %d: members differ", p)
		}
	}
	recs := r.Records()
	if len(recs) != len(store) {
		t.Fatalf("%d records, %d left by the writes", len(recs), len(store))
	}
	l, err := Load(g, pairs(recs))
	if err != nil {
		t.Fatal(err)
	}
	if !slices.EqualFunc(l.Records(), recs, sameWrite) || l.Admins() != r.Admins() {
		t.Fatal("reload differs")
	}
	t.Logf("%d positions: %d members, %d used strands, %d admins", n, r.Len(), r.UsedCount(), r.Admins())
}
