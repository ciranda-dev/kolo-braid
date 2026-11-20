package roster

import (
	"math"
	"slices"
	"testing"

	"github.com/ciranda-dev/kolo-braid/internal/dag"
)

// grow admits keys one by one with the defaults, applying the AddMember and the Settlers entry each time, as Admit does.
func grow(t *testing.T, s *seq, minSettlers int, keys ...Key) (roles []Role, sets [][]Key) {
	t.Helper()
	for i, k := range keys {
		_, seats := s.r.Settlers()
		r, st := s.r.AdmitDefaults(k, seats, minSettlers)
		roles, sets = append(roles, r), append(sets, st)
		s.must(sA, addW(k, sid(50+i), r))
		if st != nil {
			s.must(sA, settlersE(Majority, st...))
		}
	}
	return roles, sets
}

func TestAdmitDefaultsGrowth(t *testing.T) {
	s := founded(t)
	roles, sets := grow(t, s, 4, kB, kC, kD, kE)
	if !slices.Equal(roles, []Role{Admin, Admin, 0, 0}) {
		t.Fatalf("roles %v", roles)
	}
	want := [][]Key{nil, {kA, kB, kC}, {kA, kB, kC, kD}, nil}
	for i := range want {
		if !slices.Equal(sets[i], want[i]) {
			t.Fatalf("joiner %d: settlers %v, want %v", i, sets[i], want[i])
		}
	}
	if s.r.Admins() != 3 {
		t.Fatal("admins")
	}
	// a seat removed later is regrown, and so is the admin count at the next admission, with the earliest-added member first
	s.must(sA, removeW(kC))
	_, seats := s.r.Settlers()
	if r, st := s.r.AdmitDefaults(kid(7), seats, 4); r != Admin || !slices.Equal(st, sortedKeys(kA, kB, kD, kid(7))) {
		t.Fatalf("regrow: %v %v", r, st)
	}
	// seats counted from a pending config that is not active yet keep a fourth joiner from stacking a change
	if _, st := s.r.AdmitDefaults(kid(7), []Key{kA, kB, kD, kE}, 4); st != nil {
		t.Fatalf("stacked change %v", st)
	}
	// seats that are no longer active are not counted
	if _, st := s.r.AdmitDefaults(kid(7), []Key{kA, kB, kD, kC}, 4); !slices.Equal(st, sortedKeys(kA, kB, kD, kid(7))) {
		t.Fatalf("inactive seat counted: %v", st)
	}
}

func sortedKeys(keys ...Key) []Key {
	keys = slices.Clone(keys)
	slices.SortFunc(keys, cmpKey)
	return keys
}

func TestAdmitDefaultsMinSettlers(t *testing.T) {
	// MinSettlers 1: one admin, one seat, forever
	s := founded(t)
	roles, sets := grow(t, s, 1, kB, kC, kD)
	if !slices.Equal(roles, []Role{0, 0, 0}) || slices.ContainsFunc(sets, func(k []Key) bool { return k != nil }) {
		t.Fatalf("min 1: %v %v", roles, sets)
	}
	// MinSettlers 2: two seats at the second member, the founder the only admin
	s = founded(t)
	roles, sets = grow(t, s, 2, kB, kC)
	if !slices.Equal(roles, []Role{0, 0}) || !slices.Equal(sets[0], []Key{kA, kB}) || sets[1] != nil {
		t.Fatalf("min 2: %v %v", roles, sets)
	}
	// MinSettlers 5: three seats first, then five
	s = founded(t)
	_, sets = grow(t, s, 5, kB, kC, kD, kE)
	if sets[0] != nil || len(sets[1]) != 3 || sets[2] != nil || !slices.Equal(sets[3], []Key{kA, kB, kC, kD, kE}) {
		t.Fatalf("min 5: %v", sets)
	}
	// MinSettlers above MaxSettlers is capped
	s = newSeq(t, genesisOf(64, 2, 0))
	_, sets = grow(t, s, 4, kB, kC)
	if !slices.Equal(sets[0], []Key{kA, kB}) || sets[1] != nil {
		t.Fatalf("capped: %v", sets)
	}
	if r, _ := s.r.AdmitDefaults(kD, nil, 0); r != 0 {
		t.Fatal("min 0")
	}
}

func TestAdmitDefaultsReplace(t *testing.T) {
	s := founded(t, addW(kB, sB, 0), addW(kC, sC, Admin))
	_, seats := s.r.Settlers()
	// an active non-admin joiner is promoted while admins are below MinSettlers-1, and is not counted twice
	r, st := s.r.AdmitDefaults(kB, seats, 4)
	if r != Admin || !slices.Equal(st, []Key{kA, kB, kC}) {
		t.Fatalf("%v %v", r, st)
	}
	s.must(sA, addW(kD, sD, Admin))
	// an active admin keeps its role when admins are at the target
	if r, _ := s.r.AdmitDefaults(kC, seats, 4); r != Admin {
		t.Fatal("replacement lost the admin role")
	}
	if r, _ := s.r.AdmitDefaults(kB, seats, 4); r != 0 {
		t.Fatal("promotion past the target")
	}
}

func TestAdmitDefaultsEarliestAdded(t *testing.T) {
	// D is added before C here, so D gets the third seat
	s := founded(t, addW(kD, sD, 0), addW(kC, sC, 0))
	_, seats := s.r.Settlers()
	if _, st := s.r.AdmitDefaults(kE, seats, 4); !slices.Equal(st, sortedKeys(kA, kD, kE, kC)) {
		t.Fatalf("%v", st)
	}
	if _, st := s.r.AdmitDefaults(kE, []Key{kA}, 3); !slices.Equal(st, sortedKeys(kA, kD, kE)) {
		t.Fatalf("%v", st)
	}
}

func TestAdmitDefaultsAddedTie(t *testing.T) {
	r, _ := New(genesisOf(64, 7, 0))
	for i, e := range []Entry{{Kind: KindGenesis, ID: eid("genesis")}, addW(kD, sD, 0), addW(kC, sC, 0)} {
		e.Strand, e.Index = sA, uint64(i)
		if !r.Apply(1, &e).Effective() {
			t.Fatal(i)
		}
	}
	// equal positions fall back to key order, so C comes before D
	if _, st := r.AdmitDefaults(kE, []Key{kA}, 3); !slices.Equal(st, sortedKeys(kA, kC, kE)) {
		t.Fatalf("%v", st)
	}
}

func TestNeeds(t *testing.T) {
	var n Needs
	if n.Linked(sA) != 0 || n.Length(sA, false, 5, 10) != 0 {
		t.Fatal("empty")
	}
	if n.Length(sA, true, 5, 10) != 15 {
		t.Fatal("active strand pulls ahead of settlement")
	}
	n.Note(sB, 3, []dag.Link{{Strand: sA, Index: 40}, {Strand: sC, Index: 2}})
	n.Note(sB, 1, []dag.Link{{Strand: sA, Index: 7}, {Strand: sC, Index: math.MaxUint64}})
	if n.Linked(sA) != 41 || n.Linked(sB) != 4 || n.Linked(sC) != 3 {
		t.Fatal(n.Linked(sA), n.Linked(sB), n.Linked(sC))
	}
	if n.Length(sA, true, 5, 10) != 41 || n.Length(sA, false, 5, 10) != 41 || n.Length(sC, true, 0, 2048) != 2048 {
		t.Fatal("length")
	}
	if n.Length(sD, false, 100, 2048) != 0 {
		t.Fatal("a removed strand nobody links")
	}
	if n.Length(sD, true, math.MaxUint64-3, 2048) != math.MaxUint64 {
		t.Fatal("overflow")
	}
}
