package roster

import (
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/ciranda-dev/kolo-braid/internal/dag"
)

var (
	kA, kB, kC, kD, kE = kid(0), kid(1), kid(2), kid(3), kid(4)
	sA, sB, sC, sD, sE = sid(0), sid(1), sid(2), sid(3), sid(4)
)

// seq applies entries one after another, giving each strand consecutive indexes.
type seq struct {
	t     *testing.T
	r     *Roster
	p     uint64
	index map[StrandID]uint64
	n     int
}

func newSeq(t *testing.T, g Genesis) *seq {
	t.Helper()
	r, err := New(g)
	if err != nil {
		t.Fatal(err)
	}
	s := &seq{t: t, r: r, index: map[StrandID]uint64{}}
	if res := s.apply(g.Strand, Entry{Kind: KindGenesis, ID: g.Group}); res.Change != Founded {
		t.Fatalf("genesis: %v", res.Reason)
	}
	return s
}

func genesisOf(maxMembers, maxSettlers int, seeds uint64) Genesis {
	return Genesis{Group: eid("genesis"), Founder: kA, Strand: sA, Rule: Majority, MaxMembers: maxMembers, MaxSettlers: maxSettlers, Seeds: seeds}
}

// founded returns a roster with A (admin, founder) and the members added by A, in order.
func founded(t *testing.T, adds ...Entry) *seq {
	t.Helper()
	s := newSeq(t, genesisOf(64, 7, 0))
	for _, e := range adds {
		s.must(sA, e)
	}
	return s
}

func (s *seq) apply(strand StrandID, e Entry) Result {
	s.n++
	if e.ID == (ID{}) {
		e.ID = eid(fmt.Sprint("seq ", s.n))
	}
	e.Strand, e.Index = strand, s.index[strand]
	s.index[strand]++
	res := s.r.Apply(s.p, &e)
	s.p++
	return res
}

func (s *seq) must(strand StrandID, e Entry) Result {
	s.t.Helper()
	res := s.apply(strand, e)
	if !res.Effective() {
		s.t.Fatalf("entry of kind %d: %v", e.Kind, res.Reason)
	}
	return res
}

func (s *seq) void(strand StrandID, e Entry, want Reason) {
	s.t.Helper()
	before := s.r.Records()
	if got := s.r.Check(&Entry{Kind: e.Kind, Add: e.Add, Target: e.Target, Settlers: e.Settlers, Accused: e.Accused,
		Malformed: e.Malformed, Strand: strand, Index: s.index[strand], ID: e.ID}); got != want {
		s.t.Errorf("check of kind %d: %v, want %v", e.Kind, got, want)
	}
	res := s.apply(strand, e)
	if res.Reason != want || res.Effective() || res.Writes != nil {
		s.t.Errorf("entry of kind %d: %v, want %v", e.Kind, res.Reason, want)
	}
	if after := s.r.Records(); !slices.EqualFunc(before, after, sameWrite) {
		s.t.Errorf("void entry of kind %d changed the roster", e.Kind)
	}
}

func sameWrite(a, b Write) bool {
	return a.Delete == b.Delete && string(a.Key) == string(b.Key) && string(a.Value) == string(b.Value)
}

func (s *seq) active(k Key) bool { _, ok := s.r.Member(k); return ok }

func TestGenesis(t *testing.T) {
	g := genesisOf(64, 7, 0)
	r, err := New(g)
	if err != nil {
		t.Fatal(err)
	}
	if res := r.Apply(0, &Entry{Kind: KindData, Strand: sA, Index: 1}); res.Reason != Inactive {
		t.Fatalf("entry before the genesis entry: %v", res.Reason)
	}
	for _, e := range []Entry{
		{Kind: KindGenesis, ID: eid("other"), Strand: sA},
		{Kind: KindGenesis, ID: g.Group, Strand: sB},
		{Kind: KindGenesis, ID: g.Group, Strand: sA, Index: 1},
		{Kind: KindGenesis, ID: g.Group, Strand: sA, Malformed: true},
	} {
		if res := r.Apply(0, &e); res.Effective() {
			t.Fatalf("%+v founded the braid", e)
		}
	}
	res := r.Apply(5, &Entry{Kind: KindGenesis, ID: g.Group, Strand: sA})
	if res.Change != Founded || !res.SettlersChanged || len(res.Writes) != 4 {
		t.Fatalf("genesis: %+v", res)
	}
	w, ok := r.Member(kA)
	if !ok || w.Strand != sA || w.Roles != Admin || w.Added != 5 || r.Admins() != 1 || r.Len() != 1 || !r.Founded() {
		t.Fatalf("founder: %+v %v", w, ok)
	}
	if rule, st := r.Settlers(); rule != Majority || !slices.Equal(st, []Key{kA}) {
		t.Fatalf("settlers %v %v", rule, st)
	}
	if seats := r.Seats(); len(seats) != 1 || seats[0] != (Seat{Key: kA, Strand: sA}) {
		t.Fatalf("seats %v", seats)
	}
	if r.UsedCount() != 1 {
		t.Fatal("used count")
	}
	if res := r.Apply(6, &Entry{Kind: KindGenesis, ID: g.Group, Strand: sA}); res.Reason != NotGenesis {
		t.Fatalf("second genesis: %v", res.Reason)
	}
	if r.Genesis() != g {
		t.Fatal("genesis")
	}
}

func TestNewRefusesGenesis(t *testing.T) {
	good := genesisOf(64, 7, 0)
	for i, f := range []func(*Genesis){
		func(g *Genesis) { g.Group = ID{} },
		func(g *Genesis) { g.Rule = 0 },
		func(g *Genesis) { g.Rule = 3 },
		func(g *Genesis) { g.MaxMembers = 1 },
		func(g *Genesis) { g.MaxMembers = -1 },
		func(g *Genesis) { g.MaxSettlers = 0 },
		func(g *Genesis) { g.MaxMembers, g.MaxSettlers = 4, 5 },
	} {
		g := good
		f(&g)
		if _, err := New(g); !errors.Is(err, ErrInvalid) {
			t.Errorf("case %d: %v", i, err)
		}
	}
	for _, g := range []Genesis{good, genesisOf(2, 2, 0), genesisOf(1024, 64, 0), genesisOf(2, 1, 0), genesisOf(0, 7, 0), genesisOf(0, 300, 0), genesisOf(2000, 65, 0)} {
		if _, err := New(g); err != nil {
			t.Errorf("%+v: %v", g, err)
		}
	}
}

func TestData(t *testing.T) {
	s := founded(t, addW(kB, sB, 0))
	s.must(sA, dataE())
	s.must(sB, dataE())
	s.void(sC, dataE(), Inactive)
	s.void(sB, Entry{Kind: KindData, Malformed: true}, Malformed)
	s.must(sA, removeW(kB))
	s.void(sB, dataE(), Inactive)
	if res := s.must(sA, Entry{Kind: KindPulse}); res.Change != None || res.Writes != nil {
		t.Fatal("pulse changed something")
	}
	s.void(sB, Entry{Kind: KindPulse}, Inactive)
	s.void(sA, Entry{Kind: 8}, Malformed)
	s.void(sA, Entry{Kind: 255}, Malformed)
}

func TestAddMemberNewKey(t *testing.T) {
	s := founded(t)
	res := s.must(sA, addW(kB, sB, Admin))
	if res.Change != Added || res.Member != (Member{Key: kB, Strand: sB, Roles: Admin, Added: 1}) || s.r.Admins() != 2 {
		t.Fatalf("add: %+v", res)
	}
	if k, ok := s.r.Used(sB); !ok || k != kB {
		t.Fatal("strand not recorded as used")
	}
	s.must(sB, addW(kC, sC, 0))
	s.void(sC, addW(kD, sD, 0), NotAdmin)
	s.void(sD, addW(kD, sD, Admin), Inactive)
	s.void(sA, addW(kD, sC, 0), UsedStrand)
	s.void(sA, addW(kD, sA, 0), UsedStrand)
	s.void(sA, addW(kD, sD, 2), Malformed)
	s.void(sA, addW(kD, sD, 3), Malformed)
	s.void(sA, Entry{Kind: KindAddMember, Add: Add{Key: kD, Strand: sD}, Malformed: true}, Malformed)
	if s.active(kD) {
		t.Fatal("D active")
	}
	// a removed member's own strand cannot come back
	s.must(sA, removeW(kC))
	s.void(sA, addW(kC, sC, 0), UsedStrand)
	s.must(sA, addW(kC, sE, 0))
	if w, _ := s.r.Member(kC); w.Strand != sE || w.Added != s.p-1 {
		t.Fatalf("re-added C: %+v", w)
	}
}

func TestAddMemberFull(t *testing.T) {
	s := newSeq(t, genesisOf(3, 3, 0))
	s.must(sA, addW(kB, sB, 0))
	s.must(sA, addW(kC, sC, 0))
	s.void(sA, addW(kD, sD, 0), Full)
	// a role change and a replacement do not add a member
	s.must(sA, addW(kB, sB, Admin))
	s.must(sA, addW(kC, sD, 0))
	s.must(sA, removeW(kC))
	s.must(sA, addW(kE, sE, 0))
	if s.r.Len() != 3 {
		t.Fatal(s.r.Len())
	}
}

// TestAddMemberBeyond1024: with no ceiling every AddMember is effective; a set ceiling still voids the one past it.
func TestAddMemberBeyond1024(t *testing.T) {
	s := newSeq(t, genesisOf(0, 7, 0))
	for i := range 2000 {
		s.must(sA, addW(kid(100+i), sid(100+i), 0))
	}
	if s.r.Len() != 2001 {
		t.Fatalf("%d members", s.r.Len())
	}
	c := newSeq(t, genesisOf(10, 7, 0))
	for i := range 9 {
		c.must(sA, addW(kid(100+i), sid(100+i), 0))
	}
	c.void(sA, addW(kid(200), sid(200), 0), Full)
}

func TestAddMemberRoles(t *testing.T) {
	s := founded(t, addW(kB, sB, 0), addW(kC, sC, 0))
	s.void(sB, addW(kB, sB, Admin), NotAdmin)
	res := s.must(sA, addW(kB, sB, Admin))
	if res.Change != RolesSet || res.Member.Roles != Admin || s.r.Admins() != 2 || len(res.Writes) != 1 {
		t.Fatalf("promote: %+v", res)
	}
	// the same roles again: effective, nothing changes
	res = s.must(sA, addW(kB, sB, Admin))
	if res.Change != RolesSet || s.r.Admins() != 2 {
		t.Fatal("repeat")
	}
	if w, _ := s.r.Member(kB); w.Added != 1 {
		t.Fatal("a role change moved Added")
	}
	s.must(sB, addW(kA, sA, 0))
	if s.r.Admins() != 1 {
		t.Fatal("demote")
	}
	s.void(sB, addW(kB, sB, 0), LastAdmin)
	s.void(sA, addW(kB, sB, 0), NotAdmin)
	// an admin clears its own role while another admin remains
	s.must(sB, addW(kC, sC, Admin))
	s.must(sB, addW(kB, sB, 0))
	if w, _ := s.r.Member(kB); w.Roles != 0 || s.r.Admins() != 1 {
		t.Fatal("self demote")
	}
}

func TestAddMemberReplace(t *testing.T) {
	s := founded(t, addW(kB, sB, Admin), settlersE(Majority, kA, kB))
	res := s.must(sA, addW(kB, sD, Admin))
	if res.Change != Replaced || res.Old != sB || res.Member.Strand != sD || res.Member.Added != 1 {
		t.Fatalf("replace: %+v", res)
	}
	s.void(sB, dataE(), Inactive)
	s.must(sD, dataE())
	if w, ok := s.r.ByStrand(sD); !ok || w.Key != kB {
		t.Fatal("new strand")
	}
	if _, ok := s.r.ByStrand(sB); ok {
		t.Fatal("old strand still active")
	}
	// the key stays a settler; its seat names the new strand
	if seats := s.r.Seats(); !slices.Contains(seats, Seat{Key: kB, Strand: sD}) {
		t.Fatalf("seats %v", seats)
	}
	s.void(sA, addW(kB, sB, Admin), UsedStrand)
	s.void(sA, addW(kB, sA, Admin), UsedStrand)
	s.void(sE, addW(kB, sE, Admin), Inactive)
	s.must(sA, addW(kC, sC, 0))
	s.void(sC, addW(kC, sE, 0), NotAdmin)
	// a replacement that would leave no admin is void
	s.must(sD, addW(kA, sA, 0))
	s.void(sA, addW(kB, sE, Admin), NotAdmin)
	s.void(sD, addW(kB, sE, 0), LastAdmin)
	s.must(sD, addW(kB, sE, Admin))
}

func TestRemoveMember(t *testing.T) {
	s := founded(t, addW(kB, sB, Admin), addW(kC, sC, 0), settlersE(Majority, kA, kB, kC))
	s.void(sC, removeW(kB), NotAdmin)
	s.void(sA, removeW(kD), NotActive)
	res := s.must(sB, removeW(kC))
	if res.Change != Removed || res.Member.Key != kC || res.Member.Strand != sC || !res.SettlersChanged || len(res.Writes) != 2 {
		t.Fatalf("remove: %+v", res)
	}
	if _, st := s.r.Settlers(); !slices.Equal(st, []Key{kA, kB}) {
		t.Fatalf("settlers %v", st)
	}
	// replay of the same removal
	s.void(sB, removeW(kC), NotActive)
	s.void(sC, dataE(), Inactive)
	if s.r.Access(kC) || !s.r.Access(kB) || s.r.Access(kD) {
		t.Fatal("access")
	}
	// an admin removes itself while another admin and settler remain
	res = s.must(sB, removeW(kB))
	if res.Change != Removed || s.r.Admins() != 1 {
		t.Fatal("self removal")
	}
	s.void(sB, removeW(kA), Inactive)
	// the last admin cannot remove itself
	s.void(sA, removeW(kA), LastAdmin)
	if !s.active(kA) {
		t.Fatal("last admin removed")
	}
}

func TestRemoveLastSettler(t *testing.T) {
	s := founded(t, addW(kB, sB, Admin))
	// A is the only settler
	s.void(sB, removeW(kA), LastSettler)
	s.must(sA, removeW(kB))
	s = founded(t, addW(kB, sB, Admin), settlersE(TwoThirds, kB))
	s.void(sA, removeW(kB), LastSettler)
	res := s.must(sB, removeW(kA))
	if res.SettlersChanged || len(res.Writes) != 1 {
		t.Fatal("non-settler removal changed the settlers")
	}
	if rule, st := s.r.Settlers(); rule != TwoThirds || !slices.Equal(st, []Key{kB}) {
		t.Fatal("settlers")
	}
	// a non-admin settler is protected the same way
	s = founded(t, addW(kB, sB, 0), settlersE(Majority, kB))
	s.void(sA, removeW(kB), LastSettler)
}

func TestSettlers(t *testing.T) {
	s := newSeq(t, genesisOf(64, 3, 0))
	s.must(sA, addW(kB, sB, Admin))
	s.must(sA, addW(kC, sC, 0))
	s.must(sA, addW(kD, sD, 0))
	res := s.must(sB, settlersE(TwoThirds, kA, kB, kC))
	if res.Change != SettlersSet || !res.SettlersChanged {
		t.Fatal(res)
	}
	if rule, st := s.r.Settlers(); rule != TwoThirds || !slices.Equal(st, []Key{kA, kB, kC}) {
		t.Fatal("settlers")
	}
	s.void(sC, settlersE(Majority, kC), NotAdmin)
	s.void(sA, settlersE(Majority, kA, kB, kC, kD), TooManySettlers)
	s.void(sA, settlersE(Majority, kA, kE), NotActive)
	s.void(sA, Entry{Kind: KindSettlers, Settlers: Settlers{Rule: Majority}}, Malformed)
	s.void(sA, Entry{Kind: KindSettlers, Settlers: Settlers{Rule: Majority, Keys: []Key{kB, kA}}}, Malformed)
	s.void(sA, Entry{Kind: KindSettlers, Settlers: Settlers{Rule: Majority, Keys: []Key{kA, kA}}}, Malformed)
	s.void(sA, Entry{Kind: KindSettlers, Settlers: Settlers{Rule: 0, Keys: []Key{kA}}}, Malformed)
	s.void(sA, Entry{Kind: KindSettlers, Settlers: Settlers{Rule: 3, Keys: []Key{kA}}}, Malformed)
	s.void(sE, settlersE(Majority, kA), Inactive)
	s.must(sA, settlersE(Majority, kD))
	if rule, st := s.r.Settlers(); rule != Majority || !slices.Equal(st, []Key{kD}) {
		t.Fatal("replaced settlers")
	}
	// the founder is no settler now and can be removed
	s.must(sB, removeW(kA))
}

func TestEvidence(t *testing.T) {
	s := founded(t, addW(kB, sB, 0), addW(kC, sC, Admin), settlersE(Majority, kA, kB, kC))
	// any active member, not only an admin
	res := s.must(sB, evidenceE(kC, sC))
	if res.Change != Fenced || res.Member.Key != kC || !res.SettlersChanged {
		t.Fatalf("fence: %+v", res)
	}
	s.void(sC, dataE(), Inactive)
	s.void(sB, evidenceE(kC, sC), NotActive)
	s.void(sD, evidenceE(kB, sB), Inactive)
	s.void(sA, evidenceE(kB, sA), NotActiveStrand)
	// evidence against a replaced strand never cuts off the replacement
	s.must(sA, addW(kB, sD, 0))
	s.void(sA, evidenceE(kB, sB), NotActiveStrand)
	if !s.active(kB) {
		t.Fatal("replacement fenced")
	}
	s.void(sA, Entry{Kind: KindEvidence, Accused: Seat{Key: kB, Strand: sD}, Malformed: true}, Malformed)
	s.must(sA, evidenceE(kB, sD))
}

func TestEvidenceLastAdminAndSettler(t *testing.T) {
	s := founded(t, addW(kB, sB, 0))
	res := s.must(sB, evidenceE(kA, sA))
	if res.Change != Fenced || !res.SettlersChanged {
		t.Fatal(res)
	}
	if s.r.Admins() != 0 || s.r.Len() != 1 {
		t.Fatal("admins")
	}
	if _, st := s.r.Settlers(); len(st) != 0 {
		t.Fatalf("settlers %v", st)
	}
	if len(s.r.Seats()) != 0 {
		t.Fatal("seats")
	}
	s.void(sB, addW(kC, sC, 0), NotAdmin)
	// a member may fence itself
	s.must(sB, evidenceE(kB, sB))
	if s.r.Len() != 0 {
		t.Fatal("self fence")
	}
}

func TestSeed(t *testing.T) {
	s := newSeq(t, genesisOf(64, 7, 2))
	s.must(sA, Entry{Kind: KindSeed})
	s.must(sA, Entry{Kind: KindSeed})
	s.void(sA, Entry{Kind: KindSeed}, NotSeed)
	s.must(sA, addW(kB, sB, 0))
	s.index[sB] = 1
	s.void(sB, Entry{Kind: KindSeed}, NotSeed)
	s.void(sA, Entry{Kind: KindSeed, Malformed: true}, Malformed)
	// no seeds in a braid that continues nothing, not even at index 1
	s = newSeq(t, genesisOf(64, 7, 0))
	s.void(sA, Entry{Kind: KindSeed}, NotSeed)
	r, _ := New(genesisOf(64, 7, 2))
	if res := r.Apply(0, &Entry{Kind: KindSeed, Strand: sA, Index: 1}); res.Reason != Inactive {
		t.Fatal("seed before genesis")
	}
	// index 0 is the genesis entry, never a seed
	s = newSeq(t, genesisOf(64, 7, 2))
	if res := s.r.Apply(9, &Entry{Kind: KindSeed, Strand: sA, Index: 0}); res.Reason != NotSeed {
		t.Fatal("seed at index 0")
	}
}

func TestClass(t *testing.T) {
	for k := Kind(0); k < 10; k++ {
		want := dag.Other
		if k >= KindAddMember && k <= KindEvidence {
			want = dag.Membership
		}
		if got := Class(k, false); got != want {
			t.Errorf("kind %d: %v", k, got)
		}
		if Class(k, true) != dag.Other {
			t.Errorf("malformed kind %d is not Other", k)
		}
	}
}

func TestClone(t *testing.T) {
	s := founded(t, addW(kB, sB, Admin), settlersE(Majority, kA, kB))
	c := s.r.Clone()
	s.must(sA, removeW(kB))
	s.must(sA, addW(kC, sC, 0))
	if _, ok := c.Member(kB); !ok || c.Admins() != 2 || c.Len() != 2 {
		t.Fatal("clone changed with the original")
	}
	if _, ok := c.Used(sC); ok {
		t.Fatal("clone shares used strands")
	}
	if _, st := c.Settlers(); len(st) != 2 {
		t.Fatal("clone settlers")
	}
	res := c.Apply(9, &Entry{Kind: KindRemoveMember, Target: kA, Strand: sB, Index: 1, ID: eid("x")})
	if !res.Effective() || !s.active(kA) {
		t.Fatal("original changed with the clone")
	}
}

func TestReasonString(t *testing.T) {
	seen := map[string]bool{}
	for r := OK; r <= TooManySettlers; r++ {
		if n := r.String(); seen[n] || n == fmt.Sprint("reason ", int(r)) {
			t.Errorf("reason %d: %q", r, n)
		}
		seen[r.String()] = true
	}
	if Reason(200).String() != "reason 200" {
		t.Fatal(Reason(200).String())
	}
}
