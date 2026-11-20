package kolobraid

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/ciranda-dev/kolo-braid/internal/dag"
	"github.com/ciranda-dev/kolo-braid/internal/settle"
)

// waitEvent reads events of p until one of kind arrives.
func waitEvent(t testing.TB, b *Braid, kind EventKind, timeout time.Duration) Event {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for {
		ev, err := b.Next(ctx)
		if err != nil {
			t.Fatalf("no event %d: %v", kind, err)
		}
		if ev.Kind == kind {
			return ev
		}
	}
}

func membersOf(s *Snapshot) []string {
	var out []string
	for _, w := range s.Members() {
		out = append(out, string(w.Key))
	}
	return out
}

func hasMember(b *Braid, k ed25519.PublicKey, settled bool) bool {
	s := b.View()
	if settled {
		s.Close()
		s = b.Settled()
	}
	defer s.Close()
	return slices.Contains(membersOf(s), string(k))
}

func TestRemoveMemberVoidsConcurrentEntries(t *testing.T) {
	nt, ps := group(t, 2, GroupConfig{}, appApply, false)
	a, w := ps[0], ps[1]
	mustAppend(t, w.b, "w1=1")
	nt.settle(nil, 10*time.Second, func() bool { return quiet(a, w) && settledGet(t, a.b, "w1") == "1" })
	nt.partition([]int{0}, []int{1})
	if _, err := a.b.RemoveMember(ctx10(t), keyOf(w)); err != nil {
		t.Fatal(err)
	}
	mustAppend(t, w.b, "w2=2")
	nt.heal()
	nt.settle(nil, 10*time.Second, func() bool { return quiet(a, w) && !hasMember(w.b, keyOf(w), true) })
	checkSame(t, ps)
	if settledGet(t, a.b, "w2") != "<none>" || settledGet(t, a.b, "w1") != "1" {
		t.Fatalf("removal kept the concurrent entry or lost the earlier one: %s", dump(t, a.b))
	}
	var void bool
	for o, err := range a.b.Entries(0) {
		if err != nil {
			t.Fatal(err)
		}
		if string(o.Body) == "w2=2" {
			void = o.Void
		}
	}
	if !void {
		t.Fatal("the entry after the removal is not void")
	}
	waitEvent(t, w.b, EventRemoved, 5*time.Second)
	if _, err := w.b.Append(ctx10(t), []byte("w3")); !errors.Is(err, ErrRemoved) {
		t.Fatalf("removed member appends: %v", err)
	}
	if _, err := a.b.RemoveMember(ctx10(t), keyOf(a)); err != nil {
		t.Fatal(err) // written, but void at its position: the last admin stays
	}
	nt.settle(nil, 10*time.Second, func() bool { return quiet(a, w) })
	if !hasMember(a.b, keyOf(a), true) {
		t.Fatal("the last admin removed itself")
	}
}

func TestWritingCallsCheckTheTentativeRoster(t *testing.T) {
	nt, ps := group(t, 3, GroupConfig{}, appApply, false)
	a, w := ps[0], ps[2]
	nt.settle(nil, 5*time.Second, func() bool { return quiet(a, w) })
	if _, err := w.b.RemoveMember(ctx10(t), keyOf(a)); !errors.Is(err, ErrNotAdmin) {
		t.Fatalf("non-admin removal: %v", err)
	}
	if _, err := w.b.SetSettlers(ctx10(t), []ed25519.PublicKey{keyOf(w)}, Majority); !errors.Is(err, ErrNotAdmin) {
		t.Fatalf("non-admin settlers: %v", err)
	}
	if _, err := a.b.SetSettlers(ctx10(t), nil, Majority); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("no settlers: %v", err)
	}
	if _, err := a.b.SetSettlers(ctx10(t), []ed25519.PublicKey{keyOf(a)}, 9); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("unknown rule: %v", err)
	}
	if _, err := a.b.RemoveMember(ctx10(t), keyOf(a)[:5]); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("short key: %v", err)
	}
	if _, err := a.b.AddMember(ctx10(t), NewMember{Key: keyOf(w)}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("member without header: %v", err)
	}
	if _, err := a.b.Append(ctx10(t), make([]byte, a.b.cfg.MaxBody+1)); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("large body: %v", err)
	}
	// a joiner whose AddMember is not held is no member
	j, _ := newJoiner(t, "j", a, appApply)
	j.start()
	if _, err := j.b.Append(ctx10(t), []byte("x")); !errors.Is(err, ErrNotMember) {
		t.Fatalf("not a member: %v", err)
	}
}

func TestMutualRemovalKeepsOneAdmin(t *testing.T) {
	nt, ps := group(t, 3, GroupConfig{}, appApply, false)
	a, b := ps[0], ps[1]
	nt.settle(nil, 10*time.Second, func() bool { return quiet(ps...) })
	nt.partition([]int{0}, []int{1, 2})
	if _, err := a.b.RemoveMember(ctx10(t), keyOf(b)); err != nil {
		t.Fatal(err)
	}
	if _, err := b.b.RemoveMember(ctx10(t), keyOf(a)); err != nil {
		t.Fatal(err)
	}
	nt.heal()
	mustAppend(t, ps[2].b, "after=1")
	nt.settle(nil, 15*time.Second, func() bool { return quiet(ps...) && settledGet(t, a.b, "after") == "1" })
	checkSame(t, ps)
	ha, hb := hasMember(ps[2].b, keyOf(a), true), hasMember(ps[2].b, keyOf(b), true)
	if ha == hb {
		t.Fatalf("mutual removal: a active %v, b active %v; exactly one must remain", ha, hb)
	}
}

func TestApplyCannotVetoMembership(t *testing.T) {
	veto := func(tx *Tx, e *Entry) error {
		if e.Kind == KindAddMember {
			if err := tx.Put([]byte("vetoed"), []byte("1")); err != nil {
				return err
			}
			return Reject("no new members")
		}
		return appApply(tx, e)
	}
	nt, ps := group(t, 2, GroupConfig{}, veto, false)
	nt.settle(nil, 5*time.Second, func() bool { return quiet(ps...) && allActive(ps, 2) })
	if settledGet(t, ps[0].b, "vetoed") != "<none>" {
		t.Fatal("writes of a rejected membership entry were kept")
	}
	mustAppend(t, ps[1].b, "from=joiner")
	nt.settle(nil, 5*time.Second, func() bool { return quiet(ps...) && settledGet(t, ps[0].b, "from") == "joiner" })
}

func TestAdmitDefaultsGrowSeatsAndAdmins(t *testing.T) {
	a := newFounder(t, "a", GroupConfig{}, appApply)
	j1, nw1 := newJoiner(t, "j1", a, appApply)
	role, settlers := a.b.admitDefaults(key32(nw1.Key))
	if role != RoleAdmin || settlers != nil {
		t.Fatalf("first joiner: roles %d, settlers %d", role, len(settlers))
	}
	nw1.Roles = role
	if _, err := a.b.AddMember(ctx10(t), nw1); err != nil {
		t.Fatal(err)
	}
	_, nw2 := newJoiner(t, "j2", a, appApply)
	role, settlers = a.b.admitDefaults(key32(nw2.Key))
	if role != RoleAdmin || len(settlers) != 3 {
		t.Fatalf("third member: roles %d, settlers %d, want admin and three seats", role, len(settlers))
	}
	_ = j1
}

func TestAccessAndWelcomeAnchor(t *testing.T) {
	nt, ps := group(t, 2, GroupConfig{}, appApply, false)
	a, w := ps[0], ps[1]
	pin := newIdentity(t).PublicKey()
	a.b.mu.Lock()
	a.b.opts.Pins = append(a.b.opts.Pins, pin)
	a.b.mu.Unlock()
	if a.b.access(keyOf(w)) != accessKeys || a.b.access(pin) != accessCiphertext || a.b.access(newIdentity(t).PublicKey()) != accessNone {
		t.Fatal("access before removal")
	}
	anc := a.b.welcomeAnchor()
	if anc == nil || anc.Config != 0 {
		t.Fatalf("anchor %+v", anc)
	}
	if _, err := a.b.RemoveMember(ctx10(t), keyOf(w)); err != nil {
		t.Fatal(err)
	}
	if a.b.access(keyOf(w)) != accessNone {
		t.Fatal("a removed member still gets keys from a peer that holds the removal")
	}
	_ = nt
}

// TestJoinerSettlesInsideTheAnchor: a joiner whose meta carries the inviter's last committed pivot settles the same prefix.
func TestJoinerSettlesInsideTheAnchor(t *testing.T) {
	nt, ps := group(t, 3, GroupConfig{}, appApply, true)
	for i := 0; i < 10; i++ {
		mustAppend(t, ps[i%3].b, "x=1")
	}
	nt.settle(nil, 20*time.Second, func() bool { return quiet(ps...) })
	anc := ps[0].b.welcomeAnchor()
	j, nw := newJoiner(t, "j", ps[0], appApply)
	v, err := j.db.Get([]byte(keyMeta))
	if err != nil {
		t.Fatal(err)
	}
	m, err := decodeMeta(v)
	if err != nil {
		t.Fatal(err)
	}
	m.Anchor = anc
	if err := j.db.Put([]byte(keyMeta), m.encode()); err != nil {
		t.Fatal(err)
	}
	if _, err := ps[0].b.AddMember(ctx10(t), nw); err != nil {
		t.Fatal(err)
	}
	j.start()
	all := append(ps, j)
	n := newNet(t, all...)
	mustAppend(t, ps[1].b, "late=1")
	n.settle(nil, 20*time.Second, func() bool { return quiet(all...) && settledGet(t, j.b, "late") == "1" })
	checkSame(t, all)
}

// TestNetMutualRemovalSettles: the two seats (q = n = 2) remove each other on the two sides of a partition. Once healed, the
// removal ordered first wins; the removed seat still counts until the config without it activates, so it must go on pulsing,
// being linked and getting entry keys for its own removal to settle.
func TestNetMutualRemovalSettles(t *testing.T) {
	m := newMesh(t)
	ps := grow(t, m, 4, func(p *peer) { p.b.sync.timeout = 300 * time.Millisecond })
	a, b := ps[1], ps[2]
	if _, err := ps[0].b.SetSettlers(ctx10(t), []ed25519.PublicKey{keyOf(a), keyOf(b)}, Majority); err != nil {
		t.Fatal(err)
	}
	waitNet(t, 60*time.Second, ps, func() bool {
		if !synced(ps...) {
			return false
		}
		mustAppend(t, ps[0].b, "grow=1")
		return !slices.ContainsFunc(ps, func(p *peer) bool { return p.b.Status().Seats != 2 })
	})
	m.cut([]*peer{ps[0], a}, []*peer{b, ps[3]})
	if _, err := a.b.RemoveMember(ctx10(t), keyOf(b)); err != nil {
		t.Fatal(err)
	}
	if _, err := b.b.RemoveMember(ctx10(t), keyOf(a)); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 10*time.Second, func() bool { return !hasMember(ps[0].b, keyOf(b), false) && !hasMember(ps[3].b, keyOf(a), false) })
	m.full(ps...)
	waitNet(t, 60*time.Second, ps, func() bool {
		for _, o := range []*peer{ps[0], ps[3]} {
			if hasMember(o.b, keyOf(a), true) == hasMember(o.b, keyOf(b), true) || o.b.Status().Seats != 1 {
				return false
			}
		}
		return true
	})
}

// TestRemovedSeatPulsesOn: a seat whose removal its own tentative order holds still counts until a config without it activates,
// so it goes on writing pulses and keeps entry keys from peers; with two seats its pulses are what settles its own removal.
func TestRemovedSeatPulsesOn(t *testing.T) {
	nt, ps := group(t, 3, GroupConfig{}, appApply, false)
	setSettlers(t, nt, ps, ps[:2])
	f, s := ps[0], ps[1]
	if _, err := f.b.RemoveMember(ctx10(t), keyOf(s)); err != nil {
		t.Fatal(err)
	}
	pull(t, f, s, 0)
	waitUntil(t, 10*time.Second, func() bool { return !hasMember(s.b, keyOf(s), false) })
	if acc := f.b.access(keyOf(s)); acc != accessKeys {
		t.Fatal("a seat whose removal is not settled gets no entry keys")
	}
	nt.settle(nil, 20*time.Second, func() bool { return !hasMember(f.b, keyOf(s), true) && f.b.Status().Seats == 1 })
}

// TestTentativeRemovalStaysKnown: a member whose removal is only tentative is still in the settled roster, so its key stays
// known until the removal settles.
func TestTentativeRemovalStaysKnown(t *testing.T) {
	nt, ps := group(t, 3, GroupConfig{}, appApply, false)
	setSettlers(t, nt, ps, ps[:2])
	f, gone := ps[0], ps[2]
	nt.partition([]int{1}, []int{0, 2})
	if _, err := f.b.RemoveMember(ctx10(t), keyOf(gone)); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 10*time.Second, func() bool { return !hasMember(f.b, keyOf(gone), false) && hasMember(f.b, keyOf(gone), true) })
	if !f.b.sync.known(keyOf(gone)) {
		t.Fatal("a member removed only tentatively is no longer known")
	}
	nt.heal()
	nt.settle(nil, 20*time.Second, func() bool { return !hasMember(f.b, keyOf(gone), true) })
	if f.b.sync.known(keyOf(gone)) {
		t.Fatal("a member whose removal settled is still known")
	}
}

// TestReplacementSettlesWithASeatOffline: seats {p0, p1, p2} under Majority with p2 offline; replacing p1 by p3 keeps the old
// config, which p0 and p1 settle, until p3 is ready, and then p0 and p3 settle: no config in between needs the offline seat.
func TestReplacementSettlesWithASeatOffline(t *testing.T) {
	nt, ps := group(t, 4, GroupConfig{}, appApply, false)
	setSettlers(t, nt, ps, ps[:3])
	a, off, nw := ps[0], ps[2], ps[3]
	nt.partition([]int{2}, []int{0, 1, 3})
	if _, err := a.b.SetSettlers(ctx10(t), []ed25519.PublicKey{keyOf(a), keyOf(off), keyOf(nw)}, Majority); err != nil {
		t.Fatal(err)
	}
	for k := range 3 {
		mustAppend(t, a.b, fmt.Sprintf("w%d=1", k))
		i := 0
		nt.settle(nil, 20*time.Second, func() bool {
			if i++; i%200 == 0 {
				mustAppend(t, a.b, fmt.Sprintf("more%d=1", i))
			}
			return settledGet(t, a.b, fmt.Sprintf("w%d", k)) == "1"
		})
	}
}

// TestReplacementKeepsTheFaultBound: under Majority, replacing one of four seats with a settler that is offline keeps the four
// old seats (q = 3, one may sign two histories) until the new one is ready; no three-seat config (q = 2, none may) comes in
// between. The removed settler keeps its seat, and entry keys, until then, and loses both when the new config activates.
func TestReplacementKeepsTheFaultBound(t *testing.T) {
	nt, ps := group(t, 5, GroupConfig{}, appApply, false)
	setSettlers(t, nt, ps, ps[:4])
	f, gone, off := ps[0], ps[3], ps[4]
	cfg := f.b.Status().Config
	nt.partition([]int{4}, []int{0, 1, 2, 3})
	if _, err := f.b.SetSettlers(ctx10(t), []ed25519.PublicKey{keyOf(f), keyOf(ps[1]), keyOf(ps[2]), keyOf(off)}, Majority); err != nil {
		t.Fatal(err)
	}
	if _, err := f.b.RemoveMember(ctx10(t), keyOf(gone)); err != nil {
		t.Fatal(err)
	}
	for k := range 5 {
		mustAppend(t, f.b, fmt.Sprintf("w%d=1", k))
		nt.settle(nil, 20*time.Second, func() bool { return settledGet(t, f.b, fmt.Sprintf("w%d", k)) == "1" })
		if s := f.b.Status(); s.Config != cfg || s.Seats != 4 || s.Quorum != 3 || len(s.AwaitingReadiness) != 1 {
			t.Fatalf("while the new settler is offline: %+v", s)
		}
	}
	if hasMember(f.b, keyOf(gone), true) || f.b.access(keyOf(gone)) != accessKeys {
		t.Fatal("a settler whose removal settled while it still holds its seat gets no entry keys")
	}
	nt.heal()
	nt.settle(nil, 30*time.Second, func() bool {
		if s := f.b.Status(); s.Seats == 4 && s.Config == cfg+1 {
			return true
		}
		mustAppend(t, f.b, "back=1")
		return false
	})
	if f.b.access(keyOf(gone)) == accessKeys {
		t.Fatal("a removed member outside the active config gets entry keys")
	}
}

// TestRemovalFirstCutsKeys: an admin that must cut a settler off before its replacement is online removes it on its own
// first: a change that only removes activates when it settles, and the removed settler gets no entry keys from then on.
func TestRemovalFirstCutsKeys(t *testing.T) {
	nt, ps := group(t, 4, GroupConfig{}, appApply, false)
	setSettlers(t, nt, ps, ps[:3])
	f, gone, off := ps[0], ps[2], ps[3]
	nt.partition([]int{3}, []int{0, 1, 2})
	if _, err := f.b.SetSettlers(ctx10(t), []ed25519.PublicKey{keyOf(f), keyOf(ps[1])}, Majority); err != nil {
		t.Fatal(err)
	}
	if _, err := f.b.RemoveMember(ctx10(t), keyOf(gone)); err != nil {
		t.Fatal(err)
	}
	nt.settle(nil, 30*time.Second, func() bool { return !hasMember(f.b, keyOf(gone), true) && f.b.Status().Seats == 2 })
	if f.b.access(keyOf(gone)) == accessKeys {
		t.Fatal("a removed settler whose removal-only change activated gets entry keys")
	}
	if _, err := f.b.SetSettlers(ctx10(t), []ed25519.PublicKey{keyOf(f), keyOf(ps[1]), keyOf(off)}, Majority); err != nil {
		t.Fatal(err)
	}
	nt.heal()
	nt.settle(nil, 30*time.Second, func() bool {
		if f.b.Status().Seats == 3 {
			return true
		}
		mustAppend(t, f.b, "back=1")
		return false
	})
}

// TestIdleHandOff: on an idle braid, a pending config whose added settlers are all ready activates without another entry,
// so the old settler can leave once it has: here a one-settler braid hands settlement to a new device.
func TestIdleHandOff(t *testing.T) {
	nt, ps := group(t, 2, GroupConfig{}, appApply, false)
	a, b := ps[0], ps[1]
	if _, err := a.b.SetSettlers(ctx10(t), []ed25519.PublicKey{keyOf(b)}, Majority); err != nil {
		t.Fatal(err)
	}
	// no entry but pulses follows the Settlers entry
	nt.settle(nil, 20*time.Second, func() bool {
		return a.b.Status().Config == 1 && b.b.Status().Config == 1 && a.b.Status().PendingConfig == nil
	})
	nt.partition([]int{0}, []int{1})
	mustAppend(t, b.b, "after=1")
	nt.settle(nil, 20*time.Second, func() bool { return settledGet(t, b.b, "after") == "1" })
}

// TestReadinessWrittenAgain: a new settler that holds an own readiness pulse naming the pending config's ID which no longer
// counts (written before the config became pending again) writes another one while it is awaited.
func TestReadinessWrittenAgain(t *testing.T) {
	nt, ps := group(t, 2, GroupConfig{}, appApply, false)
	a, b := ps[0], ps[1]
	seats := []settle.Seat{
		{Key: key32(a.b.key), Strand: dag.StrandID(a.b.meta.OwnStrand)},
		{Key: key32(b.b.key), Strand: dag.StrandID(b.b.meta.OwnStrand)},
	}
	slices.SortFunc(seats, func(x, y settle.Seat) int { return bytes.Compare(x.Key[:], y.Key[:]) })
	b.b.mu.Lock()
	b.b.ps.ready, b.b.ps.hasReady = configID(settle.Config{Number: 1, Rule: settle.Majority, Seats: seats}.ID()), true
	b.b.mu.Unlock()
	if _, err := a.b.SetSettlers(ctx10(t), []ed25519.PublicKey{keyOf(a), keyOf(b)}, Majority); err != nil {
		t.Fatal(err)
	}
	nt.settle(nil, 20*time.Second, func() bool { return a.b.Status().Config == 1 && b.b.Status().Config == 1 })
}

// TestJoinerAfterPendingChange: a joiner that settles through its welcome anchor in one walk, after a pending config that
// a seat was ready for was replaced by one it is ready for again, awaits the same seats as every other peer.
func TestJoinerAfterPendingChange(t *testing.T) {
	m := newMesh(t)
	ps := grow(t, m, 5)
	f := ps[0]
	var none Role
	c := m.join(f, "p5", &AdmitOptions{Roles: &none}) // never online
	on := ps[:5]
	waitNet(t, 30*time.Second, on, func() bool { return synced(on...) && f.b.Status().Seats == 4 })
	if _, err := f.b.SetSettlers(ctx10(t), []ed25519.PublicKey{keyOf(f), keyOf(ps[1]), keyOf(ps[2]), keyOf(ps[3]), keyOf(ps[4]), keyOf(c)}, Majority); err != nil {
		t.Fatal(err)
	}
	mustAppend(t, f.b, "a=1")
	waitNet(t, 30*time.Second, on, func() bool {
		return synced(on...) && settledGet(t, ps[4].b, "a") == "1" && len(f.b.Status().AwaitingReadiness) == 1
	})
	mustAppend(t, f.b, "a2=1")
	waitNet(t, 30*time.Second, on, func() bool { return synced(on...) && settledGet(t, ps[4].b, "a2") == "1" })
	if _, err := f.b.SetSettlers(ctx10(t), []ed25519.PublicKey{keyOf(f), keyOf(ps[1]), keyOf(ps[2]), keyOf(ps[4]), keyOf(c)}, Majority); err != nil {
		t.Fatal(err)
	}
	mustAppend(t, f.b, "b=1")
	waitNet(t, 30*time.Second, on, func() bool {
		s := f.b.Status()
		return synced(on...) && settledGet(t, ps[4].b, "b") == "1" && len(s.PendingConfig) == 5 && len(s.AwaitingReadiness) == 1
	})
	mustAppend(t, f.b, "c=1")
	waitNet(t, 30*time.Second, on, func() bool { return synced(on...) && settledGet(t, ps[1].b, "c") == "1" })
	late := m.join(f, "p6", &AdmitOptions{Roles: &none})
	live := append(slices.Clone(on), late)
	for _, p := range ps[:3] {
		m.connect(late, p)
	}
	waitNet(t, 30*time.Second, live, func() bool { return synced(live...) && settledGet(t, late.b, "c") == "1" })
	for _, p := range live {
		if n := len(p.b.Status().AwaitingReadiness); n != 1 {
			t.Fatalf("%s awaits %d seats, %s awaits %d", p.name, n, f.name, len(f.b.Status().AwaitingReadiness))
		}
	}
}
