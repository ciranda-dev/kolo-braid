package kolobraid

import (
	"context"
	"errors"
	"testing"
	"time"

	kolostore "github.com/ciranda-dev/kolo-store"
)

func TestSingleMemberSettles(t *testing.T) {
	p := newFounder(t, "a", GroupConfig{}, appApply)
	mustAppend(t, p.b, "a=1")
	mustAppend(t, p.b, "b=2")
	waitUntil(t, 5*time.Second, func() bool { return quiet(p) && settledGet(t, p.b, "b") == "2" })
	if got := settledGet(t, p.b, "#"); got != "2" {
		t.Fatalf("counter %s", got)
	}
}

func TestTwoMembersConverge(t *testing.T) {
	a := newFounder(t, "a", GroupConfig{}, appApply)
	j, nw := newJoiner(t, "b", a, appApply)
	if _, err := a.b.AddMember(ctx10(t), nw); err != nil {
		t.Fatal(err)
	}
	j.start()
	n := newNet(t, a, j)
	n.settle(nil, 5*time.Second, func() bool { return quiet(a, j) && j.b.Status().Settled > 1 })
	mustAppend(t, a.b, "x=1")
	mustAppend(t, j.b, "y=2")
	n.settle(nil, 5*time.Second, func() bool {
		return quiet(a, j) && settledGet(t, a.b, "y") == "2" && settledGet(t, j.b, "x") == "1"
	})
	if da, dj := dump(t, a.b), dump(t, j.b); da != dj {
		t.Fatalf("views differ:\n%s\n%s", da, dj)
	}
}

// TestPulsesArePaced: a seat writes at most two pulses per PulseGap under constant activity (the pivot pulse is exempt from the
// gap, the vote is not).
func TestPulsesArePaced(t *testing.T) {
	db := kolostore.NewMemory()
	o := testOptions(db, appApply)
	o.Limits.PulseGap = 100 * time.Millisecond
	b, err := Create(context.Background(), newIdentity(t), GroupConfig{App: "test/v1"}, o)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	start := time.Now()
	for time.Since(start) < time.Second {
		mustAppend(t, b, "x")
		time.Sleep(5 * time.Millisecond)
	}
	pulses := 0
	for o, err := range b.Entries(0) {
		if err != nil {
			t.Fatal(err)
		}
		if o.Kind == KindPulse {
			pulses++
		}
	}
	if limit := 2*int(time.Since(start)/(100*time.Millisecond)) + 4; pulses > limit || pulses < 4 {
		t.Fatalf("%d pulses in %v, want 4 to %d", pulses, time.Since(start), limit)
	}
}

// TestNestedPrefixRefused: braids on one backend must not nest their prefixes, since Open clears parts of its own prefix; one
// that nests with a recorded prefix, or with the registry, is refused, and the same prefix opens again.
func TestNestedPrefixRefused(t *testing.T) {
	db := kolostore.NewMemory()
	id := newIdentity(t)
	opts := func(p string) Options {
		o := testOptions(db, appApply)
		o.Prefix = []byte(p)
		return o
	}
	a, err := Create(context.Background(), id, GroupConfig{App: "test/v1"}, opts("a"))
	if err != nil {
		t.Fatal(err)
	}
	a.Close()
	for _, p := range []string{"at", "", "kolo", "kolo-braid/prefixes/v0/x"} {
		if _, err := Create(context.Background(), newIdentity(t), GroupConfig{App: "test/v1"}, opts(p)); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("prefix %q: %v", p, err)
		}
		if _, err := NewJoin(context.Background(), newIdentity(t), opts(p)); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("join under prefix %q: %v", p, err)
		}
	}
	b, err := Create(context.Background(), newIdentity(t), GroupConfig{App: "test/v1"}, opts("b"))
	if err != nil {
		t.Fatal(err)
	}
	b.Close()
	if a, err = Open(context.Background(), id, opts("a")); err != nil {
		t.Fatal(err)
	}
	a.Close()
}

// TestStalePrefixFreed: a recorded prefix whose braid an application deleted, with no pending join, no longer blocks a
// prefix that nests with it.
func TestStalePrefixFreed(t *testing.T) {
	db := kolostore.NewMemory()
	o := testOptions(db, appApply)
	o.Prefix = []byte("a")
	a, err := Create(context.Background(), newIdentity(t), GroupConfig{App: "test/v1"}, o)
	if err != nil {
		t.Fatal(err)
	}
	a.Close()
	bt := db.NewBatch()
	bt.DeleteRange([]byte("a"), []byte("b"))
	if err := bt.Commit(); err != nil {
		t.Fatal(err)
	}
	o.Prefix = []byte("at")
	b, err := Create(context.Background(), newIdentity(t), GroupConfig{App: "test/v1"}, o)
	if err != nil {
		t.Fatalf("a nesting prefix after the old braid is gone: %v", err)
	}
	b.Close()
}

// TestPendingJoinKeepsItsPrefix: a prefix that holds only a pending join, with no braid yet, still blocks a prefix that nests
// with it.
func TestPendingJoinKeepsItsPrefix(t *testing.T) {
	db := kolostore.NewMemory()
	o := testOptions(db, appApply)
	o.Prefix = []byte("a")
	if _, err := NewJoin(context.Background(), newIdentity(t), o); err != nil {
		t.Fatal(err)
	}
	o.Prefix = []byte("at")
	if _, err := Create(context.Background(), newIdentity(t), GroupConfig{App: "test/v1"}, o); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("a prefix nesting with a pending join: %v", err)
	}
}
