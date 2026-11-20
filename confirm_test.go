package kolobraid

import (
	"context"
	"errors"
	"testing"
	"time"

	kololog "github.com/ciranda-dev/kolo-log"
	kolostore "github.com/ciranda-dev/kolo-store"
)

// copyBackend returns a memory backend holding every key of b: an old copy of a device.
func copyBackend(t testing.TB, b kolostore.Backend) kolostore.Backend {
	t.Helper()
	c := kolostore.NewMemory()
	bt := c.NewBatch()
	if err := b.Scan(nil, nil, false, 0, func(k, v []byte) bool {
		bt.Put(k, v)
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if err := bt.Commit(); err != nil {
		t.Fatal(err)
	}
	return c
}

func shortCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 50*time.Millisecond)
}

func TestRestoredDeviceWaitsAndCatchesUp(t *testing.T) {
	nt, ps := group(t, 2, GroupConfig{}, appApply, false)
	a, w := ps[0], ps[1]
	mustAppend(t, w.b, "old=1")
	nt.settle(nil, 10*time.Second, func() bool { return quiet(a, w) })
	backup := copyBackend(t, w.db)
	mustAppend(t, w.b, "new=2")
	nt.settle(nil, 10*time.Second, func() bool { return quiet(a, w) && settledGet(t, a.b, "new") == "2" })
	w.close()
	w.db, w.opts.Backend = backup, backup
	w.reopen()
	if !w.b.Status().Unconfirmed {
		t.Fatal("a reopened member of a braid with other members is confirmed without asking")
	}
	ctx, cancel := shortCtx()
	defer cancel()
	if _, err := w.b.Append(ctx, []byte("x")); !errors.Is(err, ErrUnconfirmed) {
		t.Fatalf("append before confirmation: %v", err)
	}
	// a member holds a longer, consistent history: the strand is behind and catches up
	pull(t, a, w, 0)
	if !w.b.Status().Unconfirmed {
		t.Fatal("a longer head confirmed the strand")
	}
	rep, err := w.b.catchUpReplica()
	if err != nil {
		t.Fatal(err)
	}
	if !transfer(t, a.b.strandFor(w.b.meta.OwnStrand), rep, ^uint64(0)) {
		t.Fatal("nothing to catch up")
	}
	if err := w.b.adoptOwn(rep); err != nil {
		t.Fatal(err)
	}
	waitEvent(t, w.b, EventStrandCaughtUp, 5*time.Second)
	// the adopted entries count toward the next span pulse
	w.b.mu.Lock()
	since := w.b.sincePulse
	w.b.mu.Unlock()
	if trailing := trailingRun(t, w); since != min(trailing, pulseSpan) || trailing == 0 {
		t.Fatalf("own entries since the last pulse after a catch-up: %d, the strand ends with %d", since, trailing)
	}
	nt.confirm(w)
	mustAppend(t, w.b, "after=3")
	nt.settle(nil, 10*time.Second, func() bool { return quiet(a, w) && settledGet(t, a.b, "after") == "3" })
	checkSame(t, ps)
	a.b.mu.Lock()
	forked := len(a.b.evidence) > 0
	a.b.mu.Unlock()
	if forked {
		t.Fatal("the restored device signed a second history")
	}
}

func TestInconsistentHeadLosesTheStrand(t *testing.T) {
	nt, ps := group(t, 2, GroupConfig{}, appApply, false)
	a, w := ps[0], ps[1]
	mustAppend(t, w.b, "one=1")
	nt.settle(nil, 10*time.Second, func() bool { return quiet(a, w) })
	backup := copyBackend(t, w.db)
	mustAppend(t, w.b, "two=2")
	nt.settle(nil, 10*time.Second, func() bool { return quiet(a, w) && settledGet(t, a.b, "two") == "2" })
	w.close()
	// the old copy writes at once and signs a second history
	w.db, w.opts.Backend = backup, backup
	w.opts.AppendUnconfirmed = true
	w.reopen()
	mustAppend(t, w.b, "fork=1")
	pull(t, a, w, 0)
	if !w.b.Status().StrandLost {
		t.Fatal("a head of the same size with another root did not lose the strand")
	}
	waitEvent(t, w.b, EventStrandLost, 5*time.Second)
	if _, err := w.b.Append(ctx10(t), []byte("more")); !errors.Is(err, ErrStrandLost) {
		t.Fatalf("append on a lost strand: %v", err)
	}
}

func TestSecondRunningCopyStopsAfterFirstHead(t *testing.T) {
	nt, ps := group(t, 2, GroupConfig{}, appApply, false)
	a, w := ps[0], ps[1]
	mustAppend(t, w.b, "one=1")
	nt.settle(nil, 10*time.Second, func() bool { return quiet(a, w) })
	copyDB := copyBackend(t, w.db)
	// the copy runs while the original is down and writes its own entry
	w.close()
	clone := &peer{t: t, name: "clone", id: w.id, db: copyDB}
	clone.opts = testOptions(copyDB, appApply)
	clone.opts.AppendUnconfirmed = true
	clone.reopen()
	mustAppend(t, clone.b, "clone=1")
	h, err := clone.b.own.Head()
	if err != nil {
		t.Fatal(err)
	}
	clone.close()
	w.reopen()
	nt.confirm(w)
	mustAppend(t, w.b, "orig=1")
	// a member shows the clone's head: the original stops at once
	if err := w.b.ownHead(keyOf(a), h); !errors.Is(err, ErrStrandLost) {
		t.Fatalf("conflicting head: %v", err)
	}
	if _, err := w.b.Append(ctx10(t), []byte("x")); !errors.Is(err, ErrStrandLost) {
		t.Fatalf("append after a conflicting head: %v", err)
	}
	// heads from non-members and forged heads are ignored or refused
	if err := w.b.ownHead(keyOf(w), h); err != nil && !errors.Is(err, ErrStrandLost) {
		t.Fatal(err)
	}
	h.Signature[0] ^= 1
	if err := w.b.ownHead(keyOf(a), h); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("forged head: %v", err)
	}
}

// TestSeatRestartWithLostSeat: four seats, one gone for good and another restarting; the restarted seat confirms with the q - 1
// others that answer after ConfirmWait, and settlement goes on.
func TestSeatRestartWithLostSeat(t *testing.T) {
	nt, ps := group(t, 4, GroupConfig{}, appApply, true)
	lost, restart := ps[3], ps[2]
	lost.close()
	restart.reopen()
	start := time.Now()
	n := newNet(t, ps[0], ps[1], ps[2])
	n.confirm(restart)
	if d := time.Since(start); d < restart.b.lim.ConfirmWait {
		t.Fatalf("a seat confirmed after %v without waiting ConfirmWait for the missing seat", d)
	}
	mustAppend(t, restart.b, "go=on")
	n.settle(nil, 20*time.Second, func() bool {
		return quiet(ps[0], ps[1], ps[2]) && settledGet(t, ps[0].b, "go") == "on"
	})
	checkSame(t, ps[:3])
	_ = nt
}

func TestSeatConfirmsAtOnceWhenEverySeatAnswers(t *testing.T) {
	_, ps := group(t, 3, GroupConfig{}, appApply, true)
	ps[1].reopen()
	start := time.Now()
	n := newNet(t, ps...)
	n.confirm(ps[1])
	if d := time.Since(start); d >= ps[1].b.lim.ConfirmWait {
		t.Fatalf("waited %v although every other seat answered", d)
	}
}

func TestAdoptRefusesAnotherHistory(t *testing.T) {
	nt, ps := group(t, 2, GroupConfig{}, appApply, false)
	a, w := ps[0], ps[1]
	mustAppend(t, w.b, "one=1")
	nt.settle(nil, 10*time.Second, func() bool { return quiet(a, w) })
	if err := w.b.adoptOwn(nil); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("nil replica: %v", err)
	}
	rep, err := w.b.catchUpReplica()
	if err != nil {
		t.Fatal(err)
	}
	// a replica that holds only what the log holds changes nothing
	transfer(t, a.b.strandFor(w.b.meta.OwnStrand), rep, ^uint64(0))
	if err := w.b.adoptOwn(rep); err != nil {
		t.Fatal(err)
	}
	if w.b.Status().StrandLost {
		t.Fatal("an equal replica lost the strand")
	}
	var _ kololog.Head
}

// cloneHead opens a copy of p's data with AppendUnconfirmed, appends one entry there and returns that copy's head.
func cloneHead(t *testing.T, p *peer) kololog.Head {
	t.Helper()
	c := &peer{t: t, name: "clone", id: p.id, db: copyBackend(t, p.db)}
	c.opts = testOptions(c.db, appApply)
	c.opts.AppendUnconfirmed = true
	b, err := Open(ctx10(t), c.id, c.opts)
	if err != nil {
		t.Fatal(err)
	}
	c.b = b
	defer c.close()
	mustAppend(t, c.b, "clone=1")
	h, err := c.b.own.Head()
	if err != nil {
		t.Fatal(err)
	}
	return h
}

// TestLongerHeadStopsWrites: after confirmation, a longer consistent head of the own strand stops every write until the strand
// caught up; once the strand wrote after its confirmation, such a head can only come from a second running copy, and the
// strand is lost.
func TestLongerHeadStopsWrites(t *testing.T) {
	nt, ps := group(t, 2, GroupConfig{}, appApply, false)
	a, w := ps[0], ps[1]
	mustAppend(t, w.b, "one=1")
	nt.settle(nil, 10*time.Second, func() bool { return quiet(a, w) })
	w.close()
	h := cloneHead(t, w)
	w.reopen()
	nt.confirm(w)
	n := w.b.own.Len()
	if err := w.b.ownHead(keyOf(a), h); err != nil {
		t.Fatalf("longer head: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if _, err := w.b.Append(ctx, []byte("orig=1")); !errors.Is(err, ErrUnconfirmed) || w.b.own.Len() != n || !w.b.Status().Unconfirmed {
		t.Fatalf("append while behind: %v, length %d of %d", err, w.b.own.Len(), n)
	}
	// as if the strand had been confirmed one entry earlier and wrote that entry since: a longer consistent head now can only
	// come from a second running copy (both copies cannot run in one process, so the state is set here)
	w.reopen()
	nt.confirm(w)
	w.b.mu.Lock()
	w.b.conf.at--
	w.b.mu.Unlock()
	if err := w.b.ownHead(keyOf(a), h); !errors.Is(err, ErrStrandLost) {
		t.Fatalf("a longer head after writing: %v", err)
	}
}

// restoredBehindAStalePeer makes a group of three where member w is restored from a backup that is longer than what the stale
// member s holds and shorter than what a holds, and reopens w.
func restoredBehindAStalePeer(t *testing.T) (nt *net, a, s, w *peer) {
	t.Helper()
	nt, ps := group(t, 3, GroupConfig{}, appApply, false)
	a, s, w = ps[0], ps[1], ps[2]
	mustAppend(t, w.b, "e0=1")
	nt.settle(nil, 10*time.Second, func() bool { return quiet(ps...) })
	mustAppend(t, w.b, "e1=1")
	backup := copyBackend(t, w.db)
	mustAppend(t, w.b, "e2=1")
	pull(t, w, a, 0)
	w.close()
	w.db, w.opts.Backend = backup, backup
	w.reopen()
	return nt, a, s, w
}

// TestRestoredDeviceIsNotConfirmedByAStalePeer: a shorter head from a member that is as far behind as the restored backup does
// not confirm a device that is no seat at once, so a longer head that arrives meanwhile makes it catch up instead of writing a
// second history; once ConfirmWait ends without one, the shorter head confirms it.
func TestRestoredDeviceIsNotConfirmedByAStalePeer(t *testing.T) {
	_, a, s, w := restoredBehindAStalePeer(t)
	pull(t, s, w, 0)
	if !w.b.Status().Unconfirmed {
		t.Fatal("a head shorter than the strand confirmed it at once")
	}
	ctx, cancel := shortCtx()
	defer cancel()
	if _, err := w.b.Append(ctx, []byte("x")); !errors.Is(err, ErrUnconfirmed) {
		t.Fatalf("append after a stale head: %v", err)
	}
	pull(t, a, w, 0)
	time.Sleep(2 * w.opts.Limits.ConfirmWait)
	if !w.b.Status().Unconfirmed {
		t.Fatal("a longer head did not keep the device from confirming")
	}
}

func TestShorterHeadConfirmsAfterConfirmWait(t *testing.T) {
	_, _, s, w := restoredBehindAStalePeer(t)
	pull(t, s, w, 0)
	waitUntil(t, 10*time.Second, func() bool { return !w.b.Status().Unconfirmed })
}

// TestEqualHeadConfirmsAtOnce: a head as long as the strand confirms a device that is no seat without waiting.
func TestEqualHeadConfirmsAtOnce(t *testing.T) {
	nt, ps := group(t, 2, GroupConfig{}, appApply, false)
	a, w := ps[0], ps[1]
	mustAppend(t, w.b, "e0=1")
	nt.settle(nil, 10*time.Second, func() bool { return quiet(ps...) })
	w.opts.Limits.ConfirmWait = time.Minute
	w.reopen()
	pull(t, a, w, 0)
	if w.b.Status().Unconfirmed {
		t.Fatal("an equal head did not confirm")
	}
}
