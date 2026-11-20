package kolobraid

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	kolostore "github.com/ciranda-dev/kolo-store"
)

func TestSucceedCopiesTheSettledView(t *testing.T) {
	big := func(tx *Tx, e *Entry) error {
		if e.Kind == KindData && string(e.Body) == "big" {
			return tx.Put(bytes.Repeat([]byte{'k'}, MaxViewKey), bytes.Repeat([]byte{'v'}, MaxViewValue))
		}
		return appApply(tx, e)
	}
	old := newFounder(t, "old", GroupConfig{}, big)
	mustAppend(t, old.b, "a=1")
	mustAppend(t, old.b, "big")
	mustAppend(t, old.b, "b=2")
	waitUntil(t, 10*time.Second, func() bool { return quiet(old) && settledGet(t, old.b, "b") == "2" })
	p := old.b.view.Progress().Next - 1
	oldView := dump(t, old.b)

	db := kolostore.NewMemory()
	opts := testOptions(db, big)
	s, err := Succeed(context.Background(), old.id, old.b, GroupConfig{App: "test/v1"}, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	st := s.Status()
	if st.SeededFrom == nil || *st.SeededFrom != old.b.GroupID() {
		t.Fatal("SeededFrom not set")
	}
	if s.cont.Position != p || s.cont.Seeds < 2 {
		t.Fatalf("continuation %+v: the largest pair needs its own seed entry", s.cont)
	}
	waitUntil(t, 10*time.Second, func() bool { return s.view.Progress().Next > p+1 })
	if dump(t, s) != oldView {
		t.Fatal("seeded view differs from the old settled view")
	}
	mustAppend(t, s, "c=3")
	waitUntil(t, 10*time.Second, func() bool { return settledGet(t, s, "c") == "3" })
	for o, err := range s.Entries(0) {
		if err != nil {
			t.Fatal(err)
		}
		if o.Position <= p {
			t.Fatalf("successor position %d does not continue after %d", o.Position, p)
		}
		if o.Kind == KindSeed {
			t.Fatal("a seed entry took a position")
		}
	}
	// reopen and rebuild keep the seeds
	s.Close()
	opts.Rebuild = true
	s2, err := Open(context.Background(), old.id, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	waitUntil(t, 10*time.Second, func() bool { return settledGet(t, s2, "c") == "3" })
	if v := settledGet(t, s2, "a"); v != "1" {
		t.Fatalf("seed lost after rebuild: a=%s", v)
	}
}

func TestSucceedRefusals(t *testing.T) {
	nt, ps := group(t, 3, GroupConfig{}, appApply, false)
	nt.settle(nil, 5*time.Second, func() bool { return quiet(ps...) })
	db := kolostore.NewMemory()
	if _, err := Succeed(context.Background(), ps[2].id, ps[0].b, GroupConfig{App: "test/v1"}, testOptions(db, appApply)); !errors.Is(err, ErrNotAdmin) {
		t.Fatalf("non-admin successor: %v", err)
	}
	if _, err := Succeed(context.Background(), ps[0].id, nil, GroupConfig{App: "test/v1"}, testOptions(db, appApply)); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("nil old braid: %v", err)
	}
}

func TestBadSeedHalts(t *testing.T) {
	old := newFounder(t, "old", GroupConfig{}, appApply)
	mustAppend(t, old.b, "a=1")
	waitUntil(t, 5*time.Second, func() bool { return quiet(old) })
	db := kolostore.NewMemory()
	cont := &continuation{Group: old.b.GroupID(), Position: 3, Content: Digest{1}, Seeds: 1}
	s, err := create(context.Background(), old.id, GroupConfig{App: "test/v1"}, testOptions(db, appApply), cont,
		func(put func([]byte) error) error {
			body, _ := encodeBody(seedBody{Pairs: []seedPair{{Key: []byte("a"), Value: []byte("forged")}}}, genesisBounds())
			return put(body)
		})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ev := waitEvent(t, s, EventHalted, 5*time.Second)
	if !errors.Is(ev.Err, ErrBadSeed) || !errors.Is(s.Status().Halted, ErrBadSeed) {
		t.Fatalf("bad seed: %+v", ev)
	}
}

// TestSucceedStorageError: a storage error while the old settled view is read stops Succeed, so no successor covers a truncated
// view.
func TestSucceedStorageError(t *testing.T) {
	f := newVtFault(kolostore.NewMemory(), 0, false)
	old := &peer{t: t, name: "old", id: newIdentity(t), db: f}
	old.opts = testOptions(f, appApply)
	b, err := Create(context.Background(), old.id, GroupConfig{App: "test/v1"}, old.opts)
	if err != nil {
		t.Fatal(err)
	}
	old.b = b
	t.Cleanup(old.close)
	for i := range 20 {
		mustAppend(t, old.b, fmt.Sprintf("k%d=%d", i, i))
	}
	waitUntil(t, 10*time.Second, func() bool { return quiet(old) && settledGet(t, old.b, "k19") == "19" })
	// the settled pairs leave memory for the backend, where the scan then fails
	old.reopen()
	f.getErr.Store(true)
	s, err := Succeed(context.Background(), old.id, old.b, GroupConfig{App: "test/v1"}, testOptions(kolostore.NewMemory(), appApply))
	f.getErr.Store(false)
	if !errors.Is(err, errInjected) {
		if s != nil {
			s.Close()
		}
		t.Fatalf("Succeed over a failing scan: %v", err)
	}
}

// TestSuccessorCheckedByOldMember: a joiner that holds the old braid checks a successor's genesis against it: an honest
// successor passes; one whose founder was not an admin at the old position, or whose content digest is not the old view's
// there, or that names a position inside a settled batch, is refused; one past what the old braid settled is refused for now.
func TestSuccessorCheckedByOldMember(t *testing.T) {
	nt, ps := group(t, 3, GroupConfig{}, appApply, false)
	a, holder, plain := ps[0], ps[1], ps[2]
	for i := range 5 {
		mustAppend(t, a.b, fmt.Sprintf("x%d=%d", i, i))
	}
	nt.settle(nil, 20*time.Second, func() bool { return quiet(ps...) && settledGet(t, plain.b, "x4") == "4" })
	p := a.b.view.Progress().Next - 1
	snap := a.b.Settled()
	content := contentDigest(p, snap.Scan(nil, nil))
	snap.Close()
	successor := func(by *peer, cont *continuation) *peer {
		s := &peer{t: t, name: "s", id: by.id, db: kolostore.NewMemory()}
		s.opts = testOptions(s.db, appApply)
		var err error
		if cont == nil {
			s.b, err = Succeed(context.Background(), by.id, by.b, GroupConfig{App: "test/v1"}, s.opts)
		} else {
			s.b, err = create(context.Background(), by.id, GroupConfig{App: "test/v1"}, s.opts, cont, nil)
		}
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(s.close)
		return s
	}
	join := func(s *peer, old *Braid, stale ...uint64) error {
		j, nw := newJoiner(t, "j", s, appApply)
		j.opts.Predecessor = old
		// a replay to another position that a crash left in the scratch prefix
		for _, at := range stale {
			if _, _, err := old.stateAt(at, PrefixBackend(j.db, []byte{scratchPrefix})); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := s.b.AddMember(ctx10(t), nw); err != nil {
			t.Fatal(err)
		}
		b, err := open(context.Background(), j.id, j.opts, true)
		if err == nil {
			b.Close()
		}
		return err
	}
	// the holder settles past p before it joins: its old view is replayed to p
	honest := successor(a, nil)
	mustAppend(t, a.b, "later=1")
	nt.settle(nil, 20*time.Second, func() bool { return quiet(ps...) && settledGet(t, holder.b, "later") == "1" })
	if holder.b.view.Progress().Next <= p+1 {
		t.Fatal("the holder did not settle past the successor's position")
	}
	if err := join(honest, holder.b, holder.b.view.Progress().Next-1); err != nil {
		t.Fatalf("an honest successor: %v", err)
	}
	group := a.b.GroupID()
	if err := join(successor(plain, &continuation{Group: group, Position: p, Content: content}), holder.b); !errors.Is(err, ErrNotAdmin) {
		t.Fatalf("a successor by a member that was not an admin: %v", err)
	}
	other := contentDigest(p, func(func([]byte, []byte) bool) {})
	if err := join(successor(a, &continuation{Group: group, Position: p, Content: other}), holder.b); !errors.Is(err, ErrBadSeed) {
		t.Fatalf("a successor with another content digest: %v", err)
	}
	if err := join(successor(a, &continuation{Group: group, Position: p + 100, Content: content}), holder.b); !errors.Is(err, ErrNotFound) || errors.Is(err, ErrBadSeed) {
		t.Fatalf("a successor past what the old braid settled, which it may settle later: %v", err)
	}
	// a position inside a settled batch is no settled position anyone took
	mid := uint64(0)
	holder.b.mu.Lock()
	for i, prev := uint64(0), holder.b.view.cfg.Next; i < holder.b.pivots && mid == 0; i++ {
		rec, err := holder.b.pivotAt(i)
		if err != nil {
			holder.b.mu.Unlock()
			t.Fatal(err)
		}
		if rec.Next >= prev+2 {
			mid = rec.Next - 2
		}
		prev = rec.Next
	}
	holder.b.mu.Unlock()
	if mid == 0 {
		t.Fatal("no batch of two positions")
	}
	// with the content digest of the view there, so only the batch rule refuses it
	at, err := holder.b.SettledAt(ctx10(t), mid)
	if err != nil {
		t.Fatal(err)
	}
	midContent := contentDigest(mid, at.Scan(nil, nil))
	at.Close()
	if err := join(successor(a, &continuation{Group: group, Position: mid, Content: midContent}), holder.b); !errors.Is(err, ErrBadSeed) {
		t.Fatalf("a successor inside a settled batch: %v", err)
	}
}

// TestPredecessorReplayLeavesTheOldBraidRunning: replaying the old braid to a successor's position does not hold the old
// braid's lock, so it keeps answering while the replay's apply runs.
func TestPredecessorReplayLeavesTheOldBraidRunning(t *testing.T) {
	var block atomic.Bool
	entered, release := make(chan struct{}, 1), make(chan struct{})
	apply := func(tx *Tx, e *Entry) error {
		if block.Load() {
			select {
			case entered <- struct{}{}:
			default:
			}
			<-release
		}
		return appApply(tx, e)
	}
	nt, ps := group(t, 2, GroupConfig{}, apply, false)
	mustAppend(t, ps[0].b, "x=1")
	nt.settle(nil, 10*time.Second, func() bool { return quiet(ps...) && settledGet(t, ps[1].b, "x") == "1" })
	old := ps[1].b
	p := old.view.Progress().Next - 1
	block.Store(true)
	done := make(chan error, 1)
	go func() {
		_, _, err := old.stateAt(p, PrefixBackend(kolostore.NewMemory(), nil))
		done <- err
	}()
	<-entered
	got := make(chan Status, 1)
	go func() { got <- old.Status() }()
	select {
	case <-got:
	case <-time.After(5 * time.Second):
		t.Fatal("the old braid is locked during the replay")
	}
	block.Store(false)
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// TestPredecessorReplayDuringCatchUp: a catch-up that replaces the old braid's own strand log while a replay to a
// successor's position runs leaves the replay reading the new log, under the old braid's lock.
func TestPredecessorReplayDuringCatchUp(t *testing.T) {
	var block atomic.Bool
	entered, release := make(chan struct{}, 1), make(chan struct{})
	apply := func(tx *Tx, e *Entry) error {
		if block.CompareAndSwap(true, false) {
			entered <- struct{}{}
			<-release
		}
		return appApply(tx, e)
	}
	nt, ps := group(t, 2, GroupConfig{}, apply, false)
	a, w := ps[0], ps[1]
	mustAppend(t, a.b, "a=1")
	nt.settle(nil, 10*time.Second, func() bool { return quiet(a, w) && settledGet(t, w.b, "a") == "1" })
	mustAppend(t, w.b, "old=1")
	nt.settle(nil, 10*time.Second, func() bool { return quiet(a, w) && settledGet(t, w.b, "old") == "1" })
	backup := copyBackend(t, w.db)
	mustAppend(t, w.b, "new=2")
	nt.settle(nil, 10*time.Second, func() bool { return quiet(a, w) && settledGet(t, a.b, "new") == "2" })
	w.close()
	w.db, w.opts.Backend = backup, backup
	w.reopen()
	p := w.b.view.Progress().Next - 1
	pull(t, a, w, 0)
	rep, err := w.b.catchUpReplica()
	if err != nil {
		t.Fatal(err)
	}
	if !transfer(t, a.b.strandFor(w.b.meta.OwnStrand), rep, ^uint64(0)) {
		t.Fatal("nothing to catch up")
	}
	block.Store(true)
	done := make(chan error, 1)
	go func() {
		_, _, err := w.b.stateAt(p, PrefixBackend(kolostore.NewMemory(), nil))
		done <- err
	}()
	// the replay reads the batch of the own entry while the catch-up runs
	<-entered
	close(release)
	if err := w.b.adoptOwn(rep); err != nil {
		t.Fatal(err)
	}
	waitEvent(t, w.b, EventStrandCaughtUp, 5*time.Second)
	if err := <-done; err != nil {
		t.Fatalf("a replay across a catch-up: %v", err)
	}
}

// TestPredecessorReplayUsesTheScratchPrefix: the replay that checks a successor writes its view to the joiner's backend under
// the scratch prefix, not to memory, and clears it afterwards.
func TestPredecessorReplayUsesTheScratchPrefix(t *testing.T) {
	var block atomic.Bool
	entered, release := make(chan struct{}, 1), make(chan struct{})
	apply := func(tx *Tx, e *Entry) error {
		if block.CompareAndSwap(true, false) {
			entered <- struct{}{}
			<-release
		}
		return appApply(tx, e)
	}
	nt, ps := group(t, 2, GroupConfig{}, apply, false)
	a, holder := ps[0], ps[1]
	mustAppend(t, a.b, "x=1")
	nt.settle(nil, 10*time.Second, func() bool { return quiet(ps...) && settledGet(t, holder.b, "x") == "1" })
	s := &peer{t: t, name: "s", id: a.id, db: kolostore.NewMemory()}
	s.opts = testOptions(s.db, appApply)
	var err error
	if s.b, err = Succeed(context.Background(), a.id, a.b, GroupConfig{App: "test/v1"}, s.opts); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.close)
	j, nw := newJoiner(t, "j", s, appApply)
	j.opts.Predecessor = holder.b
	if _, err := s.b.AddMember(ctx10(t), nw); err != nil {
		t.Fatal(err)
	}
	scratch := PrefixBackend(j.db, []byte{scratchPrefix})
	block.Store(true)
	done := make(chan error, 1)
	go func() {
		b, err := open(context.Background(), j.id, j.opts, true)
		if err == nil {
			b.Close()
		}
		done <- err
	}()
	<-entered
	v, err := getOpt(scratch, []byte(progressKey))
	close(release)
	if err != nil || v == nil {
		t.Fatalf("no replay progress under the scratch prefix during the check: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if v, err := getOpt(scratch, []byte(progressKey)); err != nil || v != nil {
		t.Fatalf("the scratch prefix is not cleared after the check: %v", err)
	}
}
