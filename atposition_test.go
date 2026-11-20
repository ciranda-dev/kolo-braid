package kolobraid

import (
	"context"
	"errors"
	"runtime"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	kolostore "github.com/ciranda-dev/kolo-store"

	"github.com/ciranda-dev/kolo-braid/internal/settle"
)

// waitParked waits until a goroutine whose stack holds fn waits for a read lock.
func waitParked(t testing.TB, fn string) {
	t.Helper()
	waitUntil(t, 10*time.Second, func() bool {
		buf := make([]byte, 1<<20)
		buf = buf[:runtime.Stack(buf, true)]
		for _, g := range strings.Split(string(buf), "\n\n") {
			if strings.Contains(g, fn) && (strings.Contains(g, "RLock") || strings.Contains(g, "SemacquireRWMutexR")) {
				return true
			}
		}
		return false
	})
}

// founderAt returns a founder whose settled view holds x=1 and the position of that entry.
func founderAt(t testing.TB) (*peer, uint64) {
	t.Helper()
	a := newFounder(t, "a", GroupConfig{}, appApply)
	ref := mustAppend(t, a.b, "x=1")
	waitUntil(t, 10*time.Second, func() bool { return quiet(a) && settledGet(t, a.b, "x") == "1" })
	for _, o := range settledEntries(t, a.b) {
		if o.ID == ref.ID {
			return a, o.Position
		}
	}
	t.Fatal("entry not settled")
	return nil, 0
}

// TestCloseDuringAReadReportsClosed: a Get or a Scan that passed its closed check when Close deletes the snapshot's storage
// returns ErrClosed, never "not found" or a short range.
func TestCloseDuringAReadReportsClosed(t *testing.T) {
	for _, c := range []struct {
		name, fn string
		read     func(s *Snapshot) error
	}{
		{"Get", "(*Snapshot).Get", func(s *Snapshot) error { _, err := s.Get([]byte("x")); return err }},
		{"Scan", "(*scanner).window", func(s *Snapshot) error {
			for range s.Scan(nil, nil) {
			}
			return s.Err()
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			a, at := founderAt(t)
			s := settledAt(t, a.b, at)
			s.v.mu.Lock()
			res := make(chan error, 1)
			go func() { res <- c.read(s) }()
			waitParked(t, c.fn)
			// what Close does while the read waits: mark the snapshot closed and delete its storage
			s.closed.Store(true)
			if err := clearPrefix(a.b.db, []byte(keySettledAt)); err != nil {
				t.Fatal(err)
			}
			s.v.mu.Unlock()
			if err := <-res; !errors.Is(err, ErrClosed) {
				t.Fatalf("read across a Close: %v, want ErrClosed", err)
			}
			s.v.release(s.h)
			s.done() // the rest of what Close does
		})
	}
}

// TestBraidCloseClosesSettledAtSnapshots: Close of the braid closes its SettledAt snapshots, so a braid opened again on the
// same prefix never shares scratch storage with them.
func TestBraidCloseClosesSettledAtSnapshots(t *testing.T) {
	a, at := founderAt(t)
	s, err := a.b.SettledAt(ctx10(t), at)
	if err != nil {
		t.Fatal(err)
	}
	old := a.b
	a.close()
	if n := scratchKeys(t, old, keySettledAt) + scratchKeys(t, old, keyReplayCursor); n != 0 {
		t.Fatalf("Close of the braid left %d scratch keys", n)
	}
	a.reopen()
	if _, err := s.Get([]byte("x")); !errors.Is(err, ErrClosed) {
		t.Fatalf("snapshot of a closed braid: %v, want ErrClosed", err)
	}
	s2 := settledAt(t, a.b, at)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if get(t, s2, "x") != "1" {
		t.Fatal("closing the old braid's snapshot changed the new braid's")
	}
}

// TestSettledRosterIsTheViewsRoster: a settled snapshot taken while a batch of more than one durable commit is applied pairs
// the view with the roster of the same position: the member an AddMember earlier in the batch added is in it.
func TestSettledRosterIsTheViewsRoster(t *testing.T) {
	var arm atomic.Bool
	entered, release := make(chan struct{}, 1), make(chan struct{})
	blocking := func(tx *Tx, e *Entry) error {
		if string(e.Body) == "block" && arm.CompareAndSwap(true, false) {
			entered <- struct{}{}
			<-release
		}
		return appApply(tx, e)
	}
	// the only seat fetches far ahead, so one pulse of it settles the writer's whole strand in one batch; it applies only
	// MaxTail entries of it tentatively, so it reaches the blocking entry only when it settles it
	lim := fastLimits()
	lim.MaxPullAhead = 4096
	a := &peer{t: t, name: "a", id: newIdentity(t), db: kolostore.NewMemory()}
	a.opts = testOptions(a.db, blocking)
	a.opts.Limits = lim
	var err error
	if a.b, err = Create(context.Background(), a.id, GroupConfig{App: "test/v1"}, a.opts); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
		a.close()
	})
	w, nw := newJoiner(t, "w", a, appApply)
	nw.Roles = RoleAdmin
	if _, err := a.b.AddMember(ctx10(t), nw); err != nil {
		t.Fatal(err)
	}
	w.start()
	nt := newNet(t, a, w)
	nt.settle(nil, 20*time.Second, func() bool { return quiet(a, w) && allActive([]*peer{a, w}, 2) && !w.b.Status().Unconfirmed })
	_, nj := newJoiner(t, "j", a, appApply)
	if _, err := w.b.AddMember(ctx10(t), nj); err != nil {
		t.Fatal(err)
	}
	for i := range chunkEntries + 40 {
		mustAppend(t, w.b, "d="+string(rune('a'+i%26)))
	}
	mustAppend(t, w.b, "block")
	arm.Store(true)
	done := make(chan struct{})
	go func() {
		defer close(done)
		deadline := time.Now().Add(30 * time.Second)
		for len(entered) == 0 && arm.Load() && time.Now().Before(deadline) {
			nt.round(nil, 0)
		}
	}()
	select {
	case <-entered:
	case <-time.After(30 * time.Second):
		t.Fatal("the settled batch did not reach the blocking entry")
	}
	<-done
	s := a.b.Settled()
	defer s.Close()
	p := s.Position()
	if batchEnd(t, a.b, p) {
		t.Fatalf("the snapshot at %d is at a batch end: no commit inside a batch", p)
	}
	at := settledAt(t, a.b, p)
	if rosterDump(s) != rosterDump(at) || snapDump(s) != snapDump(at) {
		t.Fatalf("settled snapshot at %d:\n%s\nSettledAt:\n%s", p, rosterDump(s), rosterDump(at))
	}
	if _, ok := hasKey(s, nj.Key); !ok {
		t.Fatal("the member the batch added before the snapshot's position is not in its roster")
	}
	close(release)
}

func hasKey(s *Snapshot, k []byte) (Member, bool) {
	for _, m := range s.Members() {
		if string(m.Key) == string(k) {
			return m, true
		}
	}
	return Member{}, false
}

// TestCommitPublishesTheRoster: every durable commit of the view publishes the roster of its last entry, so a settled snapshot
// inside a batch, and in the first batch under a new settler config, holds that entry's roster and config.
func TestCommitPublishesTheRoster(t *testing.T) {
	cfg := vtConfig()
	cfg.chunk = 2
	v := vtOpen(t, kolostore.NewMemory(), cfg, vtApply)
	es := vtScript(5, 9)
	rv := func(n uint64) *rosterView {
		return &rosterView{idx: map[string]int{}, cfg: settle.Config{Number: n, Rule: settle.Majority, Seats: []settle.Seat{{}}}}
	}
	var seen []uint64
	steps := func(yield func(applyStep, error) bool) {
		for i, e := range es {
			s := v.Settled()
			seen = append(seen, s.Settlers().Config)
			s.Close()
			st := vtStep(e)
			st.Roster = rv(uint64(10 + i))
			if !yield(st, nil) {
				return
			}
		}
	}
	if _, err := v.Settle(0, steps); err != nil {
		t.Fatal(err)
	}
	// before entry i the view has committed the entries before the last multiple of the chunk
	for i, got := range seen {
		var want uint64
		if n := i / 2 * 2; n > 0 {
			want = uint64(10 + n - 1)
		}
		if got != want {
			t.Fatalf("before entry %d: settled config %d, want %d (%v)", i, got, want, seen)
		}
	}
	s := v.Settled()
	defer s.Close()
	if got := s.Settlers().Config; got != uint64(10+len(es)-1) {
		t.Fatalf("after the batch: config %d", got)
	}
}

// TestRestartOfAHaltedViewKeepsItsRoster: after Open, a view that halted keeps the roster of its own position, not that of
// the batches settled after the halt.
func TestRestartOfAHaltedViewKeepsItsRoster(t *testing.T) {
	a := newFounder(t, "a", GroupConfig{}, appApply)
	mustAppend(t, a.b, "x=1")
	mustAppend(t, a.b, "fail")
	waitUntil(t, 10*time.Second, func() bool { return a.b.Status().Halted != nil })
	_, nw := newJoiner(t, "j", a, appApply)
	ref, err := a.b.AddMember(ctx10(t), nw)
	if err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 10*time.Second, func() bool {
		for _, o := range settledEntries(t, a.b) {
			if o.ID == ref.ID {
				return true
			}
		}
		return false
	})
	for range 2 {
		s := a.b.Settled()
		if _, ok := hasKey(s, keyOf(a)); !ok {
			t.Fatalf("the halted view's roster lacks the founder: %s", rosterDump(s))
		}
		_, in := hasKey(s, nw.Key)
		live := rosterDump(s)
		s.Close()
		if in {
			t.Fatalf("the halted view's roster holds a member added after the halt: %s", live)
		}
		a.reopen()
	}
}

// TestSettledAtStopsWhenTheContextEnds: a context that ends while the replay applies a batch ends SettledAt with its error at
// once, also when the entry that ended it is the last one to apply.
func TestSettledAtStopsWhenTheContextEnds(t *testing.T) {
	var cancel atomic.Pointer[context.CancelFunc]
	var after atomic.Int64
	apply := func(tx *Tx, e *Entry) error {
		if c := cancel.Load(); c != nil {
			switch string(e.Body) {
			case "stop":
				(*c)()
			case "after":
				after.Add(1)
			}
		}
		return appApply(tx, e)
	}
	a := newFounder(t, "a", GroupConfig{}, apply)
	refs := []EntryRef{mustAppend(t, a.b, "stop"), mustAppend(t, a.b, "after"), mustAppend(t, a.b, "after")}
	waitUntil(t, 10*time.Second, func() bool { return quiet(a) && settledGet(t, a.b, "#") == "3" })
	pos := map[EntryID]uint64{}
	for _, o := range settledEntries(t, a.b) {
		pos[o.ID] = o.Position
	}
	for _, c := range []struct {
		name string
		at   uint64
	}{
		{"the stopping entry last", pos[refs[0].ID]},
		{"entries after it", pos[refs[2].ID]},
	} {
		t.Run(c.name, func(t *testing.T) {
			if batchEnd(t, a.b, pos[refs[0].ID]) && c.at != pos[refs[0].ID] {
				t.Skip("the entries after the stopping one are in another batch")
			}
			ctx, stop := context.WithCancel(context.Background())
			defer stop()
			cancel.Store(&stop)
			defer cancel.Store(nil)
			after.Store(0)
			if s, err := a.b.SettledAt(ctx, c.at); !errors.Is(err, context.Canceled) {
				if s != nil {
					s.Close()
				}
				t.Fatalf("SettledAt after its context ended: %v", err)
			}
			if n := after.Load(); n != 0 {
				t.Fatalf("%d entries applied after the context ended", n)
			}
			if n := scratchKeys(t, a.b, keySettledAt); n != 0 {
				t.Fatalf("%d scratch keys left", n)
			}
		})
	}
}

// TestBraidCloseWaitsForSettledAt: Close waits for a SettledAt replay in flight, which ends with ErrClosed, ends one that waits
// for its turn, and leaves no scratch storage.
func TestBraidCloseWaitsForSettledAt(t *testing.T) {
	var arm atomic.Bool
	entered, release := make(chan struct{}, 1), make(chan struct{})
	apply := func(tx *Tx, e *Entry) error {
		if string(e.Body) == "x=1" && arm.CompareAndSwap(true, false) {
			entered <- struct{}{}
			<-release
		}
		return appApply(tx, e)
	}
	a := newFounder(t, "a", GroupConfig{}, apply)
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	ref := mustAppend(t, a.b, "x=1")
	waitUntil(t, 10*time.Second, func() bool { return quiet(a) && settledGet(t, a.b, "x") == "1" })
	var at uint64
	for _, o := range settledEntries(t, a.b) {
		if o.ID == ref.ID {
			at = o.Position
		}
	}
	b := a.b
	arm.Store(true)
	first, second := make(chan error, 1), make(chan error, 1)
	go func() {
		s, err := b.SettledAt(context.Background(), at)
		if s != nil {
			s.Close()
		}
		first <- err
	}()
	<-entered
	go func() {
		s, err := b.SettledAt(context.Background(), at)
		if s != nil {
			s.Close()
		}
		second <- err
	}()
	// the second call waits for its turn before Close starts
	waitUntil(t, 10*time.Second, func() bool {
		buf := make([]byte, 1<<20)
		buf = buf[:runtime.Stack(buf, true)]
		for _, g := range strings.Split(string(buf), "\n\n") {
			if strings.Contains(g, "(*Braid).SettledAt") && strings.Contains(g, "[select") {
				return true
			}
		}
		return false
	})
	closed := make(chan error, 1)
	go func() { closed <- b.Close() }()
	waitUntil(t, 10*time.Second, func() bool { return len(second) > 0 })
	if err := <-second; !errors.Is(err, ErrClosed) {
		t.Fatalf("a call waiting for its turn: %v", err)
	}
	select {
	case err := <-closed:
		t.Fatalf("Close returned during a replay: %v", err)
	default:
	}
	select {
	case <-release:
	default:
		close(release)
	}
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	if err := <-first; !errors.Is(err, ErrClosed) {
		t.Fatalf("the replay in flight: %v", err)
	}
	if n := scratchKeys(t, b, keySettledAt) + scratchKeys(t, b, keyReplayCursor); n != 0 {
		t.Fatalf("%d scratch keys after Close", n)
	}
	a.b = nil
}

// TestLeakedSettledAtSnapshotIsDeleted: the storage of a SettledAt snapshot that is dropped without Close is deleted once it is
// collected.
func TestLeakedSettledAtSnapshotIsDeleted(t *testing.T) {
	a, at := founderAt(t)
	func() {
		s, err := a.b.SettledAt(ctx10(t), at)
		if err != nil {
			t.Fatal(err)
		}
		if get(t, s, "x") != "1" {
			t.Fatal("no replay")
		}
	}()
	waitUntil(t, 10*time.Second, func() bool {
		runtime.GC()
		return scratchKeys(t, a.b, keySettledAt) == 0
	})
}

// TestSettledAtReplaysOnlyWhatFollows: a SettledAt after another one at an earlier position replays only the positions after
// the batch end the first one reached.
func TestSettledAtReplaysOnlyWhatFollows(t *testing.T) {
	var counting atomic.Bool
	var calls atomic.Int64
	apply := func(tx *Tx, e *Entry) error {
		if counting.Load() && e.Kind == KindData {
			calls.Add(1)
		}
		return appApply(tx, e)
	}
	a := newFounder(t, "a", GroupConfig{}, apply)
	var refs []EntryRef
	for i := range 30 {
		refs = append(refs, mustAppend(t, a.b, "k="+string(rune('a'+i%26))))
		waitUntil(t, 10*time.Second, func() bool { return quiet(a) && settledGet(t, a.b, "#") == itoa(i+1) })
	}
	pos := map[EntryID]uint64{}
	for _, o := range settledEntries(t, a.b) {
		pos[o.ID] = o.Position
	}
	// the end of the batch that holds entry 19
	end := pos[refs[19].ID]
	for !batchEnd(t, a.b, end) {
		end++
	}
	if end >= pos[refs[20].ID] {
		t.Fatal("entries 19 and 20 share a batch")
	}
	settledAt(t, a.b, end).Close()
	counting.Store(true)
	s := settledAt(t, a.b, pos[refs[29].ID])
	counting.Store(false)
	if get(t, s, "#") != "30" {
		t.Fatalf("# = %q", get(t, s, "#"))
	}
	if n := calls.Load(); n != 10 {
		t.Fatalf("the second SettledAt applied %d data entries, want the 10 after the first one's position", n)
	}
	// an earlier position after a later one replays from the start
	if s := settledAt(t, a.b, pos[refs[4].ID]); get(t, s, "#") != "5" {
		t.Fatalf("earlier position: # = %q", get(t, s, "#"))
	}
}

func itoa(n int) string {
	var b []byte
	for {
		b = append([]byte{byte('0' + n%10)}, b...)
		if n /= 10; n == 0 {
			return string(b)
		}
	}
}

// TestViewRosterFollowsTheTail: a tentative snapshot taken while the tail is applied holds the roster of its last applied
// entry: an AddMember already in the tail is in it.
func TestViewRosterFollowsTheTail(t *testing.T) {
	var arm atomic.Bool
	entered, release := make(chan struct{}, 1), make(chan struct{})
	apply := func(tx *Tx, e *Entry) error {
		if string(e.Body) == "block" && arm.CompareAndSwap(true, false) {
			entered <- struct{}{}
			<-release
		}
		return appApply(tx, e)
	}
	_, ps := group(t, 2, GroupConfig{}, apply, true)
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	a := ps[0]
	_, nw := newJoiner(t, "j", a, apply)
	arm.Store(true)
	// nothing settles without the other seat: both entries stay tentative
	if _, err := a.b.AddMember(ctx10(t), nw); err != nil {
		t.Fatal(err)
	}
	mustAppend(t, a.b, "block")
	select {
	case <-entered:
	case <-time.After(30 * time.Second):
		t.Fatal("the tail did not reach the blocking entry")
	}
	s := a.b.View()
	defer s.Close()
	if _, ok := hasKey(s, nw.Key); !ok {
		t.Fatal("the member an applied tentative entry added is not in the tentative roster")
	}
	if st := s.Settlers(); st.Config != a.b.Status().Config {
		t.Fatalf("tentative settlers %+v", st)
	}
	close(release)
}

// TestRunningDigestUnchanged: the running digest of a fixed history, with and without library writes and at every chunk size,
// is the one this package computed before snapshots carried rosters.
func TestRunningDigestUnchanged(t *testing.T) {
	for _, c := range []struct {
		chunk int
		lib   bool
		want  string
	}{
		{1, false, "15c0b7c94c93024a8bb67315c3b686d8d80d2a48fbc56976e1de1977c093e017"},
		{7, true, "e1bd27afdffa9f0be6c5bf9127615b709ec16b957410d6378f277857136ff4f5"},
		{256, false, "15c0b7c94c93024a8bb67315c3b686d8d80d2a48fbc56976e1de1977c093e017"},
	} {
		cfg := vtConfig()
		cfg.chunk = c.chunk
		v := vtOpen(t, kolostore.NewMemory(), cfg, vtApply)
		es := vtScript(4242, 600)
		var last Digest
		for i := 0; i < len(es); i += 50 {
			steps := vtSteps(es[i:min(i+50, len(es))])
			if c.lib {
				steps = libSteps(es[i:min(i+50, len(es))], i)
			}
			res, err := v.Settle(uint64(i/50), steps)
			if err != nil {
				t.Fatal(err)
			}
			last = res.Progress.Digest
		}
		if last.String() != c.want {
			t.Fatalf("chunk %d lib %v: digest %s, want %s", c.chunk, c.lib, last, c.want)
		}
	}
}

// hookBackend calls commit before each batch commit, with whether the batch writes or deletes keys that start with hooked, and
// records the most bytes one scan from a key that starts with scanned delivered.
type hookBackend struct {
	kolostore.Backend
	hooked, scanned string
	commit          *atomic.Pointer[func(put, del bool)]
	most            *atomic.Int64
}

func newHookBackend(hooked, scanned string) hookBackend {
	return hookBackend{Backend: kolostore.NewMemory(), hooked: hooked, scanned: scanned,
		commit: new(atomic.Pointer[func(put, del bool)]), most: new(atomic.Int64)}
}

func (h hookBackend) Scan(start, end []byte, reverse bool, limit int, fn func(k, v []byte) bool) error {
	if h.scanned == "" || !strings.HasPrefix(string(start), h.scanned) {
		return h.Backend.Scan(start, end, reverse, limit, fn)
	}
	// each pair is handed over in a buffer that is overwritten once fn returns, as an iterator reuses its buffers
	n := int64(0)
	err := h.Backend.Scan(start, end, reverse, limit, func(k, v []byte) bool {
		n += int64(len(k) + len(v))
		kb, vb := slices.Clone(k), slices.Clone(v)
		more := fn(kb, vb)
		for i := range kb {
			kb[i] = 0xff
		}
		for i := range vb {
			vb[i] = 0xff
		}
		return more
	})
	for {
		if m := h.most.Load(); n <= m || h.most.CompareAndSwap(m, n) {
			return err
		}
	}
}

func (h hookBackend) NewBatch() kolostore.Batch { return &hookBatch{Batch: h.Backend.NewBatch(), h: h} }

type hookBatch struct {
	kolostore.Batch
	h        hookBackend
	put, del bool
}

func (b *hookBatch) Put(k, v []byte) {
	b.put = b.put || strings.HasPrefix(string(k), b.h.hooked)
	b.Batch.Put(k, v)
}

func (b *hookBatch) DeleteRange(start, end []byte) {
	b.del = b.del || strings.HasPrefix(string(start), b.h.hooked)
	b.Batch.DeleteRange(start, end)
}

func (b *hookBatch) Commit() error {
	if f := b.h.commit.Load(); f != nil && (b.put || b.del) {
		(*f)(b.put, b.del)
	}
	return b.Batch.Commit()
}

// founderOn is newFounder on db; it returns the position of an entry that put x=1, settled.
func founderOn(t testing.TB, db kolostore.Backend, apply ApplyFunc, bodies ...string) (*peer, []uint64) {
	t.Helper()
	a := &peer{t: t, name: "a", id: newIdentity(t), db: db}
	a.opts = testOptions(db, apply)
	var err error
	if a.b, err = Create(context.Background(), a.id, GroupConfig{App: "test/v1"}, a.opts); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(a.close)
	var refs []EntryRef
	for _, body := range bodies {
		refs = append(refs, mustAppend(t, a.b, body))
	}
	waitUntil(t, 10*time.Second, func() bool { return quiet(a) && a.b.Status().Settled > 1 && settledGet(t, a.b, "end") == "1" })
	pos := map[EntryID]uint64{}
	for _, o := range settledEntries(t, a.b) {
		pos[o.ID] = o.Position
	}
	out := make([]uint64, len(refs))
	for i, r := range refs {
		out[i] = pos[r.ID]
	}
	return a, out
}

// TestBraidCloseWaitsForASnapshotsDeletion: Close of the braid returns only after the deletion of a snapshot's storage that
// the snapshot's own Close started, so a backend closed after it is never written to.
func TestBraidCloseWaitsForASnapshotsDeletion(t *testing.T) {
	h := newHookBackend(keySettledAt, "")
	a, at := founderOn(t, h, appApply, "x=1", "end=1")
	s, err := a.b.SettledAt(ctx10(t), at[0])
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}, 1), make(chan struct{})
	var once atomic.Bool
	park := func(put, del bool) {
		if del && once.CompareAndSwap(false, true) {
			entered <- struct{}{}
			<-release
		}
	}
	h.commit.Store(&park)
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	go s.Close()
	<-entered
	b := a.b
	closed := make(chan error, 1)
	go func() { closed <- b.Close() }()
	waitUntil(t, 10*time.Second, func() bool {
		buf := make([]byte, 1<<20)
		buf = buf[:runtime.Stack(buf, true)]
		return strings.Contains(string(buf), "(*Braid).closeSettledAt")
	})
	select {
	case err := <-closed:
		t.Fatalf("Close returned while a snapshot's storage was being deleted: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	if n := scratchKeys(t, b, keySettledAt); n != 0 {
		t.Fatalf("%d scratch keys after Close", n)
	}
	a.b = nil
}

// TestBraidCloseWaitsForASnapshotsClose: Close of the braid also waits for a snapshot's Close that took the snapshot but has
// not started deleting its storage yet.
func TestBraidCloseWaitsForASnapshotsClose(t *testing.T) {
	a, at := founderOn(t, kolostore.NewMemory(), appApply, "x=1", "end=1")
	s, err := a.b.SettledAt(ctx10(t), at[0])
	if err != nil {
		t.Fatal(err)
	}
	s.v.mu.Lock()
	unlocked := false
	t.Cleanup(func() {
		if !unlocked {
			s.v.mu.Unlock()
		}
	})
	go s.Close()
	waitUntil(t, 10*time.Second, func() bool {
		buf := make([]byte, 1<<20)
		buf = buf[:runtime.Stack(buf, true)]
		return strings.Contains(string(buf), "(*view).release")
	})
	b := a.b
	closed := make(chan error, 1)
	go func() { closed <- b.Close() }()
	waitUntil(t, 10*time.Second, func() bool {
		buf := make([]byte, 1<<20)
		buf = buf[:runtime.Stack(buf, true)]
		return strings.Contains(string(buf), "(*Braid).closeSettledAt") || len(closed) > 0
	})
	select {
	case err := <-closed:
		t.Fatalf("Close returned before a snapshot's Close deleted its storage: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	unlocked = true
	s.v.mu.Unlock()
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	if n := scratchKeys(t, b, keySettledAt); n != 0 {
		t.Fatalf("%d scratch keys after Close", n)
	}
	a.b = nil
}

// TestSettledAtCopyIsPaged: the copy of the kept replay's view reads at most one page of bytes at a time, also when the view
// holds values of the largest size.
func TestSettledAtCopyIsPaged(t *testing.T) {
	big := func(tx *Tx, e *Entry) error {
		if k, ok := strings.CutPrefix(string(e.Body), "big"); ok && e.Kind == KindData {
			return tx.Put([]byte("big/"+k), make([]byte, MaxViewValue))
		}
		return appApply(tx, e)
	}
	bodies := []string{}
	for i := range 6 {
		bodies = append(bodies, "big"+itoa(i))
	}
	h := newHookBackend("", keyReplayCursor)
	a, at := founderOn(t, h, big, append(bodies, "end=1")...)
	end := at[len(at)-1]
	for !batchEnd(t, a.b, end) {
		end++
	}
	settledAt(t, a.b, end).Close() // the kept replay now holds every large value: the next call copies them
	h.most.Store(0)
	s := settledAt(t, a.b, end)
	n := 0
	for _, v := range s.Scan([]byte("big/"), []byte("big0")) {
		if len(v) != MaxViewValue || slices.ContainsFunc(v, func(c byte) bool { return c != 0 }) {
			t.Fatal("a copied value differs")
		}
		n++
	}
	if n != 6 {
		t.Fatalf("%d large values in the snapshot", n)
	}
	if most := h.most.Load(); most == 0 || most > pageBytes+MaxViewValue+MaxViewKey {
		t.Fatalf("one scan of the copy read %d bytes, a page is %d", most, pageBytes)
	}
}

// TestSettledAtCopyStopsWhenTheContextEnds: a context that ends while the kept replay's view is copied ends SettledAt with its
// error after the page being copied, also when no entry is left to replay.
func TestSettledAtCopyStopsWhenTheContextEnds(t *testing.T) {
	big := func(tx *Tx, e *Entry) error {
		if k, ok := strings.CutPrefix(string(e.Body), "big"); ok && e.Kind == KindData {
			return tx.Put([]byte("big/"+k), make([]byte, MaxViewValue))
		}
		return appApply(tx, e)
	}
	h := newHookBackend(keySettledAt, "")
	a, at := founderOn(t, h, big, "big0", "big1", "big2", "big3", "end=1")
	end := at[len(at)-1]
	for !batchEnd(t, a.b, end) {
		end++
	}
	settledAt(t, a.b, end).Close() // the kept replay now ends at end: the next call only copies, a page per large value
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var pages atomic.Int64
	stop := func(put, del bool) {
		if put && pages.Add(1) == 1 {
			cancel()
		}
	}
	h.commit.Store(&stop)
	s, err := a.b.SettledAt(ctx, end)
	h.commit.Store(nil)
	if s != nil {
		s.Close()
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("SettledAt after its context ended during the copy: %v", err)
	}
	if n := pages.Load(); n != 1 {
		t.Fatalf("%d pages copied, the context ended at the first", n)
	}
	if n := scratchKeys(t, a.b, keySettledAt); n != 0 {
		t.Fatalf("%d scratch keys left", n)
	}
}
