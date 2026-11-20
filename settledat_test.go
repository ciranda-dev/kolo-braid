package kolobraid

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	kolostore "github.com/ciranda-dev/kolo-store"

	"github.com/ciranda-dev/kolo-braid/internal/roster"
	"github.com/ciranda-dev/kolo-braid/internal/settle"
)

// settledEntries returns the settled prefix of b in order.
func settledEntries(t testing.TB, b *Braid) []Ordered {
	t.Helper()
	var out []Ordered
	for o, err := range b.Entries(0) {
		if err != nil {
			t.Fatal(err)
		}
		if o.Settled {
			out = append(out, o)
		}
	}
	return out
}

func settledAt(t testing.TB, b *Braid, p uint64) *Snapshot {
	t.Helper()
	s, err := b.SettledAt(ctx10(t), p)
	if err != nil {
		t.Fatalf("SettledAt(%d): %v", p, err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func snapDump(s *Snapshot) string {
	var sb strings.Builder
	for k, v := range s.Scan(nil, nil) {
		fmt.Fprintf(&sb, "%q=%q;", k, v)
	}
	return sb.String()
}

func rosterDump(s *Snapshot) string {
	var sb strings.Builder
	for _, m := range s.Members() {
		fmt.Fprintf(&sb, "%x/%x/%d/%v;", m.Key[:4], m.Strand[:4], m.Roles, m.Seat)
	}
	st := s.Settlers()
	fmt.Fprintf(&sb, "config %d rule %d q %d:", st.Config, st.Rule, st.Quorum)
	for _, m := range st.Seats {
		fmt.Fprintf(&sb, "%x/%x/%d;", m.Key[:4], m.Strand[:4], m.Roles)
	}
	return sb.String()
}

// TestSettledAtReplaysEveryPosition: the snapshot at each settled position, inside a batch or at its end, holds the view that
// the entries up to it give, by a model of appApply.
func TestSettledAtReplaysEveryPosition(t *testing.T) {
	a := newFounder(t, "a", GroupConfig{}, appApply)
	bodies := []string{"a=1", "b=2", "!", "a=3", "-b", "c=4", "!", "b=5", "a=6", "d", "-a", "c=7"}
	for _, body := range bodies {
		mustAppend(t, a.b, body)
	}
	waitUntil(t, 10*time.Second, func() bool { return quiet(a) && settledGet(t, a.b, "c") == "7" })
	model, count := map[string]string{}, 0
	es := settledEntries(t, a.b)
	inBatch := false
	for _, o := range es {
		if o.Kind == KindData && !o.Void && !o.Rejected {
			body := string(o.Body)
			switch {
			case strings.HasPrefix(body, "-"):
				delete(model, body[1:])
			default:
				k, v, _ := strings.Cut(body, "=")
				model[k] = v
			}
			count++
		}
		s := settledAt(t, a.b, o.Position)
		if s.Position() != o.Position {
			t.Fatalf("Position %d, want %d", s.Position(), o.Position)
		}
		for _, k := range []string{"a", "b", "c", "d"} {
			want, ok := model[k]
			if !ok {
				want = "<none>"
			}
			if got := get(t, s, k); got != want {
				t.Fatalf("position %d: %s = %q, want %q", o.Position, k, got, want)
			}
		}
		if want := fmt.Sprint(count); count > 0 && get(t, s, "#") != want {
			t.Fatalf("position %d: # = %q, want %q", o.Position, get(t, s, "#"), want)
		}
		if ms := s.Members(); len(ms) != 1 || !bytes32Equal(ms[0].Key, keyOf(a)) || !ms[0].Seat {
			t.Fatalf("position %d: roster %s", o.Position, rosterDump(s))
		}
		if st := s.Settlers(); st.Config != 0 || st.Quorum != 1 || len(st.Seats) != 1 {
			t.Fatalf("position %d: settlers %+v", o.Position, st)
		}
		if err := s.Err(); err != nil {
			t.Fatal(err)
		}
		s.Close()
		inBatch = inBatch || !batchEnd(t, a.b, o.Position)
	}
	if !inBatch {
		t.Fatal("no position inside a batch was replayed")
	}
	// from the last position down: each call is earlier than the replay kept by the one before
	want := map[uint64]string{}
	for _, o := range es {
		s := settledAt(t, a.b, o.Position)
		want[o.Position] = snapDump(s) + rosterDump(s)
		s.Close()
	}
	for i := len(es) - 1; i >= 0; i-- {
		p := es[i].Position
		s := settledAt(t, a.b, p)
		if s.Position() != p || snapDump(s)+rosterDump(s) != want[p] {
			t.Fatalf("position %d after a later one: snapshot at %d differs", p, s.Position())
		}
		s.Close()
	}
	waitUntil(t, 10*time.Second, func() bool { return a.b.view.Progress().Next > es[len(es)-1].Position })
	live := a.b.Settled()
	defer live.Close()
	last := settledAt(t, a.b, live.Position())
	if snapDump(last) != snapDump(live) || rosterDump(last) != rosterDump(live) || live.Position() != last.Position() {
		t.Fatal("the snapshot at the last settled position differs from the settled view")
	}
}

func bytes32Equal(a, b ed25519.PublicKey) bool { return string(a) == string(b) }

// membersOf2 describes the members of a roster.
func membersOf2(r *roster.Roster) string {
	var sb strings.Builder
	for _, m := range r.Members() {
		fmt.Fprintf(&sb, "%x/%d;", m.Key[:4], m.Roles)
	}
	return sb.String()
}

// batchEnd reports whether p is the last position of a stored batch.
func batchEnd(t testing.TB, b *Braid, p uint64) bool {
	t.Helper()
	b.mu.Lock()
	n := b.pivots
	b.mu.Unlock()
	for i := uint64(0); i < n; i++ {
		rec, err := b.pivotAt(i)
		if err != nil {
			t.Fatal(err)
		}
		if rec.Next == p+1 {
			return true
		}
	}
	return false
}

// TestSettledAtRoster: the roster at a position names the members added before it, with their roles, and not those removed
// before it; every peer gives the same snapshot for the same position.
func TestSettledAtRoster(t *testing.T) {
	nt, ps := group(t, 3, GroupConfig{}, appApply, false)
	a, adm, w := ps[0], ps[1], ps[2]
	mustAppend(t, w.b, "w=1")
	nt.settle(nil, 10*time.Second, func() bool { return quiet(ps...) && settledGet(t, a.b, "w") == "1" })
	if _, err := a.b.RemoveMember(ctx10(t), keyOf(w)); err != nil {
		t.Fatal(err)
	}
	mustAppend(t, a.b, "after=1")
	nt.settle(nil, 10*time.Second, func() bool { return quiet(a, adm) && settledGet(t, adm.b, "after") == "1" })
	var added, removed []uint64
	for _, o := range settledEntries(t, a.b) {
		switch o.Kind {
		case KindAddMember:
			added = append(added, o.Position)
		case KindRemoveMember:
			removed = append(removed, o.Position)
		}
	}
	if len(added) != 2 || len(removed) != 1 {
		t.Fatalf("added at %v, removed at %v", added, removed)
	}
	has := func(s *Snapshot, k ed25519.PublicKey) (Member, bool) {
		for _, m := range s.Members() {
			if bytes32Equal(m.Key, k) {
				return m, true
			}
		}
		return Member{}, false
	}
	for _, c := range []struct {
		name   string
		pos    uint64
		member *peer
		in     bool
		roles  Role
	}{
		{"before the admin is added", added[0] - 1, adm, false, 0},
		{"at the admin's AddMember", added[0], adm, true, RoleAdmin},
		{"before the writer is added", added[1] - 1, w, false, 0},
		{"at the writer's AddMember", added[1], w, true, 0},
		{"before the removal", removed[0] - 1, w, true, 0},
		{"at the removal", removed[0], w, false, 0},
	} {
		a.b.mu.Lock()
		var batch uint64
		var before string
		if cur := a.b.at.cursor; cur != nil {
			batch, before = cur.batch, membersOf2(cur.r)
		}
		a.b.mu.Unlock()
		s := settledAt(t, a.b, c.pos)
		// a replay into a copy never changes the roster of the replay kept
		if cur := a.b.at.cursor; cur != nil && cur.batch == batch && before != "" && membersOf2(cur.r) != before {
			t.Fatalf("%s: the kept replay's roster changed", c.name)
		}
		m, in := has(s, keyOf(c.member))
		if in != c.in || m.Roles != c.roles {
			t.Fatalf("%s (position %d): member %v roles %d, want %v %d", c.name, c.pos, in, m.Roles, c.in, c.roles)
		}
		o := settledAt(t, adm.b, c.pos)
		if rosterDump(o) != rosterDump(s) || snapDump(o) != snapDump(s) {
			t.Fatalf("%s: two peers give different snapshots at position %d", c.name, c.pos)
		}
	}
	if s := settledAt(t, a.b, removed[0]); get(t, s, "w") != "1" {
		t.Fatal("the removed writer's earlier entry lost its effect")
	}
}

// activation returns the index and the record of the first stored pivot whose batch activates a config.
func activation(t testing.TB, b *Braid) (uint64, pivotRec) {
	t.Helper()
	b.mu.Lock()
	n := b.pivots
	b.mu.Unlock()
	for i := uint64(0); i < n; i++ {
		rec, err := b.pivotAt(i)
		if err != nil {
			t.Fatal(err)
		}
		if rec.Activates {
			return i, rec
		}
	}
	t.Fatal("no batch activates a config")
	return 0, pivotRec{}
}

// TestSettlersAcrossAnActivation: a snapshot gives the seats and the quorum of one config, the one that settled its last
// entry: the old config through the activating batch, the new one after it.
func TestSettlersAcrossAnActivation(t *testing.T) {
	nt, ps := group(t, 3, GroupConfig{Quorum: TwoThirds}, appApply, true)
	a := ps[0]
	mustAppend(t, a.b, "x=1")
	nt.settle(nil, 20*time.Second, func() bool { return quiet(ps...) && settledGet(t, a.b, "x") == "1" })
	_, rec := activation(t, a.b)
	end := rec.Next - 1
	old, cur := settledAt(t, a.b, end).Settlers(), settledAt(t, a.b, end+1).Settlers()
	if old.Config != rec.Config || len(old.Seats) != 1 || old.Quorum != 1 {
		t.Fatalf("at the activating batch's end: %+v, want config %d with one seat", old, rec.Config)
	}
	if cur.Config != rec.Config+1 || len(cur.Seats) != 3 || cur.Quorum != 3 || cur.Rule != TwoThirds {
		t.Fatalf("after it: %+v", cur)
	}
	s := a.b.Settled()
	defer s.Close()
	st := s.Settlers()
	if st.Config != a.b.Status().Config || st.Quorum != settle.Quorum(settle.Rule(st.Rule), len(st.Seats)) {
		t.Fatalf("settled view: %+v, status %+v", st, a.b.Status())
	}
	if !slices.IsSortedFunc(st.Seats, func(x, y Member) int { return strings.Compare(string(x.Key), string(y.Key)) }) {
		t.Fatal("seats are not in key order")
	}
	for _, m := range s.Members() {
		in := slices.ContainsFunc(st.Seats, func(x Member) bool { return bytes32Equal(x.Key, m.Key) && x.Strand == m.Strand })
		if in != m.Seat {
			t.Fatalf("member %x: Seat %v, in Settlers %v", m.Key[:4], m.Seat, in)
		}
		if in {
			i := slices.IndexFunc(st.Seats, func(x Member) bool { return bytes32Equal(x.Key, m.Key) })
			if st.Seats[i].Roles != m.Roles || !st.Seats[i].Seat {
				t.Fatalf("seat %x: %+v, member %+v", m.Key[:4], st.Seats[i], m)
			}
		}
	}
	tv := a.b.View()
	defer tv.Close()
	if got := tv.Settlers(); got.Config != a.b.Status().Config || len(got.Seats) != 3 {
		t.Fatalf("tentative view: %+v", got)
	}
	s.Close()
	if got := s.Settlers(); got.Seats != nil || got.Config != 0 || got.Quorum != 0 {
		t.Fatalf("closed snapshot: %+v", got)
	}
	// a seat whose member the roster no longer holds keeps no roles
	v := &rosterView{idx: map[string]int{}, cfg: settle.Config{Number: 7, Rule: settle.TwoThirds,
		Seats: []settle.Seat{{Key: key32(keyOf(a))}, {Key: key32(keyOf(ps[1]))}, {Key: key32(keyOf(ps[2]))}}}}
	got := (&Snapshot{roster: v}).Settlers()
	if got.Config != 7 || got.Rule != TwoThirds || got.Quorum != 3 || len(got.Seats) != 3 {
		t.Fatalf("settlers of a frozen roster: %+v", got)
	}
	for _, m := range got.Seats {
		if m.Roles != 0 || !m.Seat {
			t.Fatalf("removed seat: %+v", m)
		}
	}
	v.cfg.Rule = settle.Majority
	if got := (&Snapshot{roster: v}).Settlers(); got.Quorum != 2 || got.Rule != Majority {
		t.Fatalf("Majority over three seats: %+v", got)
	}
}

// TestRestoreMarksTheSeatsOfTheViewsBatch: after Open, the settled roster marks the seats of the config of the batch that holds
// the view's last entry, as before the restart, also when that batch activated the next config.
func TestRestoreMarksTheSeatsOfTheViewsBatch(t *testing.T) {
	nt, ps := group(t, 2, GroupConfig{}, appApply, true)
	a := ps[0]
	nt.settle(nil, 20*time.Second, func() bool { return quiet(ps...) })
	i, rec := activation(t, a.b)
	a.b.mu.Lock()
	a.b.view.mu.Lock()
	prog := a.b.view.prog
	a.b.view.prog = progress{Next: rec.Next, Batches: i + 1}
	a.b.view.mu.Unlock()
	got, err := a.b.viewBatchConfig()
	cases := []struct {
		p    progress
		want uint64
	}{
		{progress{Batches: i + 1, Offset: 1}, rec.Config + 1},
		{progress{Batches: 0}, 0},
	}
	var others []uint64
	for _, c := range cases {
		a.b.view.mu.Lock()
		a.b.view.prog = c.p
		a.b.view.mu.Unlock()
		o, err := a.b.viewBatchConfig()
		if err != nil {
			t.Fatal(err)
		}
		others = append(others, o.Number)
	}
	a.b.view.mu.Lock()
	a.b.view.prog = prog
	a.b.view.mu.Unlock()
	cur := a.b.st.Config().Number
	a.b.mu.Unlock()
	if err != nil || got.Number != rec.Config || got.Number == cur || len(got.Seats) != 1 {
		t.Fatalf("config of the activating batch: %d %v (current %d), want %d", got.Number, err, cur, rec.Config)
	}
	for k, c := range cases {
		if others[k] != c.want {
			t.Fatalf("view at %+v: config %d, want %d", c.p, others[k], c.want)
		}
	}
	a.reopen()
	s := a.b.Settled()
	defer s.Close()
	if at := settledAt(t, a.b, s.Position()); rosterDump(at) != rosterDump(s) {
		t.Fatalf("after Open:\n%s\nSettledAt:\n%s", rosterDump(s), rosterDump(at))
	}
}

// TestSettledAtEdges: positions the call refuses, and the first position of a successor braid.
func TestSettledAtEdges(t *testing.T) {
	a := newFounder(t, "a", GroupConfig{}, appApply)
	mustAppend(t, a.b, "x=1")
	waitUntil(t, 10*time.Second, func() bool { return quiet(a) && settledGet(t, a.b, "x") == "1" })
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	for _, c := range []struct {
		name string
		ctx  context.Context
		pos  uint64
		want error
	}{
		{"not settled", context.Background(), a.b.Status().Settled, ErrNotFound},
		{"far ahead", context.Background(), ^uint64(0), ErrNotFound},
		{"context ended", canceled, 0, context.Canceled},
	} {
		if s, err := a.b.SettledAt(c.ctx, c.pos); !errors.Is(err, c.want) || s != nil {
			t.Errorf("%s: %v, want %v", c.name, err, c.want)
		}
	}
	g := settledAt(t, a.b, 0)
	if g.Position() != 0 || len(g.Members()) != 1 || get(t, g, "x") != "<none>" {
		t.Fatalf("genesis: position %d, roster %s", g.Position(), rosterDump(g))
	}

	p := a.b.view.Progress().Next - 1
	s := &peer{t: t, name: "s", id: a.id, db: kolostore.NewMemory()}
	s.opts = testOptions(s.db, appApply)
	var err error
	if s.b, err = Succeed(context.Background(), a.id, a.b, GroupConfig{App: "test/v1"}, s.opts); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.close)
	waitUntil(t, 10*time.Second, func() bool { return s.b.Status().Settled > p+1 })
	if _, err := s.b.SettledAt(ctx10(t), p); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("before the successor's first position: %v", err)
	}
	if _, _, err := s.b.stateAt(p, kolostore.NewMemory()); !errors.Is(err, ErrBadSeed) {
		t.Fatalf("predecessor check before the old braid: %v", err)
	}
	first := settledAt(t, s.b, p+1)
	if get(t, first, "x") != "1" || len(first.Members()) != 1 {
		t.Fatalf("successor's genesis: x %q, roster %s", get(t, first, "x"), rosterDump(first))
	}
}

// TestSettledAtClosedBraid: a closed braid refuses the call.
func TestSettledAtClosedBraid(t *testing.T) {
	a := newFounder(t, "a", GroupConfig{}, appApply)
	b := a.b
	a.close()
	if _, err := b.SettledAt(ctx10(t), 0); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed braid: %v", err)
	}
}

func scratchKeys(t testing.TB, b *Braid, prefix string) int {
	t.Helper()
	n := 0
	if err := scanPrefix(b.db, []byte(prefix), func(k, v []byte) error { n++; return nil }); err != nil {
		t.Fatal(err)
	}
	return n
}

// TestSettledAtScratch: the replay's storage lives under the scratch prefix until Close, and Open removes what a snapshot never
// closed left behind; a refused or failed call leaves nothing.
func TestSettledAtScratch(t *testing.T) {
	a := newFounder(t, "a", GroupConfig{}, appApply)
	mustAppend(t, a.b, "x=1")
	mustAppend(t, a.b, "fail")
	waitUntil(t, 10*time.Second, func() bool {
		st := a.b.Status()
		return st.Halted != nil && st.Settled == st.Tentative
	})
	var at, failed uint64
	for _, o := range settledEntries(t, a.b) {
		switch string(o.Body) {
		case "x=1":
			at = o.Position
		case "fail":
			failed = o.Position
		}
	}
	if failed == 0 {
		t.Fatal("the failing entry did not settle")
	}
	s, err := a.b.SettledAt(ctx10(t), at)
	if err != nil {
		t.Fatal(err)
	}
	if get(t, s, "x") != "1" || scratchKeys(t, a.b, keySettledAt) == 0 {
		t.Fatal("no replay under the scratch prefix")
	}
	if err := s.Close(); err != nil || scratchKeys(t, a.b, keySettledAt) != 0 {
		t.Fatalf("Close = %v, %d scratch keys left", err, scratchKeys(t, a.b, keySettledAt))
	}
	a.b.mu.Lock()
	open := len(a.b.at.open)
	a.b.mu.Unlock()
	if open != 0 {
		t.Fatalf("%d closed snapshots still tracked", open)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second Close = %v", err)
	}
	if _, err := a.b.SettledAt(ctx10(t), failed); !errors.Is(err, ErrHalted) {
		t.Fatalf("at the entry whose apply fails: %v", err)
	}
	if n := scratchKeys(t, a.b, keySettledAt); n != 0 {
		t.Fatalf("a failed replay left %d scratch keys", n)
	}
	// two snapshots at once keep apart
	s1, s2 := settledAt(t, a.b, at), settledAt(t, a.b, at)
	if err := s2.Close(); err != nil || get(t, s1, "x") != "1" || len(vtDump(s1.Scan(nil, nil))) == 0 {
		t.Fatalf("Close of one snapshot changed the other: %v", err)
	}
	s1.Close()
	kept, err := a.b.SettledAt(ctx10(t), at)
	if err != nil {
		t.Fatal(err)
	}
	_ = kept // never closed
	a.reopen()
	if n := scratchKeys(t, a.b, keySettledAt); n != 0 {
		t.Fatalf("Open left %d scratch keys of an unclosed snapshot", n)
	}
}

// TestSnapshotErr: a Scan cut short by a storage error or by Close reports it through Err, the first one only; a whole scan
// reports nil.
func TestSnapshotErr(t *testing.T) {
	setup := func(t *testing.T) (vtFault, *Snapshot) {
		f := newVtFault(kolostore.NewMemory(), 0, false)
		cfg := vtConfig()
		v := vtOpen(t, f, cfg, vtApply)
		if _, err := v.Settle(0, vtSteps(vtScript(7, 40))); err != nil {
			t.Fatal(err)
		}
		s := v.Settled()
		t.Cleanup(func() { s.Close() })
		return f, s
	}
	for _, c := range []struct {
		name string
		run  func(f vtFault, s *Snapshot) int
		want error
	}{
		{"whole scan", func(f vtFault, s *Snapshot) int { return len(vtDump(s.Scan(nil, nil))) }, nil},
		{"storage error", func(f vtFault, s *Snapshot) int {
			f.getErr.Store(true)
			defer f.getErr.Store(false)
			return len(vtDump(s.Scan(nil, nil)))
		}, errInjected},
		{"closed before", func(f vtFault, s *Snapshot) int { s.Close(); return len(vtDump(s.Scan(nil, nil))) }, ErrClosed},
		{"closed during", func(f vtFault, s *Snapshot) int {
			n := 0
			for range s.Scan(nil, nil) {
				if n++; n == 1 {
					s.Close()
				}
			}
			return n
		}, ErrClosed},
		{"stopped by the caller", func(f vtFault, s *Snapshot) int {
			for range s.Scan(nil, nil) {
				break
			}
			return 1
		}, nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			f, s := setup(t)
			n := c.run(f, s)
			if !errors.Is(s.Err(), c.want) || (c.want == nil) != (s.Err() == nil) {
				t.Fatalf("Err = %v after %d pairs, want %v", s.Err(), n, c.want)
			}
			if c.want == nil && c.name == "whole scan" && n == 0 {
				t.Fatal("nothing scanned")
			}
		})
	}
	// ScanErr reports its own scan's error, and Err the first of every scan
	f, s := setup(t)
	f.getErr.Store(true)
	seq, scanErr := s.ScanErr(nil, nil)
	for range seq {
	}
	f.getErr.Store(false)
	seq2, scanErr2 := s.ScanErr(nil, nil)
	n := 0
	for range seq2 {
		n++
	}
	if !errors.Is(scanErr(), errInjected) || scanErr2() != nil || n == 0 || !errors.Is(s.Err(), errInjected) {
		t.Fatalf("ScanErr: %v then %v after %d pairs, Err %v", scanErr(), scanErr2(), n, s.Err())
	}
	s.Close()
	// the first error stays
	f, s = setup(t)
	f.getErr.Store(true)
	for range s.Scan(nil, nil) {
	}
	f.getErr.Store(false)
	s.Close()
	for range s.Scan(nil, nil) {
	}
	if !errors.Is(s.Err(), errInjected) {
		t.Fatalf("Err = %v, want the first error", s.Err())
	}
	if (&Snapshot{}).Settlers().Seats != nil || s.Settlers().Config != 0 {
		t.Fatal("a snapshot without a roster has settlers")
	}
	if got := (&Snapshot{roster: &rosterView{}}).Settlers(); got.Quorum != 0 || got.Seats != nil {
		t.Fatalf("a roster without a config: %+v", got)
	}
}
