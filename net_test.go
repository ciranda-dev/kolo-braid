package kolobraid

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"sync"
	"testing"
	"time"

	kololog "github.com/ciranda-dev/kolo-log"
	kolosecure "github.com/ciranda-dev/kolo-secure"
	kolostore "github.com/ciranda-dev/kolo-store"
)

// mesh links peers over kolo-secure on 127.0.0.1; ServeConn runs on both ends of every link.
type mesh struct {
	t      testing.TB
	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.Mutex
	ls     map[*peer]*kolosecure.Listener
	links  map[[2]*peer]*meshLink
}

type meshLink struct {
	ab, ba     *kolosecure.Conn
	wg         sync.WaitGroup
	errA, errB error
}

func newMesh(t testing.TB) *mesh {
	m := &mesh{t: t, ls: map[*peer]*kolosecure.Listener{}, links: map[[2]*peer]*meshLink{}}
	m.ctx, m.cancel = context.WithCancel(context.Background())
	t.Cleanup(func() {
		m.mu.Lock()
		links := slices.Collect(maps.Values(m.links))
		clear(m.links)
		m.mu.Unlock()
		m.cancel()
		for _, lk := range links {
			lk.ab.Close()
			lk.ba.Close()
			lk.wg.Wait()
		}
		for _, l := range m.ls {
			l.Close()
		}
	})
	return m
}

func linkKey(a, c *peer) [2]*peer {
	if a.name > c.name {
		a, c = c, a
	}
	return [2]*peer{a, c}
}

// connect links a to c unless they are linked: a dials c's listener.
func (m *mesh) connect(a, c *peer) *meshLink {
	m.t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	if lk, ok := m.links[linkKey(a, c)]; ok {
		return lk
	}
	l, ok := m.ls[c]
	if !ok {
		var err error
		if l, err = kolosecure.Listen(m.ctx, "127.0.0.1:0", c.id, nil); err != nil {
			m.t.Fatal(err)
		}
		m.ls[c] = l
	}
	type res struct {
		c   *kolosecure.Conn
		err error
	}
	acc := make(chan res, 1)
	go func() {
		conn, err := l.Accept(m.ctx)
		acc <- res{conn, err}
	}()
	ab, err := kolosecure.Dial(m.ctx, l.Addr().String(), a.id, c.id.PublicKey(), nil)
	if err != nil {
		m.t.Fatal(err)
	}
	r := <-acc
	if r.err != nil {
		m.t.Fatal(r.err)
	}
	lk := &meshLink{ab: ab, ba: r.c}
	ba, bc := a.b, c.b
	lk.wg.Go(func() { lk.errA = ba.ServeConn(m.ctx, ab) })
	lk.wg.Go(func() { lk.errB = bc.ServeConn(m.ctx, r.c) })
	m.links[linkKey(a, c)] = lk
	return lk
}

// disconnect closes the link between a and c, if any, and waits for both ServeConn calls.
func (m *mesh) disconnect(a, c *peer) {
	m.mu.Lock()
	lk, ok := m.links[linkKey(a, c)]
	delete(m.links, linkKey(a, c))
	m.mu.Unlock()
	if ok {
		lk.ab.Close()
		lk.ba.Close()
		lk.wg.Wait()
	}
}

func (m *mesh) full(ps ...*peer) {
	for i := range ps {
		for j := i + 1; j < len(ps); j++ {
			m.connect(ps[i], ps[j])
		}
	}
}

func (m *mesh) cut(one, two []*peer) {
	for _, a := range one {
		for _, c := range two {
			m.disconnect(a, c)
		}
	}
}

func (m *mesh) isolate(p *peer) {
	m.mu.Lock()
	var others []*peer
	for k := range m.links {
		switch p {
		case k[0]:
			others = append(others, k[1])
		case k[1]:
			others = append(others, k[0])
		}
	}
	m.mu.Unlock()
	for _, o := range others {
		m.disconnect(p, o)
	}
}

// join admits a new peer through inviter with Admit, NewJoin and Complete; the peer's braid waits for a link.
func (m *mesh) join(inviter *peer, name string, opts *AdmitOptions) *peer {
	t := m.t
	t.Helper()
	p := &peer{t: t, name: name, id: newIdentity(t), db: kolostore.NewMemory()}
	p.opts = testOptions(p.db, appApply)
	j, err := NewJoin(ctx10(t), p.id, p.opts)
	if err != nil {
		t.Fatal(err)
	}
	req, err := j.Request([]byte(name))
	if err != nil {
		t.Fatal(err)
	}
	if opts == nil {
		opts = growOpts
	}
	w, err := inviter.b.Admit(ctx10(t), keyOf(p), req, opts)
	if err != nil {
		t.Fatal(err)
	}
	if p.b, err = j.Complete(ctx10(t), w); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.close)
	return p
}

func waiting(p *peer) bool {
	p.b.mu.Lock()
	defer p.b.mu.Unlock()
	return p.b.waiting
}

func lengths(p *peer) map[kololog.LogID]uint64 {
	p.b.mu.Lock()
	defer p.b.mu.Unlock()
	out := map[kololog.LogID]uint64{}
	for id, st := range p.b.strands {
		out[id] = st.length()
	}
	return out
}

// synced reports whether the peers hold the same length of every strand, have settled what they hold and agree on it.
func synced(peers ...*peer) bool {
	for _, p := range peers {
		if waiting(p) {
			return false
		}
	}
	if !quiet(peers...) {
		return false
	}
	ref := lengths(peers[0])
	for _, p := range peers[1:] {
		if !maps.Equal(lengths(p), ref) {
			return false
		}
	}
	return true
}

func waitNet(t testing.TB, timeout time.Duration, peers []*peer, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			for _, p := range peers {
				if p.b != nil {
					t.Logf("%s: %+v lengths %v", p.name, p.b.Status(), slices.Sorted(maps.Values(lengths(p))))
				}
			}
			t.Fatal("peers did not converge in time")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func counter(t testing.TB, p *peer) string { return settledGet(t, p.b, "#") }

// TestNetTwoMembers: a founder admits a member with Admit, the member completes with the welcome, and both sync over one
// kolo-secure connection to the same settled view.
func TestNetTwoMembers(t *testing.T) {
	m := newMesh(t)
	f := newFounder(t, "f", GroupConfig{}, appApply)
	j := m.join(f, "j", nil)
	if !waiting(j) {
		t.Fatal("a joiner without the genesis entry started")
	}
	if s := j.b.View(); s.Members() != nil || s.Close() != nil {
		t.Fatal("a waiting joiner's view is not empty")
	}
	ctx, cancel := shortCtx()
	_, err := j.b.Append(ctx, []byte("early=1"))
	cancel()
	if !errors.Is(err, ErrUnconfirmed) {
		t.Fatalf("append while waiting for the genesis: %v", err)
	}
	m.connect(j, f)
	waitNet(t, 20*time.Second, []*peer{f, j}, func() bool { return !waiting(j) && hasMember(j.b, keyOf(j), false) })
	mustAppend(t, f.b, "a=1")
	mustAppend(t, j.b, "b=2")
	ps := []*peer{f, j}
	waitNet(t, 20*time.Second, ps, func() bool {
		return synced(ps...) && settledGet(t, j.b, "a") == "1" && settledGet(t, f.b, "b") == "2"
	})
	checkSame(t, ps)
	j.b.mu.Lock()
	w := j.b.welcome
	j.b.mu.Unlock()
	if w != nil {
		t.Fatal("the welcome's AddMember entry was not checked")
	}
	if v, _ := getOpt(j.b.db, []byte(keyWelcome)); v != nil {
		t.Fatal("the welcome record was kept after its check")
	}
}

// grow admits n-1 members through the founder with the default roles and links every peer to every other.
func grow(t testing.TB, m *mesh, n int, setup ...func(*peer)) []*peer {
	t.Helper()
	f := newFounder(t, "p0", GroupConfig{}, appApply)
	ps := []*peer{f}
	for _, fn := range setup {
		fn(f)
	}
	for i := 1; i < n; i++ {
		p := m.join(f, fmt.Sprintf("p%d", i), nil)
		// setup runs before the peer's first link
		for _, fn := range setup {
			fn(p)
		}
		ps = append(ps, p)
		m.full(ps...)
		waitNet(t, 30*time.Second, ps, func() bool { return synced(ps...) && hasMember(p.b, keyOf(p), false) })
	}
	return ps
}

// TestNetConvergence is the network form of the central property: 3 to 5 members admitted with the default roles append from
// every peer while links are cut and healed at random; once healed, every peer has the same settled order, running digest and
// view, every data entry is settled, and an observer fed the same strands in process computes the same digest.
func TestNetConvergence(t *testing.T) {
	for _, n := range []int{3, 4, 5} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(uint64(n), 7))
			m := newMesh(t)
			ps := grow(t, m, n)
			appended := 0
			for step := range 40 {
				p := ps[rng.IntN(n)]
				mustAppend(t, p.b, fmt.Sprintf("k%d=%s", step%7, p.name))
				appended++
				switch rng.IntN(8) {
				case 0:
					k := 1 + rng.IntN(n-1)
					m.cut(ps[:k], ps[k:])
				case 1:
					m.full(ps...)
				}
				time.Sleep(time.Duration(rng.IntN(15)) * time.Millisecond)
			}
			m.full(ps...)
			want := fmt.Sprint(appended)
			waitNet(t, 60*time.Second, ps, func() bool {
				if !synced(ps...) {
					return false
				}
				for _, p := range ps {
					if counter(t, p) != want {
						return false
					}
				}
				return true
			})
			checkSame(t, ps)
			obs, _ := newJoiner(t, "observer", ps[0], appApply)
			obs.start()
			on := newNet(t, append([]*peer{obs}, ps[0])...)
			on.settle(nil, 30*time.Second, func() bool { return counter(t, obs) == want && quiet(obs) })
			if obs.b.view.Progress().Digest != ps[0].b.view.Progress().Digest || dump(t, obs.b) != dump(t, ps[0].b) {
				t.Fatal("the in-process observer settled another state than the network")
			}
		})
	}
}

// TestNetRemoveMember: peers that hold a removal stop serving the removed member at once, so it pulls nothing more, and what it
// writes afterwards is void: they never fetch it, and it never reaches their view.
func TestNetRemoveMember(t *testing.T) {
	m := newMesh(t)
	ps := grow(t, m, 3)
	f, gone := ps[0], ps[2]
	mustAppend(t, gone.b, "before=1")
	waitNet(t, 20*time.Second, ps, func() bool { return synced(ps...) && settledGet(t, f.b, "before") == "1" })
	if _, err := f.b.RemoveMember(ctx10(t), keyOf(gone)); err != nil {
		t.Fatal(err)
	}
	stay := ps[:2]
	// the removed seat's last entries may reach one peer and not the other: nothing links them once its seat is gone
	waitNet(t, 20*time.Second, stay, func() bool { return quiet(stay...) && !hasMember(ps[1].b, keyOf(gone), true) })
	for _, p := range stay {
		if p.b.access(keyOf(gone)) != accessNone {
			t.Fatalf("%s still serves the removed member", p.name)
		}
	}
	fs := f.b.meta.OwnStrand
	held := lengths(gone)[fs]
	ignored := f.b.sync.ignored.Load()
	// it holds its removal, so its own client refuses; a modified one writes anyway
	gone.b.mu.Lock()
	_, err := gone.b.appendLocked(KindData, []byte("after=1"))
	gone.b.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		mustAppend(t, f.b, "later=1")
	}
	waitNet(t, 20*time.Second, stay, func() bool { return quiet(stay...) && settledGet(t, ps[1].b, "later") == "1" })
	// the removed member still syncs: its announcement of the entry it wrote after the removal reaches the founder, which
	// ignores it, and the founder announces nothing to a key it no longer knows
	waitUntil(t, 20*time.Second, func() bool { return f.b.sync.ignored.Load() > ignored })
	if got := lengths(gone)[fs]; got > held {
		t.Fatalf("the removed member pulled %d more entries of the founder strand", got-held)
	}
	for _, p := range stay {
		if settledGet(t, p.b, "after") != "<none>" {
			t.Fatalf("%s applied an entry written after the removal", p.name)
		}
	}
	checkSame(t, stay)
}

// TestNetPartitionAndReopen: four members split 2/2 keep writing tentatively (neither side has q = 3 seats), settle once healed,
// and a member that reopens waits for confirmation from a peer before it writes.
func TestNetPartitionAndReopen(t *testing.T) {
	m := newMesh(t)
	ps := grow(t, m, 4)
	m.cut(ps[:2], ps[2:])
	for i, p := range ps {
		mustAppend(t, p.b, fmt.Sprintf("side%d=1", i))
	}
	for _, p := range ps {
		waitUntil(t, 10*time.Second, func() bool { s := p.b.Status(); return s.Tentative > s.Settled })
	}
	m.full(ps...)
	waitNet(t, 40*time.Second, ps, func() bool { return synced(ps...) && counter(t, ps[0]) == "4" })
	checkSame(t, ps)
	r := ps[3]
	m.isolate(r)
	r.reopen()
	if !r.b.Status().Unconfirmed {
		t.Fatal("a reopened member of four is confirmed before any peer answered")
	}
	ctx, cancel := shortCtx()
	_, err := r.b.Append(ctx, []byte("x=1"))
	cancel()
	if !errors.Is(err, ErrUnconfirmed) {
		t.Fatalf("append before confirmation: %v", err)
	}
	m.full(ps...)
	mustAppend(t, r.b, "reopened=1")
	waitNet(t, 30*time.Second, ps, func() bool { return synced(ps...) && settledGet(t, ps[0].b, "reopened") == "1" })
	checkSame(t, ps)
}

// TestNetRestoredMember: a member restored from an older copy of its data directory catches up from a peer's longer head
// before it writes, so it signs no second history; a copy that wrote on its own is lost.
func TestNetRestoredMember(t *testing.T) {
	m := newMesh(t)
	ps := grow(t, m, 2)
	f, w := ps[0], ps[1]
	mustAppend(t, w.b, "a=1")
	waitNet(t, 20*time.Second, ps, func() bool { return synced(ps...) && settledGet(t, f.b, "a") == "1" })
	old := copyBackend(t, w.db)
	mustAppend(t, w.b, "b=2")
	mustAppend(t, w.b, "c=3")
	waitNet(t, 20*time.Second, ps, func() bool { return synced(ps...) && settledGet(t, f.b, "c") == "3" })
	m.isolate(w)
	w.close()
	forked := copyBackend(t, old)

	w.db, w.opts = old, testOptions(old, appApply)
	w.reopen()
	m.connect(w, f)
	waitEvent(t, w.b, EventStrandCaughtUp, 20*time.Second)
	mustAppend(t, w.b, "d=4")
	waitNet(t, 20*time.Second, ps, func() bool { return synced(ps...) && settledGet(t, f.b, "d") == "4" })
	checkSame(t, ps)
	if f.b.holdsEvidence(w.b.meta.OwnStrand) {
		t.Fatal("the restored member signed a second history")
	}

	m.isolate(w)
	w.close()
	clone := &peer{t: t, name: "clone", id: w.id, db: forked}
	clone.opts = testOptions(forked, appApply)
	clone.opts.AppendUnconfirmed = true
	clone.reopen()
	t.Cleanup(clone.close)
	mustAppend(t, clone.b, "fork=1")
	m.connect(clone, f)
	waitEvent(t, clone.b, EventStrandLost, 20*time.Second)
	if !clone.b.Status().StrandLost {
		t.Fatal("a copy that signed another history is not lost")
	}
}

// TestNetWelcomeChecks: a welcome whose AddMember entry is another one, or whose group is another, halts the joiner once it holds
// the strands that show it.
func TestNetWelcomeChecks(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*welcomeMsg)
		want error
	}{
		{"entry", func(w *welcomeMsg) { w.AddID[0] ^= 1 }, ErrNotMember},
		{"index", func(w *welcomeMsg) { w.AddIndex = 0 }, ErrNotMember},
		{"group", func(w *welcomeMsg) { w.Group[0] ^= 1 }, ErrProtocol},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := newMesh(t)
			f := newFounder(t, "f", GroupConfig{}, appApply)
			mustAppend(t, f.b, "x=1")
			p := &peer{t: t, name: "j", id: newIdentity(t), db: kolostore.NewMemory()}
			p.opts = testOptions(p.db, appApply)
			j, err := NewJoin(ctx10(t), p.id, p.opts)
			if err != nil {
				t.Fatal(err)
			}
			req, _ := j.Request(nil)
			raw, err := f.b.Admit(ctx10(t), keyOf(p), req, growOpts)
			if err != nil {
				t.Fatal(err)
			}
			w, _ := parseWelcome(raw)
			tc.edit(&w)
			raw, err = encodeWelcome(w)
			if err != nil {
				t.Fatal(err)
			}
			if p.b, err = j.Complete(ctx10(t), raw); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(p.close)
			lk := m.connect(p, f)
			if tc.want == ErrProtocol {
				// the inviter's messages name another group than the welcome: the joiner closes the connection
				lk.wg.Wait()
				if !errors.Is(lk.errA, ErrProtocol) || !waiting(p) {
					t.Fatalf("joiner: %v", lk.errA)
				}
				return
			}
			waitUntil(t, 20*time.Second, func() bool { return errors.Is(p.b.Status().Halted, tc.want) })
			if _, err := p.b.Append(ctx10(t), []byte("y=1")); !errors.Is(err, ErrHalted) {
				t.Fatalf("append after a bad welcome: %v", err)
			}
		})
	}
}

// TestNetJoinRefusals: Admit refuses an active key, a known strand and a request of another key; Complete refuses a second
// call and a welcome that names the own strand.
func TestNetJoinRefusals(t *testing.T) {
	m := newMesh(t)
	ps := grow(t, m, 2)
	f, j := ps[0], ps[1]
	other := newIdentity(t)
	db := kolostore.NewMemory()
	jo := testOptions(db, appApply)
	jo.AppendUnconfirmed = true
	jn, err := NewJoin(ctx10(t), other, jo)
	if err != nil {
		t.Fatal(err)
	}
	req, _ := jn.Request(nil)
	if _, err := f.b.Admit(ctx10(t), keyOf(j), req, growOpts); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("request of another key: %v", err)
	}
	j.b.mu.Lock()
	jr, _ := encodeJoinRequest(JoinRequest{Strand: j.b.meta.OwnStrand, Header: j.b.own.Header().Encode()})
	j.b.mu.Unlock()
	if _, err := f.b.Admit(ctx10(t), keyOf(j), jr, growOpts); !errors.Is(err, ErrActiveKey) {
		t.Fatalf("active key: %v", err)
	}
	if _, err := f.b.Admit(ctx10(t), keyOf(j), jr, &AdmitOptions{Replace: true}); !errors.Is(err, ErrKnownStrand) {
		t.Fatalf("known strand: %v", err)
	}
	if _, err := NewJoin(ctx10(t), other, f.opts); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("NewJoin over a stored braid: %v", err)
	}
	raw, err := f.b.Admit(ctx10(t), other.PublicKey(), req, growOpts)
	if err != nil {
		t.Fatal(err)
	}
	w, _ := parseWelcome(raw)
	w2 := w
	w2.InviterStrand, w2.InviterHeader, w2.InviterKey = jn.strand, jn.header, other.PublicKey()
	if bad, err := encodeWelcome(w2); err != nil {
		t.Fatal(err)
	} else if _, err := jn.Complete(ctx10(t), bad); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("welcome naming the own strand: %v", err)
	}
	b, err := jn.Complete(ctx10(t), raw)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	ctx, cancel := shortCtx()
	_, err = b.Append(ctx, []byte("early=1"))
	cancel()
	if !errors.Is(err, ErrUnconfirmed) {
		t.Fatalf("append with AppendUnconfirmed before the genesis: %v", err)
	}
	if _, err := jn.Complete(ctx10(t), raw); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("second Complete: %v", err)
	}
}

// TestNetForkedWriter: a member that is not a seat signs two histories and shows one to each half; once healed, peers fetch the
// other history with ENTRY_REQ, verify it against the signed head and fence the writer with evidence, settlement goes on, and a
// later member that is only linked to one peer gets the second history from that peer's stored copy.
func TestNetForkedWriter(t *testing.T) {
	m := newMesh(t)
	ps := grow(t, m, 5)
	seats, w := ps[:4], ps[4]
	fourSeats(t, ps)
	if w.b.Status().Seats != 4 || slices.ContainsFunc(w.b.Status().PendingConfig, func(x Member) bool { return bytes.Equal(x.Key, keyOf(w)) }) {
		t.Fatal("the fifth member is a seat")
	}
	one, two := seats[:2], seats[2:]
	m.cut(one, two)
	m.isolate(w)
	backup := copyBackend(t, w.db)
	mustAppend(t, w.b, "h=1")
	for _, p := range one {
		m.connect(w, p)
	}
	waitNet(t, 20*time.Second, one, func() bool {
		return lengths(one[0])[w.b.meta.OwnStrand] == w.b.own.Len() && lengths(one[1])[w.b.meta.OwnStrand] == w.b.own.Len()
	})
	m.isolate(w)
	w.close()
	clone := &peer{t: t, name: "w-clone", id: w.id, db: backup}
	clone.opts = testOptions(backup, appApply)
	clone.opts.AppendUnconfirmed = true
	clone.reopen()
	mustAppend(t, clone.b, "h=2")
	strand := clone.b.meta.OwnStrand
	for _, p := range two {
		m.connect(clone, p)
	}
	waitNet(t, 20*time.Second, two, func() bool {
		return lengths(two[0])[strand] == clone.b.own.Len() && lengths(two[1])[strand] == clone.b.own.Len()
	})
	m.isolate(clone)
	clone.close()
	for _, p := range seats {
		mustAppend(t, p.b, "during="+p.name)
	}
	m.full(seats...)
	waitNet(t, 60*time.Second, seats, func() bool {
		if !synced(seats...) {
			return false
		}
		for _, p := range seats {
			if hasMember(p.b, keyOf(w), true) || !p.b.holdsEvidence(strand) {
				return false
			}
		}
		return true
	})
	checkSame(t, seats)
	for _, p := range seats {
		if p.b.Status().SettlementHalted != nil {
			t.Fatalf("%s: the fork of a member that is not a seat halted settlement", p.name)
		}
	}
	served := 0
	_ = scanPrefix(PrefixBackend(seats[0].db, nil), []byte(keyServed), func(k, v []byte) error {
		if _, err := decodeServed(k, v); err != nil {
			t.Fatal(err)
		}
		served++
		return nil
	})
	if served == 0 {
		t.Fatal("no entry of the second history is stored to be served on")
	}
	mustAppend(t, seats[0].b, "late=1")
	waitNet(t, 30*time.Second, seats, func() bool { return synced(seats...) && settledGet(t, seats[3].b, "late") == "1" })
	m.cut(seats[:1], seats[1:])
	late := m.join(seats[0], "late", nil)
	m.connect(late, seats[0])
	all := []*peer{seats[0], late}
	waitNet(t, 60*time.Second, all, func() bool {
		return !waiting(late) && settledGet(t, late.b, "late") == "1" &&
			late.b.view.Progress() == seats[0].b.view.Progress()
	})
	checkSame(t, all)
}

// fourSeats appends until the default roles' fourth seat is active on every peer.
func fourSeats(t testing.TB, ps []*peer) {
	t.Helper()
	for range 20 {
		mustAppend(t, ps[0].b, "grow=1")
		waitNet(t, 30*time.Second, ps, func() bool { return synced(ps...) })
		if !slices.ContainsFunc(ps, func(p *peer) bool { return p.b.Status().Seats != 4 }) {
			return
		}
	}
	t.Fatal("the fourth seat did not activate")
}

// TestNetRestoredSeat: a settler restored from an older copy catches up from one peer, then has the other seats' heads asked
// again, which it needs (at least q - 1 of them) to confirm and write.
func TestNetRestoredSeat(t *testing.T) {
	m := newMesh(t)
	ps := grow(t, m, 4)
	fourSeats(t, ps)
	s := ps[3]
	mustAppend(t, s.b, "a=1")
	waitNet(t, 20*time.Second, ps, func() bool { return synced(ps...) && settledGet(t, ps[0].b, "a") == "1" })
	old := copyBackend(t, s.db)
	mustAppend(t, s.b, "b=2")
	waitNet(t, 20*time.Second, ps, func() bool { return synced(ps...) && settledGet(t, ps[0].b, "b") == "2" })
	m.isolate(s)
	s.close()
	s.db, s.opts = old, testOptions(old, appApply)
	s.reopen()
	// hold the catch-up until every other seat's longer head is in, so those heads must be asked again afterwards
	sy, ol := s.b.sync, s.b.own.Len()
	sy.mu.Lock()
	sy.catch = true
	sy.mu.Unlock()
	m.full(ps...)
	waitUntil(t, 20*time.Second, func() bool {
		sy.mu.Lock()
		defer sy.mu.Unlock()
		n := 0
		for r := range sy.remotes {
			if r.ownSize > ol {
				n++
			}
		}
		return n == 3
	})
	sy.mu.Lock()
	sy.catch = false
	sy.mu.Unlock()
	sy.signal()
	waitEvent(t, s.b, EventStrandCaughtUp, 20*time.Second)
	mustAppend(t, s.b, "c=3")
	waitNet(t, 30*time.Second, ps, func() bool { return synced(ps...) && settledGet(t, ps[0].b, "c") == "3" })
	checkSame(t, ps)
	if ps[0].b.holdsEvidence(s.b.meta.OwnStrand) {
		t.Fatal("the restored settler signed a second history")
	}
}

// TestNetSeatBurstAfterAbsence: a settler writes more than MaxPullAhead entries while a member is away; the member fetches the
// settler's strand whole, since settlement needs the pulses at its end, and settles everything once back.
func TestNetSeatBurstAfterAbsence(t *testing.T) {
	m := newMesh(t)
	ps := grow(t, m, 2)
	f, w := ps[0], ps[1]
	m.isolate(w)
	for range 100 {
		mustAppend(t, f.b, "burst=1")
	}
	m.connect(w, f)
	waitNet(t, 30*time.Second, ps, func() bool { return synced(ps...) && counter(t, w) == "100" })
	checkSame(t, ps)
}

// growOpts is what the tests that build a braid of several settlers pass to Admit: the first joiners become admins and settlers.
var growOpts = &AdmitOptions{GrowSettlers: true}
