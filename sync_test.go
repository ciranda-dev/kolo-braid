package kolobraid

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	kolocodec "github.com/ciranda-dev/kolo-codec"
	kololog "github.com/ciranda-dev/kolo-log"
	kolostore "github.com/ciranda-dev/kolo-store"
	kolosync "github.com/ciranda-dev/kolo-sync"

	"github.com/ciranda-dev/kolo-braid/internal/dag"
	"github.com/ciranda-dev/kolo-braid/internal/prefixdb"
)

func sampleServed(t testing.TB) ([]byte, []byte, servedEntry) {
	t.Helper()
	p := newFounder(t, "s", GroupConfig{}, appApply)
	mustAppend(t, p.b, "a=1")
	l := p.b.own
	h, err := l.Head()
	if err != nil {
		t.Fatal(err)
	}
	e, err := l.Entry(1)
	if err != nil {
		t.Fatal(err)
	}
	rec, _ := kololog.DecodeRecord(e.Record)
	pr, err := l.InclusionProof(1, h.Size)
	if err != nil {
		t.Fatal(err)
	}
	prev, _ := l.Record(0)
	se := servedEntry{Record: rec, Prev: entryIDOf(prev.Encode()), Ciphertext: e.Ciphertext, Key: e.Key, Proof: pr, Head: h}
	return servedKey(l.ID(), 1, entryIDOf(e.Record)), se.encode(), se
}

func TestServedEntryRecord(t *testing.T) {
	k, v, want := sampleServed(t)
	got, err := decodeServed(k, v)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.encode(), v) || got.Prev != want.Prev || !bytes.Equal(got.Key, want.Key) {
		t.Fatal("served entry does not round-trip")
	}
	other := bytes.Clone(k)
	other[len(other)-1] ^= 1
	wrongIndex := bytes.Clone(k)
	wrongIndex[len(keyServed)+32+7] ^= 1
	var badKey kolocodec.Encoder
	badKey.PutBytes(want.Record.Encode())
	badKey.PutFixed(want.Prev[:])
	badKey.PutBytes(want.Ciphertext)
	badKey.PutBytes([]byte{1, 2, 3})
	badKey.PutBytes(want.Proof.Encode())
	badKey.PutBytes(want.Head.Encode())
	for name, c := range map[string][2][]byte{
		"short key":      {k[:10], v},
		"another ID":     {other, v},
		"another index":  {wrongIndex, v},
		"trailing bytes": {k, append(bytes.Clone(v), 0)},
		"truncated":      {k, v[:len(v)-1]},
		"key size":       {k, badKey.Bytes()},
	} {
		if _, err := decodeServed(c[0], c[1]); !errors.Is(err, ErrCorrupt) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestCursorRecord(t *testing.T) {
	var e kolocodec.Encoder
	e.PutUvarint(300)
	if c, err := decodeCursor(e.Bytes()); err != nil || c != 300 {
		t.Fatalf("%d %v", c, err)
	}
	for _, b := range [][]byte{nil, {0x80}, {0x80, 0x00}, append(e.Bytes(), 1)} {
		if _, err := decodeCursor(b); !errors.Is(err, ErrCorrupt) {
			t.Errorf("%x: %v", b, err)
		}
	}
}

func TestWelcomeRecord(t *testing.T) {
	w := welcomeRec{Strand: kololog.LogID{1}, Index: 7, ID: EntryID{2}}
	got, err := decodeWelcomeRec(w.encode())
	if err != nil || got != w {
		t.Fatalf("%+v %v", got, err)
	}
	enc := w.encode()
	for _, b := range [][]byte{enc[:len(enc)-1], append(bytes.Clone(enc), 0), nil} {
		if _, err := decodeWelcomeRec(b); !errors.Is(err, ErrCorrupt) {
			t.Errorf("%x: %v", b, err)
		}
	}
}

// TestKeysFromCursor: a pull stores the replica's KeysFrom cursor, and a restart reads it back instead of scanning from 0.
func TestKeysFromCursor(t *testing.T) {
	m := newMesh(t)
	ps := grow(t, m, 2)
	f, j := ps[0], ps[1]
	for range 3 {
		mustAppend(t, f.b, "a=1")
	}
	waitNet(t, 20*time.Second, ps, func() bool { return synced(ps...) })
	id := f.b.meta.OwnStrand
	v, err := getOpt(j.b.db, append([]byte(keyCursor), id[:]...))
	if err != nil || v == nil {
		t.Fatalf("no cursor stored: %v", err)
	}
	c, err := decodeCursor(v)
	if err != nil || c != lengths(j)[id] {
		t.Fatalf("cursor %d, replica %d: %v", c, lengths(j)[id], err)
	}
	m.isolate(j)
	j.reopen()
	j.b.sync.mu.Lock()
	got := j.b.sync.cursorLocked(id, ^uint64(0))
	j.b.sync.mu.Unlock()
	if got != c {
		t.Fatalf("cursor after a restart %d, want %d", got, c)
	}
}

// TestPullAheadBound: a member that writes far ahead of settlement is pulled only up to MaxPullAhead past its settled length, a
// message of entries more at most, by a peer that has nothing linking further.
func TestPullAheadBound(t *testing.T) {
	m := newMesh(t)
	f := newFounder(t, "f", GroupConfig{}, appApply)
	plain := Role(0)
	s := m.join(f, "s", &AdmitOptions{Roles: &plain})
	v := m.join(f, "v", &AdmitOptions{Roles: &plain})
	all := []*peer{f, s, v}
	m.full(all...)
	waitNet(t, 30*time.Second, all, func() bool { return synced(all...) })
	m.isolate(f)
	m.disconnect(s, v)
	body := bytes.Repeat([]byte{'x'}, 64<<10)
	for range 300 {
		if _, err := s.b.Append(ctx10(t), body); err != nil {
			t.Fatal(err)
		}
	}
	m.connect(s, v)
	strand := s.b.meta.OwnStrand
	waitUntil(t, 20*time.Second, func() bool { return lengths(v)[strand] >= 64 })
	// wait until nothing more moves: no pull in flight and the length unchanged for a second
	last, since := uint64(0), time.Now()
	waitUntil(t, 30*time.Second, func() bool {
		v.b.sync.mu.Lock()
		busy := v.b.sync.running > 0
		v.b.sync.mu.Unlock()
		if n := lengths(v)[strand]; busy || n != last {
			last, since = n, time.Now()
		}
		return time.Since(since) > time.Second
	})
	// one ENTRIES message holds about 60 entries of 64 KiB: the cap stops the pull at the message that passes it
	if got := lengths(v)[strand]; got > 64+3*64 || got >= 300 {
		t.Fatalf("pulled %d entries of a strand nothing links, MaxPullAhead 64", got)
	}
}

// TestEntryReqBound: a link to an entry that never exists, at an index the strand holds another entry at, costs one ENTRY_REQ
// per connected peer; after every peer answered without it nothing more is asked until a new head of that strand or a new peer.
func TestEntryReqBound(t *testing.T) {
	m := newMesh(t)
	f := newFounder(t, "f", GroupConfig{}, appApply)
	plain := Role(0)
	ps := []*peer{f}
	for _, name := range []string{"q", "r", "z"} {
		ps = append(ps, m.join(f, name, &AdmitOptions{Roles: &plain}))
	}
	m.full(ps...)
	q, r := ps[1], ps[2]
	mustAppend(t, q.b, "q=1")
	waitNet(t, 30*time.Second, ps, func() bool { return synced(ps...) && settledGet(t, f.b, "q") == "1" })
	r.b.mu.Lock()
	prev := r.b.strands[r.b.meta.OwnStrand].last
	r.b.mu.Unlock()
	// ten dangling links into one strand: only the lowest is asked for. Only requests for these targets are counted: a peer
	// may also ask for a real entry it saw linked before its strand's pull brought it.
	dangling := map[EntryID]bool{}
	var reqs atomic.Uint64
	tap := func(m entryReqMsg) {
		if m.Strand == q.b.meta.OwnStrand && dangling[m.ID] {
			reqs.Add(1)
		}
	}
	for i := range 10 {
		dangling[EntryID{byte(3 + i)}] = true
	}
	for _, p := range ps {
		p.b.sync.entryReqTap.Store(&tap)
	}
	for i := range 10 {
		target := Link{Strand: q.b.meta.OwnStrand, Index: 0, ID: EntryID{byte(3 + i)}}
		prev = rogueAppend(t, r, rogueContent(t, r, entryContent{Group: f.b.meta.Group, Kind: KindData, Prev: prev, Links: []Link{target},
			Body: []byte("y=dangling")}))
	}
	count := reqs.Load
	askers := []*peer{f, r, ps[3]}
	waitUntil(t, 20*time.Second, func() bool {
		for _, p := range askers {
			p.b.sync.mu.Lock()
			missing := len(p.b.sync.missing)
			p.b.sync.mu.Unlock()
			if missing == 0 {
				return false
			}
		}
		return true
	})
	n := count()
	if n == 0 || n > uint64(len(askers)*(len(ps)-1)) {
		t.Fatalf("%d ENTRY_REQ for one dangling target among %d peers", n, len(ps))
	}
	mustAppend(t, f.b, "more=1")
	waitNet(t, 20*time.Second, ps, func() bool { return settledGet(t, ps[3].b, "more") == "1" })
	// every asker's scheduler ran again after it settled the new entry: any request it would make is made by then
	marks := make([]uint64, len(askers))
	for i, p := range askers {
		marks[i] = p.b.sync.runs.Load()
	}
	mustAppend(t, f.b, "more=2")
	waitUntil(t, 20*time.Second, func() bool {
		for i, p := range askers {
			if p.b.sync.runs.Load() <= marks[i] {
				return false
			}
		}
		return true
	})
	if got := count(); got != n {
		t.Fatalf("%d more ENTRY_REQ for a missing target without a new head of its strand", got-n)
	}
	mustAppend(t, q.b, "q=2")
	waitUntil(t, 20*time.Second, func() bool { return count() > n })
	waitUntil(t, 20*time.Second, func() bool {
		for _, p := range askers {
			p.b.sync.mu.Lock()
			missing := len(p.b.sync.missing)
			p.b.sync.mu.Unlock()
			if missing == 0 {
				return false
			}
		}
		return true
	})
	if got := count(); got > 2*n {
		t.Fatalf("%d ENTRY_REQ after one new head of the strand, %d before", got, n)
	}
}

// TestAnnounceCoalesced: a burst of appends reaches a connection as at most one announcement per AnnounceGap.
func TestAnnounceCoalesced(t *testing.T) {
	m := newMesh(t)
	v := newFounder(t, "v", GroupConfig{}, appApply)
	v.close()
	v.opts.Limits = fastLimits()
	v.opts.Limits.AnnounceGap = 100 * time.Millisecond
	v.reopen()
	j := m.join(v, "j", nil)
	m.connect(j, v)
	mustAppend(t, j.b, "j=1")
	waitNet(t, 20*time.Second, []*peer{v, j}, func() bool { return synced(v, j) })
	m.isolate(j)
	rl := m.raw(j.id, v)
	jstrand := j.b.meta.OwnStrand
	j.close()
	var mu sync.Mutex
	announces, heads := 0, 0
	vs := v.b.meta.OwnStrand
	var lastV uint64
	go func() {
		for {
			st, err := rl.conn.AcceptUniStream(m.ctx)
			if err != nil {
				return
			}
			msg, err := readMessage(st, v.b.groupBounds(), nil)
			if a, ok := msg.(announceMsg); ok && err == nil {
				mu.Lock()
				announces++
				if announces > 1 && slices.ContainsFunc(a.Heads, func(h kololog.Head) bool { return h.LogID == jstrand }) {
					heads++
				}
				for _, h := range a.Heads {
					if h.LogID == vs {
						lastV = max(lastV, h.Size)
					}
				}
				mu.Unlock()
			}
		}
	}()
	// the first announcement, with every head, has arrived
	waitUntil(t, 10*time.Second, func() bool { mu.Lock(); defer mu.Unlock(); return announces > 0 })
	mu.Lock()
	initial := announces
	mu.Unlock()
	start := time.Now()
	for range 30 {
		mustAppend(t, v.b, "a=1")
		time.Sleep(10 * time.Millisecond) // spreads the appends over about 300 ms
	}
	// count once the head of the last append has been announced
	final := v.b.own.Len()
	waitUntil(t, 10*time.Second, func() bool { mu.Lock(); defer mu.Unlock(); return lastV >= final })
	mu.Lock()
	got := announces - initial
	mu.Unlock()
	mu.Lock()
	extra := heads
	mu.Unlock()
	if extra != 0 {
		t.Fatalf("%d announcements carried heads that did not change", extra)
	}
	if limit := 1 + int(time.Since(start)/(100*time.Millisecond)); got > limit {
		t.Fatalf("%d announcements in %v at a 100 ms gap", got, time.Since(start))
	}
	if got < 2 {
		t.Fatalf("%d announcements", got)
	}
}

// signAt makes h, whose braid is closed, sign one more entry on a copy of its data db, and returns the record.
func signAt(t testing.TB, h *peer, strand kololog.LogID, db kolostore.Backend) kololog.Record {
	t.Helper()
	st := kolostore.New(prefixdb.New(PrefixBackend(db, nil), append([]byte{storePrefix}, keyOf(h)...)))
	defer st.Close()
	l, err := st.Open(h.id, strand, kololog.WithValueEncoding(kololog.Bytes))
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	i, err := l.Append([]byte("fork"))
	if err != nil {
		t.Fatal(err)
	}
	rec, err := l.Record(i)
	if err != nil {
		t.Fatal(err)
	}
	return rec
}

// TestRecordAnswers: a RECORD answer for another index or strand closes the connection; one that differs from the held record
// at that index becomes evidence.
func TestRecordAnswers(t *testing.T) {
	m, v, h := hostileSetup(t)
	bd := v.b.groupBounds()
	strand := h.b.meta.OwnStrand
	old := copyBackend(t, h.db)
	at := h.b.own.Len()
	mustAppend(t, h.b, "h=2")
	m.connect(h, v)
	waitUntil(t, 20*time.Second, func() bool { return lengths(v)[strand] > at })
	m.isolate(h)
	rec0, _ := h.b.own.Record(0)
	vrec, _ := v.b.own.Record(0)
	h.close()
	fork := signAt(t, h, strand, old)
	ask := func(r *remote) {
		v.b.sync.mu.Lock()
		r.recordReq = &recordReq{strand: strand, index: at, timer: time.AfterFunc(time.Hour, func() {})}
		v.b.sync.mu.Unlock()
	}
	for name, rec := range map[string]kololog.Record{"another index": rec0, "another strand": vrec} {
		t.Run(name, func(t *testing.T) {
			rl := m.raw(h.id, v)
			ask(remoteOf(t, v, h.id.PublicKey()))
			rl.msg(t, recordMsg{Record: &rec}, bd)
			if err := rl.closed(t); !errors.Is(err, ErrProtocol) {
				t.Fatalf("ServeConn: %v", err)
			}
		})
	}
	rl := m.raw(h.id, v)
	ask(remoteOf(t, v, h.id.PublicKey()))
	rl.msg(t, recordMsg{Record: &fork}, bd)
	waitUntil(t, 10*time.Second, func() bool { return v.b.holdsEvidence(strand) })
	if !rl.open() {
		t.Fatalf("evidence closed the connection: %v", rl.closed(t))
	}
}

// TestConcurrentSyncCalls: Append, ServeConn, View, Next, Status and Close at once, under the race detector.
func TestConcurrentSyncCalls(t *testing.T) {
	m := newMesh(t)
	ps := grow(t, m, 3)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	for _, p := range ps {
		b := p.b
		wg.Go(func() {
			for i := 0; ctx.Err() == nil && i < 20; i++ {
				b.Append(ctx, []byte("c=1"))
			}
		})
		wg.Go(func() {
			for ctx.Err() == nil {
				s := b.View()
				s.Get([]byte("c"))
				s.Close()
				b.Status()
			}
		})
		wg.Go(func() {
			for ctx.Err() == nil {
				c, cl := context.WithTimeout(ctx, 10*time.Millisecond)
				b.Next(c)
				cl()
			}
		})
	}
	m.connect(ps[0], ps[2])
	time.Sleep(300 * time.Millisecond)
	ps[2].close()
	time.Sleep(100 * time.Millisecond)
	cancel()
	wg.Wait()
}

// TestCloseEndsServeConn: Close returns ServeConn with ErrClosed and leaves no goroutine of the sync layer behind.
func TestCloseEndsServeConn(t *testing.T) {
	m := newMesh(t)
	ps := grow(t, m, 2)
	lk := m.connect(ps[0], ps[1])
	mustAppend(t, ps[1].b, "a=1")
	ps[0].close()
	ps[1].close()
	lk.wg.Wait()
	if !errors.Is(lk.errA, ErrClosed) || !errors.Is(lk.errB, ErrClosed) {
		t.Fatalf("ServeConn after Close: %v %v", lk.errA, lk.errB)
	}
	waitUntil(t, 10*time.Second, func() bool {
		buf := make([]byte, 1<<20)
		st := string(buf[:runtime.Stack(buf, true)])
		return !strings.Contains(st, "kolo-braid.(*remote)") && !strings.Contains(st, "kolo-braid.(*syncer)")
	})
}

// TestPendingFullPullsOnlyLinked: past MaxPendingBytes a peer fetches only what pending entries link, not MaxPullAhead more of
// every active strand.
func TestPendingFullPullsOnlyLinked(t *testing.T) {
	nt, ps := group(t, 3, GroupConfig{}, appApply, false)
	a, r, z := ps[0], ps[1], ps[2]
	nt.settle(nil, 10*time.Second, func() bool { return quiet(ps...) })
	r.b.mu.Lock()
	prev := r.b.strands[r.b.meta.OwnStrand].last
	r.b.mu.Unlock()
	zs := z.b.meta.OwnStrand
	rogueAppend(t, r, rogueContent(t, r, entryContent{Group: a.b.meta.Group, Kind: KindData, Prev: prev,
		Links: []Link{{Strand: zs, Index: 999, ID: EntryID{1}}}, Body: []byte("y=dangling")}))
	nt.round(nil, 0)
	need := func(id kololog.LogID) (uint64, uint64) {
		v, ok := a.b.syncState()
		if !ok {
			t.Fatal("no sync state")
		}
		return v.strands[id].need, v.strands[id].held
	}
	rs := r.b.meta.OwnStrand
	if n, held := need(rs); n <= held {
		t.Fatalf("an active strand is not fetched ahead: need %d, held %d", n, held)
	}
	a.b.mu.Lock()
	limit := a.b.lim.MaxPendingBytes
	a.b.lim.MaxPendingBytes = 1
	a.b.mu.Unlock()
	defer func() {
		a.b.mu.Lock()
		a.b.lim.MaxPendingBytes = limit
		a.b.mu.Unlock()
	}()
	if n, held := need(rs); n != held {
		t.Fatalf("pending full: need %d of a strand nothing pending links, held %d", n, held)
	}
	// what a held entry links is fetched whatever the bound: a peer must be able to place every entry it holds
	if n, _ := need(zs); n != 1000 {
		t.Fatalf("pending full: need %d of the strand a pending entry links at 999", n)
	}
}

// TestUnanchoredEntryWidens: an ENTRY answer whose first entry this peer does not hold places nothing and widens the next
// request; the connection stays.
func TestUnanchoredEntryWidens(t *testing.T) {
	m, v, h := hostileSetup(t)
	strand := h.b.meta.OwnStrand
	mustAppend(t, h.b, "h=2")
	mustAppend(t, h.b, "h=3")
	hh := headOfPeer(t, h)
	n := hh.Size
	e, _ := h.b.own.Entry(n - 1)
	rec, _ := kololog.DecodeRecord(e.Record)
	pr, _ := h.b.own.InclusionProof(n-1, n)
	id := entryIDOf(e.Record)
	rl := m.raw(h.id, v)
	r := remoteOf(t, v, h.id.PublicKey())
	expect(v, r, dag.Link{Strand: dag.StrandID(strand), Index: n - 1, ID: dag.ID(id)}, n-1)
	rl.msg(t, entryMsg{Head: &hh, Entries: []entryItem{{Record: rec, Ciphertext: e.Ciphertext, Key: e.Key, Proof: pr}}},
		v.b.groupBounds())
	waitUntil(t, 5*time.Second, func() bool {
		v.b.sync.mu.Lock()
		defer v.b.sync.mu.Unlock()
		return v.b.sync.window[strand] == min(4, n)
	})
	v.b.mu.Lock()
	_, placed := v.b.nodes[id]
	v.b.mu.Unlock()
	if placed {
		t.Fatal("an entry not linked to a held one was placed")
	}
	if !rl.open() {
		t.Fatalf("an unanchored answer closed the connection: %v", rl.closed(t))
	}
}

// TestAnswersFollowAccess: ENTRY and RECORD answers give keys to members only, ciphertext to pins, and nothing to strangers.
func TestAnswersFollowAccess(t *testing.T) {
	pin := newIdentity(t)
	p := &peer{t: t, name: "f", id: newIdentity(t), db: kolostore.NewMemory()}
	p.opts = testOptions(p.db, appApply)
	p.opts.Pins = append(p.opts.Pins, pin.PublicKey())
	b, err := Create(context.Background(), p.id, GroupConfig{App: "test/v1"}, p.opts)
	if err != nil {
		t.Fatal(err)
	}
	p.b = b
	t.Cleanup(p.close)
	mustAppend(t, b, "a=1")
	rec, _ := b.own.Record(1)
	q := entryReqMsg{Strand: b.meta.OwnStrand, Start: 1, Index: 1, ID: entryIDOf(rec.Encode())}
	rq := recordReqMsg{Strand: b.meta.OwnStrand, Index: 1}
	for name, c := range map[string]struct {
		key          []byte
		items, key32 bool
	}{
		"member":   {keyOf(p), true, true},
		"pin":      {pin.PublicKey(), true, false},
		"stranger": {newIdentity(t).PublicKey(), false, false},
	} {
		r := &remote{s: b.sync, key: c.key}
		m := b.sync.entryAnswer(r, q)
		switch {
		case (len(m.Entries) == 1) != c.items:
			t.Errorf("%s: %d entries", name, len(m.Entries))
		case c.items && (len(m.Entries[0].Key) == entryKeySize) != c.key32:
			t.Errorf("%s: key of %d bytes", name, len(m.Entries[0].Key))
		}
		if got := b.sync.recordAnswer(r, rq); (got.Record != nil) != c.items {
			t.Errorf("%s: record %v", name, got.Record != nil)
		}
	}
}

// TestUnknownHeadsBounded: heads of strands not followed are kept, the newest per strand, at most Limits.MaxUnknownHeads per
// connection, and taken once the strand is followed.
func TestUnknownHeadsBounded(t *testing.T) {
	f := newFounder(t, "f", GroupConfig{MaxMembers: 4}, appApply)
	f.b.mu.Lock()
	f.b.lim.MaxUnknownHeads = 8
	f.b.mu.Unlock()
	s := f.b.sync
	r := &remote{s: s, key: keyOf(f), heads: map[kololog.LogID]uint64{}, unknown: map[kololog.LogID]kololog.Head{},
		alive: true}
	for i := range 20 {
		r.keepUnknown(kololog.Head{LogID: kololog.LogID{byte(i + 1)}, Size: 5})
	}
	if len(r.unknown) != 8 {
		t.Fatalf("%d unknown heads kept, bound 8", len(r.unknown))
	}
	id := kololog.LogID{1}
	r.keepUnknown(kololog.Head{LogID: id, Size: 9})
	r.keepUnknown(kololog.Head{LogID: id, Size: 7})
	if r.unknown[id].Size != 9 {
		t.Fatalf("kept size %d, want the newest 9", r.unknown[id].Size)
	}
	h := headOfPeer(t, f)
	own := f.b.meta.OwnStrand
	j, _ := newJoiner(t, "j", f, appApply)
	j.start()
	jr := &remote{s: j.b.sync, key: keyOf(f), heads: map[kololog.LogID]uint64{}, unknown: map[kololog.LogID]kololog.Head{own: h},
		alive: true}
	v, _ := j.b.syncState()
	jr.s.mu.Lock()
	jr.s.recheck(jr, v)
	got := jr.heads[own]
	jr.s.mu.Unlock()
	if got != h.Size || len(jr.unknown) != 0 {
		t.Fatalf("a kept head of a followed strand was not taken: %d", got)
	}
}

// TestEvidencePassedOn: evidence a peer holds is sent once on every connection, as an EVIDENCE message.
func TestEvidencePassedOn(t *testing.T) {
	m := newMesh(t)
	ps := grow(t, m, 3)
	v, h, u := ps[0], ps[1], ps[2]
	strand := h.b.meta.OwnStrand
	m.isolate(u)
	m.isolate(h)
	h.close()
	ev := forkEvidence(t, h, strand)
	if err := v.b.addEvidence(ev); err != nil {
		t.Fatal(err)
	}
	rl := m.raw(u.id, v)
	got := make(chan evidenceBody, 4)
	go func() {
		for {
			st, err := rl.conn.AcceptUniStream(m.ctx)
			if err != nil {
				return
			}
			if msg, err := readMessage(st, v.b.groupBounds(), nil); err == nil {
				if e, ok := msg.(evidenceMsg); ok {
					got <- e.Evidence
				}
			}
		}
	}()
	select {
	case e := <-got:
		if e.Strand != strand || !bytes.Equal(e.A, ev.A) {
			t.Fatal("other evidence sent")
		}
	case <-time.After(20 * time.Second):
		t.Fatal("no EVIDENCE message")
	}
	select {
	case <-got:
		t.Fatal("evidence sent twice on one connection")
	case <-time.After(300 * time.Millisecond):
	}
}

// TestKeylessEntriesGetKeys: entries of a member that is not a seat, pulled without their keys from a peer that serves
// ciphertext only, get their keys later from a member without being fetched again (the pull is capped), and are then placed.
func TestKeylessEntriesGetKeys(t *testing.T) {
	m := newMesh(t)
	f := newFounder(t, "f", GroupConfig{}, appApply)
	plain := Role(0)
	x := m.join(f, "x", &AdmitOptions{Roles: &plain})
	w := m.join(f, "w", &AdmitOptions{Roles: &plain})
	all := []*peer{f, x, w}
	m.full(all...)
	waitNet(t, 30*time.Second, all, func() bool { return synced(all...) })
	m.isolate(w)
	for range 80 {
		mustAppend(t, x.b, "k=1")
	}
	strand := x.b.meta.OwnStrand
	rl := m.raw(f.id, w)
	srv := kolosync.NewServer(func(_, _ ed25519.PublicKey, _ kololog.LogID) kolosync.Access { return kolosync.Ciphertext })
	if err := srv.Offer(x.b.own); err != nil {
		t.Fatal(err)
	}
	go srv.Serve(m.ctx, rl.conn)
	head := headOfPeer(t, x)
	rl.msg(t, announceMsg{Group: f.b.meta.Group, Heads: []kololog.Head{head}}, f.b.groupBounds())
	// the strand is read up to the first entry without its key
	waitUntil(t, 20*time.Second, func() bool {
		w.b.mu.Lock()
		defer w.b.mu.Unlock()
		st := w.b.strands[strand]
		return st.length() >= 64 && errors.Is(st.stop, kololog.ErrKeyUnavailable)
	})
	held := lengths(w)[strand]
	rl.conn.Close()
	m.connect(w, x)
	waitUntil(t, 20*time.Second, func() bool {
		w.b.mu.Lock()
		defer w.b.mu.Unlock()
		return w.b.strands[strand].next >= held
	})
	m.full(all...)
	waitNet(t, 30*time.Second, all, func() bool { return synced(all...) && settledGet(t, w.b, "#") == settledGet(t, f.b, "#") })
	checkSame(t, all)
}

// TestAnswerKeepsTheRequest: while an ENTRY answer is checked, its request stays outstanding, so the scheduler cannot ask the
// peer again and the request's timer cannot close the connection; once checked, an empty answer is recorded.
func TestAnswerKeepsTheRequest(t *testing.T) {
	m, v, h := hostileSetup(t)
	h.close()
	rl := m.raw(h.id, v)
	r := remoteOf(t, v, keyOf(h))
	s := v.b.sync
	tg := dag.Link{Strand: dag.StrandID(v.b.meta.OwnStrand), Index: 0, ID: dag.ID{7}}
	q := &entryReq{target: tg, timer: time.NewTimer(time.Hour)}
	s.mu.Lock()
	r.entryReq = q
	s.mu.Unlock()
	during := false
	answerRead = func(r *remote, got *entryReq) {
		s.mu.Lock()
		during = r.entryReq == got && got.answering
		s.mu.Unlock()
		s.expired(r, got)
	}
	defer func() { answerRead = nil }()
	if err := r.onEntry(entryMsg{}); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	after, asked := r.entryReq, s.asked[tg][r]
	s.mu.Unlock()
	if !during || after != nil || !asked || !rl.open() {
		t.Fatalf("request outstanding while checked %v, cleared after %v, answer recorded %v", during, after == nil, asked)
	}
}

// TestGraceTimerFiresFast: with a timeout of a few milliseconds the check of a connection's key runs many times, which the
// race detector sees if the timer is read before it is assigned.
func TestGraceTimerFiresFast(t *testing.T) {
	m, v, h := hostileSetup(t)
	v.b.sync.timeout = 2 * time.Millisecond
	h.close()
	rl := m.raw(h.id, v)
	waitUntil(t, 5*time.Second, func() bool { return v.b.sync.graces.Load() >= 20 })
	if !rl.open() {
		t.Fatal("a member's connection was closed")
	}
}
