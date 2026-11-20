package kolobraid

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"strings"
	"testing"
	"time"

	koloid "github.com/ciranda-dev/kolo-id"
	kololog "github.com/ciranda-dev/kolo-log"
	kolosecure "github.com/ciranda-dev/kolo-secure"
	kolostore "github.com/ciranda-dev/kolo-store"
	kolosync "github.com/ciranda-dev/kolo-sync"

	"github.com/ciranda-dev/kolo-braid/internal/dag"
	"github.com/ciranda-dev/kolo-braid/internal/prefixdb"
)

// rawLink is a connection to a victim's braid whose other end the test drives by hand, as a hostile peer would.
type rawLink struct {
	conn Conn
	done chan error
}

// raw connects as id to v's listener; v serves the connection with ServeConn.
func (m *mesh) raw(id *koloid.Identity, v *peer) *rawLink {
	m.t.Helper()
	m.mu.Lock()
	l, ok := m.ls[v]
	if !ok {
		var err error
		if l, err = kolosecure.Listen(m.ctx, "127.0.0.1:0", v.id, nil); err != nil {
			m.t.Fatal(err)
		}
		m.ls[v] = l
	}
	m.mu.Unlock()
	acc := make(chan *kolosecure.Conn, 1)
	go func() {
		c, _ := l.Accept(m.ctx)
		acc <- c
	}()
	c, err := kolosecure.Dial(m.ctx, l.Addr().String(), id, v.id.PublicKey(), nil)
	if err != nil {
		m.t.Fatal(err)
	}
	vc := <-acc
	rl := &rawLink{conn: c, done: make(chan error, 1)}
	b := v.b
	go func() { rl.done <- b.ServeConn(m.ctx, vc) }()
	m.t.Cleanup(func() {
		c.Close()
		vc.Close()
		<-rl.done
		rl.done <- nil
	})
	return rl
}

func (rl *rawLink) send(t testing.TB, frame []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	st, err := rl.conn.OpenUniStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	st.Write(frame)
	st.Close()
}

func (rl *rawLink) msg(t testing.TB, m message, bd formatBounds) {
	t.Helper()
	f, err := encodeMessage(m, bd)
	if err != nil {
		t.Fatal(err)
	}
	rl.send(t, f)
}

// closed waits for the victim's ServeConn and returns its error.
func (rl *rawLink) closed(t testing.TB) error {
	t.Helper()
	select {
	case err := <-rl.done:
		rl.done <- err
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("the victim kept the connection open")
		return nil
	}
}

// open reports whether the victim still serves the connection after a while.
func (rl *rawLink) open() bool {
	select {
	case err := <-rl.done:
		rl.done <- err
		return false
	case <-time.After(300 * time.Millisecond):
		return true
	}
}

func frameOf(version, typ byte, rest []byte) []byte {
	p := append([]byte{version, typ}, rest...)
	return append(binary.LittleEndian.AppendUint32(nil, uint32(len(p))), p...)
}

// hostileSetup returns a synced founder v and member h on a mesh, with h's links closed so the test speaks for h.
func hostileSetup(t *testing.T) (*mesh, *peer, *peer) {
	m := newMesh(t)
	ps := grow(t, m, 2)
	v, h := ps[0], ps[1]
	mustAppend(t, h.b, "h=1")
	waitNet(t, 20*time.Second, ps, func() bool { return synced(ps...) && settledGet(t, v.b, "h") == "1" })
	m.isolate(h)
	return m, v, h
}

func headOfPeer(t testing.TB, p *peer) kololog.Head {
	t.Helper()
	h, err := p.b.own.Head()
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func remoteOf(t testing.TB, v *peer, key []byte) *remote {
	t.Helper()
	var r *remote
	waitUntil(t, 5*time.Second, func() bool {
		v.b.sync.mu.Lock()
		defer v.b.sync.mu.Unlock()
		for x := range v.b.sync.remotes {
			if bytes.Equal(x.key, key) {
				r = x
				return true
			}
		}
		return false
	})
	return r
}

// expect makes the victim wait for an ENTRY answer to t from the hostile peer, as if it had asked.
func expect(v *peer, r *remote, t dag.Link, start uint64) {
	s := v.b.sync
	s.mu.Lock()
	defer s.mu.Unlock()
	r.entryReq = &entryReq{target: t, start: start, timer: time.AfterFunc(time.Hour, func() {})}
}

// TestHostileMessages: every message that breaks the stream protocol, from a member, closes the connection with ErrProtocol.
func TestHostileMessages(t *testing.T) {
	m, v, h := hostileSetup(t)
	bd := v.b.groupBounds()
	group := v.b.meta.Group
	head := headOfPeer(t, h)
	bad := head
	bad.Size += 5
	other := GroupID{9}
	ann, _ := encodeMessage(announceMsg{Group: group, Heads: []kololog.Head{head}}, bd)
	rec, err := h.b.own.Record(0)
	if err != nil {
		t.Fatal(err)
	}
	strand := h.b.meta.OwnStrand
	h.close()
	ev := forkEvidence(t, h, strand)
	cases := []struct {
		name  string
		frame []byte
		msg   message
	}{
		{name: "over the message bound", frame: binary.LittleEndian.AppendUint32(nil, maxMessageSize+1)},
		{name: "over the bound of its type", frame: frameOf(0, byte(msgRecordReq), make([]byte, 200))},
		{name: "truncated", frame: binary.LittleEndian.AppendUint32(nil, 100)[:4:4]},
		{name: "trailing bytes", frame: append(ann, 0)},
		{name: "another version", frame: frameOf(1, byte(msgAnnounce), ann[6:])},
		{name: "unknown type", frame: frameOf(0, 9, nil)},
		{name: "empty stream", frame: []byte{}},
		{name: "announce of another group", msg: announceMsg{Group: other, Heads: []kololog.Head{head}}},
		{name: "evidence of another group", msg: evidenceMsg{Group: other, Evidence: ev}},
		{name: "head that does not verify", msg: announceMsg{Group: group, Heads: []kololog.Head{bad}}},
		{name: "record answer without a request", msg: recordMsg{Record: &rec}},
		{name: "entry answer without a request", msg: entryMsg{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rl := m.raw(h.id, v)
			if tc.msg != nil {
				rl.msg(t, tc.msg, bd)
			} else {
				rl.send(t, tc.frame)
			}
			if err := rl.closed(t); !errors.Is(err, ErrProtocol) {
				t.Fatalf("ServeConn: %v", err)
			}
		})
	}
}

// TestHostileEntryAnswers: an ENTRY answer with a head of another strand, a bad proof, a missing ciphertext (a delete marker
// has no place in it), entries outside the range asked or of another history closes the connection, and nothing is placed.
func TestHostileEntryAnswers(t *testing.T) {
	m, v, h := hostileSetup(t)
	bd := v.b.groupBounds()
	strand := h.b.meta.OwnStrand
	mustAppend(t, h.b, "h=2")
	hh := headOfPeer(t, h)
	item := func(i uint64) entryItem {
		e, err := h.b.own.Entry(i)
		if err != nil {
			t.Fatal(err)
		}
		rec, _ := kololog.DecodeRecord(e.Record)
		p, err := h.b.own.InclusionProof(i, hh.Size)
		if err != nil {
			t.Fatal(err)
		}
		return entryItem{Record: rec, Ciphertext: e.Ciphertext, Key: e.Key, Proof: p}
	}
	n := hh.Size
	last := item(n - 1)
	target := dag.Link{Strand: dag.StrandID(strand), Index: n - 1, ID: dag.ID(entryIDOf(last.Record.Encode()))}
	vh := headOfPeer(t, v)
	badProof := last
	badProof.Proof.Path = append([][32]byte{{1}}, badProof.Proof.Path...)
	noCipher := last
	noCipher.Ciphertext = nil
	cases := []struct {
		name   string
		target dag.Link
		start  uint64
		m      entryMsg
	}{
		{"head of another strand", target, n - 1, entryMsg{Head: &vh, Entries: []entryItem{last}}},
		{"bad proof", target, n - 1, entryMsg{Head: &hh, Entries: []entryItem{badProof}}},
		{"no ciphertext", target, n - 1, entryMsg{Head: &hh, Entries: []entryItem{noCipher}}},
		{"below the start asked", target, n - 1, entryMsg{Head: &hh, Entries: []entryItem{item(n - 2), last}}},
		{"another history", dag.Link{Strand: target.Strand, Index: n - 1, ID: dag.ID{7}}, n - 1, entryMsg{Head: &hh, Entries: []entryItem{last}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rl := m.raw(h.id, v)
			r := remoteOf(t, v, h.id.PublicKey())
			expect(v, r, tc.target, tc.start)
			rl.msg(t, tc.m, bd)
			if err := rl.closed(t); !errors.Is(err, ErrProtocol) {
				t.Fatalf("ServeConn: %v", err)
			}
		})
	}
	v.b.mu.Lock()
	pending, _ := v.b.d.Pending()
	v.b.mu.Unlock()
	if pending != 0 {
		t.Fatalf("%d entries of hostile answers wait in the DAG", pending)
	}
}

// forkEvidence makes h, whose braid is closed, sign two records at one index of its strand, on two copies of its data, and
// returns them as evidence.
func forkEvidence(t testing.TB, h *peer, strand kololog.LogID) evidenceBody {
	t.Helper()
	var recs [][]byte
	for _, v := range []string{"one", "two"} {
		db := PrefixBackend(copyBackend(t, h.db), nil)
		st := kolostore.New(prefixdb.New(db, append([]byte{storePrefix}, keyOf(h)...)))
		l, err := st.Open(h.id, strand, kololog.WithValueEncoding(kololog.Bytes))
		if err != nil {
			t.Fatal(err)
		}
		i, err := l.Append([]byte(v))
		if err != nil {
			t.Fatal(err)
		}
		r, err := l.Record(i)
		if err != nil {
			t.Fatal(err)
		}
		recs = append(recs, r.Encode())
		l.Close()
		st.Close()
	}
	return evidenceBody{Key: keyOf(h), Strand: strand, Type: evidenceRecords, A: recs[0], B: recs[1]}
}

// TestHostileEvidence: evidence from a member is taken once and fences the strand it names; the same evidence again on one
// connection closes it.
func TestHostileEvidence(t *testing.T) {
	m := newMesh(t)
	ps := grow(t, m, 3)
	v, h, w := ps[0], ps[1], ps[2]
	mustAppend(t, h.b, "h=1")
	waitNet(t, 20*time.Second, ps, func() bool { return synced(ps...) && settledGet(t, v.b, "h") == "1" })
	m.isolate(h)
	m.isolate(w)
	bd := v.b.groupBounds()
	strand := h.b.meta.OwnStrand
	h.close()
	ev := forkEvidence(t, h, strand)
	rl := m.raw(w.id, v)
	rl.msg(t, evidenceMsg{Group: v.b.meta.Group, Evidence: ev}, bd)
	waitUntil(t, 10*time.Second, func() bool { return v.b.holdsEvidence(ev.Strand) })
	waitEvent(t, v.b, EventEquivocation, 10*time.Second)
	rl.msg(t, evidenceMsg{Group: v.b.meta.Group, Evidence: ev}, bd)
	if err := rl.closed(t); !errors.Is(err, ErrProtocol) {
		t.Fatalf("ServeConn: %v", err)
	}
	// evidence against a strand the victim does not follow is ignored
	stranger := newFounder(t, "s", GroupConfig{}, appApply)
	sstrand := stranger.b.meta.OwnStrand
	stranger.close()
	sev := forkEvidence(t, stranger, sstrand)
	rl = m.raw(w.id, v)
	before := v.b.sync.ignored.Load()
	rl.msg(t, evidenceMsg{Group: v.b.meta.Group, Evidence: sev}, bd)
	waitUntil(t, 5*time.Second, func() bool { return v.b.sync.ignored.Load() > before })
	if v.b.holdsEvidence(sstrand) {
		t.Fatal("evidence against a strand not followed was kept")
	}
}

// TestAnnounceReplay: an old head, smaller than what the victim holds, is ignored without being verified; the connection stays.
func TestAnnounceReplay(t *testing.T) {
	m, v, h := hostileSetup(t)
	old := headOfPeer(t, h)
	mustAppend(t, h.b, "x=1")
	waitUntil(t, 5*time.Second, func() bool { return h.b.own.Len() > old.Size })
	rl := m.raw(h.id, v)
	stale := old
	stale.Size--
	stale.Signature = bytes.Repeat([]byte{1}, len(old.Signature))
	rl.msg(t, announceMsg{Group: v.b.meta.Group, Heads: []kololog.Head{stale, old}}, v.b.groupBounds())
	if !rl.open() {
		t.Fatalf("a replayed old head closed the connection: %v", rl.closed(t))
	}
}

// TestSecondRequestBeforeAnswer: a second request of one type before the first is answered breaks the protocol.
func TestSecondRequestBeforeAnswer(t *testing.T) {
	r := &remote{records: make(chan recordReqMsg, 1), entries: make(chan entryReqMsg, 1), wake: make(chan struct{}, 1),
		s: &syncer{b: &Braid{}}}
	for _, m := range []message{recordReqMsg{}, entryReqMsg{}} {
		if err := r.handle(m); err != nil {
			t.Fatal(err)
		}
		if err := r.handle(m); !errors.Is(err, ErrProtocol) {
			t.Fatalf("second %T: %v", m, err)
		}
	}
}

// TestNonMemberRefused: a key that is neither a member's nor a pin's gets nothing from kolo-sync, its braid messages are ignored
// and counted, and the connection is closed once the grace ends.
func TestNonMemberRefused(t *testing.T) {
	m, v, _ := hostileSetup(t)
	v.b.sync.timeout = 500 * time.Millisecond
	stranger := newIdentity(t)
	rl := m.raw(stranger, v)
	_, err := kolosync.FetchHead(ctx10(t), rl.conn, keyOf(v), v.b.meta.OwnStrand)
	if !errors.Is(err, kolosync.ErrNotFound) {
		t.Fatalf("FetchHead by a stranger: %v", err)
	}
	before := v.b.sync.ignored.Load()
	rl.msg(t, recordMsg{}, v.b.groupBounds())
	waitUntil(t, 5*time.Second, func() bool { return v.b.sync.ignored.Load() > before })
	if err := rl.closed(t); !errors.Is(err, ErrNotMember) {
		t.Fatalf("ServeConn: %v", err)
	}
}

// TestServeConnNilOrClosed: a nil connection is refused without a panic, and a closed braid serves nothing.
func TestServeConnNilOrClosed(t *testing.T) {
	f := newFounder(t, "f", GroupConfig{}, appApply)
	if err := f.b.ServeConn(context.Background(), nil); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("ServeConn(nil): %v", err)
	}
	f.close()
	m := newMesh(t)
	g := newFounder(t, "g", GroupConfig{}, appApply)
	b := g.b
	b.Close()
	if err := b.ServeConn(context.Background(), m.dialOnly(t, g)); !errors.Is(err, ErrClosed) {
		t.Fatalf("ServeConn after Close: %v", err)
	}
}

// dialOnly returns a connection to a listener of p's key that nobody serves.
func (m *mesh) dialOnly(t testing.TB, p *peer) *kolosecure.Conn {
	t.Helper()
	l, err := kolosecure.Listen(m.ctx, "127.0.0.1:0", p.id, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	go l.Accept(m.ctx)
	c, err := kolosecure.Dial(m.ctx, l.Addr().String(), newIdentity(t), p.id.PublicKey(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

// deleteOwn erases entry i of p's own strand while p's braid is closed, as kolo-store Log.Delete would, and returns its marker.
func deleteOwn(t testing.TB, p *peer, strand kololog.LogID, i uint64) []byte {
	t.Helper()
	db := PrefixBackend(p.db, nil)
	st := kolostore.New(prefixdb.New(db, append([]byte{storePrefix}, keyOf(p)...)))
	defer st.Close()
	l, err := st.Open(p.id, strand, kololog.WithValueEncoding(kololog.Bytes))
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if err := l.Delete(i); err != nil {
		t.Fatal(err)
	}
	e, err := l.Entry(i)
	if err != nil || len(e.Marker) == 0 {
		t.Fatalf("no marker: %v", err)
	}
	return e.Marker
}

// noMarkers fails if any replica of p holds a delete marker, other than the ones listed as already there.
func noMarkers(t testing.TB, p *peer, allowed map[kololog.LogID]uint64) {
	t.Helper()
	p.b.mu.Lock()
	defer p.b.mu.Unlock()
	for id, st := range p.b.strands {
		if st.rep == nil {
			continue
		}
		for i := range st.rep.Len() {
			info, err := st.rep.EntryInfo(i)
			if err != nil {
				t.Fatal(err)
			}
			if a, ok := allowed[id]; info.Deleted && (!ok || a != i) {
				t.Fatalf("%s: a delete marker at entry %d of a braid strand", p.name, i)
			}
		}
	}
}

// TestMarkersNeverStored: a member erases one of its entries and serves the marker. Pulls stop before it and go on from a
// peer with the live entry (ErrMarked), a replica that holds the marker repairs the entry from that peer, and every peer
// settles the same view with the entry and no marker stored.
func TestMarkersNeverStored(t *testing.T) {
	m := newMesh(t)
	f := newFounder(t, "f", GroupConfig{}, appApply)
	plain := Role(0)
	var ps []*peer
	for _, name := range []string{"w", "x", "y"} {
		ps = append(ps, m.join(f, name, &AdmitOptions{Roles: &plain}))
	}
	all := append([]*peer{f}, ps...)
	m.full(all...)
	waitNet(t, 30*time.Second, all, func() bool { return synced(all...) })
	w, x, y := ps[0], ps[1], ps[2]
	m.isolate(f)
	base := w.b.own.Len()
	for _, s := range []string{"e1=1", "e2=2", "e3=3"} {
		mustAppend(t, w.b, s)
	}
	strand := w.b.meta.OwnStrand
	waitNet(t, 20*time.Second, ps, func() bool {
		return lengths(x)[strand] == w.b.own.Len() && lengths(y)[strand] == w.b.own.Len()
	})
	header := mustHeader(t, w, strand)
	for _, p := range ps {
		m.isolate(p)
		p.close()
	}
	marker := deleteOwn(t, w, strand, base+1)
	db := PrefixBackend(x.db, nil)
	st := kolostore.New(prefixdb.New(db, append([]byte{storePrefix}, keyOf(w)...)))
	rep, err := st.OpenReplica(keyOf(w), strand, header)
	if err != nil {
		t.Fatal(err)
	}
	if err := rep.ApplyDelete(marker); err != nil {
		t.Fatal(err)
	}
	rep.Close()
	st.Close()
	for _, p := range ps {
		p.reopen()
	}
	// the writer serves its erased entry as a marker: the pull stops right before it
	m.connect(f, w)
	waitUntil(t, 20*time.Second, func() bool {
		r := remoteOf(t, f, keyOf(w))
		f.b.sync.mu.Lock()
		defer f.b.sync.mu.Unlock()
		return r.marked[strand]
	})
	if got := lengths(f)[strand]; got != base+1 {
		t.Fatalf("the founder holds %d entries of the strand, want %d", got, base+1)
	}
	m.full(all...)
	waitNet(t, 60*time.Second, all, func() bool {
		return synced(all...) && settledGet(t, f.b, "e3") == "3" && settledGet(t, x.b, "e2") == "2"
	})
	checkSame(t, all)
	noMarkers(t, f, nil)
	noMarkers(t, y, nil)
	noMarkers(t, x, map[kololog.LogID]uint64{strand: base + 1})
	for _, p := range []*peer{x, w} {
		rec, err := p.b.strandFor(strand).recordAt(base + 1)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok, err := p.b.hist.entry(strand, base+1, entryIDOf(rec.Encode())); err != nil || !ok {
			t.Fatalf("%s: no repair copy of the marked entry: %v", p.name, err)
		}
	}
}

func mustHeader(t testing.TB, p *peer, strand kololog.LogID) []byte {
	t.Helper()
	p.b.mu.Lock()
	defer p.b.mu.Unlock()
	if st := p.b.strands[strand]; st != nil {
		return st.header
	}
	t.Fatal("strand not followed")
	return nil
}

// TestRemovedMemberClosed: a connection of a member stays open while it is a member, and is closed as an unknown peer once
// its removal is in the roster.
func TestRemovedMemberClosed(t *testing.T) {
	m, v, h := hostileSetup(t)
	v.b.sync.timeout = 300 * time.Millisecond
	h.close()
	rl := m.raw(h.id, v)
	r := remoteOf(t, v, keyOf(h))
	waitUntil(t, 5*time.Second, func() bool { return r.known.Load() })
	// the first check of the key passed while it was a member: the connection stays and is checked again later
	waitUntil(t, 5*time.Second, func() bool { return v.b.sync.graces.Load() > 0 })
	if !rl.open() {
		t.Fatal("a member's connection was closed")
	}
	if _, err := v.b.RemoveMember(ctx10(t), keyOf(h)); err != nil {
		t.Fatal(err)
	}
	if err := rl.closed(t); !errors.Is(err, ErrNotMember) {
		t.Fatalf("ServeConn of a removed member: %v", err)
	}
}

// TestSameSizeHeadIsEvidence: a head of the size the victim holds with another root, from a member, is checked once and turned
// into evidence against the strand, though it is no larger than what is held.
func TestSameSizeHeadIsEvidence(t *testing.T) {
	m := newMesh(t)
	ps := grow(t, m, 2)
	v, h := ps[0], ps[1]
	mustAppend(t, h.b, "a=1")
	waitNet(t, 20*time.Second, ps, func() bool { return synced(ps...) && settledGet(t, v.b, "a") == "1" })
	copied := copyBackend(t, h.db)
	mustAppend(t, h.b, "b=1")
	waitNet(t, 20*time.Second, ps, func() bool { return synced(ps...) && settledGet(t, v.b, "b") == "1" })
	m.isolate(h)
	strand := h.b.meta.OwnStrand
	held, _ := v.b.heldHead(strand)
	h.close()
	st := kolostore.New(prefixdb.New(PrefixBackend(copied, nil), append([]byte{storePrefix}, keyOf(h)...)))
	defer st.Close()
	l, err := st.Open(h.id, strand, kololog.WithValueEncoding(kololog.Bytes))
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if _, err := l.Append([]byte("c")); err != nil {
		t.Fatal(err)
	}
	fork, err := l.Head()
	if err != nil || fork.Size != held.Size {
		t.Fatalf("fork head of size %d, victim holds %d: %v", fork.Size, held.Size, err)
	}
	rl := m.raw(h.id, v)
	rl.msg(t, announceMsg{Group: v.b.meta.Group, Heads: []kololog.Head{fork}}, v.b.groupBounds())
	waitUntil(t, 10*time.Second, func() bool { return v.b.holdsEvidence(strand) })
	if !rl.open() {
		t.Fatal("the victim closed the connection of a peer that showed a fork")
	}
}

// TestUnknownHeadBlocksNothing: a head that another key signed for a strand nobody follows does not make Admit refuse that
// strand, whoever announced it; the joiner's own head does.
func TestUnknownHeadBlocksNothing(t *testing.T) {
	m, v, _ := hostileSetup(t)
	s := newSigner(t)
	s.append(t, "x")
	other := newSigner(t)
	rl := m.raw(newIdentity(t), v)
	before := v.b.sync.ignored.Load()
	rl.msg(t, announceMsg{Group: v.b.meta.Group, Heads: []kololog.Head{s.head(t)}}, v.b.groupBounds())
	waitUntil(t, 5*time.Second, func() bool { return v.b.sync.ignored.Load() > before })
	id := s.head(t).LogID
	if v.b.sync.pendingHead(id, other.log.Header().LogKey) {
		t.Fatal("a head of another key blocks the admission of a strand")
	}
	if !v.b.sync.pendingHead(id, s.log.Header().LogKey) {
		t.Fatal("the joiner's own head is not counted")
	}
}

// TestUnknownPeerFramesSkipped: before a key is known, a frame other than ANNOUNCE is dropped from its prefix, so an EVIDENCE
// frame whose signatures do not verify costs no check and breaks nothing; the key is closed as unknown after the grace.
func TestUnknownPeerFramesSkipped(t *testing.T) {
	m, v, h := hostileSetup(t)
	v.b.sync.timeout = 500 * time.Millisecond
	strand := h.b.meta.OwnStrand
	h.close()
	ev := forkEvidence(t, h, strand)
	f, err := encodeMessage(evidenceMsg{Group: v.b.meta.Group, Evidence: ev}, v.b.groupBounds())
	if err != nil {
		t.Fatal(err)
	}
	f[len(f)-1] ^= 1 // the last signature no longer verifies
	rl := m.raw(newIdentity(t), v)
	before := v.b.sync.ignored.Load()
	rl.send(t, f)
	waitUntil(t, 5*time.Second, func() bool { return v.b.sync.ignored.Load() > before })
	if err := rl.closed(t); !errors.Is(err, ErrNotMember) {
		t.Fatalf("ServeConn: %v", err)
	}
	if v.b.holdsEvidence(ev.Strand) {
		t.Fatal("evidence from an unknown key was taken")
	}
}

// TestAnswerWithoutRequestRefusedAtPrefix: an ENTRY frame from a member that was asked nothing breaks the protocol from its
// prefix, before its payload is read.
func TestAnswerWithoutRequestRefusedAtPrefix(t *testing.T) {
	m, v, h := hostileSetup(t)
	h.close()
	rl := m.raw(h.id, v)
	// the prefix of a 1000-byte ENTRY frame, with no payload after it
	rl.send(t, append(binary.LittleEndian.AppendUint32(nil, 1000), 0, byte(msgEntry)))
	err := rl.closed(t)
	if !errors.Is(err, ErrProtocol) || !strings.Contains(err.Error(), "without a request") {
		t.Fatalf("ServeConn: %v", err)
	}
}
