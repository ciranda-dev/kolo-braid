package kolobraid

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	kololog "github.com/ciranda-dev/kolo-log"
	kolosync "github.com/ciranda-dev/kolo-sync"
	kolotransport "github.com/ciranda-dev/kolo-transport"

	"github.com/ciranda-dev/kolo-braid/internal/dag"
)

const (
	// maxPulls is how many strand pulls run at once per braid: kolo-sync's streams per connection.
	maxPulls = kolosync.MaxStreams
	// codeProtocol closes a connection whose peer broke the protocol.
	codeProtocol = 1
)

// streamTimeout bounds each stream message, the answer to a request and the grace of an unknown key: kolo-sync's Timeout.
var streamTimeout = kolosync.Timeout

var errUnknownPeer = fmt.Errorf("%w: the peer's key is neither a member's nor a pin's", ErrNotMember)

// syncer is a braid's sync layer: one kolo-sync Server for its strands and the state shared by its connections.
type syncer struct {
	b   *Braid
	srv *kolosync.Server
	// wake asks the scheduler to look again; started is set, under b.mu, once its goroutine runs.
	wake    chan struct{}
	started bool
	// ignored counts the messages of unknown keys; entryReqTap, when a test sets it, sees every ENTRY_REQ received.
	ignored     atomic.Uint64
	entryReqTap atomic.Pointer[func(entryReqMsg)]
	// runs counts the scheduler's runs and graces the checks of a connection's key, so a test can tell that one ran.
	runs    atomic.Uint64
	graces  atomic.Uint64
	timeout time.Duration

	mu      sync.Mutex
	remotes map[*remote]struct{}
	pulling map[kololog.LogID]bool
	running int
	catch   bool
	asked   map[dag.Link]map[*remote]bool
	missing map[dag.Link]bool
	window  map[kololog.LogID]uint64
	cursors map[kololog.LogID]uint64
	// topHead is the largest verified head size seen per strand.
	topHead map[kololog.LogID]uint64
}

func newSyncer(b *Braid) *syncer {
	s := &syncer{b: b, wake: make(chan struct{}, 1), timeout: streamTimeout, remotes: map[*remote]struct{}{}, pulling: map[kololog.LogID]bool{},
		asked: map[dag.Link]map[*remote]bool{}, missing: map[dag.Link]bool{}, window: map[kololog.LogID]uint64{},
		cursors: map[kololog.LogID]uint64{}, topHead: map[kololog.LogID]uint64{}}
	s.srv = kolosync.NewServer(func(peer, _ ed25519.PublicKey, _ kololog.LogID) kolosync.Access {
		switch b.access(peer) {
		case accessKeys:
			return kolosync.Keys
		case accessCiphertext:
			return kolosync.Ciphertext
		}
		return kolosync.NoAccess
	})
	return s
}

func (s *syncer) signal() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *syncer) offer(st *strand) {
	h, err := kololog.DecodeLogHeader(st.header)
	if err != nil {
		return
	}
	_ = s.srv.Offer(strandSource{b: s.b, id: st.id, header: h})
}

// strandSource serves whatever log or replica holds a strand now, so the own strand stays served after a catch-up replaced it.
type strandSource struct {
	b      *Braid
	id     kololog.LogID
	header kololog.LogHeader
}

func (x strandSource) src() (kolosync.Source, error) {
	x.b.mu.Lock()
	defer x.b.mu.Unlock()
	st := x.b.strands[x.id]
	switch {
	case st == nil:
		return nil, kololog.ErrOutOfRange
	case st.log != nil:
		return st.log, nil
	}
	return st.rep, nil
}

func (x strandSource) ID() kololog.LogID         { return x.id }
func (x strandSource) Header() kololog.LogHeader { return x.header }
func (x strandSource) Head() (h kololog.Head, err error) {
	if src, err := x.src(); err == nil {
		return src.Head()
	}
	return h, kololog.ErrNoHead
}

func (x strandSource) Record(i uint64) (kololog.Record, error) {
	src, err := x.src()
	if err != nil {
		return kololog.Record{}, err
	}
	return src.Record(i)
}

func (x strandSource) Entry(i uint64) (kololog.Entry, error) {
	src, err := x.src()
	if err != nil {
		return kololog.Entry{}, err
	}
	return src.Entry(i)
}

func (x strandSource) EntryInfo(i uint64) (kololog.EntryInfo, error) {
	src, err := x.src()
	if err != nil {
		return kololog.EntryInfo{}, err
	}
	return src.EntryInfo(i)
}

func (x strandSource) Key(i uint64) (kololog.EntryKey, error) {
	src, err := x.src()
	if err != nil {
		return nil, err
	}
	return src.Key(i)
}

func (x strandSource) InclusionProof(i, size uint64) (kololog.InclusionProof, error) {
	src, err := x.src()
	if err != nil {
		return kololog.InclusionProof{}, err
	}
	return src.InclusionProof(i, size)
}

func (x strandSource) ConsistencyProof(a, c uint64) (kololog.ConsistencyProof, error) {
	src, err := x.src()
	if err != nil {
		return kololog.ConsistencyProof{}, err
	}
	return src.ConsistencyProof(a, c)
}

// ServeConn syncs the braid with the peer on conn, direct or relayed, until ctx or the connection ends or the braid closes: it
// serves the strands this peer holds through kolo-sync, pulls the peer's, and runs the braid's own messages on unidirectional
// streams. It must be the only caller of conn's AcceptStream and AcceptUniStream. It returns nil when ctx or conn ends,
// ErrClosed when the braid closes, an error wrapping ErrProtocol after it closed the conn of a peer that broke the protocol,
// and one wrapping ErrNotMember after it closed the conn of a peer that was neither a member nor a pin after 60 s. Pass only a
// connection whose peer key was proved in a handshake: the braid trusts conn.RemotePublicKey and hands that key's owner what a member
// or a pin gets.
func (b *Braid) ServeConn(ctx context.Context, conn Conn) error {
	if noConn(conn) {
		return fmt.Errorf("%w: nil connection", ErrInvalidArgument)
	}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return ErrClosed
	}
	b.wg.Add(1)
	b.mu.Unlock()
	defer b.wg.Done()
	return b.sync.serve(ctx, conn)
}

// remote is one connection that ServeConn runs.
type remote struct {
	s      *syncer
	conn   Conn
	key    ed25519.PublicKey
	ctx    context.Context
	cancel context.CancelCauseFunc
	wg     sync.WaitGroup
	wake   chan struct{}
	// records and entries hold the peer's request being answered; out holds the own requests to send.
	records chan recordReqMsg
	entries chan entryReqMsg
	out     chan message
	// known is the scheduler's last answer to whether the peer is a member or a pin.
	known atomic.Bool

	// Under s.mu: heads are the largest verified head sizes the peer announced per strand, unknown the unverified heads of strands
	// not followed, failed the head size at which a pull brought nothing, marked the strands it served a delete marker of, ownSize
	// its verified head of the own strand.
	alive     bool
	heads     map[kololog.LogID]uint64
	unknown   map[kololog.LogID]kololog.Head
	failed    map[kololog.LogID]uint64
	marked    map[kololog.LogID]bool
	ownSize   uint64
	entryReq  *entryReq
	recordReq *recordReq

	// Reader only: sameSeen marks the strands whose head of the held size with other bytes was compared.
	ownSeen  []byte
	gotEv    map[kololog.LogID]bool
	sameSeen map[kololog.LogID]bool
	// Sender only.
	sent   map[kololog.LogID][]byte
	sentEv map[kololog.LogID]bool
}

// entryReq is an outstanding ENTRY_REQ: its target and the start asked for; answering is set once its answer is read.
type entryReq struct {
	target    dag.Link
	start     uint64
	timer     *time.Timer
	answering bool
}

type recordReq struct {
	strand kololog.LogID
	index  uint64
	timer  *time.Timer
}

func (s *syncer) serve(ctx context.Context, conn Conn) error {
	cctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	defer context.AfterFunc(s.b.life, func() { cancel(ErrClosed) })()
	defer context.AfterFunc(conn.Context(), func() { cancel(nil) })()
	r := &remote{s: s, conn: conn, key: conn.RemotePublicKey(), ctx: cctx, cancel: cancel, wake: make(chan struct{}, 1),
		records: make(chan recordReqMsg, 1), entries: make(chan entryReqMsg, 1), out: make(chan message, 2),
		heads: map[kololog.LogID]uint64{}, unknown: map[kololog.LogID]kololog.Head{}, failed: map[kololog.LogID]uint64{},
		marked: map[kololog.LogID]bool{}, gotEv: map[kololog.LogID]bool{}, sameSeen: map[kololog.LogID]bool{}, sent: map[kololog.LogID][]byte{},
		sentEv: map[kololog.LogID]bool{}}
	r.known.Store(s.known(r.key))
	s.mu.Lock()
	r.alive = true
	s.remotes[r] = struct{}{}
	// a new peer may hold what every other peer answered without
	clear(s.missing)
	clear(s.asked)
	// checked again every timeout, so a peer that stops being a member is closed too; the timer is assigned under s.mu, which its
	// callback takes before it reads it
	var grace *time.Timer
	grace = time.AfterFunc(s.timeout, func() {
		defer s.graces.Add(1)
		if !s.known(r.key) {
			r.cancel(errUnknownPeer)
			r.conn.Close()
			return
		}
		s.mu.Lock()
		if r.alive {
			grace.Reset(s.timeout)
		}
		s.mu.Unlock()
	})
	s.mu.Unlock()
	r.wg.Go(func() { s.srv.Serve(cctx, conn) })
	r.wg.Go(r.readLoop)
	r.wg.Go(r.sendLoop)
	r.wg.Go(r.fetchOwnHead)
	s.signal()
	<-cctx.Done()
	s.mu.Lock()
	r.alive = false
	delete(s.remotes, r)
	for _, t := range []*time.Timer{timerOf(r.entryReq), timerOfRecord(r.recordReq)} {
		if t != nil {
			t.Stop()
		}
	}
	s.mu.Unlock()
	grace.Stop()
	r.wg.Wait()
	s.signal()
	switch err := context.Cause(cctx); {
	case errors.Is(err, ErrProtocol), errors.Is(err, ErrNotMember), errors.Is(err, ErrClosed):
		return err
	}
	return nil
}

func timerOf(q *entryReq) *time.Timer {
	if q == nil {
		return nil
	}
	return q.timer
}

func timerOfRecord(q *recordReq) *time.Timer {
	if q == nil {
		return nil
	}
	return q.timer
}

func (r *remote) violate(err error) {
	if !errors.Is(err, ErrProtocol) {
		err = fmt.Errorf("%w: %w", ErrProtocol, err)
	}
	r.s.b.logf("kolobraid: closing a connection", "reason", err.Error())
	// the cause first: closing the connection ends its context, which cancels without one
	r.cancel(err)
	r.conn.CloseWithError(codeProtocol, "kolobraid: protocol violation")
}

func (r *remote) signal() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

// known reports whether a key may exchange braid messages: a member of the tentative or the settled roster, a seat of the
// current or the pending config, a pin, and until a joiner's own AddMember entry is ordered, its inviter and the founder. A
// member whose removal is only tentative still settles, so its links stay.
func (s *syncer) known(k ed25519.PublicKey) bool {
	b := s.b
	if b.access(k) != accessNone {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(k) != ed25519.PublicKeySize {
		return false
	}
	if b.settled != nil {
		if _, ok := b.settled.Member(rosterKey(k)); ok {
			return true
		}
	}
	if !b.waiting && !b.joining() {
		return false
	}
	if bytes.Equal(b.meta.FounderKey, k) {
		return true
	}
	st := b.strands[b.joinInviter]
	return st != nil && bytes.Equal(st.key, k)
}

func (s *syncer) bounds() formatBounds {
	return s.b.groupBounds()
}

func (r *remote) readLoop() {
	for {
		st, err := r.conn.AcceptUniStream(r.ctx)
		if err != nil {
			return
		}
		known := r.known.Load() || r.s.known(r.key)
		m, err := r.read(st, known)
		if errors.Is(err, errSkipped) {
			r.s.ignored.Add(1)
			continue
		}
		if err != nil {
			if r.ctx.Err() == nil && r.conn.Context().Err() == nil {
				r.violate(err)
			}
			return
		}
		if !known {
			r.s.ignored.Add(1)
			if a, ok := m.(announceMsg); ok && a.Group == r.s.b.meta.Group {
				for _, h := range a.Heads {
					r.keepUnknown(h)
				}
			}
			continue
		}
		r.known.Store(true)
		if err := r.handle(m); err != nil {
			r.violate(err)
			return
		}
	}
}

// errSkipped is a message of an unknown peer that is dropped from its prefix: only ANNOUNCE is read before a key is known.
var errSkipped = errors.New("kolobraid: message skipped")

// read reads the one message of a stream, which must end right after it. Before the peer's key is known only an ANNOUNCE is
// read past its prefix, and an answer is read only while a request of its type is outstanding.
func (r *remote) read(st *kolotransport.ReceiveStream, known bool) (message, error) {
	defer st.CancelRead(0)
	if err := st.SetReadDeadline(time.Now().Add(r.s.timeout)); err != nil {
		return nil, err
	}
	m, err := readMessage(st, r.s.bounds(), func(t messageType) error {
		if !known {
			if t != msgAnnounce {
				return errSkipped
			}
			return nil
		}
		r.s.mu.Lock()
		asked := (t != msgEntry || r.entryReq != nil) && (t != msgRecord || r.recordReq != nil)
		r.s.mu.Unlock()
		if !asked {
			return fmt.Errorf("%w: an answer without a request", ErrProtocol)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	var one [1]byte
	if n, err := io.ReadFull(st, one[:]); n > 0 || !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("%w: bytes after the message", ErrProtocol)
	}
	return m, nil
}

func (r *remote) handle(m message) error {
	group := r.s.b.meta.Group
	switch m := m.(type) {
	case announceMsg:
		if m.Group != group {
			return fmt.Errorf("%w: announce of another group", ErrProtocol)
		}
		return r.onAnnounce(m)
	case evidenceMsg:
		if m.Group != group {
			return fmt.Errorf("%w: evidence of another group", ErrProtocol)
		}
		return r.onEvidence(m)
	case recordReqMsg:
		select {
		case r.records <- m:
		default:
			return fmt.Errorf("%w: a second record request before the answer", ErrProtocol)
		}
	case entryReqMsg:
		if tap := r.s.entryReqTap.Load(); tap != nil {
			(*tap)(m)
		}
		select {
		case r.entries <- m:
		default:
			return fmt.Errorf("%w: a second entry request before the answer", ErrProtocol)
		}
	case recordMsg:
		return r.onRecord(m)
	case entryMsg:
		return r.onEntry(m)
	}
	r.signal()
	return nil
}

func (r *remote) sendLoop() {
	t := time.NewTimer(time.Hour)
	defer t.Stop()
	var last time.Time
	for {
		select {
		case <-r.ctx.Done():
			return
		case q := <-r.records:
			r.send(r.s.recordAnswer(r, q))
		case q := <-r.entries:
			r.send(r.s.entryAnswer(r, q))
		case m := <-r.out:
			r.send(m)
		case <-r.wake:
		case <-t.C:
		}
		if !r.known.Load() {
			continue
		}
		if d := r.s.gap() - time.Since(last); d > 0 {
			t.Reset(d)
			continue
		}
		if r.announce() {
			last = time.Now()
		}
		r.sendEvidence()
	}
}

func (r *remote) send(m message) {
	frame, err := encodeMessage(m, r.s.bounds())
	if err != nil {
		r.s.b.logf("kolobraid: message not sent", "error", err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(r.ctx, r.s.timeout)
	defer cancel()
	st, err := r.conn.OpenUniStream(ctx)
	if err != nil {
		return
	}
	defer context.AfterFunc(ctx, func() { st.CancelWrite(0) })()
	if err := st.SetWriteDeadline(time.Now().Add(r.s.timeout)); err != nil {
		st.CancelWrite(0)
		return
	}
	if _, err := st.Write(frame); err != nil {
		st.CancelWrite(0)
		return
	}
	st.Close()
}

// fetchOwnHead asks the peer for its head of the own strand, which confirms or catches up the own strand after Open.
func (r *remote) fetchOwnHead() {
	b := r.s.b
	vh, err := kolosync.FetchHead(r.ctx, r.conn, b.key, b.meta.OwnStrand)
	switch {
	case errors.Is(err, kolosync.ErrInvalid):
		r.violate(err)
	case errors.Is(err, kolosync.ErrNotFound):
		b.ownNone(r.key)
	case err == nil:
		if err := r.s.ownHead(r, vh.Head()); err != nil {
			r.violate(err)
		}
	}
}

// ownHead hands a head of the own strand that the peer holds to the confirmation rules; a head that does not verify breaks the
// protocol.
func (s *syncer) ownHead(r *remote, h kololog.Head) error {
	err := s.b.ownHead(r.key, h)
	if errors.Is(err, ErrInvalidArgument) {
		return fmt.Errorf("%w: a head of the own strand that does not verify", ErrProtocol)
	}
	if err == nil {
		s.mu.Lock()
		r.ownSize = max(r.ownSize, h.Size)
		s.mu.Unlock()
		s.signal()
	}
	return nil
}

// gap is the least time between two announcements on one connection.
func (s *syncer) gap() time.Duration {
	s.b.mu.Lock()
	defer s.b.mu.Unlock()
	return s.b.lim.AnnounceGap
}

// heldHeads returns the head of every followed strand that holds an entry, in strand order; none while waiting for the genesis.
func (b *Braid) heldHeads() []kololog.Head {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.waiting || b.closed {
		return nil
	}
	ids := make([]kololog.LogID, 0, len(b.strands))
	for id := range b.strands {
		ids = append(ids, id)
	}
	slices.SortFunc(ids, func(a, c kololog.LogID) int { return bytes.Compare(a[:], c[:]) })
	var out []kololog.Head
	for _, id := range ids {
		st := b.strands[id]
		var h kololog.Head
		var err error
		if st.log != nil {
			if st.log.Len() == 0 {
				continue
			}
			h, err = st.log.Head()
		} else {
			h, err = st.rep.Head()
		}
		if err == nil {
			out = append(out, h)
		}
	}
	return out
}

// heldHead returns the head this peer holds of a followed strand, ok false when it holds none.
func (b *Braid) heldHead(id kololog.LogID) (kololog.Head, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	st, ok := b.strands[id]
	if !ok || st.length() == 0 {
		return kololog.Head{}, false
	}
	var h kololog.Head
	var err error
	if st.log != nil {
		h, err = st.log.Head()
	} else {
		h, err = st.rep.Head()
	}
	return h, err == nil
}

// strandInfo returns a followed strand's key and the length it holds.
func (b *Braid) strandInfo(id kololog.LogID) (ed25519.PublicKey, uint64, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	st, ok := b.strands[id]
	if !ok {
		return nil, 0, false
	}
	return st.key, st.length(), true
}

func (b *Braid) strandSrc(id kololog.LogID) (kolosync.Source, ed25519.PublicKey, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	st, ok := b.strands[id]
	if !ok {
		return nil, nil, false
	}
	if st.log != nil {
		return st.log, st.key, true
	}
	return st.rep, st.key, true
}
