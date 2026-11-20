package kolobraid

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	koloid "github.com/ciranda-dev/kolo-id"
	kolorelay "github.com/ciranda-dev/kolo-relay"
	kolosecure "github.com/ciranda-dev/kolo-secure"
	kolosync "github.com/ciranda-dev/kolo-sync"
	kolotransport "github.com/ciranda-dev/kolo-transport"
)

func loopSecure() *kolosecure.Config {
	return &kolosecure.Config{Transport: &kolotransport.Config{MaxConns: 256, MaxConnsPerIP: 256, MaxDialed: 256}}
}

// tapRelay stands where a relay operator stands: peers reserve and dial through it, and it passes every stream on to a real
// kolo-relay on 127.0.0.1 as the same peer, keeping every byte the operator can read. It stands in only for the identities
// it is given.
type tapRelay struct {
	addr  string
	key   ed25519.PublicKey
	to    string
	toKey ed25519.PublicKey
	ids   map[string]*koloid.Identity
	wg    sync.WaitGroup
	mu    sync.Mutex
	seen  []byte
}

func startTapRelay(t *testing.T, ids ...*koloid.Identity) *tapRelay {
	t.Helper()
	rid := newIdentity(t)
	srv, err := kolorelay.Listen(context.Background(), "127.0.0.1:0", rid, &kolorelay.ServerConfig{Secure: loopSecure()})
	if err != nil {
		t.Fatal(err)
	}
	id := newIdentity(t)
	ctx, cancel := context.WithCancel(context.Background())
	ln, err := kolosecure.Listen(ctx, "127.0.0.1:0", id, loopSecure())
	if err != nil {
		t.Fatal(err)
	}
	tap := &tapRelay{addr: ln.Addr().String(), key: id.PublicKey(), to: srv.Addr().String(), toKey: rid.PublicKey(),
		ids: map[string]*koloid.Identity{}}
	for _, x := range ids {
		tap.ids[string(x.PublicKey())] = x
	}
	tap.wg.Go(func() {
		for {
			c, err := ln.Accept(ctx)
			if err != nil {
				return
			}
			tap.wg.Go(func() { tap.serve(ctx, c) })
		}
	})
	t.Cleanup(func() {
		cancel()
		ln.Close()
		tap.wg.Wait()
		srv.Close()
	})
	return tap
}

func (tap *tapRelay) serve(ctx context.Context, c *kolosecure.Conn) {
	defer c.Close()
	id := tap.ids[string(c.RemotePublicKey())]
	if id == nil {
		return
	}
	rc, err := kolosecure.Dial(ctx, tap.to, id, tap.toKey, &kolosecure.Config{Transport: &kolotransport.Config{KeepAlivePeriod: 5 * time.Second}})
	if err != nil {
		return
	}
	defer rc.Close()
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	context.AfterFunc(c.Context(), cancel)
	context.AfterFunc(rc.Context(), cancel)
	var wg sync.WaitGroup
	wg.Go(func() { tap.join(cctx, c, rc) })
	wg.Go(func() { tap.join(cctx, rc, c) })
	<-cctx.Done()
	c.Close()
	rc.Close()
	wg.Wait()
}

// join passes every stream that from opens to a new stream of to, both ways.
func (tap *tapRelay) join(ctx context.Context, from, to *kolosecure.Conn) {
	for {
		a, err := from.AcceptStream(ctx)
		if err != nil {
			return
		}
		b, err := to.OpenStream(ctx)
		if err != nil {
			a.CancelRead(0)
			a.CancelWrite(0)
			return
		}
		tap.wg.Go(func() { tap.pipe(b, a) })
		tap.wg.Go(func() { tap.pipe(a, b) })
	}
}

// sendSide and readSide are the halves of a stream that pipe uses, bidirectional or unidirectional.
type sendSide interface {
	Write([]byte) (int, error)
	Close() error
	CancelWrite(uint64) error
}

type readSide interface {
	Read([]byte) (int, error)
	CancelRead(uint64) error
}

func (tap *tapRelay) pipe(dst sendSide, src readSide) {
	buf := make([]byte, 32<<10)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			tap.mu.Lock()
			tap.seen = append(tap.seen, buf[:n]...)
			tap.mu.Unlock()
			if _, werr := dst.Write(buf[:n]); werr != nil {
				src.CancelRead(0)
				return
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				dst.Close()
			} else {
				dst.CancelWrite(0)
			}
			return
		}
	}
}

func (tap *tapRelay) captured() []byte {
	tap.mu.Lock()
	defer tap.mu.Unlock()
	return bytes.Clone(tap.seen)
}

// relayPair connects as id to target only through the tap: target holds a reservation there and id dials target's key. It
// returns the dialer's and the target's ends.
func relayPair(t testing.TB, ctx context.Context, tap *tapRelay, id, target *koloid.Identity) (*kolorelay.Conn, *kolorelay.Conn) {
	t.Helper()
	res, err := kolorelay.Reserve(ctx, tap.addr, tap.key, target, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { res.Close() })
	type accepted struct {
		c   *kolorelay.Conn
		err error
	}
	acc := make(chan accepted, 1)
	go func() {
		c, err := res.Accept(ctx)
		acc <- accepted{c, err}
	}()
	ab, err := kolorelay.Dial(ctx, tap.addr, tap.key, id, target.PublicKey(), nil)
	if err != nil {
		t.Fatal(err)
	}
	r := <-acc
	if r.err != nil {
		t.Fatal(r.err)
	}
	return ab, r.c
}

// relayLink links a to c only through the tap and runs ServeConn on both ends.
func relayLink(t *testing.T, tap *tapRelay, a, c *peer) (ab, ba *kolorelay.Conn) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	ab, ba = relayPair(t, ctx, tap, a.id, c.id)
	var wg sync.WaitGroup
	sa, sc := a.b, c.b
	wg.Go(func() { sa.ServeConn(ctx, ab) })
	wg.Go(func() { sc.ServeConn(ctx, ba) })
	t.Cleanup(func() {
		cancel()
		ab.Close()
		ba.Close()
		wg.Wait()
	})
	return ab, ba
}

// rawRelayed connects as id to v only through the tap; v serves the connection with ServeConn, the test drives the other end.
func rawRelayed(t *testing.T, tap *tapRelay, id *koloid.Identity, v *peer) *rawLink {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	c, vc := relayPair(t, ctx, tap, id, v.id)
	rl := &rawLink{conn: c, done: make(chan error, 1)}
	b := v.b
	go func() { rl.done <- b.ServeConn(ctx, vc) }()
	t.Cleanup(func() {
		cancel()
		c.Close()
		vc.Close()
		<-rl.done
		rl.done <- nil
	})
	return rl
}

// TestRelayedSync: a founder and a joiner that reach each other only through a real kolo-relay on 127.0.0.1 complete the
// join, a write on each side arrives and settles on the other, and both end with the same settled order, view and running
// digest. The relay operator's bytes hold neither the group ID, which every ANNOUNCE carries in the clear, nor any entry key.
func TestRelayedSync(t *testing.T) {
	f := newFounder(t, "f", GroupConfig{}, appApply)
	j := newMesh(t).join(f, "j", nil)
	tap := startTapRelay(t, f.id, j.id)
	jf, fj := relayLink(t, tap, j, f)
	if !jf.RelayPublicKey().Equal(tap.key) || !fj.RelayPublicKey().Equal(tap.key) {
		t.Fatal("the link does not go through the tap")
	}
	if r := remoteOf(t, f, keyOf(j)); !r.key.Equal(keyOf(j)) {
		t.Fatalf("the founder serves key %x, not the joiner's", r.key)
	}
	ps := []*peer{f, j}
	twoWrites(t, ps)
	seen := tap.captured()
	if len(seen) == 0 {
		t.Fatal("the tap saw no bytes")
	}
	n, leak := leaked(seen, ps)
	if leak != "" {
		t.Fatalf("the relay operator saw %s", leak)
	}
	t.Logf("the tap saw %d bytes, without the group ID or any of %d entry keys", len(seen), n-1)
}

// twoWrites waits for the joiner ps[1] to start, then writes on both peers and waits until each settles the other's write.
func twoWrites(t *testing.T, ps []*peer) {
	t.Helper()
	f, j := ps[0], ps[1]
	waitNet(t, 30*time.Second, ps, func() bool { return !waiting(j) && hasMember(j.b, keyOf(j), false) })
	var nonce [16]byte
	rand.Read(nonce[:])
	text := hex.EncodeToString(nonce[:])
	mustAppend(t, j.b, "m="+text)
	mustAppend(t, f.b, "a=1")
	waitNet(t, 30*time.Second, ps, func() bool {
		return synced(ps...) && settledGet(t, f.b, "m") == text && settledGet(t, j.b, "a") == "1"
	})
	checkSame(t, ps)
}

// leaked looks in seen for the group ID and every entry key the peers hold; it returns how many needles it looked for and the
// first one found.
func leaked(seen []byte, ps []*peer) (int, string) {
	group := ps[0].b.meta.Group
	n := 1
	if bytes.Contains(seen, group[:]) {
		return n, "the group ID"
	}
	for _, p := range ps {
		for _, id := range p.b.followed() {
			st := p.b.strandFor(id)
			for i := range st.length() {
				k, err := keyAt(st, i)
				if err != nil {
					continue
				}
				n++
				if bytes.Contains(seen, k) {
					return n, fmt.Sprintf("the key of entry %d of a strand of %s", i, p.name)
				}
			}
		}
	}
	return n, ""
}

// clearTap copies every stream between two kolo-secure connections it terminates: the bytes an operator would see if the
// braid link were not encrypted end to end.
type clearTap struct {
	tapRelay
}

func (x *clearTap) join(ctx context.Context, from, to *kolosecure.Conn) {
	x.wg.Go(func() { x.tapRelay.join(ctx, from, to) })
	x.wg.Go(func() {
		for {
			a, err := from.AcceptUniStream(ctx)
			if err != nil {
				return
			}
			b, err := to.OpenUniStream(ctx)
			if err != nil {
				a.CancelRead(0)
				return
			}
			x.wg.Go(func() { x.pipe(b, a) })
		}
	})
}

// TestLeakCheckSeesCleartext: the check of TestRelayedSync, run on the plaintext of a braid link, finds the group ID and the
// entry keys, so it would fail on a link in the clear.
func TestLeakCheckSeesCleartext(t *testing.T) {
	f := newFounder(t, "f", GroupConfig{}, appApply)
	j := newMesh(t).join(f, "j", nil)
	ctx, cancel := context.WithCancel(context.Background())
	pair := func(dialer, target *koloid.Identity) (*kolosecure.Conn, *kolosecure.Conn) {
		l, err := kolosecure.Listen(ctx, "127.0.0.1:0", target, loopSecure())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { l.Close() })
		acc := make(chan *kolosecure.Conn, 1)
		go func() {
			c, _ := l.Accept(ctx)
			acc <- c
		}()
		d, err := kolosecure.Dial(ctx, l.Addr().String(), dialer, target.PublicKey(), loopSecure())
		if err != nil {
			t.Fatal(err)
		}
		return d, <-acc
	}
	x := &clearTap{}
	jx, xj := pair(j.id, f.id)
	xf, fx := pair(j.id, f.id)
	x.join(ctx, xj, xf)
	x.join(ctx, xf, xj)
	var wg sync.WaitGroup
	wg.Go(func() { j.b.ServeConn(ctx, jx) })
	wg.Go(func() { f.b.ServeConn(ctx, fx) })
	t.Cleanup(func() {
		cancel()
		for _, c := range []*kolosecure.Conn{jx, xj, xf, fx} {
			c.Close()
		}
		wg.Wait()
		x.wg.Wait()
	})
	ps := []*peer{f, j}
	twoWrites(t, ps)
	if _, leak := leaked(x.captured(), ps); leak != "the group ID" {
		t.Fatalf("the plaintext of the link: %q, want the group ID", leak)
	}
	group := f.b.meta.Group
	if _, leak := leaked(bytes.ReplaceAll(x.captured(), group[:], nil), ps); leak == "" {
		t.Fatal("no entry key in the plaintext of the link")
	}
}

func keyAt(st *strand, i uint64) ([]byte, error) {
	if st.log != nil {
		return st.log.Key(i)
	}
	return st.rep.Key(i)
}

// TestRelayedRefusals: a relayed connection is refused exactly like a direct one when its proved key is not a member's: a
// stranger and a member of another braid get nothing from kolo-sync, their braid messages are ignored and counted, and the
// connection is closed with ErrNotMember once the grace ends. The key the victim checks is the one proved end to end.
func TestRelayedRefusals(t *testing.T) {
	other := newFounder(t, "other", GroupConfig{}, appApply)
	for _, who := range []string{"stranger", "other group"} {
		type outcome struct{ fetch, closed error }
		var got [2]outcome
		for i, relayed := range []bool{false, true} {
			m, v, _ := hostileSetup(t)
			v.b.sync.timeout = 500 * time.Millisecond
			id := newIdentity(t)
			if who == "other group" {
				id = other.id
			}
			var rl *rawLink
			if relayed {
				rl = rawRelayed(t, startTapRelay(t, id, v.id), id, v)
				if _, ok := rl.conn.(*kolorelay.Conn); !ok {
					t.Fatalf("%s: the link is %T, not relayed", who, rl.conn)
				}
			} else {
				rl = m.raw(id, v)
			}
			if r := remoteOf(t, v, id.PublicKey()); !r.key.Equal(id.PublicKey()) {
				t.Fatalf("%s relayed=%v: victim checks key %x", who, relayed, r.key)
			}
			told := make(chan bool, 1)
			go func() {
				_, err := rl.conn.AcceptUniStream(ctx10(t))
				told <- err == nil
			}()
			_, got[i].fetch = kolosync.FetchHead(ctx10(t), rl.conn, keyOf(v), v.b.meta.OwnStrand)
			before := v.b.sync.ignored.Load()
			rl.msg(t, recordMsg{}, v.b.groupBounds())
			waitUntil(t, 5*time.Second, func() bool { return v.b.sync.ignored.Load() > before })
			got[i].closed = rl.closed(t)
			if <-told {
				t.Fatalf("%s relayed=%v: the victim sent a braid message to an unknown key", who, relayed)
			}
		}
		for i, o := range got {
			if !errors.Is(o.fetch, kolosync.ErrNotFound) || !errors.Is(o.closed, ErrNotMember) {
				t.Fatalf("%s relayed=%v: FetchHead %v, ServeConn %v", who, i == 1, o.fetch, o.closed)
			}
		}
	}
}
