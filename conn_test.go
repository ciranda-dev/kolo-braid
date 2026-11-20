package kolobraid

import (
	"context"
	"crypto/ed25519"
	"errors"
	"sync"
	"testing"
	"time"

	kolorelay "github.com/ciranda-dev/kolo-relay"
	kolosecure "github.com/ciranda-dev/kolo-secure"
	koloswarm "github.com/ciranda-dev/kolo-swarm"
	kolosync "github.com/ciranda-dev/kolo-sync"
	kolotransport "github.com/ciranda-dev/kolo-transport"
)

var (
	_ Conn           = (*kolosecure.Conn)(nil)
	_ Conn           = (*kolorelay.Conn)(nil)
	_ Conn           = koloswarm.Conn(nil)
	_ koloswarm.Conn = Conn(nil)
	_ kolosync.Conn  = Conn(nil)
)

// countConn is a Conn of its own type over another one; it counts the calls of each method.
type countConn struct {
	c     Conn
	mu    sync.Mutex
	calls map[string]int
}

func (c *countConn) count(name string) {
	c.mu.Lock()
	c.calls[name]++
	c.mu.Unlock()
}

func (c *countConn) called(name string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls[name]
}

func (c *countConn) RemotePublicKey() ed25519.PublicKey {
	c.count("RemotePublicKey")
	return c.c.RemotePublicKey()
}

func (c *countConn) OpenStream(ctx context.Context) (*kolotransport.Stream, error) {
	c.count("OpenStream")
	return c.c.OpenStream(ctx)
}

func (c *countConn) AcceptStream(ctx context.Context) (*kolotransport.Stream, error) {
	c.count("AcceptStream")
	return c.c.AcceptStream(ctx)
}

func (c *countConn) OpenUniStream(ctx context.Context) (*kolotransport.SendStream, error) {
	c.count("OpenUniStream")
	return c.c.OpenUniStream(ctx)
}

func (c *countConn) AcceptUniStream(ctx context.Context) (*kolotransport.ReceiveStream, error) {
	c.count("AcceptUniStream")
	return c.c.AcceptUniStream(ctx)
}

func (c *countConn) Context() context.Context {
	c.count("Context")
	return c.c.Context()
}

func (c *countConn) Close() error {
	c.count("Close")
	return c.c.Close()
}

func (c *countConn) CloseWithError(code uint64, msg string) error {
	c.count("CloseWithError")
	return c.c.CloseWithError(code, msg)
}

// wrappedPair returns both ends of a loopback kolo-secure connection from a to c, each behind a countConn.
func wrappedPair(t *testing.T, ctx context.Context, a, c *peer) (ac, ca *countConn) {
	t.Helper()
	l, err := kolosecure.Listen(ctx, "127.0.0.1:0", c.id, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	acc := make(chan *kolosecure.Conn, 1)
	go func() {
		x, _ := l.Accept(ctx)
		acc <- x
	}()
	d, err := kolosecure.Dial(ctx, l.Addr().String(), a.id, c.id.PublicKey(), nil)
	if err != nil {
		t.Fatal(err)
	}
	x := <-acc
	if x == nil {
		t.Fatal("no connection accepted")
	}
	t.Cleanup(func() {
		d.Close()
		x.Close()
	})
	return &countConn{c: d, calls: map[string]int{}}, &countConn{c: x, calls: map[string]int{}}
}

// TestServeConnOnAnyConn: ServeConn needs nothing but the Conn methods: two members sync over connections of a type of the
// test's own, and every one of the eight methods is used, so none can be left out. A typed nil is refused like nil.
func TestServeConnOnAnyConn(t *testing.T) {
	f := newFounder(t, "f", GroupConfig{}, appApply)
	j := newMesh(t).join(f, "j", nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	jf, fj := wrappedPair(t, ctx, j, f)
	var wg sync.WaitGroup
	defer wg.Wait()
	defer cancel()
	wg.Go(func() { j.b.ServeConn(ctx, jf) })
	wg.Go(func() { f.b.ServeConn(ctx, fj) })
	ps := []*peer{f, j}
	waitNet(t, 30*time.Second, ps, func() bool { return !waiting(j) && hasMember(j.b, keyOf(j), false) })
	mustAppend(t, j.b, "b=2")
	waitNet(t, 30*time.Second, ps, func() bool { return synced(ps...) && settledGet(t, f.b, "b") == "2" })
	checkSame(t, ps)
	for _, name := range []string{"RemotePublicKey", "OpenStream", "AcceptStream", "OpenUniStream", "AcceptUniStream", "Context"} {
		if fj.called(name) == 0 || jf.called(name) == 0 {
			t.Errorf("%s was not used", name)
		}
	}

	// a stranger is closed with Close after the grace, a member that breaks the protocol with CloseWithError
	m, v, h := hostileSetup(t)
	v.b.sync.timeout = 300 * time.Millisecond
	for _, tc := range []struct {
		who  *peer
		bad  bool
		want error
		call string
	}{
		{&peer{id: newIdentity(t)}, false, ErrNotMember, "Close"},
		{h, true, ErrProtocol, "CloseWithError"},
	} {
		ws, sv := wrappedPair(t, m.ctx, tc.who, v)
		done := make(chan error, 1)
		go func() { done <- v.b.ServeConn(m.ctx, sv) }()
		if tc.bad {
			waitUntil(t, 5*time.Second, func() bool { return remoteOf(t, v, keyOf(h)).known.Load() })
			rl := &rawLink{conn: ws}
			rl.send(t, frameOf(0xff, 0xff, nil))
		}
		select {
		case err := <-done:
			if !errors.Is(err, tc.want) {
				t.Fatalf("%s: ServeConn %v", tc.call, err)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("%s: the connection stayed open", tc.call)
		}
		if sv.called(tc.call) == 0 {
			t.Errorf("%s was not used", tc.call)
		}
	}

	for _, c := range []Conn{nil, (*kolosecure.Conn)(nil), (*kolorelay.Conn)(nil), (*countConn)(nil)} {
		if err := f.b.ServeConn(context.Background(), c); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("ServeConn(%T nil): %v", c, err)
		}
	}
}
