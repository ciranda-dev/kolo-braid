package kv_test

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	koloid "github.com/ciranda-dev/kolo-id"
	kolosecure "github.com/ciranda-dev/kolo-secure"
	kolostore "github.com/ciranda-dev/kolo-store"

	kolobraid "github.com/ciranda-dev/kolo-braid"
	"github.com/ciranda-dev/kolo-braid/kv"
)

// members starts a braid of two members on memory backends whose apply routes kv entries to s; connect links them over
// kolo-secure on 127.0.0.1 and returns a function that cuts the link.
func members(t *testing.T, s *kv.Store) (a, b *kolobraid.Braid, connect func() (cut func())) {
	t.Helper()
	return gatedMembers(t, s, nil)
}

// gatedMembers is members whose apply waits for gate to close before it handles a data entry; a nil gate never waits.
func gatedMembers(t *testing.T, s *kv.Store, gate <-chan struct{}) (a, b *kolobraid.Braid, connect func() (cut func())) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	apply := func(tx *kolobraid.Tx, e *kolobraid.Entry) error {
		if gate != nil && e.Kind == kolobraid.KindData {
			<-gate
		}
		_, err := s.Apply(tx, e)
		return err
	}
	lim := &kolobraid.Limits{PulseGap: 10 * time.Millisecond, PivotWait: 100 * time.Millisecond, AnnounceGap: 10 * time.Millisecond,
		ConfirmWait: time.Second}
	ids := make([]*koloid.Identity, 2)
	opts := make([]kolobraid.Options, 2)
	for i := range ids {
		id, err := koloid.Generate()
		if err != nil {
			t.Fatal(err)
		}
		db := kolostore.NewMemory()
		t.Cleanup(func() { db.Close() })
		ids[i], opts[i] = id, kolobraid.Options{Backend: db, App: "kv/test", Apply: apply, Limits: lim}
	}
	a, err := kolobraid.Create(ctx, ids[0], kolobraid.GroupConfig{App: "kv/test"}, opts[0])
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { a.Close() })
	j, err := kolobraid.NewJoin(ctx, ids[1], opts[1])
	if err != nil {
		t.Fatal(err)
	}
	req, err := j.Request(nil)
	if err != nil {
		t.Fatal(err)
	}
	w, err := a.Admit(ctx, ids[1].PublicKey(), req, nil)
	if err != nil {
		t.Fatal(err)
	}
	if b, err = j.Complete(ctx, w); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	connect = func() func() {
		l, err := kolosecure.Listen(ctx, "127.0.0.1:0", ids[0], nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { l.Close() })
		go func() {
			if c, err := l.Accept(ctx); err == nil {
				a.ServeConn(ctx, c)
			}
		}()
		c, err := kolosecure.Dial(ctx, l.Addr().String(), ids[1], ids[0].PublicKey(), nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close() })
		go b.ServeConn(ctx, c)
		return func() { c.Close() }
	}
	return a, b, connect
}

func settledPairs(b *kolobraid.Braid, s *kv.Store) []string {
	snap := b.Settled()
	defer snap.Close()
	var out []string
	for k, v := range s.Range(snap, nil, nil) {
		out = append(out, string(k)+"="+string(v))
	}
	return out
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("timed out")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestConcurrentPuts: two members put the same key at once; both settle the same value, the one of the put that is last in the
// settled order.
func TestConcurrentPuts(t *testing.T) {
	s := kv.New([]byte("s/"))
	a, b, connect := members(t, s)
	ctx := context.Background()
	connect()
	if err := s.Put(ctx, b, []byte("ready"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return slices.Contains(settledPairs(a, s), "ready=1") })
	for i := range 5 {
		if err := s.Put(ctx, a, []byte("theme"), []byte{'a', '0' + byte(i)}); err != nil {
			t.Fatal(err)
		}
		if err := s.Put(ctx, b, []byte("theme"), []byte{'b', '0' + byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Delete(ctx, a, []byte("ready")); err != nil {
		t.Fatal(err)
	}
	// the settled value is the last put on theme in the settled order, on both members, once the delete is settled
	waitFor(t, func() bool {
		pa, pb := settledPairs(a, s), settledPairs(b, s)
		if len(pa) != 1 || slices.Contains(pa, "ready=1") || !slices.Equal(pa, pb) {
			return false
		}
		var last []byte
		for o, err := range a.Entries(0) {
			if err != nil || !o.Settled {
				break
			}
			if o.Kind == kolobraid.KindData && !o.Void && !o.Rejected && bytes.Contains(o.Body, []byte("theme")) {
				last = o.Body[len(o.Body)-2:]
			}
		}
		return pa[0] == "theme="+string(last) && slices.Equal(settledPairs(a, s), pa)
	})
}

func TestPutRefusals(t *testing.T) {
	ctx := context.Background()
	s := kv.New([]byte("prefix"))
	if err := s.Put(ctx, nil, nil, nil); !errors.Is(err, kolobraid.ErrInvalidArgument) {
		t.Fatalf("nil braid: %v", err)
	}
	a, _, _ := members(t, s)
	if err := s.Put(ctx, a, make([]byte, kv.MaxKeySize-5), nil); !errors.Is(err, kolobraid.ErrTooLarge) {
		t.Fatalf("key that does not fit with the prefix: %v", err)
	}
	if err := s.Delete(ctx, a, make([]byte, kv.MaxKeySize-5)); !errors.Is(err, kolobraid.ErrTooLarge) {
		t.Fatalf("delete of a key that does not fit with the prefix: %v", err)
	}
	if err := s.Put(ctx, a, []byte("k"), make([]byte, kv.MaxValueSize+1)); !errors.Is(err, kolobraid.ErrTooLarge) {
		t.Fatalf("value over the bound: %v", err)
	}
	// the largest value does not fit in a default MaxBody entry with its tag and key
	if err := s.Put(ctx, a, []byte("k"), make([]byte, kv.MaxValueSize)); !errors.Is(err, kolobraid.ErrTooLarge) {
		t.Fatalf("body over MaxBody: %v", err)
	}
	if err := s.Put(ctx, a, make([]byte, kv.MaxKeySize-6), []byte("v")); err != nil {
		t.Fatalf("largest key: %v", err)
	}
	waitFor(t, func() bool { return len(settledPairs(a, s)) == 1 })
}
