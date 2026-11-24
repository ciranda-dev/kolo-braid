package kv_test

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	koloid "github.com/ciranda-dev/kolo-id"
	kolokv "github.com/ciranda-dev/kolo-kv"
	"github.com/ciranda-dev/kolo-kv/kolokvtest"
	kolostore "github.com/ciranda-dev/kolo-store"

	kolobraid "github.com/ciranda-dev/kolo-braid"
	"github.com/ciranda-dev/kolo-braid/kv"
)

// founder starts a braid of one member whose apply routes kv entries to s.
func founder(t *testing.T, s *kv.Store) *kolobraid.Braid {
	t.Helper()
	id, err := koloid.Generate()
	if err != nil {
		t.Fatal(err)
	}
	db := kolostore.NewMemory()
	t.Cleanup(func() { db.Close() })
	lim := &kolobraid.Limits{PulseGap: 10 * time.Millisecond, PivotWait: 100 * time.Millisecond, AnnounceGap: 10 * time.Millisecond,
		ConfirmWait: time.Second}
	b, err := kolobraid.Create(context.Background(), id, kolobraid.GroupConfig{App: "kv/test"}, kolobraid.Options{
		Backend: db, App: "kv/test", Limits: lim,
		Apply: func(tx *kolobraid.Tx, e *kolobraid.Entry) error {
			_, err := s.Apply(tx, e)
			return err
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	return b
}

// counted is a DB that counts its accepted writes, so that settle knows how many entries to wait for.
type counted struct {
	*kv.DB
	b       *kolobraid.Braid
	read    kv.Read
	written atomic.Int64
}

func (c *counted) Put(ctx context.Context, key, value []byte) error {
	err := c.DB.Put(ctx, key, value)
	if err == nil {
		c.written.Add(1)
	}
	return err
}

func (c *counted) Delete(ctx context.Context, key []byte) error {
	err := c.DB.Delete(ctx, key)
	if err == nil {
		c.written.Add(1)
	}
	return err
}

func openCounted(t *testing.T, s *kv.Store, read kv.Read) *counted {
	t.Helper()
	b := founder(t, s)
	db, err := kv.Open(b, s, read)
	if err != nil {
		t.Fatal(err)
	}
	return &counted{DB: db, b: b, read: read}
}

// settle waits until every data entry the DB wrote is in the order and applied in the view the DB reads.
func settle(t *testing.T, s kolokv.Store) {
	t.Helper()
	c := s.(*counted)
	waitFor(t, func() bool {
		var n int64
		var last uint64
		for o, err := range c.b.Entries(0) {
			if err != nil {
				return false
			}
			if o.Kind == kolobraid.KindData {
				n++
				last = o.Position
			}
		}
		if n < c.written.Load() {
			return false
		}
		snap := c.b.View()
		if c.read == kv.Settled {
			snap = c.b.Settled()
		}
		defer snap.Close()
		return snap.Position() >= last
	})
}

func TestDBStore(t *testing.T) {
	for _, c := range []struct {
		name   string
		prefix string
		read   kv.Read
	}{{"Tentative", "", kv.Tentative}, {"Settled", "", kv.Settled}, {"Prefixed", "app/", kv.Tentative}} {
		t.Run(c.name, func(t *testing.T) {
			kolokvtest.TestStore(t, func(t *testing.T) kolokv.Store {
				return openCounted(t, kv.New([]byte(c.prefix)), c.read)
			}, settle)
		})
	}
}

// TestDBReadsItsWritesTentatively: a write shows in a Tentative read once the braid applied it, not when Put returns, and a
// Settled read does not show it while it is unsettled.
func TestDBReadsItsWritesTentatively(t *testing.T) {
	ctx := context.Background()
	s := kv.New([]byte("s/"))
	gate := make(chan struct{})
	_, b, connect := gatedMembers(t, s, gate)
	closed := false
	release := func() {
		if !closed {
			close(gate)
			closed = true
		}
	}
	t.Cleanup(release)
	cut := connect()
	// b writes after a cut, so a never receives the entry and it cannot settle
	waitFor(t, func() bool { return !b.Status().Unconfirmed })
	cut()
	tent, err := kv.Open(b, s, kv.Tentative)
	if err != nil {
		t.Fatal(err)
	}
	sett, err := kv.Open(b, s, kv.Settled)
	if err != nil {
		t.Fatal(err)
	}
	if err := tent.Put(ctx, []byte("theme"), []byte("dark")); err != nil {
		t.Fatal(err)
	}
	if _, err := tent.Get([]byte("theme")); !errors.Is(err, kolokv.ErrNotFound) {
		t.Fatalf("read while apply is blocked: %v", err)
	}
	release()
	waitFor(t, func() bool {
		v, err := tent.Get([]byte("theme"))
		return err == nil && string(v) == "dark"
	})
	if _, err := sett.Get([]byte("theme")); !errors.Is(err, kolokv.ErrNotFound) {
		t.Fatalf("a settled read shows an unsettled write: %v", err)
	}
}

// position returns the position of the data entry that wrote key to value in b's order, found by its body.
func position(b *kolobraid.Braid, key, value string) (uint64, bool) {
	for o, err := range b.Entries(0) {
		if err != nil {
			return 0, false
		}
		if o.Kind == kolobraid.KindData && bytes.Contains(o.Body, []byte(key)) && bytes.HasSuffix(o.Body, []byte(value)) {
			return o.Position, true
		}
	}
	return 0, false
}

// TestDBSettledNeverChanges: a concurrent write from the other side settles before ours, so our tentative entry moves in the
// order; a Settled read and a Settled iterator made before keep what they read, and later reads follow the settled order.
func TestDBSettledNeverChanges(t *testing.T) {
	ctx := context.Background()
	s := kv.New([]byte("s/"))
	a, b, connect := members(t, s)
	cut := connect()
	waitFor(t, func() bool { return !b.Status().Unconfirmed })
	da, _ := kv.Open(a, s, kv.Settled)
	tent, _ := kv.Open(b, s, kv.Tentative)
	sett, _ := kv.Open(b, s, kv.Settled)
	if err := da.Put(ctx, []byte("base"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	get := func(db *kv.DB, k string) string {
		v, err := db.Get([]byte(k))
		if err != nil {
			return ""
		}
		return string(v)
	}
	waitFor(t, func() bool { return get(sett, "base") == "1" })
	cut()
	if err := tent.Put(ctx, []byte("theme"), []byte("b")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return get(tent, "theme") == "b" })
	before, ok := position(b, "theme", "b")
	if !ok {
		t.Fatal("own entry not in the order")
	}
	it := sett.Range(nil, nil)
	defer it.Close()
	if err := da.Put(ctx, []byte("theme"), []byte("a")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return get(da, "theme") == "a" })
	if got := get(sett, "theme"); got != "" {
		t.Fatalf("settled read shows %q before the other side's write reaches b", got)
	}
	connect()
	waitFor(t, func() bool {
		after, ok := position(b, "theme", "b")
		return ok && after > before
	})
	// the other side's write settled first, so it is the earlier one and ours wins
	waitFor(t, func() bool { return get(sett, "theme") == "b" })
	if got := get(tent, "theme"); got != "b" {
		t.Fatalf("tentative read %q", got)
	}
	if got := get(sett, "base"); got != "1" {
		t.Fatalf("settled value of base changed to %q", got)
	}
	var pairs []string
	for it.Next() {
		pairs = append(pairs, string(it.Key())+"="+string(it.Value()))
	}
	if !slices.Equal(pairs, []string{"base=1"}) {
		t.Fatalf("iterator made before the reorg yields %q", pairs)
	}
}

func TestCloseLeavesTheBraidOpen(t *testing.T) {
	ctx := context.Background()
	s := kv.New(nil)
	b := founder(t, s)
	first, err := kv.Open(b, s, kv.Tentative)
	if err != nil {
		t.Fatal(err)
	}
	second, err := kv.Open(b, s, kv.Tentative)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if err := first.Put(ctx, []byte("k"), nil); !errors.Is(err, kolokv.ErrClosed) {
		t.Fatalf("Put on a closed DB: %v", err)
	}
	if err := second.Put(ctx, []byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put on another DB of the braid: %v", err)
	}
	if err := s.Put(ctx, b, []byte("j"), []byte("w")); err != nil {
		t.Fatalf("Put on the braid: %v", err)
	}
	waitFor(t, func() bool {
		v, err := second.Get([]byte("k"))
		return err == nil && string(v) == "v"
	})
	if h := b.Status().Halted; h != nil {
		t.Fatal(h)
	}
}

// TestDBErrorMapping checks every row of the error table of decision 0022.
func TestDBErrorMapping(t *testing.T) {
	ctx := context.Background()
	s := kv.New([]byte("p/"))
	b := founder(t, s)
	db, _ := kv.Open(b, s, kv.Tentative)
	closedDB, _ := kv.Open(b, s, kv.Tentative)
	closedDB.Close()
	sb := kv.New(nil)
	closedBraid := founder(t, sb)
	cdb, _ := kv.Open(closedBraid, sb, kv.Tentative)
	closedBraid.Close()
	done, cancel := context.WithCancel(ctx)
	cancel()

	keyFits := make([]byte, kv.MaxKeySize-2)
	keyOver := make([]byte, kv.MaxKeySize-1)
	valueOver := make([]byte, kolokv.MaxValueSize+1)
	bodyOver := make([]byte, kolokv.MaxValueSize)
	put := func(db *kv.DB, ctx context.Context, k, v []byte) func() error {
		return func() error { return db.Put(ctx, k, v) }
	}
	del := func(db *kv.DB, ctx context.Context, k []byte) func() error {
		return func() error { return db.Delete(ctx, k) }
	}
	get := func(db *kv.DB, k []byte) func() error {
		return func() error { _, err := db.Get(k); return err }
	}
	rows := []struct {
		name string
		call func() error
		want []error
	}{
		{"get missing", get(db, []byte("none")), []error{kolokv.ErrNotFound, kolobraid.ErrNotFound}},
		{"put key over the bound with the prefix", put(db, ctx, keyOver, nil), []error{kolokv.ErrKeyTooLarge, kolobraid.ErrTooLarge}},
		{"delete key over the bound with the prefix", del(db, ctx, keyOver), []error{kolokv.ErrKeyTooLarge, kolobraid.ErrTooLarge}},
		{"put key that fits with the prefix", put(db, ctx, keyFits, nil), nil},
		{"put value over MaxValueSize", put(db, ctx, []byte("k"), valueOver), []error{kolokv.ErrValueTooLarge, kolobraid.ErrTooLarge}},
		{"put body over MaxBody", put(db, ctx, []byte("k"), bodyOver), []error{kolokv.ErrValueTooLarge, kolobraid.ErrTooLarge}},
		{"size before ctx", put(db, done, keyOver, nil), []error{kolokv.ErrKeyTooLarge, kolobraid.ErrTooLarge}},
		{"put with a done ctx", put(db, done, []byte("k"), nil), []error{context.Canceled}},
		{"delete with a done ctx", del(db, done, []byte("k")), []error{context.Canceled}},
		{"put on a closed DB", put(closedDB, ctx, []byte("k"), nil), []error{kolokv.ErrClosed}},
		{"delete on a closed DB", del(closedDB, ctx, []byte("k")), []error{kolokv.ErrClosed}},
		{"get on a closed DB", get(closedDB, []byte("k")), []error{kolokv.ErrClosed}},
		{"range on a closed DB", func() error { return closedDB.Range(nil, nil).Close() }, []error{kolokv.ErrClosed}},
		{"put on a closed braid", put(cdb, ctx, []byte("k"), nil), []error{kolokv.ErrClosed, kolobraid.ErrClosed}},
		{"delete on a closed braid", del(cdb, ctx, []byte("k")), []error{kolokv.ErrClosed, kolobraid.ErrClosed}},
		{"open without a braid", func() error { _, err := kv.Open(nil, s, kv.Tentative); return err }, []error{kolobraid.ErrInvalidArgument}},
		{"open without a store", func() error { _, err := kv.Open(b, nil, kv.Tentative); return err }, []error{kolobraid.ErrInvalidArgument}},
		{"open with an unknown read", func() error { _, err := kv.Open(b, s, kv.Read(9)); return err }, []error{kolobraid.ErrInvalidArgument}},
	}
	for _, r := range rows {
		t.Run(r.name, func(t *testing.T) {
			err := r.call()
			if r.want == nil {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			for _, w := range r.want {
				if !errors.Is(err, w) {
					t.Fatalf("error %v does not match %v", err, w)
				}
			}
			if err != nil && len(err.Error()) > 200 {
				t.Fatalf("error text of %d bytes", len(err.Error()))
			}
		})
	}
}

func TestDBRangeEmptyEnd(t *testing.T) {
	c := openCounted(t, kv.New([]byte("s/")), kv.Tentative)
	for _, k := range []string{"a", "b"} {
		if err := c.Put(context.Background(), []byte(k), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	settle(t, c)
	count := func(it *kolokv.Iterator) int {
		n := 0
		for it.Next() {
			n++
		}
		if err := it.Close(); err != nil {
			t.Fatal(err)
		}
		return n
	}
	if n := count(c.Range(nil, nil)); n != 2 {
		t.Fatalf("nil end yields %d", n)
	}
	if n := count(c.Range(nil, []byte{})); n != 0 {
		t.Fatalf("empty end yields %d", n)
	}
	if n := count(c.Range([]byte("a"), []byte{})); n != 0 {
		t.Fatalf("empty end with a start yields %d", n)
	}
	if n := count(c.Prefix(nil)); n != 2 {
		t.Fatalf("empty prefix yields %d", n)
	}
}

// TestDBIteratorIsASnapshot: an iterator yields the pairs of the snapshot it was made on, whatever is written after.
func TestDBIteratorIsASnapshot(t *testing.T) {
	ctx := context.Background()
	c := openCounted(t, kv.New(nil), kv.Tentative)
	c.Put(ctx, []byte("a"), []byte("1"))
	c.Put(ctx, []byte("b"), []byte("2"))
	settle(t, c)
	it := c.Range(nil, nil)
	c.Put(ctx, []byte("a"), []byte("9"))
	c.Put(ctx, []byte("c"), []byte("3"))
	settle(t, c)
	var got []string
	for it.Next() {
		got = append(got, string(it.Key())+"="+string(it.Value()))
	}
	if err := it.Close(); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, []string{"a=1", "b=2"}) {
		t.Fatalf("iterator yields %q", got)
	}
	if err := it.Close(); err != nil || it.Next() {
		t.Fatalf("after Close: %v", err)
	}
	it = c.Range(nil, nil)
	defer it.Close()
	n := 0
	for it.Next() {
		n++
	}
	if n != 3 {
		t.Fatalf("a new iterator yields %d pairs", n)
	}
}

// TestDBRefusedPutReturnsNil: a put that Allow refuses is accepted for ordering, so Put returns nil, and it has no effect.
func TestDBRefusedPutReturnsNil(t *testing.T) {
	ctx := context.Background()
	s := kv.NewWith(nil, kv.Options{Allow: func(_ *kolobraid.Tx, _ *kolobraid.Entry, key []byte, _ bool) bool {
		return !bytes.HasPrefix(key, []byte("locked/"))
	}})
	c := openCounted(t, s, kv.Tentative)
	if err := c.Put(ctx, []byte("locked/k"), []byte("v")); err != nil {
		t.Fatalf("Put of a refused key: %v", err)
	}
	if err := c.Put(ctx, []byte("open"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	settle(t, c)
	if _, err := c.Get([]byte("locked/k")); !errors.Is(err, kolokv.ErrNotFound) {
		t.Fatalf("refused key: %v", err)
	}
	waitFor(t, func() bool {
		for o, err := range c.b.Entries(0) {
			if err == nil && o.Kind == kolobraid.KindData && o.Rejected {
				return true
			}
		}
		return false
	})
}
