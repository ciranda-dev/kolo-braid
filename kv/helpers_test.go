package kv_test

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"maps"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	kolocodec "github.com/ciranda-dev/kolo-codec"
	koloid "github.com/ciranda-dev/kolo-id"
	kolosecure "github.com/ciranda-dev/kolo-secure"
	kolostore "github.com/ciranda-dev/kolo-store"

	kolobraid "github.com/ciranda-dev/kolo-braid"
	"github.com/ciranda-dev/kolo-braid/kv"
)

// spec describes a cluster: n members, the ones from index plain on admitted as plain members (neither admin nor settler), and
// the member at index pebble kept on Pebble in a temp dir; 0 means none for both, since member 0 is the founder.
type spec struct{ n, plain, pebble int }

// member is one peer of a cluster: a braid whose apply routes kv entries to s, read through a Tentative and a Settled DB.
type member struct {
	id        *koloid.Identity
	db        kolostore.Backend
	dir       string
	s         *kv.Store
	b         *kolobraid.Braid
	tent, set *kv.DB
	l         *kolosecure.Listener
}

// link is one kolo-secure connection between two members; cut closes it and waits for both ServeConn calls.
type link struct {
	ab, ba *kolosecure.Conn
	wg     sync.WaitGroup
}

func (l *link) cut() {
	l.ab.Close()
	l.ba.Close()
	l.wg.Wait()
}

// cluster is a braid of several members on 127.0.0.1 whose links the test cuts and restores.
type cluster struct {
	t     *testing.T
	ctx   context.Context
	ms    []*member
	mu    sync.Mutex
	links map[[2]int][]*link
	// upto, when not 0, is the last position of the settled order that checkSame compares.
	upto uint64
}

func newCluster(t *testing.T, sp spec) *cluster {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	c := &cluster{t: t, ctx: ctx, links: map[[2]int][]*link{}}
	t.Cleanup(func() {
		cancel()
		c.cutAll()
		for _, m := range c.ms {
			if m.b != nil {
				m.b.Close()
			}
			if m.l != nil {
				m.l.Close()
			}
			m.db.Close()
		}
	})
	for i := range sp.n {
		id, err := koloid.Generate()
		if err != nil {
			t.Fatal(err)
		}
		m := &member{id: id}
		if i == sp.pebble && i != 0 {
			m.dir = filepath.Join(t.TempDir(), "pebble")
			if m.db, err = kolostore.OpenPebble(m.dir); err != nil {
				t.Fatal(err)
			}
		} else {
			m.db = kolostore.NewMemory()
		}
		m.s = kv.New([]byte("s/"))
		c.ms = append(c.ms, m)
		if i == 0 {
			if m.b, err = kolobraid.Create(ctx, id, kolobraid.GroupConfig{App: "kv/test"}, c.options(m)); err != nil {
				t.Fatal(err)
			}
			m.open(t)
			continue
		}
		j, err := kolobraid.NewJoin(ctx, id, c.options(m))
		if err != nil {
			t.Fatal(err)
		}
		req, err := j.Request(nil)
		if err != nil {
			t.Fatal(err)
		}
		adm := &kolobraid.AdmitOptions{GrowSettlers: true}
		if sp.plain > 0 && i >= sp.plain {
			none := kolobraid.Role(0)
			adm = &kolobraid.AdmitOptions{Roles: &none}
		}
		w, err := c.ms[0].b.Admit(ctx, id.PublicKey(), req, adm)
		if err != nil {
			t.Fatal(err)
		}
		if m.b, err = j.Complete(ctx, w); err != nil {
			t.Fatal(err)
		}
		m.open(t)
		for k := range i {
			c.connect(i, k)
		}
		c.waitFor(func() bool {
			for _, o := range c.ms {
				if !o.hasMember(id.PublicKey()) {
					return false
				}
			}
			return true
		})
	}
	return c
}

// options are the options of m's braid: a fresh apply over m.s and the short limits of kv/braid_test.go.
func (c *cluster) options(m *member) kolobraid.Options {
	s := m.s
	return kolobraid.Options{Backend: m.db, App: "kv/test",
		Apply: func(tx *kolobraid.Tx, e *kolobraid.Entry) error {
			_, err := s.Apply(tx, e)
			return err
		},
		Limits: &kolobraid.Limits{PulseGap: 10 * time.Millisecond, PivotWait: 100 * time.Millisecond, AnnounceGap: 10 * time.Millisecond,
			ConfirmWait: time.Second}}
}

// open makes the DBs of m over its current braid and store.
func (m *member) open(t *testing.T) {
	t.Helper()
	var err error
	if m.tent, err = kv.Open(m.b, m.s, kv.Tentative); err != nil {
		t.Fatal(err)
	}
	if m.set, err = kv.Open(m.b, m.s, kv.Settled); err != nil {
		t.Fatal(err)
	}
}

func (m *member) key() ed25519.PublicKey { return m.id.PublicKey() }

func (m *member) hasMember(k ed25519.PublicKey) bool {
	snap := m.b.Settled()
	defer snap.Close()
	return slices.ContainsFunc(snap.Members(), func(w kolobraid.Member) bool { return string(w.Key) == string(k) })
}

func (c *cluster) put(i int, k, v string) {
	c.t.Helper()
	if err := c.ms[i].tent.Put(c.ctx, []byte(k), []byte(v)); err != nil {
		c.t.Fatalf("member %d put %s: %v", i, k, err)
	}
}

func (c *cluster) del(i int, k string) {
	c.t.Helper()
	if err := c.ms[i].tent.Delete(c.ctx, []byte(k)); err != nil {
		c.t.Fatalf("member %d delete %s: %v", i, k, err)
	}
}

// connect links i to j unless they are linked: i dials j's listener.
func (c *cluster) connect(i, j int) {
	c.t.Helper()
	if len(c.linksOf(i, j)) == 0 {
		c.dial(i, j)
	}
}

// dial adds a connection from i to j, also when they are linked.
func (c *cluster) dial(i, j int) {
	c.t.Helper()
	a, b := c.ms[i], c.ms[j]
	if b.l == nil {
		var err error
		if b.l, err = kolosecure.Listen(c.ctx, "127.0.0.1:0", b.id, nil); err != nil {
			c.t.Fatal(err)
		}
	}
	type res struct {
		c   *kolosecure.Conn
		err error
	}
	acc := make(chan res, 1)
	go func() {
		conn, err := b.l.Accept(c.ctx)
		acc <- res{conn, err}
	}()
	ab, err := kolosecure.Dial(c.ctx, b.l.Addr().String(), a.id, b.id.PublicKey(), nil)
	if err != nil {
		c.t.Fatal(err)
	}
	r := <-acc
	if r.err != nil {
		c.t.Fatal(r.err)
	}
	lk := &link{ab: ab, ba: r.c}
	lk.wg.Go(func() { a.b.ServeConn(c.ctx, ab) })
	lk.wg.Go(func() { b.b.ServeConn(c.ctx, r.c) })
	c.mu.Lock()
	k := pair(i, j)
	c.links[k] = append(c.links[k], lk)
	c.mu.Unlock()
}

func pair(i, j int) [2]int { return [2]int{min(i, j), max(i, j)} }

func (c *cluster) linksOf(i, j int) []*link {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.links[pair(i, j)])
}

// full links every pair of the given members, all of them when none is given.
func (c *cluster) full(idx ...int) {
	c.t.Helper()
	if len(idx) == 0 {
		for i := range c.ms {
			idx = append(idx, i)
		}
	}
	for x, i := range idx {
		for _, j := range idx[x+1:] {
			c.connect(i, j)
		}
	}
}

// cut closes every connection between i and j.
func (c *cluster) cut(i, j int) {
	c.mu.Lock()
	ls := c.links[pair(i, j)]
	delete(c.links, pair(i, j))
	c.mu.Unlock()
	for _, l := range ls {
		l.cut()
	}
}

// split cuts every link between the members of one and those of two.
func (c *cluster) split(one, two []int) {
	for _, i := range one {
		for _, j := range two {
			c.cut(i, j)
		}
	}
}

// isolate cuts every link of member i.
func (c *cluster) isolate(i int) {
	for j := range c.ms {
		if j != i {
			c.cut(i, j)
		}
	}
}

func (c *cluster) cutAll() {
	for i := range c.ms {
		c.isolate(i)
	}
}

// restart closes member i, and its Pebble store when it has one, and opens it again from its storage with a new Store, as a
// process restart does.
func (c *cluster) restart(i int) {
	c.t.Helper()
	m := c.ms[i]
	c.isolate(i)
	if err := m.b.Close(); err != nil {
		c.t.Fatal(err)
	}
	if m.dir != "" {
		if err := m.db.Close(); err != nil {
			c.t.Fatal(err)
		}
		var err error
		if m.db, err = kolostore.OpenPebble(m.dir); err != nil {
			c.t.Fatal(err)
		}
	}
	m.s = kv.New([]byte("s/"))
	var err error
	if m.b, err = kolobraid.Open(c.ctx, m.id, c.options(m)); err != nil {
		c.t.Fatal(err)
	}
	m.open(c.t)
}

// waitFor polls cond for up to a minute and fails the test with the members' status if it stays false.
func (c *cluster) waitFor(cond func() bool) {
	c.t.Helper()
	deadline := time.Now().Add(time.Minute)
	for !cond() {
		if time.Now().After(deadline) {
			for i, m := range c.ms {
				c.t.Logf("member %d: %+v, %d settled data entries", i, m.b.Status(), len(settledData(m.b)))
			}
			c.t.Fatal("members did not converge in time")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// settledData returns the data entries of the settled prefix of b, in order.
func settledData(b *kolobraid.Braid) []kolobraid.Ordered {
	var out []kolobraid.Ordered
	for o, err := range b.Entries(0) {
		if err != nil || !o.Settled {
			break
		}
		if o.Kind == kolobraid.KindData {
			o.Body = slices.Clone(o.Body)
			out = append(out, o)
		}
	}
	return out
}

// waitSettled waits until each of the given members (all when none) has settled n data entries, voided ones included.
func (c *cluster) waitSettled(n int, idx ...int) {
	c.t.Helper()
	if len(idx) == 0 {
		for i := range c.ms {
			idx = append(idx, i)
		}
	}
	c.waitFor(func() bool {
		for _, i := range idx {
			if len(settledData(c.ms[i].b)) != n {
				return false
			}
		}
		return true
	})
}

// mapOf returns the pairs db reads.
func mapOf(t *testing.T, db *kv.DB) map[string]string {
	t.Helper()
	out := map[string]string{}
	it := db.Range(nil, nil)
	defer it.Close()
	for it.Next() {
		out[string(it.Key())] = string(it.Value())
	}
	if err := it.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// modelOf applies the effective kv entries of es, which are in their settled order, to an empty map: the last effective put or
// delete of a key wins. It decodes the documented body, so it shares no code with kv.Store.
func modelOf(t *testing.T, es []kolobraid.Ordered) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, e := range es {
		if e.Void || e.Rejected {
			continue
		}
		d := kolocodec.NewDecoder(e.Body)
		tag, err := d.String(64)
		if err != nil || tag != "kolo-braid/kv/v0" {
			t.Fatalf("entry %d is not a kv entry", e.Position)
		}
		op, _ := d.Uvarint()
		k, _ := d.Bytes(kv.MaxKeySize)
		v, err := d.Bytes(kv.MaxValueSize)
		if err != nil {
			t.Fatalf("entry %d: %v", e.Position, err)
		}
		switch op {
		case 1:
			out[string(k)] = string(v)
		case 2:
			delete(out, string(k))
		default:
			t.Fatalf("entry %d: kv op %d", e.Position, op)
		}
	}
	return out
}

// digestOf hashes the settled data entries (position, ID, void, rejected) and the pairs of m. The braid's running digest is not
// exported; EventDiverged reports a mismatch with the digest the seats certified.
func digestOf(es []kolobraid.Ordered, m map[string]string) [32]byte {
	h := sha256.New()
	for _, e := range es {
		h.Write(binary.BigEndian.AppendUint64(nil, e.Position))
		h.Write(e.ID[:])
		h.Write([]byte{b2u(e.Void), b2u(e.Rejected)})
	}
	for _, k := range slices.Sorted(maps.Keys(m)) {
		fmt.Fprintf(h, "%d:%s%d:%s", len(k), k, len(m[k]), m[k])
	}
	return [32]byte(h.Sum(nil))
}

func b2u(b bool) byte {
	if b {
		return 1
	}
	return 0
}

// checkSame fails t unless the given members (all when none) hold the same settled data entries in the same order, the same
// settled map, equal to the model of that order, and the same map in a Tentative read, and none reported a divergence from
// the certified digest, a halt or a lost strand. It returns the settled data entries and the map.
func (c *cluster) checkSame(idx ...int) ([]kolobraid.Ordered, map[string]string) {
	t := c.t
	t.Helper()
	if len(idx) == 0 {
		for i := range c.ms {
			idx = append(idx, i)
		}
	}
	var refEs []kolobraid.Ordered
	var ref map[string]string
	var refDigest [32]byte
	for n, i := range idx {
		m := c.ms[i]
		es := settledData(m.b)
		if c.upto != 0 {
			es = slices.DeleteFunc(es, func(e kolobraid.Ordered) bool { return e.Position > c.upto })
		}
		got := mapOf(t, m.set)
		if want := modelOf(t, es); !maps.Equal(got, want) {
			t.Fatalf("member %d: settled map %v differs from the model of its settled order %v", i, got, want)
		}
		if tent := mapOf(t, m.tent); !maps.Equal(tent, got) {
			t.Fatalf("member %d: tentative read %v differs from the settled read %v", i, tent, got)
		}
		d := digestOf(es, got)
		if n == 0 {
			refEs, ref, refDigest = es, got, d
		} else if d != refDigest {
			t.Fatalf("member %d: settled order or map differs from member %d: %v against %v", i, idx[0], got, ref)
		}
		if st := m.b.Status(); st.Halted != nil || st.StrandLost {
			t.Fatalf("member %d: halted %v, strand lost %v", i, st.Halted, st.StrandLost)
		}
		gone, cancel := context.WithCancel(context.Background())
		cancel()
		for {
			ev, err := m.b.Next(gone)
			if err != nil {
				break
			}
			if ev.Kind == kolobraid.EventDiverged || ev.Kind == kolobraid.EventHalted || ev.Kind == kolobraid.EventStrandLost {
				t.Fatalf("member %d: event %v: %v", i, ev.Kind, ev.Err)
			}
		}
	}
	return refEs, ref
}
