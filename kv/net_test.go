package kv_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	kolobraid "github.com/ciranda-dev/kolo-braid"
)

// ops makes n writes of member i from op number from on over 20 keys: every fourth one a delete, the others puts.
func (c *cluster) ops(i, from, n int) error {
	for k := from; k < from+n; k++ {
		key := []byte(fmt.Sprintf("key%02d", (k*7+i*3)%20))
		var err error
		if k%4 == 3 {
			err = c.ms[i].tent.Delete(c.ctx, key)
		} else {
			err = c.ms[i].tent.Put(c.ctx, key, fmt.Appendf(nil, "m%d-%d", i, k))
		}
		if err != nil {
			return fmt.Errorf("member %d op %d: %w", i, k, err)
		}
	}
	return nil
}

// run calls each fn in its own goroutine and fails the test with the errors once all returned.
func run(t *testing.T, fns ...func() error) {
	t.Helper()
	during(t, func() {}, fns...)
}

// during runs main on the test goroutine while each fn runs in its own goroutine.
func during(t *testing.T, main func(), fns ...func() error) {
	t.Helper()
	errs := make([]error, len(fns))
	var wg sync.WaitGroup
	for i, fn := range fns {
		wg.Go(func() { errs[i] = fn() })
	}
	main()
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		t.Fatal(err)
	}
}

// dataCount returns how many data entries b orders, tentative ones included.
func dataCount(b *kolobraid.Braid) int {
	n := 0
	for o, err := range b.Entries(0) {
		if err != nil {
			break
		}
		if o.Kind == kolobraid.KindData {
			n++
		}
	}
	return n
}

// TestKVPartitionConverges: four members split 2/2 write one key on both sides; neither side has the 3 seats that settle, so
// each reads its own side's value tentatively and settles nothing. After the heal all four settle the same map, which is the
// model of the settled order.
func TestKVPartitionConverges(t *testing.T) {
	c := newCluster(t, spec{n: 4})
	c.full()
	for i := range c.ms {
		c.put(i, fmt.Sprintf("base/%d", i), "1")
	}
	c.waitSettled(4)
	c.split([]int{0, 1}, []int{2, 3})
	for r := range 3 {
		c.put(0, "theme", fmt.Sprintf("left-%d", r))
		c.put(2, "theme", fmt.Sprintf("right-%d", r))
	}
	c.put(1, "side/1", "1")
	c.put(3, "side/3", "1")
	for i, want := range []string{"left-2", "left-2", "right-2", "right-2"} {
		c.waitFor(func() bool {
			v, err := c.ms[i].tent.Get([]byte("theme"))
			return err == nil && string(v) == want
		})
		if n := len(settledData(c.ms[i].b)); n != 4 {
			t.Fatalf("member %d settled %d data entries while split, want 4", i, n)
		}
	}
	c.full()
	c.waitSettled(12)
	_, got := c.checkSame()
	if v := got["theme"]; v != "left-2" && v != "right-2" {
		t.Fatalf("theme settled as %q, not the last write of a side", v)
	}
	if got["side/1"] != "1" || got["side/3"] != "1" {
		t.Fatalf("a write made during the split was lost: %v", got)
	}
}

// TestKVDisconnectedPeerCatchesUp: a member is away while the other three write 500 ops, then reconnects and settles the same
// map, and its own write after that reaches everyone.
func TestKVDisconnectedPeerCatchesUp(t *testing.T) {
	c := newCluster(t, spec{n: 4})
	c.full()
	c.isolate(3)
	run(t,
		func() error { return c.ops(0, 0, 167) },
		func() error { return c.ops(1, 0, 167) },
		func() error { return c.ops(2, 0, 166) })
	c.waitSettled(500, 0, 1, 2)
	if n := len(settledData(c.ms[3].b)); n != 0 {
		t.Fatalf("the isolated member settled %d entries", n)
	}
	c.checkSame(0, 1, 2)
	c.full()
	c.waitSettled(500)
	c.checkSame()
	c.put(3, "back", "1")
	c.waitSettled(501)
	if _, got := c.checkSame(); got["back"] != "1" {
		t.Fatalf("the write of the returned member is missing: %v", got)
	}
}

// TestKVRestartOnPebble: a member on Pebble is closed and reopened while the others write; it settles the same map and order,
// and its writes and deletes after the restart take effect on every member, its own included.
func TestKVRestartOnPebble(t *testing.T) {
	c := newCluster(t, spec{n: 4, pebble: 3})
	c.full()
	run(t, func() error { return c.ops(0, 0, 20) }, func() error { return c.ops(1, 0, 20) },
		func() error { return c.ops(2, 0, 20) }, func() error { return c.ops(3, 0, 20) })
	for i := range c.ms {
		c.put(i, fmt.Sprintf("pre/%d", i), "1")
	}
	c.waitSettled(84)
	during(t, func() { c.restart(3) }, func() error { return c.ops(0, 20, 30) }, func() error { return c.ops(1, 20, 30) },
		func() error { return c.ops(2, 20, 30) })
	if !c.ms[3].b.Status().Unconfirmed {
		t.Fatal("a reopened member of four is confirmed before any peer answered")
	}
	c.full()
	c.waitSettled(174)
	c.checkSame()
	c.waitFor(func() bool { return !c.ms[3].b.Status().Unconfirmed })
	c.put(3, "after/restart", "1")
	c.del(3, "pre/0")
	c.del(0, "pre/3")
	c.waitSettled(177)
	_, got := c.checkSame()
	_, gone0 := got["pre/0"]
	_, gone3 := got["pre/3"]
	if gone0 || gone3 || got["after/restart"] != "1" || got["pre/1"] != "1" {
		t.Fatalf("writes after the restart did not take effect: %v", got)
	}
}

// TestKVRedeliveryIsNoOp: reconnecting and a second connection per pair make the peers offer their strands again; what is settled
// does not change and nothing is applied twice.
func TestKVRedeliveryIsNoOp(t *testing.T) {
	c := newCluster(t, spec{n: 3})
	c.full()
	run(t, func() error { return c.ops(0, 0, 20) }, func() error { return c.ops(1, 0, 20) }, func() error { return c.ops(2, 0, 20) })
	c.waitSettled(60)
	es, got := c.checkSame()
	want := digestOf(es, got)
	for r := range 3 {
		c.cutAll()
		c.full()
		c.dial(0, 1)
		c.dial(1, 2)
		c.dial(0, 2)
		// a write that settles everywhere shows the new connections synced
		c.put(r, fmt.Sprintf("marker/%d", r), "1")
		c.waitSettled(61 + r)
		all, _ := c.checkSame()
		seen := map[kolobraid.EntryID]bool{}
		for _, e := range all {
			if seen[e.ID] {
				t.Fatalf("entry %x is in the order twice", e.ID[:4])
			}
			seen[e.ID] = true
		}
		if d := digestOf(all[:60], modelOf(t, all[:60])); d != want {
			t.Fatalf("round %d: the settled prefix changed", r)
		}
	}
}

// TestKVRetriedPut: a Put retried after a success is a second entry with the same effect, and it is ordered after what its
// writer had seen; a Put that failed in the confirmation wait wrote nothing, so its retry is one entry.
func TestKVRetriedPut(t *testing.T) {
	c := newCluster(t, spec{n: 3})
	c.full()
	c.put(0, "k", "1")
	c.put(0, "k", "1")
	c.waitSettled(2)
	es, got := c.checkSame()
	if es[0].ID == es[1].ID || !bytes.Equal(es[0].Body, es[1].Body) || es[0].Void || es[1].Void || es[0].Rejected || es[1].Rejected {
		t.Fatalf("the retried put is not a second effective entry: %+v %+v", es[0], es[1])
	}
	if got["k"] != "1" {
		t.Fatalf("k = %q", got["k"])
	}
	c.put(0, "theme", "a")
	c.waitFor(func() bool { v, err := c.ms[1].tent.Get([]byte("theme")); return err == nil && string(v) == "a" })
	c.put(1, "theme", "b")
	c.waitSettled(4)
	if _, got := c.checkSame(); got["theme"] != "b" {
		t.Fatalf("theme = %q before the retry, want b", got["theme"])
	}
	c.waitFor(func() bool { v, err := c.ms[0].tent.Get([]byte("theme")); return err == nil && string(v) == "b" })
	c.put(0, "theme", "a")
	c.waitSettled(5)
	if _, got := c.checkSame(); got["theme"] != "a" {
		t.Fatalf("a retry seen after b did not win: theme = %q", got["theme"])
	}

	c.restart(2)
	m := c.ms[2]
	if !m.b.Status().Unconfirmed {
		t.Fatal("a reopened member of three is confirmed before any peer answered")
	}
	held := dataCount(m.b)
	ctx, cancel := context.WithTimeout(c.ctx, 50*time.Millisecond)
	defer cancel()
	if err := m.tent.Put(ctx, []byte("retry"), []byte("1")); !errors.Is(err, kolobraid.ErrUnconfirmed) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("put before confirmation: %v", err)
	}
	if n := dataCount(m.b); n != held {
		t.Fatalf("the failed put wrote %d entries", n-held)
	}
	c.full()
	c.waitFor(func() bool { return !m.b.Status().Unconfirmed })
	c.put(2, "retry", "1")
	c.waitSettled(held + 1)
	es, got = c.checkSame()
	n := 0
	for _, e := range es {
		if bytes.Contains(e.Body, []byte("retry")) {
			n++
		}
	}
	if n != 1 || got["retry"] != "1" {
		t.Fatalf("the retry wrote %d entries, retry = %q", n, got["retry"])
	}
}

// TestKVRemovedMemberWritesVoid: four members split 2/2, so nothing settles on either side; one side removes a member while that
// member, on the other side, writes on without knowing. Once healed, its entries ordered before the removal keep their effect and
// those ordered after it are void, on every member.
func TestKVRemovedMemberWritesVoid(t *testing.T) {
	c := newCluster(t, spec{n: 4})
	x := c.ms[3].key()
	c.full()
	c.put(3, "x/before", "1")
	c.put(0, "a", "1")
	c.waitSettled(2)
	c.split([]int{0, 1}, []int{2, 3})
	if _, err := c.ms[0].b.RemoveMember(c.ctx, x); err != nil {
		t.Fatal(err)
	}
	for i := range 20 {
		c.put(3, fmt.Sprintf("x/%d", i), "1")
	}
	c.put(1, "b", "1")
	c.put(2, "c", "1")
	c.full()
	// the others stop serving the removed member, so how many of its entries they order varies: z, written once the others'
	// writes are seen, bounds the part of the order that is compared
	c.waitFor(func() bool {
		got := mapOf(t, c.ms[0].tent)
		return got["b"] == "1" && got["c"] == "1"
	})
	c.put(0, "z", "1")
	c.waitFor(func() bool {
		for _, i := range []int{0, 1, 2} {
			if mapOf(t, c.ms[i].set)["z"] != "1" {
				return false
			}
		}
		return !c.ms[0].hasMember(x) && !c.ms[1].hasMember(x) && !c.ms[2].hasMember(x)
	})
	for _, e := range settledData(c.ms[0].b) {
		if bytes.Equal(e.Writer, c.ms[0].key()) {
			c.upto = e.Position
		}
	}
	es, got := c.checkSame(0, 1, 2)
	var removed uint64
	for o, err := range c.ms[0].b.Entries(0) {
		if err != nil {
			t.Fatal(err)
		}
		if o.Kind == kolobraid.KindRemoveMember {
			removed = o.Position
		}
	}
	voids := 0
	for _, e := range es {
		if !bytes.Equal(e.Writer, x) {
			continue
		}
		if after := e.Position > removed; after != e.Void {
			t.Fatalf("entry of the removed member at %d, removal at %d: void %v", e.Position, removed, e.Void)
		} else if after {
			voids++
		}
	}
	if voids == 0 {
		t.Fatal("no entry of the removed member was ordered after its removal")
	}
	if got["x/before"] != "1" || got["a"] != "1" || got["b"] != "1" || got["c"] != "1" {
		t.Fatalf("settled map %v", got)
	}
	if err := c.ms[3].tent.Put(c.ctx, []byte("late"), []byte("1")); !errors.Is(err, kolobraid.ErrRemoved) {
		t.Fatalf("put by the removed member: %v", err)
	}
}
