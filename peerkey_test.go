package kolobraid

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	kololog "github.com/ciranda-dev/kolo-log"
)

// heard collects the braid messages a test-driven peer receives on its link.
type heard struct {
	mu   sync.Mutex
	msgs []message
}

func listen(t *testing.T, rl *rawLink, bd formatBounds) *heard {
	t.Helper()
	h := &heard{}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})
	wg.Go(func() {
		for {
			st, err := rl.conn.AcceptUniStream(ctx)
			if err != nil {
				return
			}
			m, err := readMessage(st, bd, func(messageType) error { return nil })
			if err != nil {
				m = nil
			}
			h.mu.Lock()
			h.msgs = append(h.msgs, m)
			h.mu.Unlock()
		}
	})
	return h
}

func (h *heard) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.msgs)
}

// announced reports whether an ANNOUNCE carried a head of strand id of at least size entries.
func (h *heard) announced(id kololog.LogID, size uint64) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, m := range h.msgs {
		if a, ok := m.(announceMsg); ok {
			for _, hd := range a.Heads {
				if hd.LogID == id && hd.Size >= size {
					return true
				}
			}
		}
	}
	return false
}

// TestHaltedBraidRefusesStrangers: a braid halted by a failure runs no scheduler, so the check of the key when ServeConn
// starts is the only one before the first message is read and sent. A stranger, direct or relayed, that asks for a record
// gets no message back, and its request is ignored and counted.
func TestHaltedBraidRefusesStrangers(t *testing.T) {
	for _, relayed := range []bool{false, true} {
		m, v, _ := hostileSetup(t)
		v.b.mu.Lock()
		v.b.fail(errors.New("halted by the test"))
		v.b.mu.Unlock()
		id := newIdentity(t)
		var rl *rawLink
		if relayed {
			rl = rawRelayed(t, startTapRelay(t, id, v.id), id, v)
		} else {
			rl = m.raw(id, v)
		}
		got := listen(t, rl, v.b.groupBounds())
		before := v.b.sync.ignored.Load()
		rl.msg(t, recordReqMsg{Strand: v.b.meta.OwnStrand, Index: 0}, v.b.groupBounds())
		waitUntil(t, 5*time.Second, func() bool { return v.b.sync.ignored.Load() > before })
		// the victim's sender runs at least every AnnounceGap (10 ms): an answer or an announcement comes well within this
		time.Sleep(500 * time.Millisecond)
		if n := got.count(); n != 0 {
			t.Fatalf("relayed=%v: a halted braid sent %d messages to a stranger", relayed, n)
		}
	}
}

// TestRemovedMemberHearsNothing: once the victim holds a member's removal, it announces nothing new to the removed key on a
// connection that was open before, though its grace ends only much later, and the removed key's messages are ignored.
func TestRemovedMemberHearsNothing(t *testing.T) {
	m, v, h := hostileSetup(t)
	v.b.sync.timeout = time.Minute
	h.close()
	rl := m.raw(h.id, v)
	r := remoteOf(t, v, keyOf(h))
	waitUntil(t, 5*time.Second, func() bool { return r.known.Load() })
	got := listen(t, rl, v.b.groupBounds())
	if _, err := v.b.RemoveMember(ctx10(t), keyOf(h)); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 20*time.Second, func() bool { return !v.b.sync.known(keyOf(h)) })
	// two run ends: the second run started after the removal was held
	for range 2 {
		runs := v.b.sync.runs.Load()
		v.b.sync.signal()
		waitUntil(t, 5*time.Second, func() bool { return v.b.sync.runs.Load() > runs })
	}
	mustAppend(t, v.b, "after=1")
	size := headOfPeer(t, v).Size
	time.Sleep(500 * time.Millisecond)
	if got.announced(v.b.meta.OwnStrand, size) {
		t.Fatal("the victim announced a new head to a removed member")
	}
	before := v.b.sync.ignored.Load()
	rl.msg(t, recordMsg{}, v.b.groupBounds())
	waitUntil(t, 5*time.Second, func() bool { return v.b.sync.ignored.Load() > before })
	if !rl.open() {
		t.Fatal("the connection was closed before the grace")
	}
}
