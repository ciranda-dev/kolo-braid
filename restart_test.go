package kolobraid

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	kolostore "github.com/ciranda-dev/kolo-store"
)

var errCrash = errors.New("crashed on purpose")

// crashing is a backend that dies at its n-th commit: that batch and every write after it are lost, as in a kill -9.
type crashing struct {
	kolostore.Backend
	mu      sync.Mutex
	commits int
	at      int
}

func (c *crashing) dead() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.at > 0 && c.commits >= c.at
}

func (c *crashing) Put(k, v []byte) error {
	if c.dead() {
		return errCrash
	}
	return c.Backend.Put(k, v)
}

func (c *crashing) Delete(k []byte) error {
	if c.dead() {
		return errCrash
	}
	return c.Backend.Delete(k)
}

func (c *crashing) NewBatch() kolostore.Batch { return &crashBatch{Batch: c.Backend.NewBatch(), c: c} }

func (c *crashing) Close() error { return nil }

type crashBatch struct {
	kolostore.Batch
	c *crashing
}

func (b *crashBatch) Commit() error {
	b.c.mu.Lock()
	b.c.commits++
	dead := b.c.at > 0 && b.c.commits >= b.c.at
	b.c.mu.Unlock()
	if dead {
		return errCrash
	}
	return b.Batch.Commit()
}

func TestSingleMemberReopenWrites(t *testing.T) {
	p := newFounder(t, "a", GroupConfig{}, appApply)
	mustAppend(t, p.b, "a=1")
	waitUntil(t, 5*time.Second, func() bool { return quiet(p) })
	p.reopen()
	if s := p.b.Status(); s.Unconfirmed {
		t.Fatal("a braid that never had another member is confirmed by its own storage")
	}
	mustAppend(t, p.b, "b=2")
	waitUntil(t, 5*time.Second, func() bool { return quiet(p) && settledGet(t, p.b, "b") == "2" })
	if settledGet(t, p.b, "a") != "1" || settledGet(t, p.b, "#") != "2" {
		t.Fatal(dump(t, p.b))
	}
}

func TestRebuildReplaysTheSameView(t *testing.T) {
	p := newFounder(t, "a", GroupConfig{}, appApply)
	for i := range 40 {
		mustAppend(t, p.b, fmt.Sprintf("k%d=%d", i%7, i))
	}
	waitUntil(t, 5*time.Second, func() bool { return quiet(p) })
	before, digest := dump(t, p.b), p.b.view.Progress()
	p.close()
	p.opts.Rebuild = true
	p.reopen()
	p.opts.Rebuild = false
	waitUntil(t, 5*time.Second, func() bool { return quiet(p) })
	if after := dump(t, p.b); after != before || p.b.view.Progress() != digest {
		t.Fatalf("rebuild changed the view:\n%s\n%s", before, after)
	}
}

// TestCrashAtEveryPoint kills one peer of a three-member braid at its k-th durable commit, reopens it from what reached the
// disk and checks that every peer still ends with the same settled order and view.
func TestCrashAtEveryPoint(t *testing.T) {
	points := []int{1, 2, 3, 5, 8, 13, 21, 34, 55}
	if testing.Short() {
		points = points[:4]
	}
	for _, k := range points {
		t.Run(fmt.Sprint(k), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(uint64(k), 5))
			nt, peers := group(t, 3, GroupConfig{}, appApply, true)
			victim := peers[1]
			raw := victim.db
			cb := &crashing{Backend: raw}
			victim.close()
			victim.opts.Backend = cb
			victim.reopen()
			nt.confirm(victim)
			cb.mu.Lock()
			cb.at = cb.commits + k
			cb.mu.Unlock()
			crashed := false
			for step := 0; step < 40; step++ {
				p := peers[rng.IntN(3)]
				if p != victim || !cb.dead() {
					if _, err := p.b.Append(ctx10(t), []byte(fmt.Sprintf("k%d=%d", step%5, step))); err != nil && p != victim {
						t.Fatal(err)
					}
				}
				nt.round(rng, 2)
				if cb.dead() && !crashed {
					crashed = true
					victim.close()
					victim.opts.Backend = raw
					victim.reopen()
					nt.confirm(victim)
				}
			}
			nt.settle(rng, 60*time.Second, func() bool { return quiet(peers...) })
			checkSame(t, peers)
		})
	}
}

// TestRebuildUsesTheNewApply: Rebuild replays the stored pivots with the apply given at Open, which is how a fixed apply is put
// in place after a halt or a divergence.
func TestRebuildUsesTheNewApply(t *testing.T) {
	p := newFounder(t, "a", GroupConfig{}, appApply)
	mustAppend(t, p.b, "a=1")
	waitUntil(t, 5*time.Second, func() bool { return quiet(p) && settledGet(t, p.b, "a") == "1" })
	p.close()
	p.opts.Apply = func(tx *Tx, e *Entry) error {
		if e.Kind == KindData {
			return tx.Put([]byte("v2"), e.Body)
		}
		return nil
	}
	p.opts.Rebuild = true
	p.reopen()
	waitUntil(t, 5*time.Second, func() bool { return quiet(p) && settledGet(t, p.b, "v2") == "a=1" })
	if settledGet(t, p.b, "a") != "<none>" {
		t.Fatal("the rebuilt view kept writes of the old apply")
	}
}
