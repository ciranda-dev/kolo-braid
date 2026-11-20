package kolobraid

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

func hasEvent(b *Braid, kind EventKind) func() bool {
	return func() bool {
		ctx, cancel := shortCtx()
		defer cancel()
		for {
			ev, err := b.Next(ctx)
			if err != nil {
				return false
			}
			if ev.Kind == kind {
				return true
			}
		}
	}
}

func TestClaimsCertifyCheckpoints(t *testing.T) {
	nt, ps := group(t, 3, GroupConfig{CheckpointEvery: 64}, appApply, true)
	for i := 0; ps[0].b.Status().Settled < 140; i++ {
		mustAppend(t, ps[i%3].b, fmt.Sprintf("k=%d", i))
		nt.round(nil, 0)
	}
	got := make([]bool, len(ps))
	nt.settle(nil, 20*time.Second, func() bool {
		all := true
		for i, p := range ps {
			if !got[i] {
				got[i] = hasEvent(p.b, EventCertified)()
			}
			all = all && got[i]
		}
		if !all {
			mustAppend(t, ps[0].b, "more")
		}
		return all
	})
	for _, p := range ps {
		if p.b.Status().Halted != nil {
			t.Fatalf("%s halted: %v", p.name, p.b.Status().Halted)
		}
	}
}

// TestDivergentApplyIsDetected: one seat runs an apply that differs; the two others certify their digest and the divergent peer
// halts with ErrDiverged.
func TestDivergentApplyIsDetected(t *testing.T) {
	odd := func(tx *Tx, e *Entry) error {
		if e.Kind == KindData {
			return tx.Put([]byte("odd"), e.Body)
		}
		return nil
	}
	nt, ps := group(t, 3, GroupConfig{CheckpointEvery: 64}, appApply, true)
	bad := ps[2]
	bad.close()
	bad.opts.Apply = odd
	bad.opts.Rebuild = true
	bad.reopen()
	bad.opts.Rebuild = false
	nt.confirm(bad)
	var diverged bool
	for i := 0; i < 400 && !diverged; i++ {
		mustAppend(t, ps[i%2].b, fmt.Sprintf("k=%d", i))
		nt.round(nil, 0)
		time.Sleep(time.Millisecond)
		if err := bad.b.Status().Halted; err != nil {
			if !errors.Is(err, ErrDiverged) {
				t.Fatalf("halted with %v", err)
			}
			diverged = true
		}
	}
	if !diverged {
		t.Fatal("divergent apply not detected")
	}
	if !hasEvent(bad.b, EventDiverged)() {
		t.Fatal("no EventDiverged")
	}
	for _, p := range ps[:2] {
		if p.b.Status().Halted != nil {
			t.Fatalf("%s: an honest peer halted", p.name)
		}
	}
}
