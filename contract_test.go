package kolobraid

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestRejectIsRecordedOnEveryPeer(t *testing.T) {
	nt, ps := group(t, 2, GroupConfig{}, appApply, false)
	mustAppend(t, ps[1].b, "!")
	mustAppend(t, ps[1].b, "ok=1")
	nt.settle(nil, 10*time.Second, func() bool { return quiet(ps...) && settledGet(t, ps[0].b, "ok") == "1" })
	checkSame(t, ps)
	for _, p := range ps {
		found := false
		for o, err := range p.b.Entries(0) {
			if err != nil {
				t.Fatal(err)
			}
			if string(o.Body) == "!" {
				found = true
				if !o.Rejected || !o.Settled || o.Void {
					t.Fatalf("%s: rejected entry reads %+v", p.name, o)
				}
			}
		}
		if !found {
			t.Fatal("rejected entry missing")
		}
	}
	if settledGet(t, ps[0].b, "#") != "1" {
		t.Fatal("a rejected entry kept its writes")
	}
}

// TestApplyFailureHaltsOnlyThisPeerAndOpenRetries: an error or a panic in apply halts the view with the position; the next Open
// with a fixed apply continues from the stored state.
func TestApplyFailureHaltsOnlyThisPeerAndOpenRetries(t *testing.T) {
	for _, body := range []string{"fail", "panic"} {
		t.Run(body, func(t *testing.T) {
			var broken atomic.Bool
			broken.Store(true)
			apply := func(tx *Tx, e *Entry) error {
				if !broken.Load() && e.Kind == KindData && (string(e.Body) == "fail" || string(e.Body) == "panic") {
					return tx.Put([]byte("fixed"), []byte(body))
				}
				return appApply(tx, e)
			}
			p := newFounder(t, "a", GroupConfig{}, apply)
			mustAppend(t, p.b, "a=1")
			mustAppend(t, p.b, body)
			mustAppend(t, p.b, "b=2")
			ev := waitEvent(t, p.b, EventHalted, 5*time.Second)
			if !errors.Is(ev.Err, ErrHalted) || ev.Position == 0 {
				t.Fatalf("halt event %+v", ev)
			}
			waitUntil(t, 5*time.Second, func() bool { return p.b.Status().Halted != nil })
			if settledGet(t, p.b, "b") != "<none>" {
				t.Fatal("entries after the failure were applied")
			}
			broken.Store(false)
			p.reopen()
			waitUntil(t, 5*time.Second, func() bool { return quiet(p) && settledGet(t, p.b, "b") == "2" })
			if settledGet(t, p.b, "fixed") != body || p.b.Status().Halted != nil {
				t.Fatalf("after reopen: %s %v", dump(t, p.b), p.b.Status().Halted)
			}
		})
	}
}

func TestViewLimitRejectsTheEntry(t *testing.T) {
	apply := func(tx *Tx, e *Entry) error {
		if e.Kind == KindData && string(e.Body) == "huge" {
			_ = tx.Put(make([]byte, MaxViewKey+1), nil) // the error is ignored: the entry is rejected anyway
			return nil
		}
		return appApply(tx, e)
	}
	p := newFounder(t, "a", GroupConfig{}, apply)
	mustAppend(t, p.b, "huge")
	mustAppend(t, p.b, "x=1")
	waitUntil(t, 5*time.Second, func() bool { return quiet(p) && settledGet(t, p.b, "x") == "1" })
	for o := range p.b.Entries(0) {
		if string(o.Body) == "huge" && !o.Rejected {
			t.Fatal("an entry over a view limit was not rejected")
		}
	}
}

func TestTxIsDeadAfterApply(t *testing.T) {
	kept := make(chan *Tx, 4)
	apply := func(tx *Tx, e *Entry) error {
		if e.Kind == KindData {
			select {
			case kept <- tx:
			default:
			}
		}
		return appApply(tx, e)
	}
	p := newFounder(t, "a", GroupConfig{}, apply)
	mustAppend(t, p.b, "a=1")
	waitUntil(t, 5*time.Second, func() bool { return quiet(p) })
	tx := <-kept
	if err := tx.Put([]byte("late"), nil); !errors.Is(err, ErrTxDone) {
		t.Fatalf("put after apply: %v", err)
	}
	if _, err := tx.Get([]byte("a")); !errors.Is(err, ErrTxDone) {
		t.Fatalf("get after apply: %v", err)
	}
}

// TestApplySeesTheRosterAtItsPosition: Tx.Writers and Tx.Writer are the roster at the entry, with the seat flag of its config.
func TestApplySeesTheRosterAtItsPosition(t *testing.T) {
	apply := func(tx *Tx, e *Entry) error {
		if e.Kind != KindData {
			return nil
		}
		w, ok := tx.Member(e.Writer)
		if !ok || !w.Seat {
			return Reject("author not a seat")
		}
		return tx.Put([]byte("members"), []byte{byte(len(tx.Members()))})
	}
	p := newFounder(t, "a", GroupConfig{}, apply)
	mustAppend(t, p.b, "x")
	waitUntil(t, 5*time.Second, func() bool { return quiet(p) && settledGet(t, p.b, "members") == "\x01" })
}
