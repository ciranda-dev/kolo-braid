package kolobraid

import (
	"bytes"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"

	kolostore "github.com/ciranda-dev/kolo-store"
)

func settleOne(v *view, batch uint64, e Entry) (settleResult, error) {
	return v.Settle(batch, vtSteps([]Entry{e}))
}

func TestRejectOnlyDeterministicFailure(t *testing.T) {
	es := []Entry{
		vtEntry(0, vtOp{kind: 'p', a: "a", b: "1"}),
		vtEntry(1, vtOp{kind: 'p', a: "b", b: "2"}, vtOp{kind: 'e'}),
		vtEntry(2, vtOp{kind: 'p', a: "c", b: "3"}),
	}
	for name, mid := range map[string]string{"error": "e", "panic": "n"} {
		es[1] = vtEntry(1, vtOp{kind: 'p', a: "b", b: "2"}, vtOp{kind: mid[0]})
		b := kolostore.NewMemory()
		cfg := vtConfig()
		v := vtOpen(t, b, cfg, vtApply)
		res, err := v.Settle(0, vtSteps(es))
		if !errors.Is(err, ErrHalted) || !strings.Contains(err.Error(), "position 1") {
			t.Fatalf("%s: %v", name, err)
		}
		if res.Entries != 1 {
			t.Fatalf("%s: %d entries applied before the halt", name, res.Entries)
		}
		if name == "panic" && !strings.Contains(err.Error(), "panicked") {
			t.Fatalf("panic error: %v", err)
		}
		if !errors.Is(v.Halted(), ErrHalted) {
			t.Fatalf("%s: Halted = %v", name, v.Halted())
		}
		// the writes of the failing entry are discarded and nothing after it is applied
		s := v.Settled()
		if got := vtDump(s.Scan(nil, nil)); len(got) != 1 || got[0] != "a\x001" {
			t.Fatalf("%s: view after the halt %q", name, got)
		}
		s.Close()
		if p := v.Progress(); p.Next != 1 || p.Offset != 1 {
			t.Fatalf("%s: progress %+v, the entries before the failing one are committed", name, p)
		}
		if _, err := settleOne(v, 0, es[2]); !errors.Is(err, ErrHalted) {
			t.Fatalf("%s: settling after the halt: %v", name, err)
		}
		// the next open retries: with a fixed apply the entry goes through
		fixed := vtOpen(t, b, cfg, vtApply)
		es[1] = vtEntry(1, vtOp{kind: 'p', a: "b", b: "2"})
		if _, err := fixed.Settle(0, vtSteps(es[1:])); err != nil {
			t.Fatalf("%s: retry: %v", name, err)
		}
		if fixed.Progress().Next != 3 {
			t.Fatalf("%s: %+v", name, fixed.Progress())
		}
	}
}

func TestPanicMessageHasNoData(t *testing.T) {
	secret := "entry-body-secret"
	for _, p := range []any{secret, fmt.Errorf("%s", secret), []byte(secret), struct{ s string }{secret}} {
		v := vtOpen(t, kolostore.NewMemory(), vtConfig(), func(tx *Tx, e *Entry) error { panic(p) })
		_, err := settleOne(v, 0, vtEntry(0))
		if !errors.Is(err, ErrHalted) || strings.Contains(err.Error(), secret) {
			t.Fatalf("%T: %v", p, err)
		}
	}
	var m map[string]int
	v := vtOpen(t, kolostore.NewMemory(), vtConfig(), func(tx *Tx, e *Entry) error { m["x"] = 1; return nil })
	_, err := settleOne(v, 0, vtEntry(0))
	if !errors.Is(err, ErrHalted) || !strings.Contains(err.Error(), "nil map") {
		t.Fatalf("a runtime error keeps its fixed message: %v", err)
	}
	v = vtOpen(t, kolostore.NewMemory(), vtConfig(), func(tx *Tx, e *Entry) error { panic(nil) })
	if _, err := settleOne(v, 0, vtEntry(0)); !errors.Is(err, ErrHalted) {
		t.Fatalf("panic(nil): %v", err)
	}
}

func TestViewLimitRejectsEvenWhenApplyIgnoresIt(t *testing.T) {
	big := func(n int) []byte { return make([]byte, n) }
	cfg := vtConfig()
	cfg.MaxBody = 1 << 10 // budget 16 KiB
	cases := map[string]func(tx *Tx) error{
		"key":         func(tx *Tx) error { return tx.Put(big(MaxViewKey+1), nil) },
		"value":       func(tx *Tx) error { return tx.Put([]byte("k"), big(MaxViewValue+1)) },
		"delete key":  func(tx *Tx) error { return tx.Delete(big(MaxViewKey + 1)) },
		"range start": func(tx *Tx) error { return tx.DeleteRange(big(MaxViewKey+1), []byte("z")) },
		"range end":   func(tx *Tx) error { return tx.DeleteRange([]byte("a"), big(MaxViewKey+1)) },
		"budget":      func(tx *Tx) error { return tx.Put([]byte("k"), big(16<<10)) },
		"budget small": func(tx *Tx) error {
			var err error
			for i := 0; i < 100 && err == nil; i++ {
				err = tx.Put([]byte{byte(i)}, nil)
			}
			return err
		},
	}
	for name, f := range cases {
		for _, mode := range []string{"ignore", "propagate", "wrapped"} {
			v := vtOpen(t, kolostore.NewMemory(), cfg, func(tx *Tx, e *Entry) error {
				tx.Put([]byte("before"), []byte("1"))
				err := f(tx)
				if !errors.Is(err, ErrViewLimit) {
					t.Errorf("%s: %v", name, err)
				}
				switch mode {
				case "propagate":
					return err
				case "wrapped":
					return fmt.Errorf("apply: %w", err)
				}
				return nil
			})
			res, err := settleOne(v, 0, vtEntry(0))
			if err != nil {
				t.Fatalf("%s/%s: a limit hit is a rejection, not a halt: %v", name, mode, err)
			}
			if rej, _ := v.Rejected(0); !rej {
				t.Fatalf("%s/%s: entry not rejected", name, mode)
			}
			s := v.Settled()
			if n := len(vtDump(s.Scan(nil, nil))); n != 0 {
				t.Fatalf("%s/%s: %d pairs of a rejected entry committed", name, mode, n)
			}
			s.Close()
			want := runningDigest(cfg.Initial, 0, vtID(0), statusRejected, nil)
			if res.Progress.Digest != want {
				t.Fatalf("%s/%s: digest", name, mode)
			}
		}
	}
}

func TestViewLimitEdges(t *testing.T) {
	v := vtOpen(t, kolostore.NewMemory(), vtConfig(), func(tx *Tx, e *Entry) error {
		if err := tx.Put(make([]byte, MaxViewKey), make([]byte, MaxViewValue)); err != nil {
			t.Errorf("a key of exactly 32 KiB and a value of exactly 1 MiB: %v", err)
		}
		if err := tx.DeleteRange(make([]byte, MaxViewKey), bytes.Repeat([]byte{1}, MaxViewKey)); err != nil {
			t.Errorf("range bounds of exactly 32 KiB: %v", err)
		}
		return nil
	})
	if _, err := settleOne(v, 0, vtEntry(0)); err != nil {
		t.Fatal(err)
	}
	if rej, _ := v.Rejected(0); rej {
		t.Fatal("rejected")
	}
	// an entry that spends exactly its budget is accepted, the next write is not
	cfg := vtConfig()
	cfg.MaxBody = 1 << 10
	budget := 16 << 10
	v2 := vtOpen(t, kolostore.NewMemory(), cfg, func(tx *Tx, e *Entry) error {
		if err := tx.Put([]byte("k"), make([]byte, budget-1-writeOpCost)); err != nil {
			t.Errorf("exactly the budget: %v", err)
		}
		if err := tx.Put(nil, nil); !errors.Is(err, ErrViewLimit) {
			t.Errorf("one write over: %v", err)
		}
		return nil
	})
	if _, err := settleOne(v2, 0, vtEntry(0)); err != nil {
		t.Fatal(err)
	}
}

func TestOperationCountBound(t *testing.T) {
	cfg := vtConfig()
	cfg.MaxBody = 1 << 10 // budget 16 KiB: 64 operations of one key byte each
	n := 0
	v := vtOpen(t, kolostore.NewMemory(), cfg, func(tx *Tx, e *Entry) error {
		for tx.Put([]byte{byte(n)}, nil) == nil {
			n++
		}
		return nil
	})
	settleOne(v, 0, vtEntry(0))
	if want := (16 << 10) / (1 + writeOpCost); n != want {
		t.Fatalf("%d operations before the budget ran out, want %d", n, want)
	}
}

func TestTxAfterApplyReturned(t *testing.T) {
	var kept *Tx
	v := vtOpen(t, kolostore.NewMemory(), vtConfig(), func(tx *Tx, e *Entry) error {
		kept = tx
		return tx.Put([]byte("a"), []byte("1"))
	})
	if _, err := settleOne(v, 0, vtEntry(0)); err != nil {
		t.Fatal(err)
	}
	checks := map[string]error{}
	_, checks["Get"] = kept.Get([]byte("a"))
	checks["Put"] = kept.Put([]byte("b"), nil)
	checks["Delete"] = kept.Delete([]byte("a"))
	checks["DeleteRange"] = kept.DeleteRange([]byte("a"), nil)
	for name, err := range checks {
		if !errors.Is(err, ErrTxDone) {
			t.Errorf("%s after apply: %v", name, err)
		}
	}
	for range kept.Scan(nil, nil) {
		t.Error("Scan after apply yielded a pair")
	}
	if _, ok := kept.Member(nil); ok || kept.Members() != nil {
		t.Error("roster after apply")
	}
	s := v.Settled()
	defer s.Close()
	if got := vtDump(s.Scan(nil, nil)); len(got) != 1 {
		t.Fatalf("a retained handle changed the view: %q", got)
	}
	// also after a panic and after an error
	for _, f := range []ApplyFunc{
		func(tx *Tx, e *Entry) error { kept = tx; panic("x") },
		func(tx *Tx, e *Entry) error { kept = tx; return errors.New("x") },
		func(tx *Tx, e *Entry) error { kept = tx; return Reject("x") },
	} {
		v := vtOpen(t, kolostore.NewMemory(), vtConfig(), f)
		settleOne(v, 0, vtEntry(0))
		if err := kept.Put(nil, nil); !errors.Is(err, ErrTxDone) {
			t.Errorf("after a failing apply: %v", err)
		}
	}
}

func TestTxEndsWhenApplyExits(t *testing.T) {
	var kept *Tx
	done := make(chan struct{})
	v := vtOpen(t, kolostore.NewMemory(), vtConfig(), func(tx *Tx, e *Entry) error {
		kept = tx
		go func() {
			defer close(done)
			runtime.Goexit()
		}()
		return nil
	})
	if _, err := settleOne(v, 0, vtEntry(0)); err != nil {
		t.Fatal(err)
	}
	<-done
	if err := kept.Put(nil, nil); !errors.Is(err, ErrTxDone) {
		t.Fatal(err)
	}
}

func TestReentrancy(t *testing.T) {
	var v *view
	var inner []error
	apply := func(tx *Tx, e *Entry) error {
		_, err := v.ApplyTail(vtStep(vtEntry(50)))
		inner = append(inner, err)
		_, err = v.Settle(1, vtSteps(nil))
		inner = append(inner, err)
		inner = append(inner, v.DropTail(0), v.Reset())
		_, err = v.Verify(0, Digest{})
		inner = append(inner, err)
		_, err = v.ContentDigest()
		inner = append(inner, err)
		inner = append(inner, v.SeedPairs(nil))
		// reads of a snapshot are allowed from inside apply
		s := v.Settled()
		defer s.Close()
		if _, err := s.Get([]byte("x")); !errors.Is(err, ErrNotFound) {
			t.Errorf("snapshot read inside apply: %v", err)
		}
		return tx.Put([]byte("ok"), nil)
	}
	v = vtOpen(t, kolostore.NewMemory(), vtConfig(), apply)
	if _, err := v.Settle(0, vtSteps([]Entry{vtEntry(0)})); err != nil {
		t.Fatalf("an apply that re-enters the view does not halt it by itself: %v", err)
	}
	if len(inner) != 7 {
		t.Fatalf("%d inner calls", len(inner))
	}
	for i, err := range inner {
		if !errors.Is(err, errReentered) || !strings.HasPrefix(err.Error(), "kolobraid: ") {
			t.Errorf("inner call %d: %v", i, err)
		}
	}
	if p := v.Progress(); p.Next != 1 {
		t.Fatalf("%+v", p)
	}
}

func TestOneDriverAtATime(t *testing.T) {
	block, release := make(chan struct{}), make(chan struct{})
	v := vtOpen(t, kolostore.NewMemory(), vtConfig(), func(tx *Tx, e *Entry) error {
		close(block)
		<-release
		return nil
	})
	done := make(chan error)
	go func() { _, err := settleOne(v, 0, vtEntry(0)); done <- err }()
	<-block
	if _, err := v.ApplyTail(vtStep(vtEntry(1))); !errors.Is(err, errReentered) {
		t.Errorf("second goroutine: %v", err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestStorageErrorHaltsInsteadOfRejecting(t *testing.T) {
	disk := kolostore.NewMemory()
	f := newVtFault(disk, 0, false)
	for name, body := range map[string]func(tx *Tx) error{
		"get": func(tx *Tx) error { _, err := tx.Get([]byte("x")); return err },
		"scan": func(tx *Tx) error {
			for range tx.Scan(nil, nil) {
			}
			return nil
		},
	} {
		f.getErr.Store(false)
		v := vtOpen(t, f, vtConfig(), func(tx *Tx, e *Entry) error {
			f.getErr.Store(true)
			err := body(tx)
			if name == "get" && !errors.Is(err, errInjected) {
				t.Errorf("Get: %v", err)
			}
			// apply swallows the error, or turns it into a Reject: neither may count as a deterministic outcome
			if name == "get" {
				return Reject("could not read")
			}
			return nil
		})
		f.getErr.Store(false)
		_, err := settleOne(v, 0, vtEntry(0))
		if !errors.Is(err, ErrHalted) || !errors.Is(err, errInjected) {
			t.Fatalf("%s: a storage error must halt, never reject: %v", name, err)
		}
		if p := v.Progress(); p.Next != 0 {
			t.Fatalf("%s: %+v", name, p)
		}
	}
}

func TestReadsStayInsideTheViewKeyspace(t *testing.T) {
	b := kolostore.NewMemory()
	outside := []string{"mview/other", "r/members", "l/strand", "a", "w", "u"}
	for _, k := range outside {
		b.Put([]byte(k), []byte("outside"))
	}
	v := vtOpen(t, b, vtConfig(), func(tx *Tx, e *Entry) error {
		for _, k := range append(outside, "mview/progress", "view/progress", "r") {
			if v, err := tx.Get([]byte(k)); err == nil {
				t.Errorf("read %q = %q from outside the view", k, v)
			}
		}
		for k, v := range tx.Scan(nil, nil) {
			t.Errorf("scan saw %q = %q", k, v)
		}
		for k, v := range tx.Scan([]byte("a"), []byte("z")) {
			t.Errorf("bounded scan saw %q = %q", k, v)
		}
		tx.DeleteRange(nil, nil)
		tx.DeleteRange([]byte("a"), nil)
		tx.Put([]byte("mview/progress"), []byte("evil"))
		return nil
	})
	if _, err := settleOne(v, 0, vtEntry(0)); err != nil {
		t.Fatal(err)
	}
	for _, k := range outside {
		if got, err := b.Get([]byte(k)); err != nil || string(got) != "outside" {
			t.Errorf("key %q outside the view was changed: %q %v", k, got, err)
		}
	}
	if got, err := b.Get([]byte("mview/progress")); err != nil || bytes.Equal(got, []byte("evil")) {
		t.Errorf("the progress record was overwritten: %q %v", got, err)
	}
	if got, err := b.Get([]byte("vmview/progress")); err != nil || string(got) != "evil" {
		t.Errorf("the write lands under the view prefix: %q %v", got, err)
	}
}

func TestHugeReadsDoNotBreakDeterminism(t *testing.T) {
	// an apply that reads a lot is slow, not wrong: two peers with different chunking agree
	es := vtScript(9, 60)
	ref := func(chunk int) Digest {
		cfg := vtConfig()
		cfg.chunk = chunk
		v := vtOpen(t, kolostore.NewMemory(), cfg, vtApply)
		res, err := v.Settle(0, vtSteps(es))
		if err != nil {
			t.Fatal(err)
		}
		return res.Progress.Digest
	}
	if a, b := ref(1), ref(256); a != b {
		t.Fatal("digest depends on the chunk size")
	}
}

func TestLimitedApplyCallCount(t *testing.T) {
	var calls atomic.Int64
	v := vtOpen(t, kolostore.NewMemory(), vtConfig(), func(tx *Tx, e *Entry) error { calls.Add(1); return nil })
	es := make([]Entry, 10)
	for i := range es {
		es[i] = vtEntry(i)
	}
	if _, err := v.Settle(0, vtSteps(es)); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 10 {
		t.Fatalf("%d calls", calls.Load())
	}
}

// TestDeleteRangeKeyBounds: with the default write budget, only the view key bound limits the two ends of a range, so an end
// one byte over it is refused and one at it is not.
func TestDeleteRangeKeyBounds(t *testing.T) {
	big := func(n int) []byte { return bytes.Repeat([]byte{'z'}, n) }
	cfg := vtConfig()
	cfg.MaxBody = defaultMaxBody
	cases := []struct {
		name       string
		start, end []byte
		refused    bool
	}{
		{"end over", []byte("a"), big(MaxViewKey + 1), true},
		{"end at", []byte("a"), big(MaxViewKey), false},
		{"start over", big(MaxViewKey + 1), nil, true},
		{"start at", big(MaxViewKey), nil, false},
	}
	for _, c := range cases {
		var got error
		v := vtOpen(t, kolostore.NewMemory(), cfg, func(tx *Tx, e *Entry) error {
			got = tx.DeleteRange(c.start, c.end)
			return nil
		})
		if _, err := settleOne(v, 0, vtEntry(0)); err != nil {
			t.Fatal(err)
		}
		if errors.Is(got, ErrViewLimit) != c.refused {
			t.Fatalf("%s: %v", c.name, got)
		}
	}
}
