package kolobraid

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	kolostore "github.com/ciranda-dev/kolo-store"
)

// countFault is a backend whose scans fail from the at-th one on, counted from the last reset; fired records that one did.
type countFault struct {
	kolostore.Backend
	n, at *atomic.Int64
	fired *atomic.Bool
}

func (f countFault) Scan(start, end []byte, reverse bool, limit int, fn func(k, v []byte) bool) error {
	if at := f.at.Load(); at > 0 && f.n.Add(1) >= at {
		f.fired.Store(true)
		return errInjected
	}
	return f.Backend.Scan(start, end, reverse, limit, fn)
}

// FuzzSnapshotScanErr: a Scan cut short by a storage error at any page yields a prefix of the range and Err reports it; a scan
// that ends normally yields the whole range and Err is nil.
func FuzzSnapshotScanErr(f *testing.F) {
	f.Add([]byte("abc"), uint8(1), uint8(0))
	f.Add(slices.Repeat([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, 30), uint8(2), uint8(7))
	f.Add(slices.Repeat([]byte{200, 100, 50}, 100), uint8(0), uint8(3))
	f.Fuzz(func(t *testing.T, script []byte, failAt, from uint8) {
		if len(script) > 600 {
			script = script[:600]
		}
		apply := func(tx *Tx, e *Entry) error {
			for i, c := range script {
				if err := tx.Put(fmt.Appendf(nil, "k%03d", c), fmt.Appendf(nil, "v%d", i)); err != nil {
					return err
				}
			}
			return nil
		}
		fb := countFault{Backend: kolostore.NewMemory(), n: new(atomic.Int64), at: new(atomic.Int64), fired: new(atomic.Bool)}
		cfg := vtConfig()
		cfg.MaxBody = maxMaxBody
		v, err := openView(fb, cfg, apply)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := settleOne(v, 0, vtEntry(0)); err != nil {
			t.Fatal(err)
		}
		start := fmt.Appendf(nil, "k%03d", from)
		whole := v.Settled()
		want := vtDump(whole.Scan(start, nil))
		if whole.Err() != nil {
			t.Fatal(whole.Err())
		}
		whole.Close()
		s := v.Settled()
		defer s.Close()
		fb.n.Store(0)
		fb.at.Store(int64(failAt % 4))
		got := vtDump(s.Scan(start, nil))
		fb.at.Store(0)
		if len(got) > len(want) || !slices.Equal(got, want[:len(got)]) {
			t.Fatalf("scan cut short is no prefix: %q of %q", got, want)
		}
		switch fired := fb.fired.Load(); {
		case fired && !errors.Is(s.Err(), errInjected):
			t.Fatalf("a storage error cut the scan at %d of %d pairs, Err %v", len(got), len(want), s.Err())
		case !fired && (s.Err() != nil || len(got) != len(want)):
			t.Fatalf("no storage error: Err %v, %d of %d pairs", s.Err(), len(got), len(want))
		}
	})
}

// FuzzSettledAt: the snapshot at every settled position of a founder's braid holds the view that a model of appApply gives
// for the entries up to it.
func FuzzSettledAt(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6, 7})
	f.Add([]byte{8, 9, 2, 12, 1, 0})
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, script []byte) {
		if len(script) > 12 {
			script = script[:12]
		}
		a := newFounder(t, "a", GroupConfig{}, appApply)
		var bodies []string
		for i, c := range script {
			k := string(rune('a' + c%4))
			switch c / 4 % 4 {
			case 0:
				bodies = append(bodies, fmt.Sprintf("%s=%d", k, i))
			case 1:
				bodies = append(bodies, "-"+k)
			case 2:
				bodies = append(bodies, "!")
			default:
				bodies = append(bodies, k)
			}
		}
		bodies = append(bodies, "end=1")
		for _, body := range bodies {
			mustAppend(t, a.b, body)
		}
		waitUntil(t, 10*time.Second, func() bool { return quiet(a) && settledGet(t, a.b, "end") == "1" })
		model := map[string]string{}
		for _, o := range settledEntries(t, a.b) {
			if o.Kind == KindData && !o.Rejected {
				body := string(o.Body)
				if k, ok := strings.CutPrefix(body, "-"); ok {
					delete(model, k)
				} else {
					k, v, _ := strings.Cut(body, "=")
					model[k] = v
				}
			}
			s, err := a.b.SettledAt(context.Background(), o.Position)
			if err != nil {
				t.Fatal(err)
			}
			for _, k := range []string{"a", "b", "c", "d", "end"} {
				want, ok := model[k]
				if !ok {
					want = "<none>"
				}
				if got := get(t, s, k); got != want {
					t.Fatalf("position %d: %s = %q, want %q", o.Position, k, got, want)
				}
			}
			if s.Position() != o.Position || s.Err() != nil {
				t.Fatalf("position %d: snapshot at %d, Err %v", o.Position, s.Position(), s.Err())
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
		}
		if n := scratchKeys(t, a.b, keySettledAt); n != 0 {
			t.Fatalf("%d scratch keys after every Close", n)
		}
	})
}
