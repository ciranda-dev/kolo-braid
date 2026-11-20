package kolobraidtest

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	kolobraid "github.com/ciranda-dev/kolo-braid"
)

// failure is a TB that records the first Fatalf and stops the check, like testing.T does.
type failure struct{ msg string }

type stop struct{}

func (f *failure) Helper() {}

func (f *failure) Fatalf(format string, args ...any) {
	f.msg = fmt.Sprintf(format, args...)
	panic(stop{})
}

func run(f func(t TB)) (msg string) {
	fl := &failure{}
	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(stop); !ok {
				panic(r)
			}
			msg = fl.msg
		}
	}()
	f(fl)
	return ""
}

func history(n int) []kolobraid.Entry {
	es := make([]kolobraid.Entry, n)
	for i := range es {
		es[i] = kolobraid.Entry{
			Writer: bytes.Repeat([]byte{byte(1 + i%3)}, 32), Kind: kolobraid.KindData, Index: uint64(i / 3),
			Body: binary.BigEndian.AppendUint32(nil, uint32(i*7%11)),
		}
	}
	return es
}

// counter is a deterministic apply: a count, a sum that depends on the order of the entries and a scan.
func counter(tx *kolobraid.Tx, e *kolobraid.Entry) error {
	n := uint32(0)
	if v, err := tx.Get([]byte("n")); err == nil {
		n = binary.BigEndian.Uint32(v)
	}
	sum := uint32(0)
	for _, v := range tx.Scan([]byte("s/"), []byte("s0")) {
		sum = sum*31 + binary.BigEndian.Uint32(v)
	}
	v := binary.BigEndian.Uint32(e.Body)
	if v == 5 {
		return kolobraid.Reject("five")
	}
	if err := tx.Put(binary.BigEndian.AppendUint64([]byte("s/"), e.Position), binary.BigEndian.AppendUint32(nil, v+sum)); err != nil {
		return err
	}
	return tx.Put([]byte("n"), binary.BigEndian.AppendUint32(nil, n+1))
}

func TestCheckApplyPassesADeterministicApply(t *testing.T) {
	if msg := run(func(tb TB) { CheckApply(tb, counter, history(60)) }); msg != "" {
		t.Fatal(msg)
	}
	CheckApply(t, counter, history(1))
}

func TestCheckApplyFindsNondeterminism(t *testing.T) {
	var calls atomic.Int64
	cases := map[string]kolobraid.ApplyFunc{
		"clock or counter": func(tx *kolobraid.Tx, e *kolobraid.Entry) error {
			return tx.Put([]byte("t"), []byte(fmt.Sprint(calls.Add(1))))
		},
		"map order": func(tx *kolobraid.Tx, e *kolobraid.Entry) error {
			m := map[string]bool{}
			for i := 0; i < 10; i++ {
				m[fmt.Sprint("k", i)] = true
			}
			for k := range m {
				if err := tx.Put([]byte("o"), []byte(k)); err != nil {
					return err
				}
			}
			return nil
		},
		"state of its own": func() kolobraid.ApplyFunc {
			seen := 0
			return func(tx *kolobraid.Tx, e *kolobraid.Entry) error {
				seen++
				return tx.Put([]byte("seen"), []byte{byte(seen)})
			}
		}(),
	}
	for name, apply := range cases {
		msg := run(func(tb TB) { CheckApply(tb, apply, history(20)) })
		if !strings.HasPrefix(msg, "kolobraidtest: ") {
			t.Errorf("%s: not found: %q", name, msg)
		}
	}
}

func TestCheckApplyFindsADependenceOnTheTail(t *testing.T) {
	// an apply that behaves differently the first time it sees an entry (a cache, say) is caught by the reorganisation path only
	seen := map[kolobraid.EntryID]bool{}
	apply := func(tx *kolobraid.Tx, e *kolobraid.Entry) error {
		first := !seen[e.ID]
		seen[e.ID] = true
		if first {
			return tx.Put([]byte("first"), e.ID[:2])
		}
		return nil
	}
	if msg := run(func(tb TB) { CheckApply(tb, apply, history(10)) }); msg == "" {
		t.Fatal("not found")
	}
}

func TestCheckApplyFindsADependenceOnDeliveryOrder(t *testing.T) {
	// right for an order that is only ever extended, wrong after a reorganisation: only the reorganisation path replays a position
	last := -1
	apply := func(tx *kolobraid.Tx, e *kolobraid.Entry) error {
		if e.Position == 0 {
			last = -1
		}
		if int(e.Position) != last+1 {
			tx.Put([]byte("jump"), []byte{byte(e.Position)})
		}
		last = int(e.Position)
		return nil
	}
	if msg := run(func(tb TB) { CheckApply(tb, apply, history(30)) }); !strings.Contains(msg, "reorganisation") {
		t.Fatalf("%q", msg)
	}
}

func TestCheckApplyReportsAHaltedApply(t *testing.T) {
	apply := func(tx *kolobraid.Tx, e *kolobraid.Entry) error {
		if e.Position == 3 {
			return errors.New("bug")
		}
		return nil
	}
	msg := run(func(tb TB) { CheckApply(tb, apply, history(10)) })
	if !strings.Contains(msg, "halted at position 3") {
		t.Fatalf("%q", msg)
	}
	if msg := run(func(tb TB) { CheckApply(tb, counter, nil) }); msg == "" {
		t.Fatal("no entries")
	}
}

func TestCheckApplyFillsMissingIDs(t *testing.T) {
	es := history(30)
	es[4].ID = kolobraid.EntryID{9}
	es[5].ID = kolobraid.EntryID{9} // a duplicate
	if msg := run(func(tb TB) { CheckApply(tb, counter, es) }); msg != "" {
		t.Fatal(msg)
	}
}

func TestCheckUpgrade(t *testing.T) {
	es := history(30)
	marker := 20
	upgraded := func(tx *kolobraid.Tx, e *kolobraid.Entry) error {
		if e.Position >= uint64(marker) {
			return tx.Put([]byte("new"), []byte("behaviour"))
		}
		return counter(tx, e)
	}
	if msg := run(func(tb TB) { CheckUpgrade(tb, counter, upgraded, es, marker) }); msg != "" {
		t.Fatal(msg)
	}
	early := func(tx *kolobraid.Tx, e *kolobraid.Entry) error {
		if e.Position >= uint64(marker-3) {
			return tx.Put([]byte("new"), []byte("behaviour"))
		}
		return counter(tx, e)
	}
	msg := run(func(tb TB) { CheckUpgrade(tb, counter, early, es, marker) })
	if !strings.Contains(msg, "position 17") {
		t.Fatalf("%q", msg)
	}
	last := func(tx *kolobraid.Tx, e *kolobraid.Entry) error {
		if e.Position == uint64(marker-1) {
			return tx.Put([]byte("new"), []byte("behaviour"))
		}
		return counter(tx, e)
	}
	msg = run(func(tb TB) { CheckUpgrade(tb, counter, last, es, marker) })
	if !strings.Contains(msg, "position 19") {
		t.Fatalf("a change at the last position before the marker: %q", msg)
	}
	for _, m := range []int{-1, 31} {
		if msg := run(func(tb TB) { CheckUpgrade(tb, counter, counter, es, m) }); msg == "" {
			t.Fatalf("marker %d accepted", m)
		}
	}
	CheckUpgrade(t, counter, counter, es, len(es))
}

func TestFindDivergencePassesADeterministicApply(t *testing.T) {
	for seed := range uint64(5) {
		d, err := FindDivergence(counter, history(60), 6, seed)
		if err != nil || d != nil {
			t.Fatalf("seed %d: %v %v", seed, d, err)
		}
	}
	if d, err := FindDivergence(counter, history(1), 2, 0); err != nil || d != nil {
		t.Fatalf("one entry: %v %v", d, err)
	}
}

func TestFindDivergenceFlagsAnImpureApply(t *testing.T) {
	calls := 0
	d, err := FindDivergence(func(tx *kolobraid.Tx, e *kolobraid.Entry) error {
		calls++
		return tx.Put([]byte("calls"), []byte(fmt.Sprint(calls)))
	}, history(20), 4, 1)
	if err != nil || d == nil || d.Position != 0 || d.Replica != 1 || d.Want == d.Got {
		t.Fatalf("a counter outside the view: %v %v", d, err)
	}
	if !strings.HasPrefix(d.String(), "position 0: replica 1 has running digest ") {
		t.Fatalf("%q", d.String())
	}
	seen := map[kolobraid.EntryID]bool{}
	cases := map[string]kolobraid.ApplyFunc{
		"map order": func(tx *kolobraid.Tx, e *kolobraid.Entry) error {
			m := map[string]bool{}
			for i := 0; i < 10; i++ {
				m[fmt.Sprint("k", i)] = true
			}
			for k := range m {
				return tx.Put(binary.BigEndian.AppendUint64([]byte("o"), e.Position), []byte(k))
			}
			return nil
		},
		"first sight": func(tx *kolobraid.Tx, e *kolobraid.Entry) error {
			if !seen[e.ID] {
				seen[e.ID] = true
				return tx.Put([]byte("first"), e.ID[:2])
			}
			return nil
		},
		"delivery order": func() kolobraid.ApplyFunc {
			// right while an order is only extended: only a reorganisation applies a position again
			last := -1
			return func(tx *kolobraid.Tx, e *kolobraid.Entry) error {
				if e.Position == 0 {
					last = -1
				}
				jump := int(e.Position) != last+1
				last = int(e.Position)
				if jump {
					return tx.Put(binary.BigEndian.AppendUint64([]byte("jump"), e.Position), nil)
				}
				return nil
			}
		}(),
	}
	for name, apply := range cases {
		if d, err := FindDivergence(apply, history(30), 4, 7); err != nil || d == nil {
			t.Errorf("%s: not found: %v %v", name, d, err)
		}
	}
}

func TestFindDivergenceReportsAHaltedApply(t *testing.T) {
	_, err := FindDivergence(func(tx *kolobraid.Tx, e *kolobraid.Entry) error {
		if e.Position == 3 {
			return errors.New("bug")
		}
		return nil
	}, history(10), 3, 0)
	if err == nil || !strings.Contains(err.Error(), "replica 0") || !strings.Contains(err.Error(), "bug") {
		t.Fatalf("%v", err)
	}
	if _, err := FindDivergence(counter, nil, 3, 0); err == nil {
		t.Fatal("no entries accepted")
	}
	if _, err := FindDivergence(counter, history(5), 1, 0); err == nil {
		t.Fatal("one replica accepted")
	}
}

func TestEarliestTakesTheLowestPosition(t *testing.T) {
	a, b := kolobraid.Digest{1}, kolobraid.Digest{2}
	runs := [][]kolobraid.Digest{{a, a, a, a}, {a, a, a, a}, {a, a, b, b}, {a, b, b, b}, {a, b, a, a}}
	if d := earliest(runs); d == nil || d.Replica != 3 || d.Position != 1 || d.Want != a || d.Got != b {
		t.Fatalf("%v", d)
	}
	if d := earliest(runs[:2]); d != nil {
		t.Fatalf("equal runs: %v", d)
	}
}
