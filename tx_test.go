package kolobraid

import (
	"bytes"
	"crypto/ed25519"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"

	kolostore "github.com/ciranda-dev/kolo-store"
)

// vtTx runs f as the apply function of one entry on a fresh view with the given pairs already settled, and returns the outcome
// of that entry and the view.
func vtTx(t *testing.T, settled []seedPair, f ApplyFunc) (*view, settleResult, error) {
	t.Helper()
	cfg := vtConfig()
	v := vtOpen(t, kolostore.NewMemory(), cfg, func(tx *Tx, e *Entry) error {
		if e.Index == 0 {
			for _, p := range settled {
				if err := tx.Put(p.Key, p.Value); err != nil {
					return err
				}
			}
			return nil
		}
		return f(tx, e)
	})
	if _, err := v.Settle(0, vtSteps([]Entry{vtEntry(0)})); err != nil {
		t.Fatal(err)
	}
	res, err := v.Settle(1, vtSteps([]Entry{vtEntry(1)}))
	return v, res, err
}

func TestTxReadsItsOwnWrites(t *testing.T) {
	_, _, err := vtTx(t, []seedPair{{Key: []byte("a"), Value: []byte("old")}, {Key: []byte("b"), Value: []byte("B")}}, func(tx *Tx, e *Entry) error {
		if v, err := tx.Get([]byte("a")); err != nil || string(v) != "old" {
			t.Errorf("Get a = %q, %v", v, err)
		}
		tx.Put([]byte("a"), []byte("new"))
		if v, _ := tx.Get([]byte("a")); string(v) != "new" {
			t.Errorf("own write not read: %q", v)
		}
		tx.Delete([]byte("b"))
		if _, err := tx.Get([]byte("b")); !errors.Is(err, ErrNotFound) {
			t.Errorf("own delete not read: %v", err)
		}
		tx.Put([]byte("c"), []byte("C"))
		tx.DeleteRange([]byte("c"), nil)
		if _, err := tx.Get([]byte("c")); !errors.Is(err, ErrNotFound) {
			t.Errorf("own range delete not read: %v", err)
		}
		tx.Put([]byte("c"), []byte("again"))
		if v, _ := tx.Get([]byte("c")); string(v) != "again" {
			t.Errorf("put after range delete: %q", v)
		}
		if _, err := tx.Get(make([]byte, MaxViewKey+1)); !errors.Is(err, ErrNotFound) {
			t.Errorf("a key over the bound cannot exist: %v", err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestTxGetReturnsCopies(t *testing.T) {
	v, _, err := vtTx(t, []seedPair{{Key: []byte("a"), Value: []byte("value")}}, func(tx *Tx, e *Entry) error {
		got, _ := tx.Get([]byte("a"))
		got[0] = 'X'
		again, _ := tx.Get([]byte("a"))
		if string(again) != "value" {
			t.Errorf("a caller changed the stored value: %q", again)
		}
		tx.Put([]byte("own"), []byte("mine"))
		own, _ := tx.Get([]byte("own"))
		own[0] = 'X'
		if again, _ := tx.Get([]byte("own")); string(again) != "mine" {
			t.Errorf("a caller changed the value of the entry's own write: %q", again)
		}
		key, val := []byte("k"), []byte("v1")
		tx.Put(key, val)
		key[0], val[1] = 'z', '2'
		if v, err := tx.Get([]byte("k")); err != nil || string(v) != "v1" {
			t.Errorf("Put kept the caller's slices: %q %v", v, err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	s := v.Settled()
	defer s.Close()
	if got, _ := s.Get([]byte("k")); string(got) != "v1" {
		t.Fatalf("committed %q", got)
	}
}

func TestTxScan(t *testing.T) {
	var settled []seedPair
	for _, k := range []string{"a", "b", "c", "d", "e", "f"} {
		settled = append(settled, seedPair{Key: []byte(k), Value: []byte(strings.ToUpper(k))})
	}
	scan := func(tx *Tx, a, b string) []string {
		var out []string
		var end []byte
		if b != "" {
			end = []byte(b)
		}
		for k, v := range tx.Scan([]byte(a), end) {
			out = append(out, string(k)+"="+string(v))
		}
		return out
	}
	_, _, err := vtTx(t, settled, func(tx *Tx, e *Entry) error {
		eq := func(what string, got []string, want ...string) {
			t.Helper()
			if !slices.Equal(got, want) {
				t.Errorf("%s: %v, want %v", what, got, want)
			}
		}
		eq("all", scan(tx, "", ""), "a=A", "b=B", "c=C", "d=D", "e=E", "f=F")
		eq("range", scan(tx, "b", "e"), "b=B", "c=C", "d=D")
		eq("empty range", scan(tx, "d", "b"))
		eq("equal bounds", scan(tx, "b", "b"))
		tx.Put([]byte("bb"), []byte("new"))
		tx.Delete([]byte("c"))
		tx.Put([]byte("d"), []byte("changed"))
		eq("own writes", scan(tx, "b", "e"), "b=B", "bb=new", "d=changed")
		tx.DeleteRange([]byte("d"), []byte("f"))
		tx.Put([]byte("dd"), []byte("late"))
		eq("after range delete", scan(tx, "", ""), "a=A", "b=B", "bb=new", "dd=late", "f=F")
		tx.DeleteRange([]byte("dd"), nil)
		eq("open range delete", scan(tx, "", ""), "a=A", "b=B", "bb=new")
		// stop early
		n := 0
		for range tx.Scan(nil, nil) {
			n++
			break
		}
		if n != 1 {
			t.Errorf("break: %d", n)
		}
		// writes made while iterating are not seen by the iteration
		var seen []string
		for k := range tx.Scan(nil, nil) {
			seen = append(seen, string(k))
			tx.Put([]byte("zz"+string(k)), nil)
		}
		eq("iteration is frozen", seen, "a", "b", "bb")
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestTxScanManyPages(t *testing.T) {
	var settled []seedPair
	for i := 0; i < pageCount*3+17; i++ {
		settled = append(settled, seedPair{Key: []byte(fmt.Sprintf("k%05d", i)), Value: []byte{byte(i)}})
	}
	_, _, err := vtTx(t, settled, func(tx *Tx, e *Entry) error {
		// an overlay of puts and deletes spread over the pages
		for i := 0; i < len(settled); i += 5 {
			tx.Delete(settled[i].Key)
		}
		tx.Put([]byte("k00003x"), []byte("mid"))
		tx.Put([]byte("zzz"), []byte("end"))
		tx.Put([]byte("a"), []byte("start"))
		var keys []string
		for k := range tx.Scan(nil, nil) {
			keys = append(keys, string(k))
		}
		if !slices.IsSorted(keys) {
			t.Error("keys not in order")
		}
		want := 3 + len(settled) - (len(settled)+4)/5
		if len(keys) != want {
			t.Errorf("%d keys, want %d", len(keys), want)
		}
		if keys[0] != "a" || keys[len(keys)-1] != "zzz" || !slices.Contains(keys, "k00003x") || slices.Contains(keys, "k00005") {
			t.Error("wrong keys")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestScanPagesByBytes(t *testing.T) {
	big := bytes.Repeat([]byte{7}, 300<<10)
	var settled []seedPair
	for i := 0; i < 9; i++ {
		settled = append(settled, seedPair{Key: []byte{byte('a' + i)}, Value: big})
	}
	_, _, err := vtTx(t, settled, func(tx *Tx, e *Entry) error {
		n := 0
		for k, v := range tx.Scan(nil, nil) {
			if len(v) != len(big) || len(k) != 1 {
				t.Errorf("pair %q has %d bytes", k, len(v))
			}
			n++
		}
		if n != 9 {
			t.Errorf("%d pairs, want 9", n)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestTxRoster(t *testing.T) {
	key := ed25519.PublicKey(bytes.Repeat([]byte{1}, 32))
	v := vtOpen(t, kolostore.NewMemory(), vtConfig(), func(tx *Tx, e *Entry) error {
		w, ok := tx.Member(key)
		if !ok || w.Roles != RoleAdmin || !w.Seat {
			t.Errorf("Member = %+v %v", w, ok)
		}
		w.Key[0] ^= 0xff
		if w2, _ := tx.Member(key); w2.Key[0] != 1 {
			t.Error("Member returned the roster's own slice")
		}
		if _, ok := tx.Member(make([]byte, 32)); ok {
			t.Error("unknown member found")
		}
		ws := tx.Members()
		ws[0].Key[1] ^= 0xff
		if again := tx.Members(); len(again) != 1 || again[0].Key[1] != 1 {
			t.Error("Members returned the roster's own slices")
		}
		return nil
	})
	if _, err := v.Settle(0, vtSteps([]Entry{vtEntry(0)})); err != nil {
		t.Fatal(err)
	}
	// no roster given: nothing found, no panic
	v2 := vtOpen(t, kolostore.NewMemory(), vtConfig(), func(tx *Tx, e *Entry) error {
		if _, ok := tx.Member(key); ok || tx.Members() != nil {
			t.Error("a step without a roster has no members")
		}
		return nil
	})
	if _, err := v2.Settle(0, func(y func(applyStep, error) bool) { y(applyStep{Entry: vtEntry(0)}, nil) }); err != nil {
		t.Fatal(err)
	}
}

func TestApplyGetsACopyOfTheEntry(t *testing.T) {
	e := vtEntry(0)
	e.Body = []byte("body")
	e.Links = []Link{{Index: 3}}
	v := vtOpen(t, kolostore.NewMemory(), vtConfig(), func(tx *Tx, e *Entry) error {
		if e.Position != 0 {
			t.Errorf("position %d", e.Position)
		}
		e.Body[0], e.Links[0].Index, e.Writer[0] = 'X', 9, 9
		return nil
	})
	body, links, w := bytes.Clone(e.Body), slices.Clone(e.Links), bytes.Clone(e.Writer)
	if _, err := v.Settle(0, vtSteps([]Entry{e})); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(e.Body, body) || !slices.Equal(e.Links, links) || !bytes.Equal(e.Writer, w) {
		t.Fatal("apply changed the caller's entry")
	}
}

func TestRejectError(t *testing.T) {
	err := Reject("too long")
	if !errors.Is(err, ErrRejected) || !strings.HasPrefix(err.Error(), "kolobraid: ") || !strings.Contains(err.Error(), "too long") {
		t.Fatalf("%v", err)
	}
	if !errors.Is(fmt.Errorf("wrapped: %w", err), ErrRejected) {
		t.Fatal("a wrapped Reject is still a rejection")
	}
	if errors.Is(errors.New("x"), ErrRejected) {
		t.Fatal("any error")
	}
}

func TestTxConcurrentUseIsSafe(t *testing.T) {
	var wg sync.WaitGroup
	v := vtOpen(t, kolostore.NewMemory(), vtConfig(), func(tx *Tx, e *Entry) error {
		for i := 0; i < 4; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 50; j++ {
					tx.Put([]byte{byte(i), byte(j)}, nil)
					tx.Get([]byte{byte(i)})
					for range tx.Scan(nil, nil) {
					}
				}
			}()
		}
		wg.Wait()
		return nil
	})
	if _, err := v.Settle(0, vtSteps([]Entry{vtEntry(0)})); err != nil {
		t.Fatal(err)
	}
	s := v.Settled()
	defer s.Close()
	n := 0
	for range s.Scan(nil, nil) {
		n++
	}
	if n != 200 {
		t.Fatalf("%d pairs, want 200", n)
	}
}
