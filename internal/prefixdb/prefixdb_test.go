package prefixdb

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"testing"

	kolostore "github.com/ciranda-dev/kolo-store"
)

func keys(t *testing.T, b kolostore.Backend, start, end []byte, reverse bool, limit int) []string {
	t.Helper()
	var out []string
	if err := b.Scan(start, end, reverse, limit, func(k, v []byte) bool {
		out = append(out, string(k)+"="+string(v))
		return true
	}); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestSuccessor(t *testing.T) {
	for _, c := range []struct{ in, want []byte }{
		{[]byte("v"), []byte("w")},
		{[]byte("ab"), []byte("ac")},
		{[]byte{1, 0xff}, []byte{2}},
		{[]byte{1, 0xff, 0xff}, []byte{2}},
		{[]byte{0xff}, nil},
		{[]byte{0xff, 0xff}, nil},
		{nil, nil},
	} {
		if got := Successor(c.in); !bytes.Equal(got, c.want) || (c.want == nil) != (got == nil) {
			t.Errorf("Successor(%x) = %x, want %x", c.in, got, c.want)
		}
	}
	in := []byte("ab")
	Successor(in)
	if string(in) != "ab" {
		t.Error("Successor changed its argument")
	}
}

func TestPrefixIsolation(t *testing.T) {
	base := kolostore.NewMemory()
	a, b := New(base, []byte("a/")), New(base, []byte("b/"))
	base.Put([]byte("a"), []byte("bare"))
	base.Put([]byte("c"), []byte("other"))
	if err := a.Put([]byte("k1"), []byte("A1")); err != nil {
		t.Fatal(err)
	}
	a.Put([]byte("k2"), []byte("A2"))
	b.Put([]byte("k1"), []byte("B1"))
	if v, err := a.Get([]byte("k1")); err != nil || string(v) != "A1" {
		t.Fatalf("%q %v", v, err)
	}
	if v, _ := base.Get([]byte("b/k1")); string(v) != "B1" {
		t.Fatalf("stored key %q", v)
	}
	if _, err := a.Get([]byte("c")); !errors.Is(err, kolostore.ErrNotFound) {
		t.Fatalf("a key of the base: %v", err)
	}
	if got := keys(t, a, nil, nil, false, 0); !slices.Equal(got, []string{"k1=A1", "k2=A2"}) {
		t.Fatalf("scan all: %v", got)
	}
	if got := keys(t, a, []byte("k2"), nil, false, 0); !slices.Equal(got, []string{"k2=A2"}) {
		t.Fatalf("scan from: %v", got)
	}
	if got := keys(t, a, nil, []byte("k2"), false, 0); !slices.Equal(got, []string{"k1=A1"}) {
		t.Fatalf("scan to: %v", got)
	}
	if got := keys(t, a, nil, []byte{}, false, 0); len(got) != 0 {
		t.Fatalf("an empty non-nil end is an empty range: %v", got)
	}
	if got := keys(t, a, nil, nil, true, 1); !slices.Equal(got, []string{"k2=A2"}) {
		t.Fatalf("reverse limit: %v", got)
	}
	if err := a.Delete([]byte("k1")); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Get([]byte("k1")); err != nil {
		t.Fatalf("delete in another prefix: %v", err)
	}
	if err := a.Delete([]byte("missing")); err != nil {
		t.Fatalf("deleting a missing key: %v", err)
	}
}

func TestNestedAndEmptyPrefix(t *testing.T) {
	base := kolostore.NewMemory()
	if New(base, nil) != base {
		t.Fatal("an empty prefix returns the backend itself")
	}
	inner := New(New(base, []byte("p/")), []byte("v"))
	inner.Put([]byte("x"), []byte("1"))
	if v, err := base.Get([]byte("p/vx")); err != nil || string(v) != "1" {
		t.Fatalf("%q %v", v, err)
	}
}

func TestCloseDoesNotCloseTheBackend(t *testing.T) {
	base := kolostore.NewMemory()
	a, b := New(base, []byte("a")), New(base, []byte("b"))
	a.Put([]byte("k"), nil)
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if _, err := a.Get([]byte("k")); !errors.Is(err, kolostore.ErrClosed) {
		t.Fatal(err)
	}
	for name, err := range map[string]error{
		"Put": a.Put(nil, nil), "Delete": a.Delete(nil),
		"Scan": a.Scan(nil, nil, false, 0, func(k, v []byte) bool { return true }),
	} {
		if !errors.Is(err, kolostore.ErrClosed) {
			t.Errorf("%s after Close: %v", name, err)
		}
	}
	if err := b.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("a sibling wrapper after Close: %v", err)
	}
	if _, err := base.Get([]byte("ak")); err != nil {
		t.Fatalf("the backend after the wrapper's Close: %v", err)
	}
}

func TestBatch(t *testing.T) {
	base := kolostore.NewMemory()
	w := New(base, []byte("v"))
	for i := 0; i < 6; i++ {
		w.Put([]byte{byte('a' + i)}, []byte{byte(i)})
	}
	base.Put([]byte("w"), []byte("past the prefix"))
	base.Put([]byte("u"), []byte("before the prefix"))
	b := w.NewBatch()
	b.Delete([]byte("a"))
	b.DeleteRange([]byte("b"), []byte("d"))
	b.DeleteRange([]byte("e"), nil)
	b.Put([]byte("x"), []byte("X"))
	if err := b.Commit(); err != nil {
		t.Fatal(err)
	}
	if got := keys(t, w, nil, nil, false, 0); !slices.Equal(got, []string{"d=\x03", "x=X"}) {
		t.Fatalf("%q", got)
	}
	for _, k := range []string{"w", "u"} {
		if _, err := base.Get([]byte(k)); err != nil {
			t.Fatalf("an open range delete left its prefix: %v", err)
		}
	}
	if err := b.Commit(); err == nil {
		t.Fatal("a batch commits once")
	}
}

func TestWrapBatchSpansPrefixes(t *testing.T) {
	base := kolostore.NewMemory()
	base.Put([]byte("v1"), []byte("old"))
	base.Put([]byte("v2"), []byte("old"))
	base.Put([]byte("m1"), []byte("old"))
	raw := base.NewBatch()
	vb := WrapBatch(raw, base, []byte("v"))
	vb.DeleteRange(nil, nil)
	vb.Put([]byte("3"), []byte("new"))
	raw.Put([]byte("m1"), []byte("new"))
	if err := raw.Commit(); err != nil {
		t.Fatal(err)
	}
	if got := keys(t, base, nil, nil, false, 0); !slices.Equal(got, []string{"m1=new", "v3=new"}) {
		t.Fatalf("%q", got)
	}
	if err := vb.Commit(); err == nil {
		t.Fatal("a wrapped batch commits once")
	}
}

func TestDeleteRangeWithoutSuccessor(t *testing.T) {
	base := kolostore.NewMemory()
	p := []byte{0xff, 0xff}
	for _, k := range []string{"\xff\xfe", "\xff\xff", "\xff\xff1", "\xff\xff2", "\xff\xff\xff"} {
		base.Put([]byte(k), []byte("v"))
	}
	raw := base.NewBatch()
	WrapBatch(raw, base, p).DeleteRange([]byte("2"), nil)
	raw.Commit()
	got := keys(t, base, nil, nil, false, 0)
	want := []string{"\xff\xfe=v", "\xff\xff=v", "\xff\xff1=v"}
	if !slices.Equal(got, want) {
		t.Fatalf("%q, want %q", got, want)
	}
	w := New(base, p)
	if got := keys(t, w, nil, nil, false, 0); !slices.Equal(got, []string{"=v", "1=v"}) {
		t.Fatalf("scan without a successor: %q", got)
	}
	// an empty prefix: everything from the start key on
	raw = base.NewBatch()
	WrapBatch(raw, base, nil).DeleteRange([]byte("\xff\xff1"), nil)
	raw.Commit()
	if got := keys(t, base, nil, nil, false, 0); len(got) != 2 {
		t.Fatalf("%q", got)
	}
}

func TestScanCopies(t *testing.T) {
	base := kolostore.NewMemory()
	w := New(base, []byte("p"))
	w.Put([]byte("k"), []byte("v"))
	var kept []byte
	w.Scan(nil, nil, false, 0, func(k, v []byte) bool { kept = k; return true })
	kept[0] = 'X'
	if got := keys(t, w, nil, nil, false, 0); got[0] != "k=v" {
		t.Fatal(got)
	}
	if fmt.Sprint(kept) == "" {
		t.Fatal()
	}
}
