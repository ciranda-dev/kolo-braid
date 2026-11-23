package kv

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	kolocodec "github.com/ciranda-dev/kolo-codec"
	kolostore "github.com/ciranda-dev/kolo-store"

	kolobraid "github.com/ciranda-dev/kolo-braid"
	ireplay "github.com/ciranda-dev/kolo-braid/internal/replay"
	"github.com/ciranda-dev/kolo-braid/kolobraidtest"
)

type replayer = ireplay.Replayer[kolobraid.Entry, kolobraid.Digest, *kolobraid.Snapshot]

var openReplayer = ireplay.Get[kolobraid.ApplyFunc, []kolobraid.Member, kolobraid.Entry, kolobraid.Digest, *kolobraid.Snapshot]()

func newReplayer(b kolostore.Backend, apply kolobraid.ApplyFunc, members []kolobraid.Member) (replayer, error) {
	return openReplayer(b, apply, members, ireplay.Options{})
}

func mustEncode(t testing.TB, o op) []byte {
	t.Helper()
	b, err := o.encode()
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestOpRoundTrip(t *testing.T) {
	for _, o := range []op{
		{Key: []byte("theme"), Value: []byte("dark")},
		{Key: nil, Value: nil},
		{Delete: true, Key: []byte("theme")},
		{Key: bytes.Repeat([]byte{'k'}, MaxKeySize), Value: bytes.Repeat([]byte{'v'}, MaxValueSize)},
	} {
		b := mustEncode(t, o)
		if !isOp(b) {
			t.Fatal("an encoded op is not recognised")
		}
		got, err := decodeOp(b)
		if err != nil {
			t.Fatal(err)
		}
		if got.Delete != o.Delete || !bytes.Equal(got.Key, o.Key) || !bytes.Equal(got.Value, o.Value) {
			t.Fatalf("round trip of %d-byte key: got %+v", len(o.Key), got)
		}
	}
	// the format is the record's: string tag, uvarint op, bytes(key), bytes(value)
	want := append(append([]byte{byte(len(tag))}, tag...), 1, 1, 'a', 1, 'b')
	if got := mustEncode(t, op{Key: []byte("a"), Value: []byte("b")}); !bytes.Equal(got, want) {
		t.Fatalf("put encoding %x, want %x", got, want)
	}
	want = append(append([]byte{byte(len(tag))}, tag...), 2, 1, 'a', 0)
	if got := mustEncode(t, op{Delete: true, Key: []byte("a")}); !bytes.Equal(got, want) {
		t.Fatalf("delete encoding %x, want %x", got, want)
	}
}

func TestEncodeRefuses(t *testing.T) {
	for name, c := range map[string]struct {
		o    op
		want error
	}{
		"key":    {op{Key: make([]byte, MaxKeySize+1)}, kolobraid.ErrTooLarge},
		"value":  {op{Value: make([]byte, MaxValueSize+1)}, kolobraid.ErrTooLarge},
		"delete": {op{Delete: true, Value: []byte("x")}, kolobraid.ErrInvalidArgument},
	} {
		if _, err := c.o.encode(); !errors.Is(err, c.want) {
			t.Errorf("%s: %v, want %v", name, err, c.want)
		}
	}
}

// raw builds a body field by field, for bodies the encoder never makes.
func raw(fields ...func(*kolocodec.Encoder)) []byte {
	var e kolocodec.Encoder
	for _, f := range fields {
		f(&e)
	}
	return e.Bytes()
}

func str(s string) func(*kolocodec.Encoder) { return func(e *kolocodec.Encoder) { e.PutString(s) } }
func uv(v uint64) func(*kolocodec.Encoder)  { return func(e *kolocodec.Encoder) { e.PutUvarint(v) } }
func bs(b []byte) func(*kolocodec.Encoder)  { return func(e *kolocodec.Encoder) { e.PutBytes(b) } }
func fixed(b ...byte) func(*kolocodec.Encoder) {
	return func(e *kolocodec.Encoder) { e.PutFixed(b) }
}

func TestDecodeOpRefuses(t *testing.T) {
	for name, b := range map[string][]byte{
		"empty":             nil,
		"other tag":         raw(str("kolo-braid/kv/v1"), uv(1), bs(nil), bs(nil)),
		"tag only":          raw(str(tag)),
		"op 0":              raw(str(tag), uv(0), bs(nil), bs(nil)),
		"op 3":              raw(str(tag), uv(3), bs(nil), bs(nil)),
		"non-minimal op":    raw(str(tag), fixed(0x81, 0x00), bs(nil), bs(nil)),
		"non-minimal len":   raw(str(tag), uv(1), fixed(0x80, 0x00), bs(nil)),
		"key over bound":    raw(str(tag), uv(1), bs(make([]byte, MaxKeySize+1)), bs(nil)),
		"value over bound":  raw(str(tag), uv(1), bs(nil), bs(make([]byte, MaxValueSize+1))),
		"delete with value": raw(str(tag), uv(2), bs([]byte("k")), bs([]byte("v"))),
		"no value":          raw(str(tag), uv(1), bs([]byte("k"))),
		"trailing byte":     raw(str(tag), uv(1), bs([]byte("k")), bs(nil), fixed(0)),
		"short key":         raw(str(tag), uv(1), fixed(5, 'a')),
	} {
		if _, err := decodeOp(b); !errors.Is(err, kolobraid.ErrMalformed) {
			t.Errorf("%s: %v, want ErrMalformed", name, err)
		}
	}
	if isOp(raw(str("kolo-braid/kv/v1"))) || isOp([]byte("k=v")) || isOp(nil) {
		t.Fatal("a body of another format is taken for a kv op")
	}
	if !isOp(raw(str(tag), uv(9))) {
		t.Fatal("a malformed kv body is not handled, so it would not be rejected")
	}
}

func TestUpper(t *testing.T) {
	for _, c := range []struct{ in, want []byte }{
		{nil, nil},
		{[]byte{0}, []byte{1}},
		{[]byte("s/"), []byte("s0")},
		{[]byte{'a', 0xff}, []byte{'b'}},
		{[]byte{0xff, 0xff}, nil},
	} {
		if got := upper(c.in); !bytes.Equal(got, c.want) || (c.want == nil) != (got == nil) {
			t.Errorf("upper(%x) = %x, want %x", c.in, got, c.want)
		}
	}
}

var writers = [][]byte{bytes.Repeat([]byte{1}, 32), bytes.Repeat([]byte{2}, 32), bytes.Repeat([]byte{3}, 32)}

func data(t testing.TB, w int, body []byte) kolobraid.Entry {
	return kolobraid.Entry{Writer: writers[w], Kind: kolobraid.KindData, Body: body}
}

func put(t testing.TB, w int, k, v string) kolobraid.Entry {
	return data(t, w, mustEncode(t, op{Key: []byte(k), Value: []byte(v)}))
}

func del(t testing.TB, w int, k string) kolobraid.Entry {
	return data(t, w, mustEncode(t, op{Delete: true, Key: []byte(k)}))
}

// settings is the store of the tests; app routes kv entries to it and counts every other data entry under "n".
var settings = New([]byte("s/"))

func app(tx *kolobraid.Tx, e *kolobraid.Entry) error {
	if ok, err := settings.Apply(tx, e); ok || err != nil {
		return err
	}
	if e.Kind != kolobraid.KindData {
		return nil
	}
	n := byte(0)
	if v, err := tx.Get([]byte("n")); err == nil {
		n = v[0]
	}
	return tx.Put([]byte("n"), []byte{n + 1})
}

// replay settles es in order on a fresh view and returns it with its replayer.
func replay(t testing.TB, apply kolobraid.ApplyFunc, es []kolobraid.Entry) replayer {
	t.Helper()
	var ms []kolobraid.Member
	for _, w := range writers {
		ms = append(ms, kolobraid.Member{Key: w})
	}
	return replayMembers(t, apply, ms, es)
}

func replayMembers(t testing.TB, apply kolobraid.ApplyFunc, ms []kolobraid.Member, es []kolobraid.Entry) replayer {
	t.Helper()
	db := kolostore.NewMemory()
	t.Cleanup(func() { db.Close() })
	r, err := newReplayer(db, apply, ms)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.Close() })
	for i := range es {
		es[i].ID = sha256.Sum256(fmt.Appendf(nil, "kv-test/%d", i))
	}
	if err := r.Settle(es); err != nil {
		t.Fatal(err)
	}
	return r
}

func pairs(s *Store, v *kolobraid.Snapshot, start, end []byte) []string {
	var out []string
	for k, val := range s.Range(v, start, end) {
		out = append(out, string(k)+"="+string(val))
	}
	return out
}

func TestApply(t *testing.T) {
	es := []kolobraid.Entry{
		put(t, 0, "theme", "light"),
		put(t, 1, "lang", "en"),
		put(t, 2, "theme", "dark"),
		data(t, 0, []byte("chat message")),
		put(t, 1, "zone", "utc"),
		del(t, 0, "zone"),
		del(t, 2, "never-set"),
		data(t, 1, raw(str(tag), uv(7), bs([]byte("theme")), bs([]byte("bad")))),
		data(t, 1, raw(str(tag), uv(1), bs([]byte("theme")), bs([]byte("x")), fixed(0))),
		{Writer: writers[0], Kind: kolobraid.KindSettlers, Body: mustEncode(t, op{Key: []byte("theme"), Value: []byte("member kind")})},
		put(t, 2, "", "empty key"),
	}
	r := replay(t, app, es)
	s := r.Settled()
	defer s.Close()
	if v, err := settings.Get(s, []byte("theme")); err != nil || string(v) != "dark" {
		t.Fatalf("theme = %q, %v: the last effective put must win and rejected ops change nothing", v, err)
	}
	if _, err := settings.Get(s, []byte("zone")); !errors.Is(err, kolobraid.ErrNotFound) {
		t.Fatalf("deleted key: %v", err)
	}
	if v, err := settings.Get(s, nil); err != nil || string(v) != "empty key" {
		t.Fatalf("empty key = %q, %v", v, err)
	}
	if got, want := pairs(settings, s, nil, nil), []string{"=empty key", "lang=en", "theme=dark"}; !slices.Equal(got, want) {
		t.Fatalf("range %q, want %q", got, want)
	}
	if got, want := pairs(settings, s, []byte("lang"), []byte("theme")), []string{"lang=en"}; !slices.Equal(got, want) {
		t.Fatalf("range [lang, theme) %q, want %q", got, want)
	}
	if got := pairs(settings, s, []byte("theme"), []byte("lang")); got != nil {
		t.Fatalf("range with start > end: %q", got)
	}
	// an empty end is no upper bound, as in Snapshot.Scan
	if got, want := pairs(settings, s, []byte("t"), []byte{}), []string{"theme=dark"}; !slices.Equal(got, want) {
		t.Fatalf("range with an empty end %q, want %q", got, want)
	}
	if got, want := pairs(New(nil), s, []byte("a"), []byte{}), pairs(New(nil), s, []byte("a"), nil); !slices.Equal(got, want) || got == nil {
		t.Fatalf("range of a store without a prefix with an empty end %q, want %q", got, want)
	}
	if got, want := pairs(settings, s, []byte("t"), nil), []string{"theme=dark"}; !slices.Equal(got, want) {
		t.Fatalf("range [t, nil) %q, want %q", got, want)
	}
	if v, err := s.Get([]byte("n")); err != nil || !bytes.Equal(v, []byte{1}) {
		t.Fatalf("the application's own entry was not routed past the store: %v %v", v, err)
	}
	// pairs outside the prefix are not the store's
	other := New([]byte("t/"))
	if got := pairs(other, s, nil, nil); got != nil {
		t.Fatalf("another prefix sees %q", got)
	}
	n := 0
	for k := range s.Scan(nil, nil) {
		if !bytes.HasPrefix(k, []byte("s/")) && string(k) != "n" {
			t.Fatalf("view key %q outside the prefix", k)
		}
		n++
	}
	if n != 4 {
		t.Fatalf("%d view keys, want 4", n)
	}
	// stopping a range early stops the scan
	for range settings.Range(s, nil, nil) {
		break
	}
}

// TestRejectedOps: a kv body that does not decode, or whose key does not fit with the prefix, is rejected; the rejection shows
// in the running digest exactly as an entry the view refused.
func TestRejectedOps(t *testing.T) {
	long := New(bytes.Repeat([]byte{'p'}, 8))
	apply := func(tx *kolobraid.Tx, e *kolobraid.Entry) error {
		_, err := long.Apply(tx, e)
		return err
	}
	tooLong := put(t, 0, string(make([]byte, MaxKeySize-7)), "v")
	fits := put(t, 0, string(make([]byte, MaxKeySize-8)), "v")
	bad := data(t, 0, raw(str(tag), uv(4)))
	refuse := func(*kolobraid.Tx, *kolobraid.Entry) error { return kolobraid.Reject("refused") }
	for name, c := range map[string]struct {
		e    kolobraid.Entry
		keep bool
	}{"key over the bound with the prefix": {tooLong, false}, "malformed": {bad, false}, "largest key": {fits, true}} {
		r := replay(t, apply, []kolobraid.Entry{c.e})
		s := r.Settled()
		n := 0
		for range s.Scan(nil, nil) {
			n++
		}
		s.Close()
		if (n == 1) != c.keep {
			t.Fatalf("%s: %d pairs written", name, n)
		}
		if rejected := replay(t, refuse, []kolobraid.Entry{c.e}).Digest() == r.Digest(); rejected == c.keep {
			t.Fatalf("%s: rejected %v", name, rejected)
		}
	}
	s := replay(t, apply, []kolobraid.Entry{fits}).Settled()
	defer s.Close()
	if _, err := long.Get(s, make([]byte, MaxKeySize-7)); !errors.Is(err, kolobraid.ErrNotFound) {
		t.Fatalf("Get of a key that cannot fit: %v", err)
	}
}

// script is a random history of kv operations from three writers on a few keys, with malformed kv bodies, foreign data bodies
// and entries of other kinds mixed in.
func script(t testing.TB, seed uint64, n int) []kolobraid.Entry {
	rng := rand.New(rand.NewPCG(seed, 11))
	es := make([]kolobraid.Entry, n)
	for i := range es {
		w, k := rng.IntN(3), fmt.Sprintf("k%d", rng.IntN(5))
		switch r := rng.IntN(10); {
		case r < 6:
			es[i] = put(t, w, k, fmt.Sprintf("v%d", i))
		case r < 8:
			es[i] = del(t, w, k)
		case r == 8:
			es[i] = data(t, w, raw(str(tag), uv(uint64(3+rng.IntN(5))), bs([]byte(k))))
		default:
			es[i] = data(t, w, []byte(k))
		}
	}
	return es
}

// model is the reference: the last effective operation on a key wins, in the given order.
func model(t testing.TB, es []kolobraid.Entry) []string {
	m := map[string]string{}
	for _, e := range es {
		if !isOp(e.Body) {
			continue
		}
		o, err := decodeOp(e.Body)
		if err != nil {
			continue
		}
		if o.Delete {
			delete(m, string(o.Key))
		} else {
			m[string(o.Key)] = string(o.Value)
		}
	}
	var out []string
	for k, v := range m {
		out = append(out, k+"="+v)
	}
	slices.Sort(out)
	return out
}

// TestDeterministic: the store keeps the apply contract (CheckApply's replays, reorgs and restarts), every replica that receives
// the same history in another shuffled delivery order reaches the same running digest at every position (FindDivergence), and
// the result is the reference map.
func TestDeterministic(t *testing.T) {
	for seed := uint64(1); seed <= 6; seed++ {
		es := script(t, seed, 120)
		kolobraidtest.CheckApply(t, app, es)
		d, err := kolobraidtest.FindDivergence(app, es, 4, seed)
		if err != nil {
			t.Fatal(err)
		}
		if d != nil {
			t.Fatalf("seed %d: %s", seed, d)
		}
		r := replay(t, app, slices.Clone(es))
		s := r.Settled()
		got := pairs(settings, s, nil, nil)
		s.Close()
		if want := model(t, es); !slices.Equal(got, want) {
			t.Fatalf("seed %d: map %q, want %q", seed, got, want)
		}
	}
}

// TestTentativeThenSettled: a tentative read sees the tail, a settled read only the settled prefix, and a reorg of the tail
// changes what the tentative read sees.
func TestTentativeThenSettled(t *testing.T) {
	db := kolostore.NewMemory()
	defer db.Close()
	r, err := newReplayer(db, app, []kolobraid.Member{{Key: writers[0]}, {Key: writers[1]}})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	a, b := put(t, 0, "theme", "light"), put(t, 1, "theme", "dark")
	a.ID, b.ID = sha256.Sum256([]byte("a")), sha256.Sum256([]byte("b"))
	read := func(s *kolobraid.Snapshot) string {
		defer s.Close()
		v, err := settings.Get(s, []byte("theme"))
		if err != nil {
			return err.Error()
		}
		return string(v)
	}
	if _, err := r.Tentative([]kolobraid.Entry{a, b}); err != nil {
		t.Fatal(err)
	}
	if got := read(r.View()); got != "dark" {
		t.Fatalf("tentative read %q", got)
	}
	if got := read(r.Settled()); got != kolobraid.ErrNotFound.Error() {
		t.Fatalf("settled read before settlement %q", got)
	}
	if _, err := r.Tentative([]kolobraid.Entry{b, a}); err != nil {
		t.Fatal(err)
	}
	if got := read(r.View()); got != "light" {
		t.Fatalf("tentative read after the reorg %q", got)
	}
	if err := r.Settle([]kolobraid.Entry{b, a}); err != nil {
		t.Fatal(err)
	}
	if got := read(r.Settled()); got != "light" {
		t.Fatalf("settled read %q", got)
	}
}

func FuzzKVOp(f *testing.F) {
	f.Add(mustEncode(f, op{Key: []byte("theme"), Value: []byte("dark")}))
	f.Add(mustEncode(f, op{Delete: true, Key: []byte("theme")}))
	f.Add(raw(str(tag), uv(2), bs([]byte("k")), bs([]byte("v"))))
	f.Add(raw(str(tag), fixed(0x81, 0x00)))
	f.Add([]byte("k=v"))
	f.Fuzz(func(t *testing.T, b []byte) {
		o, err := decodeOp(b)
		if err != nil {
			if !errors.Is(err, kolobraid.ErrMalformed) {
				t.Fatalf("error %v does not wrap ErrMalformed", err)
			}
			return
		}
		if !isOp(b) {
			t.Fatal("a decoded op is not recognised")
		}
		again, err := o.encode()
		if err != nil || !bytes.Equal(again, b) {
			t.Fatalf("a decoded op is not canonical: %x then %x, %v", b, again, err)
		}
	})
}

// TestStoresSideBySide: two stores in one view see only their own pairs, also with no upper bound.
func TestStoresSideBySide(t *testing.T) {
	a, b := New([]byte("a/")), New([]byte("b/"))
	apply := func(tx *kolobraid.Tx, e *kolobraid.Entry) error {
		st := a
		if e.Writer[0] == 2 {
			st = b
		}
		_, err := st.Apply(tx, e)
		return err
	}
	r := replay(t, apply, []kolobraid.Entry{put(t, 0, "x", "1"), put(t, 1, "y", "2")})
	s := r.Settled()
	defer s.Close()
	if got := pairs(a, s, nil, nil); !slices.Equal(got, []string{"x=1"}) {
		t.Fatalf("store a sees %q", got)
	}
	if got := pairs(b, s, nil, nil); !slices.Equal(got, []string{"y=2"}) {
		t.Fatalf("store b sees %q", got)
	}
}

func owned(i int, k string) string { return string(writers[i]) + "/" + k }

// ownerStore lets a member write only the keys that start with its own public key.
var ownerStore = NewWith([]byte("s/"), Options{Allow: func(_ *kolobraid.Tx, e *kolobraid.Entry, key []byte, _ bool) bool {
	return bytes.HasPrefix(key, e.Writer)
}})

func ownerApply(tx *kolobraid.Tx, e *kolobraid.Entry) error {
	_, err := ownerStore.Apply(tx, e)
	return err
}

// TestAllowRejectsEverywhere: a refused op is a reject in the digest, the same on every peer, and changes nothing.
func TestAllowRejectsEverywhere(t *testing.T) {
	es := []kolobraid.Entry{
		put(t, 0, owned(0, "a"), "1"),
		put(t, 1, owned(0, "a"), "stolen"),
		del(t, 2, owned(0, "a")),
		put(t, 1, owned(1, "b"), "2"),
		put(t, 2, "", "no owner"),
	}
	kolobraidtest.CheckApply(t, ownerApply, es)
	d, err := kolobraidtest.FindDivergence(ownerApply, es, 4, 1)
	if err != nil || d != nil {
		t.Fatalf("divergence %v, %v", d, err)
	}
	r := replay(t, ownerApply, slices.Clone(es))
	s := r.Settled()
	got := pairs(ownerStore, s, nil, nil)
	s.Close()
	if want := []string{owned(0, "a") + "=1", owned(1, "b") + "=2"}; !slices.Equal(got, want) {
		t.Fatalf("map %q, want %q", got, want)
	}
	// the same history with the refused entries rejected by hand must give the same digest, and the open store another
	byHand := func(tx *kolobraid.Tx, e *kolobraid.Entry) error {
		if o, err := decodeOp(e.Body); err == nil && !bytes.HasPrefix(o.Key, e.Writer) {
			return kolobraid.Reject("by hand")
		}
		_, err := New([]byte("s/")).Apply(tx, e)
		return err
	}
	if replay(t, byHand, slices.Clone(es)).Digest() != r.Digest() {
		t.Fatal("a refused op is not a reject in the digest")
	}
	if replay(t, app, slices.Clone(es)).Digest() == r.Digest() {
		t.Fatal("the refused ops took effect")
	}
}

// TestAllowOwnerKeys: keys under a member's key are writable only by that member, for puts and deletes.
func TestAllowOwnerKeys(t *testing.T) {
	r := replay(t, ownerApply, []kolobraid.Entry{
		put(t, 0, owned(0, "x"), "mine"),
		put(t, 0, owned(0, "y"), "mine"),
		put(t, 1, owned(0, "x"), "theirs"),
		del(t, 1, owned(0, "y")),
		put(t, 1, owned(1, "x"), "own"),
		del(t, 0, owned(0, "y")),
	})
	s := r.Settled()
	defer s.Close()
	if got, want := pairs(ownerStore, s, nil, nil), []string{owned(0, "x") + "=mine", owned(1, "x") + "=own"}; !slices.Equal(got, want) {
		t.Fatalf("map %q, want %q", got, want)
	}
}

// TestAllowSeesRoles: Allow reads the roster of the entry's position through tx, here admins only.
func TestAllowSeesRoles(t *testing.T) {
	st := NewWith(nil, Options{Allow: func(tx *kolobraid.Tx, e *kolobraid.Entry, _ []byte, _ bool) bool {
		m, ok := tx.Member(e.Writer)
		return ok && m.Roles&kolobraid.RoleAdmin != 0
	}})
	apply := func(tx *kolobraid.Tx, e *kolobraid.Entry) error {
		_, err := st.Apply(tx, e)
		return err
	}
	ms := []kolobraid.Member{{Key: writers[0], Roles: kolobraid.RoleAdmin}, {Key: writers[1]}, {Key: writers[2]}}
	r := replayMembers(t, apply, ms, []kolobraid.Entry{put(t, 1, "a", "member"), put(t, 0, "b", "admin"), del(t, 2, "b")})
	s := r.Settled()
	defer s.Close()
	if got, want := pairs(st, s, nil, nil), []string{"b=admin"}; !slices.Equal(got, want) {
		t.Fatalf("map %q, want %q", got, want)
	}
}

// TestAllowPanicHalts: a panic in Allow halts the view with ErrHalted and leaks no entry data.
func TestAllowPanicHalts(t *testing.T) {
	st := NewWith(nil, Options{Allow: func(*kolobraid.Tx, *kolobraid.Entry, []byte, bool) bool { panic("secret-key") }})
	apply := func(tx *kolobraid.Tx, e *kolobraid.Entry) error {
		_, err := st.Apply(tx, e)
		return err
	}
	var ms []kolobraid.Member
	for _, w := range writers {
		ms = append(ms, kolobraid.Member{Key: w})
	}
	db := kolostore.NewMemory()
	defer db.Close()
	r, err := newReplayer(db, apply, ms)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	e := put(t, 0, "k", "v")
	e.ID = sha256.Sum256([]byte("panic"))
	err = r.Settle([]kolobraid.Entry{e})
	if !errors.Is(err, kolobraid.ErrHalted) || strings.Contains(err.Error(), "secret-key") {
		t.Fatalf("settle returned %v, want ErrHalted without the panic value", err)
	}
}
