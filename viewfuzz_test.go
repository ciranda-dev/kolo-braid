package kolobraid

import (
	"errors"
	"slices"
	"testing"

	kolostore "github.com/ciranda-dev/kolo-store"
)

func FuzzDecodeProgress(f *testing.F) {
	f.Add(progress{}.encode())
	f.Add(progress{Next: 1000, Digest: Digest(vtPat(32, 3)), Batches: 4, Offset: 9}.encode())
	f.Add(progress{Next: ^uint64(0), Batches: ^uint64(0), Offset: ^uint64(0)}.encode())
	f.Add([]byte{})
	f.Add([]byte("kolo-braid/view/v0"))
	f.Fuzz(func(t *testing.T, b []byte) {
		p, err := decodeProgress(b)
		if err != nil {
			if !errors.Is(err, ErrCorrupt) {
				t.Fatalf("error %v does not wrap ErrCorrupt", err)
			}
			return
		}
		if got := p.encode(); string(got) != string(b) {
			t.Fatalf("decoded record encodes to other bytes: %x != %x", got, b)
		}
	})
}

// fuzzKeys is a small key space, so the operations collide.
var fuzzKeys = []string{"", "a", "a\x00", "b", "ba", "c", "d\xff", "\xff"}

// FuzzTxOps runs a script of operations on a Tx over a settled base and compares every read, then the committed view and the
// overlay, with a naive map model.
func FuzzTxOps(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 1, 1, 3, 2, 4, 0, 7, 5, 1, 6})
	f.Add([]byte{2, 0, 7, 2, 1, 2, 3, 3, 4, 0, 0, 5, 2, 4, 1, 1})
	f.Add([]byte{})
	f.Add([]byte{3, 3, 3, 3, 4, 2, 5, 0, 1, 1, 0, 0, 2, 5, 4, 7, 7})
	f.Fuzz(func(t *testing.T, script []byte) {
		if len(script) > 400 {
			script = script[:400]
		}
		base := []seedPair{{Key: []byte("a"), Value: []byte("A")}, {Key: []byte("b"), Value: []byte("B")}, {Key: []byte("d\xff"), Value: []byte("D")}}
		model := map[string]string{}
		for _, p := range base {
			model[string(p.Key)] = string(p.Value)
		}
		inRange := func(k, a, b string) bool { return k >= a && (b == "" || k < b) }
		sorted := func(a, b string) []string {
			var keys, out []string
			for k := range model {
				if inRange(k, a, b) {
					keys = append(keys, k)
				}
			}
			slices.Sort(keys)
			for _, k := range keys {
				out = append(out, k+"\x00"+model[k])
			}
			return out
		}
		apply := func(tx *Tx, e *Entry) error {
			if e.Index == 0 {
				for _, p := range base {
					if err := tx.Put(p.Key, p.Value); err != nil {
						return err
					}
				}
				return nil
			}
			for i := 0; i+2 < len(script); i += 3 {
				op, a, b := script[i]%6, fuzzKeys[script[i+1]%8], fuzzKeys[script[i+2]%8]
				switch op {
				case 0:
					v := a + "|" + b
					if err := tx.Put([]byte(a), []byte(v)); err != nil {
						t.Fatal(err)
					}
					model[a] = v
				case 1:
					if err := tx.Delete([]byte(a)); err != nil {
						t.Fatal(err)
					}
					delete(model, a)
				case 2:
					if err := tx.DeleteRange([]byte(a), []byte(b)); err != nil {
						t.Fatal(err)
					}
					for k := range model {
						if a < b || b == "" {
							if inRange(k, a, b) {
								delete(model, k)
							}
						}
					}
				case 3:
					got, err := tx.Get([]byte(a))
					want, ok := model[a]
					if ok != (err == nil) || (ok && string(got) != want) {
						t.Fatalf("Get(%q) = %q, %v; model %q %v", a, got, err, want, ok)
					}
				case 4, 5:
					var got []string
					for k, v := range tx.Scan([]byte(a), []byte(b)) {
						got = append(got, string(k)+"\x00"+string(v))
					}
					want := sorted(a, b)
					if a >= b && b != "" {
						want = nil
					}
					if !slices.Equal(got, want) {
						t.Fatalf("Scan(%q, %q) = %q, model %q", a, b, got, want)
					}
				}
			}
			return nil
		}
		cfg := vtConfig()
		v, err := openView(kolostore.NewMemory(), cfg, apply)
		if err != nil {
			t.Fatal(err)
		}
		e0, e1 := vtEntry(0), vtEntry(1)
		if _, err := settleOne(v, 0, e0); err != nil {
			t.Fatal(err)
		}
		// the same script tentatively, then settled by promotion
		if res, err := v.ApplyTail(vtStep(e1)); err != nil || res.Status != statusApplied {
			t.Fatalf("%+v %v", res, err)
		}
		tentative := v.View()
		want := sorted("", "")
		vtEq(t, "tentative view", vtDump(tentative.Scan(nil, nil)), want)
		tentative.Close()
		// settle e1: promotion must give the same view as the tentative run
		res, err := settleOne(v, 1, e1)
		if err != nil || res.Promoted != 1 {
			t.Fatalf("%+v %v", res, err)
		}
		s := v.Settled()
		vtEq(t, "settled view", vtDump(s.Scan(nil, nil)), want)
		s.Close()
		// and applying it afresh on a second view, with the script run again against the model, gives the same digest
		model = map[string]string{}
		for _, p := range base {
			model[string(p.Key)] = string(p.Value)
		}
		v2, _ := openView(kolostore.NewMemory(), cfg, apply)
		settleOne(v2, 0, e0)
		res2, err := settleOne(v2, 1, e1)
		if err != nil || res2.Progress.Digest != res.Progress.Digest {
			t.Fatalf("fresh application: %+v %v", res2.Progress, err)
		}
	})
}
