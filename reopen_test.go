package kolobraid

import (
	"path/filepath"
	"testing"

	kolostore "github.com/ciranda-dev/kolo-store"
)

// TestReopenEmptyStrandConfirmed: a member that is not a seat and never appended is confirmed after a reopen by a member that
// holds no head of its strand, since an empty strand has nothing such a peer could show; a stranger's answer does not count,
// and a member that appended is not confirmed that way. The peers' answers are given in process.
func TestReopenEmptyStrandConfirmed(t *testing.T) {
	for _, wrote := range []bool{false, true} {
		f := newFounder(t, "f", GroupConfig{}, appApply)
		dir := filepath.Join(t.TempDir(), "w")
		open := func() *peer {
			db, err := kolostore.OpenPebble(dir)
			if err != nil {
				t.Fatal(err)
			}
			return &peer{t: t, name: "w", db: db, opts: testOptions(db, appApply)}
		}
		w := open()
		w.id = newIdentity(t)
		j, err := NewJoin(ctx10(t), w.id, w.opts)
		if err != nil {
			t.Fatal(err)
		}
		req, _ := j.Request(nil)
		none := Role(0)
		welcome, err := f.b.Admit(ctx10(t), keyOf(w), req, &AdmitOptions{Roles: &none})
		if err != nil {
			t.Fatal(err)
		}
		if w.b, err = j.Complete(ctx10(t), welcome); err != nil {
			t.Fatal(err)
		}
		pull(t, f, w, 0)
		if wrote {
			mustAppend(t, w.b, "w=1")
		}
		w.close()
		w.db.Close()
		id := w.id
		w = open()
		w.id = id
		w.reopen()
		t.Cleanup(func() { w.close(); w.db.Close() })
		if w.b.Status().Seats != 1 || !w.b.Status().Unconfirmed {
			t.Fatalf("wrote %v: seats %d, confirmed before any peer answered", wrote, w.b.Status().Seats)
		}
		w.b.ownNone(newIdentity(t).PublicKey())
		if !w.b.Status().Unconfirmed {
			t.Fatalf("wrote %v: a stranger confirmed the strand", wrote)
		}
		w.b.ownNone(keyOf(f))
		if got := w.b.Status().Unconfirmed; got != wrote {
			t.Fatalf("wrote %v: unconfirmed %v after a member that holds no head answered", wrote, got)
		}
		if !wrote {
			ctx, cancel := shortCtx()
			_, err := w.b.Append(ctx, []byte("a=1"))
			cancel()
			if err != nil {
				t.Fatalf("append after the reopen: %v", err)
			}
		}
	}
}

// TestNetReopenBeforeFirstWrite: over kolo-secure, a plain member that never appended writes after a reopen; its peers hold no
// head of its strand, and their answer confirms it.
func TestNetReopenBeforeFirstWrite(t *testing.T) {
	m := newMesh(t)
	ps := grow(t, m, 5)
	w := ps[4]
	if w.b.own.Len() != 0 {
		t.Fatal("the member already wrote")
	}
	m.isolate(w)
	w.reopen()
	for _, p := range ps[:4] {
		m.connect(w, p)
	}
	if _, err := w.b.Append(ctx10(t), []byte("a=1")); err != nil {
		t.Fatalf("a member that never wrote cannot write after a reopen: %v", err)
	}
}
