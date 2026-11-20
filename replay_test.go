package kolobraid

import (
	"errors"
	"testing"

	kolostore "github.com/ciranda-dev/kolo-store"

	"github.com/ciranda-dev/kolo-braid/internal/replay"
)

func vtReplayer(t *testing.T, b kolostore.Backend, opts replay.Options) *replayer {
	t.Helper()
	r, err := newReplayer(b, vtApply, vtMembers(), opts)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestReplayerTailAndSettle(t *testing.T) {
	es := vtScript(51, 60)
	ref, m := vtRun(Digest{}, 0, es)
	r := vtReplayer(t, kolostore.NewMemory(), replay.Options{})
	defer r.Close()
	if n, err := r.Tentative(es[:40]); err != nil || n != 40 {
		t.Fatalf("%d %v", n, err)
	}
	if n, err := r.Tentative(es); err != nil || n != 60 {
		t.Fatalf("a longer order extends the tail: %d %v", n, err)
	}
	if r.TailDigest() != ref[59] {
		t.Fatal("tail digest")
	}
	if err := r.Settle(es[:25]); err != nil {
		t.Fatal(err)
	}
	if r.Position() != 25 || r.Digest() != ref[24] || r.TailDigest() != ref[59] {
		t.Fatalf("position %d", r.Position())
	}
	if n, err := r.Tentative(es[25:]); err != nil || n != 35 {
		t.Fatalf("%d %v", n, err)
	}
	if err := r.Settle(es[25:]); err != nil {
		t.Fatal(err)
	}
	s := r.Settled()
	defer s.Close()
	vtEq(t, "view", vtDump(s.Scan(nil, nil)), m.dump())
	v := r.View()
	v.Close()
	if r.TailDigest() != r.Digest() {
		t.Fatal("an empty tail reports the settled digest")
	}
	if d, err := r.Content(); err != nil || d == (Digest{}) {
		t.Fatalf("%s %v", d, err)
	}
}

func TestReplayerReopenAndLimits(t *testing.T) {
	b := kolostore.NewMemory()
	es := vtScript(52, 30)
	r := vtReplayer(t, b, replay.Options{MaxTail: 64, Chunk: 3})
	if err := r.Settle(es[:10]); err != nil {
		t.Fatal(err)
	}
	r.Tentative(es[10:])
	if err := r.Reopen(); err != nil {
		t.Fatal(err)
	}
	if r.Position() != 10 || r.v.Tail() != 0 {
		t.Fatalf("after a restart: position %d, tail %d", r.Position(), r.v.Tail())
	}
	if err := r.Settle(es[10:]); err != nil {
		t.Fatal(err)
	}
	ref, _ := vtRun(Digest{}, 0, es)
	if r.Digest() != ref[29] {
		t.Fatal("digest")
	}
	// the tail stops at its cap
	long := vtScript(53, 100)
	r2 := vtReplayer(t, kolostore.NewMemory(), replay.Options{MaxTail: 64})
	if n, err := r2.Tentative(long); err != nil || n != 64 {
		t.Fatalf("%d %v", n, err)
	}
	// a failing apply is reported
	bad := vtEntry(999, vtOp{kind: 'e'})
	if _, err := r2.Tentative([]Entry{bad}); !errors.Is(err, ErrHalted) {
		t.Fatal(err)
	}
	if err := r2.Settle([]Entry{bad}); !errors.Is(err, ErrHalted) {
		t.Fatal(err)
	}
	if err := r2.Close(); err != nil {
		t.Fatal(err)
	}
	if err := r2.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := newReplayer(b, vtApply, nil, replay.Options{MaxBody: 5}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("%v", err)
	}
	if r.Position() == 0 {
		t.Fatal()
	}
}
