package kolobraid

import (
	"errors"
	"fmt"
	"slices"
	"testing"

	kolostore "github.com/ciranda-dev/kolo-store"

	"github.com/ciranda-dev/kolo-braid/internal/replay"
)

func TestContentDigestPagesAndErrors(t *testing.T) {
	disk := kolostore.NewMemory()
	f := newVtFault(disk, 0, false)
	v := vtOpen(t, f, vtConfig(), vtApply)
	var pairs []seedPair
	for i := 0; i < pageCount*2+5; i++ {
		pairs = append(pairs, seedPair{Key: []byte(fmt.Sprintf("k%04d", i)), Value: []byte{byte(i)}})
	}
	big := []seedPair{{Key: []byte("zz1"), Value: make([]byte, 600<<10)}, {Key: []byte("zz2"), Value: make([]byte, 600<<10)}, {Key: []byte("zz3"), Value: nil}}
	pairs = append(pairs, big...)
	if err := v.SeedPairs(pairs); err != nil {
		t.Fatal(err)
	}
	got, err := v.ContentDigest()
	if err != nil {
		t.Fatal(err)
	}
	want := contentDigest(0, func(yield func([]byte, []byte) bool) {
		for _, p := range pairs {
			if !yield(p.Key, p.Value) {
				return
			}
		}
	})
	if got != want {
		t.Fatal("content digest over several pages")
	}
	f.getErr.Store(true)
	if _, err := v.ContentDigest(); !errors.Is(err, errInjected) {
		t.Fatalf("%v", err)
	}
	if _, _, err := v.Checkpoint(1); !errors.Is(err, errInjected) {
		t.Fatalf("%v", err)
	}
	if _, err := v.Rejected(1); !errors.Is(err, errInjected) {
		t.Fatalf("%v", err)
	}
	if known, err := v.Verify(1, Digest{}); known || !errors.Is(err, errInjected) {
		t.Fatalf("%v %v", known, err)
	}
	// a snapshot read fails with the storage error; a scan ends
	s := v.Settled()
	if _, err := s.Get([]byte("k0001")); !errors.Is(err, errInjected) {
		t.Fatalf("%v", err)
	}
	for range s.Scan(nil, nil) {
		t.Fatal("scan over a failing backend yielded a pair")
	}
	f.getErr.Store(false)
	// a commit that cannot read the values it replaces halts rather than commit without the undo records
	f.getErr.Store(true)
	_, err = settleOne(v, 0, vtEntry(0, vtOp{kind: 'p', a: "a", b: "1"}))
	if !errors.Is(err, errInjected) || !errors.Is(err, ErrHalted) {
		t.Fatalf("%v", err)
	}
	s.Close()
}

func TestSettleStopsAtARefusedStep(t *testing.T) {
	v := vtOpen(t, kolostore.NewMemory(), vtConfig(), vtApply)
	es := vtScript(2, 10)
	bad := vtStep(es[6])
	bad.Lib = []viewWrite{{Op: opPut, A: []byte("nope")}}
	steps := func(y func(applyStep, error) bool) {
		for i, e := range es {
			s := vtStep(e)
			if i == 6 {
				s = bad
			}
			if !y(s, nil) {
				return
			}
		}
	}
	if _, err := v.Settle(0, steps); !errors.Is(err, ErrInvalidArgument) {
		t.Fatal(err)
	}
	if p := v.Progress(); p.Next != 6 || p.Offset != 6 {
		t.Fatalf("the steps before the refused one are committed: %+v", p)
	}
}

func TestCommitAfterClose(t *testing.T) {
	var v *view
	v = vtOpen(t, kolostore.NewMemory(), vtConfig(), func(tx *Tx, e *Entry) error {
		v.Close()
		return nil
	})
	if _, err := settleOne(v, 0, vtEntry(0)); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}

func TestReplayerRoster(t *testing.T) {
	var seen []Member
	apply := func(tx *Tx, e *Entry) error {
		seen = tx.Members()
		if _, ok := tx.Member(seen[0].Key); !ok {
			t.Error("member not found")
		}
		return nil
	}
	r, err := newReplayer(kolostore.NewMemory(), apply, vtMembers(), replay.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Settle([]Entry{vtEntry(0)}); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(vtMembers()[0].Key, seen[0].Key) {
		t.Fatal(seen)
	}
}

func TestOpenViewStorageErrors(t *testing.T) {
	f := newVtFault(kolostore.NewMemory(), 0, false)
	f.getErr.Store(true)
	if _, err := openView(f, vtConfig(), vtApply); !errors.Is(err, errInjected) {
		t.Fatal(err)
	}
}
