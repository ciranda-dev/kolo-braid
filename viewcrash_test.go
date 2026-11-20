package kolobraid

import (
	"encoding/binary"
	"errors"
	"fmt"
	"testing"

	kolostore "github.com/ciranda-dev/kolo-store"
)

// libStep gives every entry a library write that records its position, like a roster change: it must be committed with the
// entry's view writes or not at all.
func libStep(e Entry, pos int) applyStep {
	s := vtStep(e)
	s.Lib = []viewWrite{{Op: opPut, A: []byte("r/last"), B: binary.BigEndian.AppendUint64(nil, uint64(pos))}}
	return s
}

func libSteps(es []Entry, first int) func(func(applyStep, error) bool) {
	return func(yield func(applyStep, error) bool) {
		for i, e := range es {
			if !yield(libStep(e, first+i), nil) {
				return
			}
		}
	}
}

// checkDisk verifies that the state on disk is the state after exactly progress.Next entries: never half a chunk.
func checkDisk(t *testing.T, disk kolostore.Backend, cfg viewConfig, es []Entry, what string) progress {
	t.Helper()
	v, err := openView(disk, cfg, vtApply)
	if err != nil {
		t.Fatalf("%s: reopen: %v", what, err)
	}
	p := v.Progress()
	ds, m := vtRunLib(cfg.Initial, 0, es[:p.Next], true)
	want := cfg.Initial
	if p.Next > 0 {
		want = ds[p.Next-1]
	}
	if p.Digest != want {
		t.Fatalf("%s: digest at %d is not the digest of those entries", what, p.Next)
	}
	s := v.Settled()
	vtEq(t, what+": view", vtDump(s.Scan(nil, nil)), m.dump())
	s.Close()
	got, err := disk.Get([]byte("r/last"))
	switch {
	case p.Next == 0 && errors.Is(err, kolostore.ErrNotFound):
	case err != nil || binary.BigEndian.Uint64(got) != p.Next-1:
		t.Fatalf("%s: library write %x %v does not match position %d", what, got, err, p.Next)
	}
	v.Close()
	return p
}

func TestCrashAtEveryCommit(t *testing.T) {
	es := vtScript(21, 90)
	cfg := vtConfig()
	cfg.chunk = 4
	cfg.CheckpointEvery = 64
	bounds := []int{0, 11, 25, 26, 60, 90}
	var batches [][]Entry
	for i := 1; i < len(bounds); i++ {
		batches = append(batches, es[bounds[i-1]:bounds[i]])
	}
	refDS, _ := vtRunLib(cfg.Initial, 0, es, true)
	// commits of an uninterrupted run
	total := 0
	{
		f := newVtFault(kolostore.NewMemory(), 0, false)
		v := vtOpen(t, f, cfg, vtApply)
		for i, b := range batches {
			if _, err := v.Settle(uint64(i), libSteps(b, bounds[i])); err != nil {
				t.Fatal(err)
			}
		}
		total = int(f.n.Load())
	}
	if total < 20 {
		t.Fatalf("only %d commits: the test would not cross chunk boundaries", total)
	}
	for at := 1; at <= total; at++ {
		for _, after := range []bool{false, true} {
			what := fmt.Sprintf("crash at commit %d (applied first: %v)", at, after)
			disk := kolostore.NewMemory()
			v := vtOpen(t, newVtFault(disk, at, after), cfg, vtApply)
			var crashed bool
			for i, b := range batches {
				if _, err := v.Settle(uint64(i), libSteps(b, bounds[i])); err != nil {
					if !errors.Is(err, errInjected) || !errors.Is(err, ErrHalted) {
						t.Fatalf("%s: %v", what, err)
					}
					crashed = true
					break
				}
			}
			if !crashed {
				t.Fatalf("%s: no crash", what)
			}
			if v.Halted() == nil {
				t.Fatalf("%s: a failed commit must halt the view", what)
			}
			p := checkDisk(t, disk, cfg, es, what)
			// restart from the stored progress: the digest of an uninterrupted run
			v2 := vtOpen(t, disk, cfg, vtApply)
			bi, off := int(p.Batches), int(p.Offset)
			for ; bi < len(batches); bi, off = bi+1, 0 {
				rest := batches[bi][off:]
				if _, err := v2.Settle(uint64(bi), libSteps(rest, bounds[bi]+off)); err != nil {
					t.Fatalf("%s: resume: %v", what, err)
				}
			}
			if got := v2.Progress(); got.Digest != refDS[len(es)-1] || got.Next != uint64(len(es)) {
				t.Fatalf("%s: final progress %+v", what, got)
			}
			checkDisk(t, disk, cfg, es, what+" after resume")
			// checkpoints exist exactly where an uninterrupted run puts them
			if d, ok, _ := v2.Checkpoint(89); !ok || d != refDS[89] {
				t.Fatalf("%s: checkpoint %v %s", what, ok, d)
			}
			for _, pos := range []uint64{10, 25, 59} {
				if _, ok, _ := v2.Checkpoint(pos); ok {
					t.Fatalf("%s: stray checkpoint at %d", what, pos)
				}
			}
		}
	}
}

func TestCrashWithTailAndPromotion(t *testing.T) {
	es := vtScript(22, 60)
	cfg := vtConfig()
	cfg.chunk = 5
	ref, _ := vtRunLib(cfg.Initial, 0, es, true)
	for at := 1; at <= 9; at++ {
		disk := kolostore.NewMemory()
		v := vtOpen(t, newVtFault(disk, at, at%2 == 0), cfg, vtApply)
		applyAllTail(t, v, es)
		if _, err := v.Settle(0, libSteps(es[:40], 0)); !errors.Is(err, errInjected) {
			// the tail's lib writes differ from libSteps: the entries are not promoted, and that is fine
			if err != nil {
				t.Fatalf("at %d: %v", at, err)
			}
		}
		p := checkDisk(t, disk, cfg, es, fmt.Sprintf("tail crash %d", at))
		v2 := vtOpen(t, disk, cfg, vtApply)
		if v2.Tail() != 0 {
			t.Fatalf("the tail is in memory only, a restart loses it: %d", v2.Tail())
		}
		if _, err := v2.Settle(p.Batches, libSteps(es[p.Offset:], int(p.Offset))); err != nil {
			t.Fatal(err)
		}
		if v2.Progress().Digest != ref[len(es)-1] {
			t.Fatalf("at %d: digest after the restart", at)
		}
	}
}

func TestCrashOfPromotedChunks(t *testing.T) {
	// the same, but the tail matches the library writes, so the chunks are committed from the overlay
	es := vtScript(23, 50)
	cfg := vtConfig()
	cfg.chunk = 6
	ref, _ := vtRunLib(cfg.Initial, 0, es, true)
	for at := 1; at <= 9; at++ {
		disk := kolostore.NewMemory()
		v := vtOpen(t, newVtFault(disk, at, at%2 == 0), cfg, vtApply)
		for i, e := range es {
			if _, err := v.ApplyTail(libStep(e, i)); err != nil {
				t.Fatal(err)
			}
		}
		res, err := v.Settle(0, libSteps(es, 0))
		if !errors.Is(err, errInjected) {
			t.Fatalf("at %d: %v", at, err)
		}
		if res.Promoted == 0 {
			t.Fatal("nothing promoted")
		}
		p := checkDisk(t, disk, cfg, es, fmt.Sprintf("promoted crash %d", at))
		v2 := vtOpen(t, disk, cfg, vtApply)
		if _, err := v2.Settle(p.Batches, libSteps(es[p.Offset:], int(p.Offset))); err != nil {
			t.Fatal(err)
		}
		if v2.Progress().Digest != ref[len(es)-1] {
			t.Fatalf("at %d: digest after the restart", at)
		}
	}
}

func TestCrashDuringResetAndSeed(t *testing.T) {
	cfg := vtConfig()
	for _, after := range []bool{false, true} {
		disk := kolostore.NewMemory()
		v := vtOpen(t, disk, cfg, vtApply)
		es := vtScript(30, 20)
		if _, err := v.Settle(0, libSteps(es, 0)); err != nil {
			t.Fatal(err)
		}
		f := newVtFault(disk, 1, after)
		v = vtOpen(t, f, cfg, vtApply)
		err := v.Reset(viewWrite{Op: opDeleteRange, A: []byte("r"), B: []byte("s")})
		if !errors.Is(err, errInjected) {
			t.Fatal(err)
		}
		reopened := vtOpen(t, disk, cfg, vtApply)
		p := reopened.Progress()
		_, rErr := disk.Get([]byte("r/last"))
		if after {
			if p.Next != 0 || !errors.Is(rErr, kolostore.ErrNotFound) {
				t.Fatalf("applied reset: progress %+v, roster key %v", p, rErr)
			}
		} else if p.Next != 20 || rErr != nil {
			t.Fatalf("dropped reset: progress %+v, roster key %v", p, rErr)
		}
		s := reopened.Settled()
		n := len(vtDump(s.Scan(nil, nil)))
		s.Close()
		if after && n != 0 || !after && n == 0 {
			t.Fatalf("view and progress disagree: %d pairs, progress %d", n, p.Next)
		}
	}
	// seeds: one batch
	disk := kolostore.NewMemory()
	v := vtOpen(t, newVtFault(disk, 1, false), cfg, vtApply)
	if err := v.SeedPairs([]seedPair{{Key: []byte("a")}, {Key: []byte("b")}}); !errors.Is(err, errInjected) {
		t.Fatal(err)
	}
	if err := disk.Scan([]byte("v"), []byte("w"), false, 0, func(k, _ []byte) bool { t.Fatalf("pair %q of a failed seed batch", k); return false }); err != nil {
		t.Fatal(err)
	}
}

func TestStorageErrorOnReadsDuringSettleHalts(t *testing.T) {
	disk := kolostore.NewMemory()
	f := newVtFault(disk, 0, false)
	v := vtOpen(t, f, vtConfig(), func(tx *Tx, e *Entry) error {
		if e.Index == 3 {
			f.getErr.Store(true)
		}
		_, err := tx.Get([]byte("k"))
		if errors.Is(err, ErrNotFound) {
			err = nil
		}
		return err
	})
	es := vtScript(1, 6)
	res, err := v.Settle(0, vtSteps(es))
	if !errors.Is(err, ErrHalted) || !errors.Is(err, errInjected) || res.Entries != 3 {
		t.Fatalf("%+v %v", res, err)
	}
	f.getErr.Store(false)
	if v.Progress().Next != 3 {
		t.Fatalf("%+v", v.Progress())
	}
}
