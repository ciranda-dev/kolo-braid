package kolobraid

import (
	"bytes"
	"errors"
	"slices"
	"testing"

	kolostore "github.com/ciranda-dev/kolo-store"
)

func TestSettleMatchesModel(t *testing.T) {
	for seed := uint64(0); seed < 20; seed++ {
		es := vtScript(seed, 150)
		b := kolostore.NewMemory()
		cfg := vtConfig()
		cfg.chunk = 7
		v := vtOpen(t, b, cfg, vtApply)
		res, err := v.Settle(0, vtSteps(es))
		if err != nil {
			t.Fatal(err)
		}
		ds, m := vtRun(cfg.Initial, 0, es)
		if res.Progress.Digest != ds[len(ds)-1] || res.Progress.Next != uint64(len(es)) || res.Progress.Batches != 1 || res.Progress.Offset != 0 {
			t.Fatalf("seed %d: progress %+v, want digest %s next %d", seed, res.Progress, ds[len(ds)-1], len(es))
		}
		s := v.Settled()
		vtEq(t, "settled view", vtDump(s.Scan(nil, nil)), m.dump())
		s.Close()
		for k, want := range m.m {
			s := v.Settled()
			got, err := s.Get([]byte(k))
			s.Close()
			if err != nil || string(got) != want {
				t.Fatalf("Get(%q) = %q, %v; want %q", k, got, err, want)
			}
		}
	}
}

func TestPositionsAndRejectedMarks(t *testing.T) {
	es := []Entry{
		vtEntry(0, vtOp{kind: 'p', a: "a", b: "1"}),
		vtEntry(1, vtOp{kind: 'p', a: "a", b: "2"}, vtOp{kind: 'x', a: "no"}),
		vtEntry(2, vtOp{kind: 'c'}),
	}
	es[2].Kind = KindData
	v := vtOpen(t, kolostore.NewMemory(), vtConfig(), vtApply)
	if _, err := v.Settle(0, vtSteps(es)); err != nil {
		t.Fatal(err)
	}
	s := v.Settled()
	defer s.Close()
	if got, _ := s.Get([]byte("a")); string(got) != "1" {
		t.Fatalf("a = %q: the writes of a rejected entry must be discarded", got)
	}
	if got, err := s.Get(append([]byte("pos/"), 0, 0, 0, 0, 0, 0, 0, 2)); err != nil || !bytes.Equal(got, es[2].ID[:4]) {
		t.Fatalf("the entry sees its position: %x, %v", got, err)
	}
	for pos, want := range []bool{false, true, false} {
		if got, err := v.Rejected(uint64(pos)); err != nil || got != want {
			t.Fatalf("Rejected(%d) = %v, %v; want %v", pos, got, err, want)
		}
	}
	if s.Position() != 2 {
		t.Fatalf("Position = %d, want 2", s.Position())
	}
}

func TestEntryKindsAndVoid(t *testing.T) {
	var kinds []Kind
	apply := func(tx *Tx, e *Entry) error {
		kinds = append(kinds, e.Kind)
		return tx.Put([]byte{byte(e.Kind)}, nil)
	}
	v := vtOpen(t, kolostore.NewMemory(), vtConfig(), apply)
	var es []applyStep
	for k := KindGenesis; k <= KindPulse; k++ {
		e := vtEntry(int(k))
		e.Kind = k
		es = append(es, vtStep(e))
	}
	void := vtEntry(20)
	es = append(es, applyStep{Entry: void, Void: true})
	seq := func(yield func(applyStep, error) bool) {
		for _, s := range es {
			if !yield(s, nil) {
				return
			}
		}
	}
	if _, err := v.Settle(0, seq); err != nil {
		t.Fatal(err)
	}
	want := []Kind{KindGenesis, KindData, KindAddMember, KindRemoveMember, KindSettlers, KindEvidence}
	if !slices.Equal(kinds, want) {
		t.Fatalf("apply saw kinds %v, want %v (never pulses, seeds or void entries)", kinds, want)
	}
	seed := vtStep(vtEntry(30))
	seed.Entry.Kind = KindSeed
	if _, err := v.Settle(1, func(yield func(applyStep, error) bool) { yield(seed, nil) }); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("a seed entry takes no position: %v", err)
	}
	if _, err := v.ApplyTail(seed); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("ApplyTail of a seed entry: %v", err)
	}
}

func TestVoidStatusInDigest(t *testing.T) {
	v := vtOpen(t, kolostore.NewMemory(), vtConfig(), vtApply)
	e := vtEntry(0, vtOp{kind: 'p', a: "a", b: "1"})
	res, err := v.Settle(0, func(yield func(applyStep, error) bool) { yield(applyStep{Entry: e, Void: true}, nil) })
	if err != nil {
		t.Fatal(err)
	}
	if want := runningDigest(vtConfig().Initial, 0, e.ID, statusVoid, nil); res.Progress.Digest != want {
		t.Fatalf("void digest %s, want %s", res.Progress.Digest, want)
	}
	s := v.Settled()
	defer s.Close()
	if _, err := s.Get([]byte("a")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("a void entry has no effect: %v", err)
	}
}

func TestLibraryWritesAreCommittedAndDigested(t *testing.T) {
	b := kolostore.NewMemory()
	v := vtOpen(t, b, vtConfig(), vtApply)
	lib := []viewWrite{{Op: opPut, A: []byte("r/w1"), B: []byte("admin")}}
	e := vtEntry(0, vtOp{kind: 'x', a: "veto"})
	e.Kind = KindAddMember
	e2 := vtEntry(1, vtOp{kind: 'x', a: "veto"})
	step := vtStep(e)
	step.Lib = lib
	res, err := v.Settle(0, func(yield func(applyStep, error) bool) { yield(step, nil); yield(vtStep(e2), nil) })
	if err != nil {
		t.Fatal(err)
	}
	if got, err := b.Get([]byte("r/w1")); err != nil || string(got) != "admin" {
		t.Fatalf("library write: %q, %v", got, err)
	}
	// the AddMember entry's own apply did not reject (the script only acts on data entries), so it is applied with the library write
	d0 := runningDigest(vtConfig().Initial, 0, e.ID, statusApplied, lib)
	d1 := runningDigest(d0, 1, e2.ID, statusRejected, nil)
	if res.Progress.Digest != d1 {
		t.Fatalf("digest %s, want %s", res.Progress.Digest, d1)
	}
	bad := []viewWrite{{Op: opPut, A: []byte("vx"), B: nil}}
	for _, w := range [][]viewWrite{bad, {{Op: opPut}}, {{Op: opDeleteRange, A: []byte("r1"), B: nil}}, {{Op: opDeleteRange, A: []byte("r1"), B: []byte("t")}}, {{Op: 9, A: []byte("r")}}} {
		s := vtStep(vtEntry(5))
		s.Lib = w
		if _, err := v.ApplyTail(s); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("library write %v accepted: %v", w, err)
		}
	}
	s := vtStep(vtEntry(5))
	s.Void, s.Lib = true, lib
	if _, err := v.ApplyTail(s); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("void entry with library writes: %v", err)
	}
}

func TestConfigValidation(t *testing.T) {
	cfg := vtConfig()
	for name, mut := range map[string]func(*viewConfig){
		"MaxBody low":      func(c *viewConfig) { c.MaxBody = 1023 },
		"MaxBody high":     func(c *viewConfig) { c.MaxBody = 3<<20 + 1 },
		"Checkpoint low":   func(c *viewConfig) { c.CheckpointEvery = 63 },
		"Checkpoint high":  func(c *viewConfig) { c.CheckpointEvery = 1<<20 + 1 },
		"MaxTail low":      func(c *viewConfig) { c.MaxTail = 63 },
		"MaxTail high":     func(c *viewConfig) { c.MaxTail = 1<<20 + 1 },
		"MaxTailBytes low": func(c *viewConfig) { c.MaxTailBytes = 1<<20 - 1 },
		"MaxTailBytes hi":  func(c *viewConfig) { c.MaxTailBytes = 4<<30 + 1 },
	} {
		c := cfg
		mut(&c)
		if _, err := openView(kolostore.NewMemory(), c, vtApply); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if _, err := openView(nil, cfg, vtApply); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("nil backend: %v", err)
	}
	if _, err := openView(kolostore.NewMemory(), cfg, nil); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("nil apply: %v", err)
	}
	for _, ok := range []func(*viewConfig){
		func(c *viewConfig) {
			c.MaxBody = 1024
			c.CheckpointEvery = 64
			c.MaxTail = 64
			c.MaxTailBytes = 1 << 20
		},
		func(c *viewConfig) {
			c.MaxBody = 3 << 20
			c.CheckpointEvery = 1 << 20
			c.MaxTail = 1 << 20
			c.MaxTailBytes = 4 << 30
		},
	} {
		c := cfg
		ok(&c)
		if _, err := openView(kolostore.NewMemory(), c, vtApply); err != nil {
			t.Errorf("edge of a range refused: %v", err)
		}
	}
}

func TestBatchNumberAndChunkResume(t *testing.T) {
	es := vtScript(3, 40)
	b := kolostore.NewMemory()
	cfg := vtConfig()
	cfg.chunk = 10
	v := vtOpen(t, b, cfg, vtApply)
	if _, err := v.Settle(1, vtSteps(es)); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("batch 1 before batch 0: %v", err)
	}
	// the source of steps fails after 25 entries: progress holds the 20 of whole chunks plus the 5, the error is returned
	failing := func(yield func(applyStep, error) bool) {
		for i, e := range es {
			if i == 25 {
				yield(applyStep{}, errInjected)
				return
			}
			yield(vtStep(e), nil)
		}
	}
	if _, err := v.Settle(0, failing); !errors.Is(err, errInjected) {
		t.Fatal(err)
	}
	p := v.Progress()
	if p.Next != 25 || p.Offset != 25 || p.Batches != 0 {
		t.Fatalf("progress after a failing source: %+v", p)
	}
	v2 := vtOpen(t, b, cfg, vtApply)
	if v2.Progress() != p {
		t.Fatalf("reopened progress %+v, want %+v", v2.Progress(), p)
	}
	if _, err := v2.Settle(0, vtSteps(es[25:])); err != nil {
		t.Fatal(err)
	}
	ds, m := vtRun(cfg.Initial, 0, es)
	if got := v2.Progress(); got.Digest != ds[len(ds)-1] || got.Batches != 1 || got.Offset != 0 || got.Next != 40 {
		t.Fatalf("resumed progress %+v", got)
	}
	s := v2.Settled()
	defer s.Close()
	vtEq(t, "resumed view", vtDump(s.Scan(nil, nil)), m.dump())
}

func TestCheckpoints(t *testing.T) {
	cfg := vtConfig() // every 64
	b := kolostore.NewMemory()
	v := vtOpen(t, b, cfg, vtApply)
	es := vtScript(5, 200)
	ds, _ := vtRun(cfg.Initial, 0, es)
	var cps []checkpointRec
	batches := [][]Entry{es[:30], es[30:63], es[63:64], es[64:130], es[130:200]}
	// batch ends: 30, 63, 64, 130, 200 settled positions; multiples of 64 reached by: the third (64), the fourth (128), the fifth (192)
	for i, bt := range batches {
		res, err := v.Settle(uint64(i), vtSteps(bt))
		if err != nil {
			t.Fatal(err)
		}
		if res.Checkpoint != nil {
			cps = append(cps, *res.Checkpoint)
		}
	}
	want := []checkpointRec{{63, ds[63]}, {129, ds[129]}, {199, ds[199]}}
	if !slices.Equal(cps, want) {
		t.Fatalf("checkpoints %v, want %v", cps, want)
	}
	for _, c := range want {
		d, ok, err := v.Checkpoint(c.Position)
		if err != nil || !ok || d != c.Digest {
			t.Fatalf("stored checkpoint %d: %s %v %v", c.Position, d, ok, err)
		}
		known, err := v.Verify(c.Position, c.Digest)
		if err != nil || !known {
			t.Fatalf("Verify of a matching digest: %v %v", known, err)
		}
	}
	if _, ok, _ := v.Checkpoint(5); ok {
		t.Fatal("a checkpoint at a position that is not one")
	}
	if known, err := v.Verify(5, Digest{1}); known || err != nil {
		t.Fatalf("an unsettled or unclaimed position is unknown: %v %v", known, err)
	}
}

func TestVerifyMismatchHalts(t *testing.T) {
	cfg := vtConfig()
	v := vtOpen(t, kolostore.NewMemory(), cfg, vtApply)
	es := vtScript(6, 64)
	res, err := v.Settle(0, vtSteps(es))
	if err != nil || res.Checkpoint == nil {
		t.Fatalf("%v %v", res.Checkpoint, err)
	}
	bad := res.Checkpoint.Digest
	bad[0] ^= 1
	known, err := v.Verify(res.Checkpoint.Position, bad)
	if !known || !errors.Is(err, ErrDiverged) {
		t.Fatalf("a digest that differs from the settled one: %v %v", known, err)
	}
	if !errors.Is(v.Halted(), ErrDiverged) {
		t.Fatalf("Halted = %v", v.Halted())
	}
	if _, err := v.Settle(1, vtSteps(vtScript(7, 3))); !errors.Is(err, ErrDiverged) {
		t.Fatalf("settling after a divergence: %v", err)
	}
	if _, err := v.ApplyTail(vtStep(vtEntry(100))); !errors.Is(err, ErrDiverged) {
		t.Fatalf("tentative apply after a divergence: %v", err)
	}
}

func TestResetReplaysFromTheStart(t *testing.T) {
	cfg := vtConfig()
	b := kolostore.NewMemory()
	v := vtOpen(t, b, cfg, vtApply)
	es := vtScript(8, 130)
	first, err := v.Settle(0, vtSteps(es))
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Put([]byte("r/keep"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := v.Reset(viewWrite{Op: opDeleteRange, A: []byte("r"), B: []byte("s")}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Get([]byte("r/keep")); !errors.Is(err, kolostore.ErrNotFound) {
		t.Fatalf("library reset write: %v", err)
	}
	if p := v.Progress(); p.Next != 0 || p.Digest != cfg.Initial || p.Batches != 0 {
		t.Fatalf("progress after Reset %+v", p)
	}
	if _, ok, _ := v.Checkpoint(129); ok {
		t.Fatal("checkpoint survived Reset")
	}
	if rej, _ := v.Rejected(1); rej {
		t.Fatal("rejection mark survived Reset")
	}
	if err := b.Scan([]byte("v"), []byte("w"), false, 0, func(k, _ []byte) bool { t.Fatalf("view pair %q survived Reset", k); return false }); err != nil {
		t.Fatal(err)
	}
	again, err := v.Settle(0, vtSteps(es))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := v.Checkpoint(129); !ok || first.Checkpoint == nil || first.Checkpoint.Position != 129 {
		t.Fatal("the checkpoint of the replayed batch")
	}
	if again.Progress != first.Progress {
		t.Fatalf("replay progress %+v, first %+v", again.Progress, first.Progress)
	}
	s := v.Settled()
	if err := v.Reset(); err == nil {
		t.Fatal("Reset with an open snapshot")
	}
	s.Close()
	if err := v.Reset(viewWrite{Op: opPut, A: []byte("x")}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Reset with a library write outside the roster keys: %v", err)
	}
}

func TestSeedPairsAndContentDigest(t *testing.T) {
	cfg := vtConfig()
	cfg.Next = 100
	b := kolostore.NewMemory()
	v := vtOpen(t, b, cfg, vtApply)
	pairs := []seedPair{{Key: nil, Value: []byte("empty key")}, {Key: []byte("a"), Value: []byte("1")}, {Key: []byte("b"), Value: nil}}
	if err := v.SeedPairs(pairs); err != nil {
		t.Fatal(err)
	}
	got, err := v.ContentDigest()
	if err != nil {
		t.Fatal(err)
	}
	want := contentDigest(99, func(yield func([]byte, []byte) bool) {
		for _, p := range pairs {
			if !yield(p.Key, p.Value) {
				return
			}
		}
	})
	if got != want {
		t.Fatalf("content digest %s, want %s (position is the last one before Next)", got, want)
	}
	if err := v.SeedPairs([]seedPair{{Key: make([]byte, MaxViewKey+1)}}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatal(err)
	}
	if err := v.SeedPairs([]seedPair{{Value: make([]byte, MaxViewValue+1)}}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatal(err)
	}
	res, err := v.Settle(0, vtSteps(vtScript(1, 3)))
	if err != nil || res.Progress.Next != 103 {
		t.Fatalf("positions continue after the seeds: %+v %v", res.Progress, err)
	}
}

func TestClosedView(t *testing.T) {
	v := vtOpen(t, kolostore.NewMemory(), vtConfig(), vtApply)
	v.Close()
	if _, err := v.ApplyTail(vtStep(vtEntry(0))); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if _, err := v.Settle(0, vtSteps(nil)); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if err := v.DropTail(0); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}
