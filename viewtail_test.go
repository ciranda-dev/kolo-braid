package kolobraid

import (
	"errors"
	"math/rand/v2"
	"testing"

	kolostore "github.com/ciranda-dev/kolo-store"
)

func applyAllTail(t testing.TB, v *view, es []Entry) {
	t.Helper()
	for _, e := range es {
		res, err := v.ApplyTail(vtStep(e))
		if err != nil || res.Paused {
			t.Fatalf("ApplyTail: %+v %v", res, err)
		}
	}
}

func TestTailMatchesFreshApplication(t *testing.T) {
	cfg := vtConfig()
	es := vtScript(11, 120)
	v := vtOpen(t, kolostore.NewMemory(), cfg, vtApply)
	applyAllTail(t, v, es)
	ds, m := vtRun(cfg.Initial, 0, es)
	if v.Tail() != len(es) {
		t.Fatal(v.Tail())
	}
	for i := range es {
		ti, ok := v.TailEntry(i)
		if !ok || ti.ID != es[i].ID || ti.Position != uint64(i) || ti.Digest != ds[i] {
			t.Fatalf("tail entry %d: %+v, want digest %s", i, ti, ds[i])
		}
	}
	if _, ok := v.TailEntry(len(es)); ok {
		t.Fatal("entry past the tail")
	}
	if _, ok := v.TailEntry(-1); ok {
		t.Fatal("negative entry")
	}
	s := v.View()
	defer s.Close()
	vtEq(t, "tentative view", vtDump(s.Scan(nil, nil)), m.dump())
	if s.Position() != uint64(len(es)-1) {
		t.Fatalf("Position %d", s.Position())
	}
	settled := v.Settled()
	defer settled.Close()
	if n := len(vtDump(settled.Scan(nil, nil))); n != 0 {
		t.Fatalf("the settled view holds %d pairs of tentative entries", n)
	}
	if err := v.DropTail(-1); !errors.Is(err, ErrInvalidArgument) {
		t.Fatal(err)
	}
}

func TestTailRejectedAndStatuses(t *testing.T) {
	v := vtOpen(t, kolostore.NewMemory(), vtConfig(), vtApply)
	es := []Entry{vtEntry(0, vtOp{kind: 'p', a: "a", b: "1"}), vtEntry(1, vtOp{kind: 'x'}), vtEntry(2)}
	es[2].Kind = KindPulse
	applyAllTail(t, v, es)
	for i, want := range []entryStatus{statusApplied, statusRejected, statusApplied} {
		if ti, _ := v.TailEntry(i); ti.Status != want {
			t.Errorf("entry %d status %d, want %d", i, ti.Status, want)
		}
	}
	void := vtStep(vtEntry(3))
	void.Void = true
	if res, err := v.ApplyTail(void); err != nil || res.Status != statusVoid {
		t.Fatalf("%+v %v", res, err)
	}
}

func TestTailCaps(t *testing.T) {
	cfg := vtConfig()
	cfg.MaxTail = 64
	v := vtOpen(t, kolostore.NewMemory(), cfg, vtApply)
	for i := 0; i < 64; i++ {
		if res, err := v.ApplyTail(vtStep(vtEntry(i))); err != nil || res.Paused {
			t.Fatalf("entry %d: %+v %v", i, res, err)
		}
	}
	res, err := v.ApplyTail(vtStep(vtEntry(64)))
	if err != nil || !res.Paused || v.Tail() != 64 {
		t.Fatalf("at MaxTail: %+v %v tail %d", res, err, v.Tail())
	}
	// settling frees room, and the tail goes on from where it is
	if _, err := v.Settle(0, vtSteps([]Entry{vtEntry(0), vtEntry(1)})); err != nil {
		t.Fatal(err)
	}
	if v.Tail() != 62 {
		t.Fatalf("tail after promotion %d", v.Tail())
	}
	if res, err := v.ApplyTail(vtStep(vtEntry(64))); err != nil || res.Paused || res.Position != 64 {
		t.Fatalf("after settling: %+v %v", res, err)
	}
}

func TestTailByteCap(t *testing.T) {
	cfg := vtConfig()
	cfg.MaxTailBytes = 1 << 20
	v := vtOpen(t, kolostore.NewMemory(), cfg, vtApply)
	big := func(i int) Entry {
		return vtEntry(i, vtOp{kind: 'p', a: string(rune('a' + i)), b: string(make([]byte, 300<<10))})
	}
	applied := 0
	for i := 0; i < 8; i++ {
		res, err := v.ApplyTail(vtStep(big(i)))
		if err != nil {
			t.Fatal(err)
		}
		if res.Paused {
			break
		}
		applied++
	}
	// each costs 1 + 300 KiB + 256 bytes: three fit under 1 MiB, the fourth would pass it
	if applied != 3 {
		t.Fatalf("%d entries before the byte cap, want 3", applied)
	}
	if v.tail.bytes > cfg.MaxTailBytes {
		t.Fatalf("overlay holds %d bytes over the cap %d", v.tail.bytes, cfg.MaxTailBytes)
	}
	// an entry that alone is over the cap is never applied tentatively, but it settles
	one := vtOpen(t, kolostore.NewMemory(), cfg, vtApply)
	huge := vtEntry(0, vtOp{kind: 'p', a: "a", b: string(make([]byte, 600<<10))}, vtOp{kind: 'p', a: "b", b: string(make([]byte, 600<<10))})
	if res, err := one.ApplyTail(vtStep(huge)); err != nil || !res.Paused {
		t.Fatalf("%+v %v", res, err)
	}
	if _, err := settleOne(one, 0, huge); err != nil {
		t.Fatal(err)
	}
}

func TestTentativeFailureDoesNotBlockSettlement(t *testing.T) {
	calls := 0
	apply := func(tx *Tx, e *Entry) error {
		calls++
		return vtApply(tx, e)
	}
	v := vtOpen(t, kolostore.NewMemory(), vtConfig(), apply)
	ok0 := vtEntry(0, vtOp{kind: 'p', a: "a", b: "1"})
	bad := vtEntry(1, vtOp{kind: 'e'})
	ok2 := vtEntry(2, vtOp{kind: 'p', a: "b", b: "2"})
	applyAllTail(t, v, []Entry{ok0})
	if _, err := v.ApplyTail(vtStep(bad)); !errors.Is(err, ErrHalted) {
		t.Fatal(err)
	}
	before := calls
	if _, err := v.ApplyTail(vtStep(bad)); !errors.Is(err, ErrHalted) || calls != before {
		t.Fatalf("the same failing entry again: %v, apply ran %d more times", err, calls-before)
	}
	if err := v.DropTail(1); err != nil {
		t.Fatal(err)
	}
	if _, err := v.ApplyTail(vtStep(bad)); !errors.Is(err, ErrHalted) || calls != before {
		t.Fatalf("no entry before it changed: %v", err)
	}
	if v.Halted() != nil {
		t.Fatalf("a tentative failure does not halt settlement: %v", v.Halted())
	}
	// another entry at that position is applied
	if res, err := v.ApplyTail(vtStep(ok2)); err != nil || res.Position != 1 {
		t.Fatalf("%+v %v", res, err)
	}
	// a change of the entries before it lets it run again
	if _, err := v.ApplyTail(vtStep(bad)); !errors.Is(err, ErrHalted) {
		t.Fatal(err)
	}
	if err := v.DropTail(0); err != nil {
		t.Fatal(err)
	}
	before = calls
	applyAllTail(t, v, []Entry{ok0, ok2})
	if _, err := v.ApplyTail(vtStep(bad)); !errors.Is(err, ErrHalted) || calls == before {
		t.Fatalf("after a reorg the entry is tried again: %v", err)
	}
	// settlement of earlier entries is unaffected; once the failing entry itself settles the view halts
	if _, err := v.Settle(0, vtSteps([]Entry{ok0})); err != nil {
		t.Fatal(err)
	}
	if _, err := v.Settle(1, vtSteps([]Entry{ok2, bad})); !errors.Is(err, ErrHalted) {
		t.Fatalf("settling the failing entry: %v", err)
	}
	if v.Halted() == nil {
		t.Fatal("settled failure must halt")
	}
}

// vtShuffle returns es with the entries from index keep on in a random order.
func vtShuffle(r *rand.Rand, es []Entry, keep int) []Entry {
	out := append([]Entry(nil), es...)
	r.Shuffle(len(out)-keep, func(i, j int) { out[keep+i], out[keep+j] = out[keep+j], out[keep+i] })
	return out
}

func firstChange(a, b []Entry) int {
	i := 0
	for i < len(a) && i < len(b) && a[i].ID == b[i].ID {
		i++
	}
	return i
}

// Reorg then replay equals a fresh replay, for random histories and random reorg points.
func TestPropertyReorgEqualsFreshReplay(t *testing.T) {
	for seed := uint64(0); seed < 60; seed++ {
		r := rand.New(rand.NewPCG(seed, 5))
		cfg := vtConfig()
		n := 20 + r.IntN(140)
		es := vtScript(seed, n)
		v := vtOpen(t, kolostore.NewMemory(), cfg, vtApply)
		cur := es
		applyAllTail(t, v, cur)
		for round := 0; round < 6; round++ {
			keep := r.IntN(len(cur) + 1)
			next := vtShuffle(r, cur, keep)
			if r.IntN(3) == 0 && len(next) > 3 { // also a shorter or longer tail
				next = next[:len(next)-r.IntN(3)]
			}
			if r.IntN(4) == 0 {
				next = append(next, vtEntry(1000+round, vtOp{kind: 'c'}))
			}
			at := firstChange(cur, next)
			if err := v.DropTail(at); err != nil {
				t.Fatal(err)
			}
			applyAllTail(t, v, next[at:])
			cur = next
			ds, m := vtRun(cfg.Initial, 0, cur)
			if v.Tail() != len(cur) {
				t.Fatalf("seed %d round %d: tail %d, want %d", seed, round, v.Tail(), len(cur))
			}
			for i := range cur {
				ti, _ := v.TailEntry(i)
				if ti.Digest != ds[i] || ti.ID != cur[i].ID {
					t.Fatalf("seed %d round %d: digest of tail entry %d differs from a fresh replay (reorg at %d)", seed, round, i, at)
				}
			}
			s := v.View()
			vtEq(t, "view after reorg", vtDump(s.Scan(nil, nil)), m.dump())
			s.Close()
		}
	}
}

// Promotion equals re-apply: settling batches that follow the tail, deviate from it or are longer than it ends in the same digest
// and view as settling them on a view that never had a tail.
func TestPropertyPromotionEqualsReapply(t *testing.T) {
	for seed := uint64(0); seed < 80; seed++ {
		r := rand.New(rand.NewPCG(seed, 6))
		cfg := vtConfig()
		cfg.chunk = 1 + r.IntN(12)
		n := 30 + r.IntN(100)
		es := vtScript(seed+100, n)
		all, _ := vtRun(cfg.Initial, 0, es)
		v := vtOpen(t, kolostore.NewMemory(), cfg, vtApply)
		ref := vtOpen(t, kolostore.NewMemory(), cfg, vtApply)

		// the tail holds the first half in the right order, or in a changed one
		tailLen := r.IntN(n)
		tailOrder := append([]Entry(nil), es[:tailLen]...)
		deviate := r.IntN(2) == 0 && tailLen > 2
		if deviate {
			tailOrder = vtShuffle(r, tailOrder, r.IntN(tailLen))
		}
		applyAllTail(t, v, tailOrder)

		settled, batch := 0, uint64(0)
		sawPromotion := false
		for settled < n {
			size := 1 + r.IntN(25)
			if settled+size > n {
				size = n - settled
			}
			b := es[settled : settled+size]
			res, err := v.Settle(batch, vtSteps(b))
			if err != nil {
				t.Fatal(err)
			}
			rres, err := ref.Settle(batch, vtSteps(b))
			if err != nil {
				t.Fatal(err)
			}
			if res.Progress != rres.Progress {
				t.Fatalf("seed %d batch %d: progress %+v, without a tail %+v", seed, batch, res.Progress, rres.Progress)
			}
			if (res.Checkpoint == nil) != (rres.Checkpoint == nil) || (res.Checkpoint != nil && *res.Checkpoint != *rres.Checkpoint) {
				t.Fatalf("seed %d: checkpoints differ", seed)
			}
			sawPromotion = sawPromotion || res.Promoted > 0
			if !deviate && settled+size <= tailLen && res.Promoted != size {
				t.Fatalf("seed %d: %d of %d entries promoted from a matching tail", seed, res.Promoted, size)
			}
			settled += size
			batch++
			// the caller re-applies the tail from what remains; entries still in it must be a prefix of the true order
			keep := 0
			for keep < v.Tail() {
				ti, _ := v.TailEntry(keep)
				if settled+keep >= n || ti.ID != es[settled+keep].ID {
					break
				}
				keep++
			}
			if err := v.DropTail(keep); err != nil {
				t.Fatal(err)
			}
			end := max(settled+keep, min(n, settled+r.IntN(40)))
			applyAllTail(t, v, es[settled+keep:end])
			// the tail digests continue the settled chain
			if v.Tail() > 0 {
				if ti, _ := v.TailEntry(v.Tail() - 1); ti.Digest != all[settled+v.Tail()-1] {
					t.Fatalf("seed %d: tail digest after settling %d", seed, settled)
				}
			}
		}
		if !deviate && tailLen > 0 && !sawPromotion {
			t.Fatalf("seed %d: nothing was promoted", seed)
		}
		a, b := v.Settled(), ref.Settled()
		vtEq(t, "settled view", vtDump(a.Scan(nil, nil)), vtDump(b.Scan(nil, nil)))
		a.Close()
		b.Close()
		cd, _ := v.ContentDigest()
		rd, _ := ref.ContentDigest()
		if cd != rd {
			t.Fatalf("seed %d: content digests differ", seed)
		}
	}
}

func TestPromotionSkipsApply(t *testing.T) {
	calls := 0
	apply := func(tx *Tx, e *Entry) error { calls++; return vtApply(tx, e) }
	v := vtOpen(t, kolostore.NewMemory(), vtConfig(), apply)
	es := vtScript(2, 30)
	applyAllTail(t, v, es)
	if calls == 0 {
		t.Fatal("apply never ran")
	}
	before := calls
	res, err := v.Settle(0, vtSteps(es))
	if err != nil {
		t.Fatal(err)
	}
	if res.Promoted != len(es) || calls != before {
		t.Fatalf("promoted %d of %d, apply ran %d more times", res.Promoted, len(es), calls-before)
	}
	if v.Tail() != 0 {
		t.Fatalf("tail %d after its entries settled", v.Tail())
	}
}

func TestMismatchDropsTheTail(t *testing.T) {
	v := vtOpen(t, kolostore.NewMemory(), vtConfig(), vtApply)
	es := vtScript(4, 20)
	applyAllTail(t, v, es)
	swapped := append([]Entry(nil), es[:10]...)
	swapped[4], swapped[5] = swapped[5], swapped[4]
	res, err := v.Settle(0, vtSteps(swapped))
	if err != nil {
		t.Fatal(err)
	}
	if res.Promoted != 4 {
		t.Fatalf("promoted %d, want the 4 before the first difference", res.Promoted)
	}
	if v.Tail() != 0 {
		t.Fatalf("the tail must be dropped on a mismatch, it holds %d", v.Tail())
	}
	// a changed library write or void flag is a mismatch too
	for name, mut := range map[string]func(*applyStep){
		"void": func(s *applyStep) { s.Void = true },
		"lib":  func(s *applyStep) { s.Lib = []viewWrite{{Op: opPut, A: []byte("r1")}} },
	} {
		v := vtOpen(t, kolostore.NewMemory(), vtConfig(), vtApply)
		applyAllTail(t, v, es[:5])
		steps := make([]applyStep, 5)
		for i := range steps {
			steps[i] = vtStep(es[i])
		}
		mut(&steps[3])
		res, err := v.Settle(0, func(y func(applyStep, error) bool) {
			for _, s := range steps {
				if !y(s, nil) {
					return
				}
			}
		})
		if err != nil || res.Promoted != 3 || v.Tail() != 0 {
			t.Fatalf("%s: %+v %v tail %d", name, res, err, v.Tail())
		}
	}
}

func TestPartialTailPromotionKeepsTheRest(t *testing.T) {
	v := vtOpen(t, kolostore.NewMemory(), vtConfig(), vtApply)
	es := vtScript(12, 40)
	applyAllTail(t, v, es)
	if _, err := v.Settle(0, vtSteps(es[:15])); err != nil {
		t.Fatal(err)
	}
	if v.Tail() != 25 {
		t.Fatalf("tail %d, want the 25 entries after the batch", v.Tail())
	}
	ds, m := vtRun(vtConfig().Initial, 0, es)
	for i := 0; i < 25; i++ {
		if ti, _ := v.TailEntry(i); ti.ID != es[15+i].ID || ti.Position != uint64(15+i) || ti.Digest != ds[15+i] {
			t.Fatalf("remaining tail entry %d: %+v", i, ti)
		}
	}
	s := v.View()
	defer s.Close()
	vtEq(t, "view after a partial promotion", vtDump(s.Scan(nil, nil)), m.dump())
}
