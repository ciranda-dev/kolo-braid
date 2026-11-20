package kolobraid

import (
	"errors"
	"fmt"
	"go/parser"
	"go/token"
	"slices"
	"strconv"
	"sync/atomic"
	"testing"

	kolostore "github.com/ciranda-dev/kolo-store"
)

func dumpPrefix(t *testing.T, b kolostore.Backend, prefix string) []string {
	t.Helper()
	var out []string
	if err := b.Scan([]byte(prefix), []byte(string(prefix[0]+1)), false, 0, func(k, v []byte) bool {
		out = append(out, string(k)+"\x00"+string(v))
		return true
	}); err != nil {
		t.Fatal(err)
	}
	return out
}

// Applying the same ordered entries on a fresh view always gives the same bytes: the digests and every stored byte of the view.
func TestPropertyFreshReplayIsByteIdentical(t *testing.T) {
	for seed := uint64(0); seed < 30; seed++ {
		es := vtScript(seed+300, 200)
		var ref []string
		var refDigest Digest
		for run, chunk := range []int{0, 1, 3, 17, 256, 1000} {
			cfg := vtConfig()
			cfg.chunk = chunk
			b := kolostore.NewMemory()
			v := vtOpen(t, b, cfg, vtApply)
			// the batch boundaries differ between runs as well
			step := []int{200, 1, 7, 64, 13, 199}[run]
			var last settleResult
			for i, batch := 0, uint64(0); i < len(es); i, batch = i+step, batch+1 {
				var err error
				if last, err = v.Settle(batch, vtSteps(es[i:min(i+step, len(es))])); err != nil {
					t.Fatal(err)
				}
			}
			view := dumpPrefix(t, b, "v")
			if run == 0 {
				ref, refDigest = view, last.Progress.Digest
				continue
			}
			if last.Progress.Digest != refDigest {
				t.Fatalf("seed %d chunk %d: digest differs from the first run", seed, chunk)
			}
			if !slices.Equal(view, ref) {
				t.Fatalf("seed %d chunk %d: stored view bytes differ", seed, chunk)
			}
		}
	}
}

func TestDigestAfterRestartEqualsUninterruptedRun(t *testing.T) {
	es := vtScript(77, 100)
	cfg := vtConfig()
	cfg.chunk = 4
	ref, _ := vtRun(cfg.Initial, 0, es)
	for cut := 1; cut < len(es); cut += 7 {
		b := kolostore.NewMemory()
		v := vtOpen(t, b, cfg, vtApply)
		if _, err := v.Settle(0, vtSteps(es[:cut])); err != nil {
			t.Fatal(err)
		}
		v2 := vtOpen(t, b, cfg, vtApply)
		if _, err := v2.Settle(1, vtSteps(es[cut:])); err != nil {
			t.Fatal(err)
		}
		if got := v2.Progress(); got.Digest != ref[len(es)-1] || got.Batches != 2 {
			t.Fatalf("cut %d: %+v", cut, got)
		}
	}
}

// An apply that depends on map iteration order or on a clock gives different digests on different peers, and a certified digest
// from the other peer is then reported as a divergence.
func TestNonDeterministicApplyIsDetected(t *testing.T) {
	mapOrder := func(tx *Tx, e *Entry) error {
		m := map[string]bool{}
		for i := 0; i < 12; i++ {
			m[fmt.Sprint("key", i)] = true
		}
		n := 0
		for k := range m { // Go randomises the order of this loop
			if err := tx.Put([]byte(k), []byte{byte(n)}); err != nil {
				return err
			}
			n++
		}
		return nil
	}
	var clock atomic.Int64
	clockApply := func(tx *Tx, e *Entry) error {
		return tx.Put([]byte("now"), []byte(strconv.FormatInt(clock.Add(1), 10)))
	}
	for name, apply := range map[string]ApplyFunc{"map order": mapOrder, "clock": clockApply} {
		es := make([]Entry, 64)
		for i := range es {
			es[i] = vtEntry(i)
		}
		cfg := vtConfig() // a checkpoint at position 63
		peers := make([]*view, 2)
		var certified Digest
		var cp *checkpointRec
		for i := range peers {
			peers[i] = vtOpen(t, kolostore.NewMemory(), cfg, apply)
			res, err := peers[i].Settle(0, vtSteps(es))
			if err != nil || res.Checkpoint == nil {
				t.Fatalf("%s: %v", name, err)
			}
			if i == 0 {
				certified, cp = res.Checkpoint.Digest, res.Checkpoint
			}
		}
		if peers[1].Progress().Digest == certified {
			t.Fatalf("%s: two peers ended with the same digest", name)
		}
		known, err := peers[1].Verify(cp.Position, certified)
		if !known || err == nil || !errors.Is(err, ErrDiverged) {
			t.Fatalf("%s: Verify = %v %v", name, known, err)
		}
		if peers[1].Halted() == nil {
			t.Fatalf("%s: the diverged peer must halt", name)
		}
		// and a peer that agrees is not halted
		if known, err := peers[0].Verify(cp.Position, certified); !known || err != nil {
			t.Fatalf("%s: agreeing peer %v %v", name, known, err)
		}
	}
}

// The apply path imports no clock and no source of randomness.
func TestNoClockImports(t *testing.T) {
	banned := []string{"time", "math/rand", "math/rand/v2", "crypto/rand"}
	for _, f := range []string{"tx.go", "view.go", "overlay.go", "digest.go", "replay.go", "internal/prefixdb/prefixdb.go"} {
		file, err := parser.ParseFile(token.NewFileSet(), f, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		for _, imp := range file.Imports {
			path, _ := strconv.Unquote(imp.Path.Value)
			if slices.Contains(banned, path) {
				t.Errorf("%s imports %s", f, path)
			}
		}
	}
}
