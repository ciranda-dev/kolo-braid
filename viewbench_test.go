package kolobraid

import (
	"encoding/binary"
	"fmt"
	"runtime"
	"testing"

	kolostore "github.com/ciranda-dev/kolo-store"
)

// kvApply is the kind of apply the record measures: decode, one read, two writes.
func kvApply(tx *Tx, e *Entry) error {
	n := uint64(0)
	if v, err := tx.Get([]byte("count")); err == nil {
		n = binary.BigEndian.Uint64(v)
	}
	if err := tx.Put(binary.BigEndian.AppendUint64([]byte("msg/"), e.Position), e.Body); err != nil {
		return err
	}
	return tx.Put([]byte("count"), binary.BigEndian.AppendUint64(nil, n+1))
}

func benchEntries(n int) []Entry {
	es := make([]Entry, n)
	for i := range es {
		es[i] = Entry{ID: vtID(i), Writer: make([]byte, 32), Kind: KindData, Body: make([]byte, 160)}
	}
	return es
}

func BenchmarkApplyTail(b *testing.B) {
	es := benchEntries(2048)
	v, _ := openView(kolostore.NewMemory(), vtConfig(), kvApply)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%2048 == 0 {
			v.DropTail(0)
		}
		if _, err := v.ApplyTail(vtStep(es[i%2048])); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSettleApply(b *testing.B) {
	es := benchEntries(1024)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		v, _ := openView(kolostore.NewMemory(), vtConfig(), kvApply)
		b.StartTimer()
		if _, err := v.Settle(0, vtSteps(es)); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*len(es)), "ns/entry")
}

func BenchmarkSettlePromote(b *testing.B) {
	es := benchEntries(1024)
	b.ReportAllocs()
	var total int64
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		v, _ := openView(kolostore.NewMemory(), vtConfig(), kvApply)
		for _, e := range es {
			v.ApplyTail(vtStep(e))
		}
		b.StartTimer()
		res, err := v.Settle(0, vtSteps(es))
		if err != nil || res.Promoted != len(es) {
			b.Fatal(err, res.Promoted)
		}
		total += int64(len(es))
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(total), "ns/entry")
}

// A reorg at the end of a full tail: drop 100 entries and apply 100 again.
func BenchmarkReorgOfFullTail(b *testing.B) {
	es := benchEntries(2048)
	v, _ := openView(kolostore.NewMemory(), vtConfig(), kvApply)
	for _, e := range es {
		v.ApplyTail(vtStep(e))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v.DropTail(1948)
		for _, e := range es[1948:] {
			if _, err := v.ApplyTail(vtStep(e)); err != nil {
				b.Fatal(err)
			}
		}
	}
}

// Reading a key that only the durable view holds, through a full tail overlay.
func BenchmarkGetThroughFullTail(b *testing.B) {
	es := benchEntries(2048)
	v, _ := openView(kolostore.NewMemory(), vtConfig(), kvApply)
	v.SeedPairs([]seedPair{{Key: []byte("settled"), Value: []byte("v")}})
	for _, e := range es {
		v.ApplyTail(vtStep(e))
	}
	s := v.View()
	defer s.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := s.Get([]byte("settled")); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSnapshotOfFullTail(b *testing.B) {
	es := benchEntries(2048)
	v, _ := openView(kolostore.NewMemory(), vtConfig(), kvApply)
	for _, e := range es {
		v.ApplyTail(vtStep(e))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v.View().Close()
	}
}

// The worst case of one hostile entry: 65,536 range deletes, then every later read has to check them.
func BenchmarkGetAfterManyRangeDeletes(b *testing.B) {
	v, _ := openView(kolostore.NewMemory(), vtConfig(), func(tx *Tx, e *Entry) error {
		for i := 0; i < 60000; i++ { // under the budget: 16 MiB / (8 + 256) bytes is 63,550 operations
			if err := tx.DeleteRange(binary.BigEndian.AppendUint32(nil, uint32(i)*2), binary.BigEndian.AppendUint32(nil, uint32(i)*2+1)); err != nil {
				return err
			}
		}
		return nil
	})
	if res, err := v.ApplyTail(vtStep(vtEntry(0))); err != nil || res.Status != statusApplied {
		b.Fatal(err)
	}
	s := v.View()
	defer s.Close()
	b.Logf("%d range deletes in the tail", len(v.tail.l.ranges))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Get([]byte("absent"))
	}
}

func BenchmarkScanThroughTail(b *testing.B) {
	es := benchEntries(2048)
	v, _ := openView(kolostore.NewMemory(), vtConfig(), kvApply)
	for _, e := range es {
		v.ApplyTail(vtStep(e))
	}
	s := v.View()
	defer s.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n := 0
		for range s.Scan(nil, nil) {
			n++
		}
		if n != 2049 {
			b.Fatal(n)
		}
	}
}

// The overlay's real cost per write stays under what the budget charges for it.
func TestOverlayHeapStaysUnderItsAccounting(t *testing.T) {
	const writes = 100_000
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	o := newOverlay()
	for i := 0; i < writes/4; i++ {
		var ws []viewWrite
		for j := 0; j < 4; j++ {
			k := []byte(fmt.Sprintf("key/%08d/%d", i, j))
			ws = append(ws, viewWrite{Op: opPut, A: k, B: make([]byte, 16)})
		}
		o.push(tailEntry{writes: ws, cost: 4 * (16 + 16 + writeOpCost)})
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(o)
	perWrite := float64(after.HeapAlloc-before.HeapAlloc) / writes
	keyValue := float64(len("key/00000000/0") + 16)
	if over := perWrite - keyValue; over >= writeOpCost {
		t.Fatalf("an overlay write costs %.0f bytes beyond its key and value, the budget charges %d", over, writeOpCost)
	} else {
		t.Logf("overlay cost per write beyond key and value: %.0f bytes (budget charges %d)", over, writeOpCost)
	}
}

func TestChunkFlushesWhenBytesPassTheBudget(t *testing.T) {
	cfg := vtConfig()
	cfg.MaxBody = 1 << 10 // budget 16 KiB
	f := newVtFault(kolostore.NewMemory(), 0, false)
	v := vtOpen(t, f, cfg, vtApply)
	var es []Entry
	for i := 0; i < 20; i++ {
		es = append(es, vtEntry(i, vtOp{kind: 'p', a: fmt.Sprint("k", i), b: string(make([]byte, 7<<10))}))
	}
	if _, err := v.Settle(0, vtSteps(es)); err != nil {
		t.Fatal(err)
	}
	// each entry costs just over 7 KiB: three of them pass the 16 KiB budget, so a chunk holds at most three
	if n := f.n.Load(); n < 7 || n > 8 {
		t.Fatalf("%d commits for 20 entries of 7 KiB, want 7 or 8 (budget 16 KiB)", n)
	}
	if v.Progress().Next != 20 {
		t.Fatal()
	}
}
