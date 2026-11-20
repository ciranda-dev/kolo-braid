package kolobraid

import (
	"errors"
	"math/rand/v2"
	"runtime"
	"sync"
	"testing"
	"time"

	kolostore "github.com/ciranda-dev/kolo-store"
)

func TestSnapshotIsFrozen(t *testing.T) {
	for seed := uint64(0); seed < 25; seed++ {
		r := rand.New(rand.NewPCG(seed, 8))
		cfg := vtConfig()
		cfg.chunk = 1 + r.IntN(8)
		es := vtScript(seed+200, 120)
		v := vtOpen(t, kolostore.NewMemory(), cfg, vtApply)
		type taken struct {
			s    *Snapshot
			want []string
			pos  uint64
		}
		var snaps []taken
		settled := 0
		batch := uint64(0)
		for settled < len(es) {
			size := 1 + r.IntN(15)
			size = min(size, len(es)-settled)
			if _, err := v.Settle(batch, vtSteps(es[settled:settled+size])); err != nil {
				t.Fatal(err)
			}
			batch++
			settled += size
			_, m := vtRun(cfg.Initial, 0, es[:settled])
			s := v.Settled()
			snaps = append(snaps, taken{s, m.dump(), uint64(settled - 1)})
			if r.IntN(3) == 0 { // close one at random
				i := r.IntN(len(snaps))
				snaps[i].s.Close()
				snaps = append(snaps[:i], snaps[i+1:]...)
			}
			for _, tk := range snaps {
				vtEq(t, "frozen snapshot", vtDump(tk.s.Scan(nil, nil)), tk.want)
				if tk.s.Position() != tk.pos {
					t.Fatalf("Position %d, want %d", tk.s.Position(), tk.pos)
				}
			}
		}
		for _, tk := range snaps {
			tk.s.Close()
		}
		v.mu.RLock()
		undos, open := len(v.undos), len(v.snaps)
		v.mu.RUnlock()
		if undos != 0 || open != 0 {
			t.Fatalf("seed %d: %d undo records and %d snapshots kept after every Close", seed, undos, open)
		}
	}
}

func TestSnapshotGetAndBoundedScanAreFrozen(t *testing.T) {
	v := vtOpen(t, kolostore.NewMemory(), vtConfig(), vtApply)
	if _, err := v.Settle(0, vtSteps([]Entry{
		vtEntry(0, vtOp{kind: 'p', a: "a", b: "1"}, vtOp{kind: 'p', a: "b", b: "2"}, vtOp{kind: 'p', a: "c", b: "3"}),
	})); err != nil {
		t.Fatal(err)
	}
	s := v.Settled()
	defer s.Close()
	if _, err := v.Settle(1, vtSteps([]Entry{
		vtEntry(1, vtOp{kind: 'r', a: "a", b: "c"}, vtOp{kind: 'p', a: "c", b: "new"}, vtOp{kind: 'p', a: "d", b: "4"}),
	})); err != nil {
		t.Fatal(err)
	}
	for k, want := range map[string]string{"a": "1", "b": "2", "c": "3"} {
		if got, err := s.Get([]byte(k)); err != nil || string(got) != want {
			t.Errorf("Get %q = %q %v, want %q", k, got, err, want)
		}
	}
	if _, err := s.Get([]byte("d")); !errors.Is(err, ErrNotFound) {
		t.Errorf("a key added after the snapshot: %v", err)
	}
	vtEq(t, "bounded", vtDump(s.Scan([]byte("b"), []byte("d"))), []string{"b\x002", "c\x003"})
	now := v.Settled()
	defer now.Close()
	vtEq(t, "current", vtDump(now.Scan(nil, nil)), []string{"c\x00new", "d\x004"})
}

func TestTentativeSnapshotIsFrozenAgainstReorg(t *testing.T) {
	v := vtOpen(t, kolostore.NewMemory(), vtConfig(), vtApply)
	es := vtScript(31, 40)
	applyAllTail(t, v, es)
	s := v.View()
	defer s.Close()
	_, m := vtRun(vtConfig().Initial, 0, es)
	if err := v.DropTail(5); err != nil {
		t.Fatal(err)
	}
	applyAllTail(t, v, []Entry{vtEntry(99, vtOp{kind: 'p', a: "new", b: "x"})})
	vtEq(t, "snapshot after a reorg", vtDump(s.Scan(nil, nil)), m.dump())
	if _, err := v.Settle(0, vtSteps(es[:5])); err != nil {
		t.Fatal(err)
	}
	vtEq(t, "snapshot after settling", vtDump(s.Scan(nil, nil)), m.dump())
}

func TestSnapshotMembersAndClose(t *testing.T) {
	v := vtOpen(t, kolostore.NewMemory(), vtConfig(), vtApply)
	v.setRoster(vtRoster(vtMembers()))
	s := v.Settled()
	ws := s.Members()
	if len(ws) != 1 || !ws[0].Seat {
		t.Fatalf("%+v", ws)
	}
	ws[0].Key[0] ^= 1
	if s.Members()[0].Key[0] != 1 {
		t.Fatal("Members returned the roster's slice")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if _, err := s.Get([]byte("a")); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	for range s.Scan(nil, nil) {
		t.Fatal("scan on a closed snapshot")
	}
	if s.Members() != nil {
		t.Fatal("members of a closed snapshot")
	}
	if _, err := v.Settled().Get(make([]byte, MaxViewKey+1)); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
}

func TestSnapshotScanStopsWhenClosed(t *testing.T) {
	v := vtOpen(t, kolostore.NewMemory(), vtConfig(), vtApply)
	if _, err := v.Settle(0, vtSteps([]Entry{vtEntry(0, vtOp{kind: 'p', a: "a"}, vtOp{kind: 'p', a: "b"}, vtOp{kind: 'p', a: "c"})})); err != nil {
		t.Fatal(err)
	}
	s := v.Settled()
	n := 0
	for range s.Scan(nil, nil) {
		n++
		s.Close()
	}
	if n != 1 {
		t.Fatalf("%d pairs after Close inside the loop", n)
	}
}

func TestForgottenSnapshotIsCollected(t *testing.T) {
	v := vtOpen(t, kolostore.NewMemory(), vtConfig(), vtApply)
	func() {
		s := v.Settled()
		_ = s.Position()
	}()
	if _, err := v.Settle(0, vtSteps([]Entry{vtEntry(0, vtOp{kind: 'p', a: "a"})})); err != nil {
		t.Fatal(err)
	}
	v.mu.RLock()
	undos := len(v.undos)
	v.mu.RUnlock()
	if undos > 1 {
		t.Fatalf("%d undo records while a snapshot is open", undos)
	}
	for i := 0; i < 200; i++ {
		runtime.GC()
		v.mu.RLock()
		n := len(v.snaps)
		v.mu.RUnlock()
		if n == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	v.mu.RLock()
	defer v.mu.RUnlock()
	if len(v.snaps) != 0 || len(v.undos) != 0 {
		t.Fatalf("a snapshot dropped without Close still holds %d undo records", len(v.undos))
	}
}

// Readers on other goroutines see whole states while settlement and reorgs go on.
func TestSnapshotsDuringSettlement(t *testing.T) {
	cfg := vtConfig()
	cfg.chunk = 3
	es := vtScript(41, 150)
	v := vtOpen(t, kolostore.NewMemory(), cfg, vtApply)
	models := make([][]string, len(es)+1)
	for i := range models {
		_, m := vtRun(cfg.Initial, 0, es[:i])
		models[i] = m.dump()
	}
	var wg sync.WaitGroup
	stop := make(chan struct{})
	errc := make(chan string, 8)
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				s := v.Settled()
				got := vtDump(s.Scan(nil, nil))
				n := 0
				if len(got) > 0 || s.Position() > 0 {
					n = int(s.Position()) + 1
				}
				// the position says how many entries the view holds: its content must be exactly that state
				if !equalStrings(got, models[n]) {
					select {
					case errc <- "snapshot at position " + string(rune('0'+n%10)) + " is not a whole state":
					default:
					}
				}
				s.Close()
				t2 := v.View()
				for range t2.Scan(nil, nil) {
				}
				t2.Get([]byte("count"))
				t2.Close()
			}
		}()
	}
	applyAllTail(t, v, es)
	pos := 0
	for batch := uint64(0); pos < len(es); batch++ {
		size := min(1+int(batch%9), len(es)-pos)
		if _, err := v.Settle(batch, vtSteps(es[pos:pos+size])); err != nil {
			t.Fatal(err)
		}
		pos += size
		if batch%4 == 0 {
			v.DropTail(v.Tail() / 2)
			applyAllTail(t, v, es[pos+v.Tail():min(len(es), pos+v.Tail()+20)])
		}
	}
	close(stop)
	wg.Wait()
	select {
	case msg := <-errc:
		t.Fatal(msg)
	default:
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
