package kolobraid

import (
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"weak"

	kololog "github.com/ciranda-dev/kolo-log"
	"golang.org/x/crypto/blake2b"
)

var (
	scopeFull   = flag.Bool("full", false, "run model-check scope f at its full size")
	scopeBudget = flag.Duration("scopebudget", 30*time.Minute, "time limit of scope f in -full mode")
	scopeShard  = flag.String("scopeshard", "", "k/m: run only the histories of scope f whose index mod m is k, logging progress")
	scopeFrom   = flag.Int64("scopefrom", 0, "with -scopeshard, skip the histories below this index (resume from a logged next)")
	scopePar    = flag.Int("scopepar", 4, "observers of scope f running at once")
	scopeWait   = flag.Duration("scopewait", time.Minute, "how long an observer of scope f may take to reach the writers' state")
)

// chainApply folds every effective data entry into one hash chain, so any change of order or content changes the view.
func chainApply(tx *Tx, e *Entry) error {
	if e.Kind != KindData {
		return nil
	}
	prev, err := tx.Get([]byte("chain"))
	if err != nil && !errors.Is(err, ErrNotFound) {
		return err
	}
	h := blake2b.Sum256(append(append(prev, e.ID[:]...), e.Body...))
	return tx.Put([]byte("chain"), h[:])
}

// TestScopeF is model-check scope f: for random braid histories, observers that receive the entries in random insertion orders
// apply them tentatively, reorganise and promote, and must end with the settled view and running digest of the members, for an
// apply that is a hash chain and for a key-value apply. Quick mode is part of go test; -full runs 1,000 histories with 10^4
// orders each. With -scopeshard each finished history logs a "modelshard" line for scripts/modelmerge.sh.
func TestScopeF(t *testing.T) {
	histories, orders := 3, 6
	if testing.Short() {
		histories, orders = 1, 3
	}
	if *scopeFull {
		histories, orders = 1000, 10000
	}
	var k, m int64 = 0, 1
	if *scopeShard != "" {
		if _, err := fmt.Sscanf(*scopeShard, "%d/%d", &k, &m); err != nil || m < 1 || k < 0 || k >= m {
			t.Fatalf("-scopeshard %q: want k/m with 0 <= k < m", *scopeShard)
		}
	}
	if *scopeFrom < 0 || (*scopeFrom > 0 && *scopeShard == "") {
		t.Fatalf("-scopefrom %d: want 0, or a history index with -scopeshard", *scopeFrom)
	}
	start := time.Now()
	deadline := start.Add(*scopeBudget)
	from, next := *scopeFrom, *scopeFrom
	var done, total int64
	logShard := func(finished bool) {
		if *scopeShard != "" {
			t.Logf("modelshard scope=%q shard=%d/%d from=%d next=%d checked=%d inner=%d total=%d done=%v elapsed=%v",
				fmt.Sprintf("f %d histories x %d orders", histories, orders), k, m, from, next, done, total, histories, finished,
				time.Since(start).Round(time.Second))
		}
	}
	for h := from; h < int64(histories); h++ {
		if h%m != k {
			continue
		}
		if time.Now().After(deadline) {
			logShard(false)
			if *scopeFull {
				t.Errorf("scope f incomplete within %v: %d histories and %d insertion orders checked, next history %d",
					*scopeBudget, done, total, next)
			}
			return
		}
		var n int64
		if !t.Run(fmt.Sprintf("history%d", h), func(t *testing.T) { n = scopeHistory(t, h, orders, deadline) }) {
			logShard(false)
			t.FailNow()
		}
		if n < int64(orders) {
			logShard(false)
			if *scopeFull {
				t.Errorf("scope f incomplete within %v: history %d stopped after %d of %d orders", *scopeBudget, h, n, orders)
			}
			return
		}
		done++
		total += n
		next = h + 1
	}
	next = int64(histories)
	logShard(true)
	t.Logf("scope f: %d histories, %d insertion orders, %v", done, total, time.Since(start).Round(time.Millisecond))
}

// scopeHistory builds history h with three members and checks orders observers against it, scopePar at a time. It returns how
// many orders were checked before the deadline.
func scopeHistory(t *testing.T, h int64, orders int, deadline time.Time) int64 {
	applies := []ApplyFunc{chainApply, appApply}
	apply := applies[h%int64(len(applies))]
	rng := rand.New(rand.NewPCG(uint64(h), 0xf))
	nt, ps := group(t, 3, GroupConfig{}, apply, true)
	for step := 0; step < 30; step++ {
		p := ps[rng.IntN(3)]
		mustAppend(t, p.b, fmt.Sprintf("k%d=%d", rng.IntN(5), step))
		if rng.IntN(3) == 0 {
			nt.round(rng, uint64(1+rng.IntN(2)))
		}
	}
	nt.settle(rng, 60*time.Second, func() bool { return quiet(ps...) })
	ref := ps[0]
	w := capture(t, ref)
	var lens []uint64
	for _, id := range ref.b.followed() {
		lens = append(lens, w.lens[id])
	}
	t.Logf("history %d: %d settled positions, digest %x, strand lengths %v", h, w.progress.Next, w.progress.Digest[:8], lens)
	var checked atomic.Int64
	var wg sync.WaitGroup
	jobs := make(chan int)
	for range max(1, *scopePar) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ot := goroutineTB{TB: t}
			for o := range jobs {
				if t.Failed() {
					continue
				}
				// a Fatal ends only this goroutine
				ok := make(chan bool, 1)
				go func() {
					r := false
					defer func() { ok <- r }()
					r = observe(ot, ref, apply, w, uint64(h), uint64(o))
				}()
				if <-ok {
					checked.Add(1)
				}
			}
		}()
	}
	for o := 0; o < orders && !t.Failed() && time.Now().Before(deadline); o++ {
		jobs <- o
	}
	close(jobs)
	wg.Wait()
	return checked.Load()
}

// wanted is the writers' state an observer must reach: the settled progress and view of ref, and the lengths of the strands
// ref held when it was taken.
type wanted struct {
	progress progress
	view     string
	lens     map[kololog.LogID]uint64
}

func capture(t testing.TB, ref *peer) wanted {
	w := wanted{progress: ref.b.view.Progress(), view: dump(t, ref.b), lens: map[kololog.LogID]uint64{}}
	for _, id := range ref.b.followed() {
		w.lens[id] = ref.b.strandFor(id).length()
	}
	return w
}

// observe opens a fresh observer, delivers ref's strands to it in the insertion order seeded by (h, o), and waits until it has
// the writers' state. It reports false when it failed the test.
func observe(t goroutineTB, ref *peer, apply ApplyFunc, w wanted, h, o uint64) bool {
	// only the genesis entry is held before Open, as a joiner needs; the rest of the founder strand arrives in the shuffled order,
	// and no strand beyond what ref held when w was taken
	obs, _ := joiner(t, "obs", ref, apply, 1)
	defer obs.close()
	if observed != nil {
		observed(obs)
	}
	obs.start()
	orng := rand.New(rand.NewPCG(h, o))
	for {
		ids := obs.b.followed()
		orng.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
		moved := false
		for _, id := range ids {
			dst := obs.b.replicaOf(id)
			src := ref.b.strandFor(id)
			if dst == nil || src == nil || dst.Len() >= w.lens[id] {
				continue
			}
			if transfer(t, src, dst, min(w.lens[id], dst.Len()+uint64(1+orng.IntN(2)))) {
				moved = true
				if err := obs.b.strandGrew(id); err != nil {
					t.Fatal(err)
				}
				break
			}
		}
		if !moved {
			break
		}
	}
	reached := func() bool {
		obs.b.mu.Lock()
		jobs := len(obs.b.jobs)
		obs.b.mu.Unlock()
		return jobs == 0 && obs.b.view.Progress() == w.progress
	}
	began := time.Now()
	for !reached() {
		if time.Since(began) > *scopeWait {
			t.Errorf("history %d order %d: observer did not reach the writers' state within %v:\n%s", h, o, *scopeWait,
				observerReport(ref, obs, w))
			return false
		}
		time.Sleep(time.Millisecond)
	}
	if got := dump(t, obs.b); got != w.view {
		t.Errorf("history %d order %d: view differs from a fresh replay", h, o)
		return false
	}
	return !t.Failed()
}

// observed, when set, is called with every observer scope f opens.
var observed func(*peer)

// TestScopeFObserverIsCollected: once an order is checked, nothing keeps its observer reachable. Each observer holds a memory
// backend of the whole history, so observers kept until the test ends make a full run grow without bound.
func TestScopeFObserverIsCollected(t *testing.T) {
	ref := newFounder(t, "ref", GroupConfig{}, chainApply)
	mustAppend(t, ref.b, "a")
	waitUntil(t, 5*time.Second, func() bool { return quiet(ref) && ref.b.view.Progress().Next > 1 })
	w := capture(t, ref)
	var wp weak.Pointer[peer]
	observed = func(p *peer) { wp = weak.Make(p) }
	defer func() { observed = nil }()
	if !observe(goroutineTB{TB: t}, ref, chainApply, w, 0, 0) {
		t.Fatal("the observer did not reach the founder's state")
	}
	if wp == (weak.Pointer[peer]{}) {
		t.Fatal("observe opened no observer")
	}
	for range 10 {
		runtime.GC()
		if wp.Value() == nil {
			return
		}
	}
	t.Fatal("a checked observer is still reachable")
}

// goroutineTB lets the test helpers run on a goroutine other than the test's: Fatal and Skip record a failure and end only that
// goroutine.
type goroutineTB struct{ testing.TB }

func (g goroutineTB) Fatal(args ...any) {
	g.TB.Helper()
	g.TB.Error(args...)
	runtime.Goexit()
}

func (g goroutineTB) Fatalf(format string, args ...any) {
	g.TB.Helper()
	g.TB.Errorf(format, args...)
	runtime.Goexit()
}

func (g goroutineTB) FailNow() {
	g.TB.Fail()
	runtime.Goexit()
}

func (g goroutineTB) Skip(args ...any) {
	g.Fatal(append([]any{"skip on an observer goroutine:"}, args...)...)
}

func (g goroutineTB) Skipf(format string, args ...any) {
	g.Fatalf("skip on an observer goroutine: "+format, args...)
}

func (g goroutineTB) SkipNow() { g.Fatal("skip on an observer goroutine") }

// observerReport says what an observer that did not reach the writers' state was waiting for: the applier's queue, the settled
// progress against the wanted one, which strands it holds less or more of than ref held, and both peers' settlement state.
func observerReport(ref, obs *peer, w wanted) string {
	var sb strings.Builder
	got := obs.b.view.Progress()
	obs.b.mu.Lock()
	fmt.Fprintf(&sb, "awaited: applier idle (jobs %d, busy %v) and settled progress %+v; have %+v\n", len(obs.b.jobs), obs.b.busy,
		w.progress, got)
	switch {
	case got.Next > w.progress.Next:
		sb.WriteString("cause: the observer settled past the wanted state (ref's strands grew after it was taken)\n")
	case got.Next == w.progress.Next && got != w.progress:
		sb.WriteString("cause: same position, different digest or batch: a divergence\n")
	}
	obs.b.mu.Unlock()
	for _, id := range ref.b.followed() {
		have := uint64(0)
		if s := obs.b.strandFor(id); s != nil {
			have = s.length()
		}
		if now := ref.b.strandFor(id).length(); have != w.lens[id] || now != w.lens[id] {
			fmt.Fprintf(&sb, "strand %x: ref held %d when taken, holds %d now; observer holds %d\n", id[:4], w.lens[id], now, have)
		}
	}
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	fmt.Fprintf(&sb, "process: heap in use %d MiB, %d GCs, %d goroutines\n", ms.HeapInuse>>20, ms.NumGC, runtime.NumGoroutine())
	fmt.Fprintf(&sb, "observer status %+v\n", obs.b.Status())
	for _, p := range []*peer{ref, obs} {
		ids := p.b.followed()
		p.b.mu.Lock()
		n, bytes := p.b.d.Pending()
		fmt.Fprintf(&sb, "%s: pivots %d next %d fatal %v viewHalt %v settle %v pending %d/%d unresolved %d nodes %d jobs %d busy %v\n",
			p.name, p.b.pivots, p.b.settledNext, p.b.fatal, p.b.viewHalt, p.b.st.Halted(), n, bytes, len(p.b.d.Unresolved()),
			len(p.b.nodes), len(p.b.jobs), p.b.busy)
		c := p.b.st.Config()
		l, _ := p.b.st.Last()
		fmt.Fprintf(&sb, "  config %d seats %d last round %d count %d predicted %d awaiting %d\n", c.Number, len(c.Seats), l.Round,
			p.b.st.Count(), len(p.b.st.Predicted()), len(p.b.st.Awaiting()))
		for _, id := range ids {
			s := p.b.strands[id]
			fmt.Fprintf(&sb, "  strand %x len %d next %d floor %d stop %v\n", id[:4], s.length(), s.next, s.floor, s.stop)
		}
		p.b.mu.Unlock()
	}
	return sb.String()
}
