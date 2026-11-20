package settle

import (
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ciranda-dev/kolo-braid/internal/dag"
)

// The model checks run the production code of this package and of internal/dag against the reference in helpers_test.go.
// Plain go test runs the quick mode; -full runs every scope at the record's size (scripts/modelcheck.sh -full).
var (
	fullModel   = flag.Bool("full", false, "run every model-checking scope at its full size")
	modelBudget = flag.Duration("modelbudget", 0, "stop an exhaustive or sampled scope after this long and fail it as incomplete (0: the scope's limit)")
	modelOnly   = flag.String("modelonly", "", "run only the scope sizes of scopes a and a' whose name is this, for example \"a n=4 q=3 rounds 0..3\"")
	modelShard  = flag.String("modelshard", "", "k/m: check only the jobs of scopes a and a' whose index mod m is k, one at a time, logging progress")
	modelFrom   = flag.Int64("modelfrom", 0, "with -modelshard, skip the jobs below this index (resume from a logged next)")
	modelEvery  = flag.Duration("modelprogress", time.Minute, "with -modelshard, how often to log progress")
)

func selected(name string) bool { return *modelOnly == "" || *modelOnly == name }

// sharded runs a scope's jobs: all of them in parallel, or with -modelshard only those of one shard, in index order on one
// goroutine, logging a progress line ("modelshard ...", read by scripts/modelmerge.sh) every -modelprogress and at the end.
// gen yields the jobs whose index keep accepts and returns how many jobs exist (the indices it went through when stopped).
func sharded[J any](sc *scope, gen func(keep func(int64) bool, yield func(int64, J) bool) int64, check func(J) (int, error)) (jobs, ideals int64, err error) {
	if *modelFrom < 0 || (*modelFrom > 0 && *modelShard == "") {
		sc.t.Fatalf("-modelfrom %d: want 0, or a job index with -modelshard", *modelFrom)
	}
	if *modelShard == "" {
		return parallel(sc, func(yield func(J) bool) {
			gen(func(int64) bool { return true }, func(_ int64, j J) bool { return yield(j) })
		}, check)
	}
	var k, m int64
	if _, e := fmt.Sscanf(*modelShard, "%d/%d", &k, &m); e != nil || m < 1 || k < 0 || k >= m {
		sc.t.Fatalf("-modelshard %q: want k/m with 0 <= k < m", *modelShard)
	}
	if *modelFrom > 0 && *modelOnly == "" {
		sc.t.Fatal("-modelfrom needs -modelonly: the index belongs to one scope size")
	}
	from, next := *modelFrom, *modelFrom
	logLine := func(total int64, done bool) {
		sc.t.Logf("modelshard scope=%q shard=%d/%d from=%d next=%d checked=%d inner=%d total=%d done=%v elapsed=%v",
			sc.name, k, m, from, next, jobs, ideals, total, done, time.Since(sc.start).Round(time.Second))
	}
	last := time.Now()
	total := gen(func(i int64) bool { return i >= from && i%m == k }, func(i int64, j J) bool {
		if sc.over() {
			err = errIncomplete
			return false
		}
		n, e := check(j)
		if e != nil {
			err = fmt.Errorf("job %d: %w", i, e)
			return false
		}
		jobs++
		ideals += int64(n)
		next = i + 1
		if time.Since(last) >= *modelEvery {
			last = time.Now()
			logLine(-1, false)
		}
		return true
	})
	if err == nil {
		next = total
	}
	logLine(total, err == nil)
	return jobs, ideals, err
}

type scope struct {
	t     *testing.T
	name  string
	start time.Time
	limit time.Duration
}

func newScope(t *testing.T, name string, limit time.Duration) *scope {
	if *modelBudget > 0 && *modelBudget < limit {
		limit = *modelBudget
	}
	return &scope{t: t, name: name, start: time.Now(), limit: limit}
}

func (s *scope) over() bool { return time.Since(s.start) > s.limit }

func (s *scope) done(format string, args ...any) {
	el := time.Since(s.start)
	s.t.Logf("scope %s: %s, %v", s.name, fmt.Sprintf(format, args...), el.Round(time.Millisecond))
	if el > s.limit {
		s.t.Errorf("scope %s exceeded its limit %v", s.name, s.limit)
	}
}

// parallel runs check on every job from gen with GOMAXPROCS workers; it stops at the first error or when the scope is
// over its limit, and returns the number of jobs checked.
func parallel[J any](sc *scope, gen func(yield func(J) bool), check func(J) (int, error)) (jobs, ideals int64, err error) {
	ch := make(chan J, 256)
	var mu sync.Mutex
	var first error
	var stop atomic.Bool
	var wg sync.WaitGroup
	for range runtime.GOMAXPROCS(0) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range ch {
				if stop.Load() {
					continue
				}
				n, e := check(j)
				atomic.AddInt64(&jobs, 1)
				atomic.AddInt64(&ideals, int64(n))
				if e != nil {
					mu.Lock()
					if first == nil {
						first = e
					}
					mu.Unlock()
					stop.Store(true)
				}
			}
		}()
	}
	gen(func(j J) bool {
		if stop.Load() {
			return false
		}
		if sc.over() {
			stop.Store(true)
			mu.Lock()
			if first == nil {
				first = errIncomplete
			}
			mu.Unlock()
			return false
		}
		ch <- j
		return true
	})
	close(ch)
	wg.Wait()
	return jobs, ideals, first
}

var errIncomplete = errors.New("stopped at the scope's time limit: incomplete")

// layered is a round-structured DAG: vc[s][r][t] is the number of seat t's pulses in the past of seat s's round-r pulse.
type layered struct {
	n, rounds int
	vc        [][][]int
}

// enumerate yields every round-structured DAG through round R where each round-r pulse links a prefix of every other
// seat's strand (at least what its predecessor saw, pulses of rounds below r), pasts canonicalized by vector clock, and
// every pulse has round exactly r. every > 1 keeps one DAG in every (for the race build).
func enumerate(n, q, R, every int, yield func(layered) bool) {
	enumerateIndexed(n, q, R, func(i int64) bool { return (i+1)%int64(every) == 0 }, func(_ int64, l layered) bool { return yield(l) })
}

// enumerateIndexed is enumerate with the DAGs numbered in enumeration order: it yields the DAGs whose index keep accepts
// and returns how many DAGs it enumerated.
func enumerateIndexed(n, q, R int, keep func(int64) bool, yield func(int64, layered) bool) int64 {
	vc := make([][][]int, n)
	for s := range vc {
		vc[s] = make([][]int, R+1)
	}
	var count int64
	stopped := false
	var place func(s, r int)
	place = func(s, r int) {
		if stopped {
			return
		}
		if r > R {
			count++
			if !keep(count - 1) {
				return
			}
			cp := layered{n: n, rounds: R + 1, vc: make([][][]int, n)}
			for i := range vc {
				cp.vc[i] = make([][]int, R+1)
				for j := range vc[i] {
					cp.vc[i][j] = slices.Clone(vc[i][j])
				}
			}
			if !yield(count-1, cp) {
				stopped = true
			}
			return
		}
		if s == n {
			place(0, r+1)
			return
		}
		base := make([]int, n)
		if r > 0 {
			copy(base, vc[s][r-1])
		}
		base[s] = r + 1
		var others []int
		for t := range n {
			if t != s {
				others = append(others, t)
			}
		}
		seen := map[string]bool{}
		var rec func(i int, cur []int)
		rec = func(i int, cur []int) {
			if stopped {
				return
			}
			if i == len(others) {
				c := slices.Clone(cur)
				for changed := true; changed; {
					changed = false
					for t := range n {
						if t == s || c[t] == 0 {
							continue
						}
						for u := range n {
							if v := vc[t][c[t]-1][u]; v > c[u] {
								c[u], changed = v, true
							}
						}
					}
				}
				key := fmt.Sprint(c)
				if seen[key] {
					return
				}
				seen[key] = true
				vc[s][r] = c
				if r == 0 || holdsQuorum(c, n, q, r) {
					place(s+1, r)
				}
				return
			}
			t := others[i]
			for a := cur[t]; a <= r; a++ {
				next := slices.Clone(cur)
				next[t] = a
				rec(i+1, next)
			}
		}
		rec(0, base)
	}
	place(0, 0)
	return count
}

// holdsQuorum reports whether a past holds the round-(r-1) pulse of q seats.
func holdsQuorum(c []int, n, q, r int) bool {
	k := 0
	for t := range n {
		if c[t] >= r {
			k++
		}
	}
	return k >= q
}

func (l layered) world() (*world, [][]*ent) {
	w := newWorld()
	ents := make([][]*ent, l.n)
	for s := range ents {
		ents[s] = make([]*ent, l.rounds)
	}
	for r := range l.rounds {
		for s := range l.n {
			var links []*ent
			for t := range l.n {
				if t != s && l.vc[s][r][t] > 0 {
					links = append(links, ents[t][l.vc[s][r][t]-1])
				}
			}
			var prev *ent
			if r > 0 {
				prev = ents[s][r-1]
			}
			ents[s][r] = w.pulse(s, prev, 0, nil, links...)
		}
	}
	return w, ents
}

// ideals yields every downward-closed set of a layered DAG as per-seat prefix lengths.
func (l layered) ideals(yield func([]int)) {
	k := make([]int, l.n)
	var rec func(s int)
	rec = func(s int) {
		if s == l.n {
			for t := range l.n {
				if k[t] > 0 {
					for u := range l.n {
						if l.vc[t][k[t]-1][u] > k[u] {
							return
						}
					}
				}
			}
			yield(k)
			return
		}
		for v := 0; v <= l.rounds; v++ {
			k[s] = v
			rec(s + 1)
		}
	}
	rec(0)
}

// checkSet compares production with the reference on the ideal D: equal lists, a prefix of the full DAG's list, nested
// batches, and equal rounds for every pulse production still holds.
func checkSet(w *world, D bitset, cfg Config, full []dag.ID, roster func(bitset) func([]dag.ID) Proposal, order []*ent) error {
	rf := newRef(w, D, cfg.Rule, keysOf(cfg.Seats), roster(D))
	rl := rf.committed()
	want := refIDs(rl)
	if !isPrefix(want, full) || len(want) > len(full) {
		return fmt.Errorf("reference list of an ideal (%d pivots) is not a prefix of the full DAG's (%d)", len(want), len(full))
	}
	for i := 1; i < len(rl); i++ {
		if !rl[i].e.past.has(rl[i-1].e.n) {
			return fmt.Errorf("pivot %d's past lacks pivot %d", i, i-1)
		}
	}
	p, err := newPeerE(w, cfg, nil, rules{})
	if err != nil {
		return err
	}
	if order == nil {
		order = w.in(D)
	}
	p.feed(order...)
	if p.err != nil {
		return p.err
	}
	if !slices.Equal(p.ids(), want) {
		return fmt.Errorf("production %v != reference %v", p.rounds(), refRounds(rl))
	}
	for i, x := range p.pivots {
		if x.Config != rl[i].cfg || x.Round != uint64(rl[i].round) {
			return fmt.Errorf("pivot %d: config %d round %d, reference %d %d", i, x.Config, x.Round, rl[i].cfg, rl[i].round)
		}
	}
	if len(rl) == 0 || rl[len(rl)-1].cfg == cfg.Number {
		for _, e := range w.in(D) {
			if r := p.s.byID[e.id]; r != nil && r.seat >= 0 && r.ready == nil && int(r.round) != rf.roundOf(e) {
				return fmt.Errorf("round of entry %d: production %d, reference %d", e.n, r.round, rf.roundOf(e))
			}
		}
	}
	return nil
}

func refRounds(l []refPivot) []int {
	out := make([]int, len(l))
	for i, p := range l {
		out[i] = p.round
	}
	return out
}

func static(cfg Config) func(bitset) func([]dag.ID) Proposal {
	return func(bitset) func([]dag.ID) Proposal { return staticRoster(Proposal{Rule: cfg.Rule, Seats: cfg.Seats}) }
}

func checkLayered(l layered, rule Rule) (int, error) {
	w, ents := l.world()
	cfg := cfg0(w, rule, seq(l.n)...)
	all := w.all()
	full := refIDs(newRef(w, all, rule, seq(l.n), staticRoster(Proposal{Rule: rule, Seats: cfg.Seats})).committed())
	n := 0
	var err error
	l.ideals(func(k []int) {
		if err != nil {
			return
		}
		var tips []*ent
		for s, v := range k {
			if v > 0 {
				tips = append(tips, ents[s][v-1])
			}
		}
		n++
		err = checkSet(w, closure(tips...), cfg, full, static(cfg), nil)
	})
	return n, err
}

// TestModelScopeA is scope a: exhaustive, honest, data entries omitted.
func TestModelScopeA(t *testing.T) {
	type sz struct{ n, R int }
	sizes := []sz{{3, 3}}
	limit := time.Minute
	if *fullModel {
		sizes, limit = []sz{{3, 3}, {3, 4}, {4, 3}, {3, 5}}, 6*time.Hour
	}
	every := 1
	if raceEnabled && !*fullModel {
		every = 97
	}
	for _, z := range sizes {
		q := Quorum(Majority, z.n)
		name := fmt.Sprintf("a n=%d q=%d rounds 0..%d", z.n, q, z.R)
		if !selected(name) {
			continue
		}
		sc := newScope(t, name, limit)
		jobs, ideals, err := sharded(sc, func(keep func(int64) bool, yield func(int64, layered) bool) int64 {
			return enumerateIndexed(z.n, q, z.R, func(i int64) bool { return (i+1)%int64(every) == 0 && keep(i) }, yield)
		}, func(l layered) (int, error) { return checkLayered(l, Majority) })
		if errors.Is(err, errIncomplete) {
			t.Errorf("scope a n=%d rounds 0..%d: %v after %d DAGs and %d ideals, all consistent", z.n, z.R, err, jobs, ideals)
			sc.done("incomplete: %d DAGs, %d ideals", jobs, ideals)
			continue
		}
		if err != nil {
			t.Fatalf("scope a n=%d rounds 0..%d after %d DAGs: %v", z.n, z.R, jobs, err)
		}
		if every == 1 && z.n == 3 && z.R == 3 && *modelShard == "" && jobs != 32697 {
			t.Fatalf("scope a n=3 through round 3: %d DAGs, the record counts 32,697", jobs)
		}
		sc.done("%d DAGs (one in %d), %d ideals", jobs, every, ideals)
	}
}

// randomLayered draws a round-structured DAG like scope a's: every link prefix is random, raised until the round holds.
func randomLayered(r *rand.Rand, n, q, R int) layered {
	l := layered{n: n, rounds: R + 1, vc: make([][][]int, n)}
	for s := range l.vc {
		l.vc[s] = make([][]int, R+1)
	}
	for rr := 0; rr <= R; rr++ {
		for s := range n {
			c := make([]int, n)
			if rr > 0 {
				copy(c, l.vc[s][rr-1])
			}
			c[s] = rr + 1
			for t := range n {
				if t != s && c[t] < rr {
					c[t] += r.IntN(rr - c[t] + 1)
				}
			}
			for {
				closeClock(l.vc, c, s, n)
				if rr == 0 || holdsQuorum(c, n, q, rr) {
					break
				}
				var low []int
				for t := range n {
					if t != s && c[t] < rr {
						low = append(low, t)
					}
				}
				c[low[r.IntN(len(low))]] = rr
			}
			l.vc[s][rr] = c
		}
	}
	return l
}

func closeClock(vc [][][]int, c []int, s, n int) {
	for changed := true; changed; {
		changed = false
		for t := range n {
			if t == s || c[t] == 0 {
				continue
			}
			for u := range n {
				if v := vc[t][c[t]-1][u]; v > c[u] {
					c[u], changed = v, true
				}
			}
		}
	}
}

func checkSampled(l layered, rule Rule, r *rand.Rand, ideals int) (int, error) {
	w, ents := l.world()
	cfg := cfg0(w, rule, seq(l.n)...)
	all := w.all()
	full := refIDs(newRef(w, all, rule, seq(l.n), staticRoster(Proposal{Rule: rule, Seats: cfg.Seats})).committed())
	// the full DAG in a random delivery order
	order := slices.Clone(w.ents)
	r.Shuffle(len(order), func(i, j int) { order[i], order[j] = order[j], order[i] })
	if err := checkSet(w, all, cfg, full, static(cfg), order); err != nil {
		return 1, err
	}
	for i := range ideals {
		var tips []*ent
		for s := range l.n {
			if v := r.IntN(l.rounds + 1); v > 0 {
				tips = append(tips, ents[s][v-1])
			}
		}
		if err := checkSet(w, closure(tips...), cfg, full, static(cfg), nil); err != nil {
			return i + 2, err
		}
	}
	return ideals + 1, nil
}

// TestModelScopeAPrime is scope a': random DAGs, both rules.
func TestModelScopeAPrime(t *testing.T) {
	type sz struct{ n, R int }
	dags, limit := 100, time.Minute
	if raceEnabled {
		dags = 10
	}
	if *fullModel {
		dags, limit = 1_000_000, 30*time.Minute
	}
	for _, z := range []sz{{4, 7}, {5, 9}, {7, 9}} {
		for _, rule := range []Rule{Majority, TwoThirds} {
			q := Quorum(rule, z.n)
			name := fmt.Sprintf("a' n=%d q=%d %s rounds 0..%d", z.n, q, map[Rule]string{Majority: "Majority", TwoThirds: "TwoThirds"}[rule], z.R)
			if !selected(name) {
				continue
			}
			sc := newScope(t, name, limit)
			jobs, ideals, err := sharded(sc, func(keep func(int64) bool, yield func(int64, uint64) bool) int64 {
				for i := range int64(dags) {
					if keep(i) && !yield(i, uint64(i)) {
						return i
					}
				}
				return int64(dags)
			}, func(seed uint64) (int, error) {
				r := rand.New(rand.NewPCG(seed, uint64(z.n*100+z.R*10)+uint64(rule)))
				return checkSampled(randomLayered(r, z.n, q, z.R), rule, r, 30)
			})
			if errors.Is(err, errIncomplete) {
				t.Errorf("scope a' n=%d q=%d: %v after %d DAGs and %d ideals, all consistent", z.n, q, err, jobs, ideals)
				sc.done("incomplete: %d DAGs, %d ideals", jobs, ideals)
				continue
			}
			if err != nil {
				t.Fatalf("scope a' n=%d q=%d after %d DAGs: %v", z.n, q, jobs, err)
			}
			sc.done("%d DAGs, %d ideals", jobs, ideals)
		}
	}
}

// TestModelScopeC is scope c: a key with two strands, both pulsing (TestSeatRule's DAG).
func TestModelScopeC(t *testing.T) {
	sc := newScope(t, "c", time.Second)
	TestSeatRule(t)
	sc.done("conflict without the seat rule, none with it")
}

// TestShardedCoversEveryJob: the shards of -modelshard check every job exactly once, and a run resumed with -modelfrom
// checks the shard's jobs from that index on.
func TestShardedCoversEveryJob(t *testing.T) {
	defer func(s string, f int64, o string) { *modelShard, *modelFrom, *modelOnly = s, f, o }(*modelShard, *modelFrom, *modelOnly)
	gen := func(keep func(int64) bool, yield func(int64, int64) bool) int64 {
		for i := range int64(100) {
			if keep(i) && !yield(i, i) {
				return i
			}
		}
		return 100
	}
	seen := map[int64]int{}
	check := func(j int64) (int, error) { seen[j]++; return 2, nil }
	*modelOnly = "test"
	for k := range 3 {
		*modelShard, *modelFrom = fmt.Sprintf("%d/3", k), 0
		jobs, inner, err := sharded(newScope(t, "test", time.Minute), gen, check)
		if err != nil || jobs != int64(34-min(k, 1)) || inner != 2*jobs {
			t.Fatalf("shard %d: %d jobs, %d inner, %v", k, jobs, inner, err)
		}
	}
	for i := range int64(100) {
		if seen[i] != 1 {
			t.Fatalf("job %d checked %d times", i, seen[i])
		}
	}
	*modelShard, *modelFrom = "1/3", 50
	if jobs, _, err := sharded(newScope(t, "test", time.Minute), gen, check); err != nil || jobs != 16 || seen[49] != 1 || seen[52] != 2 {
		t.Fatalf("resumed from 50: %d jobs, %v", jobs, err)
	}
}
