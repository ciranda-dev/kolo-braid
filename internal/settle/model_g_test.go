package settle

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/ciranda-dev/kolo-braid/internal/dag"
)

// The liveness model of scope g: seats follow the link and pulse rules 1 to 5 of the record over simulated time, each with
// its own DAG and settler, with random one-way delays; an observer that holds every entry checks that every round whose
// pivot seat is online is directly committed, and that no gap between commits exceeds Theorem 3's bound.

const (
	simTick      = 5 // ms
	simPivotWait = 3000
	simDataGap   = 250
	simMaxDelay  = 150
)

type simMsg struct {
	at int64
	to int
	e  *ent
}

type simSeat struct {
	key, gap  int
	online    bool
	p         *peer
	head      *ent
	lastRound int64
	lastAt    int64
	waitFrom  map[uint64]int64
	newest    map[int][]*ent // placed entries per strand, in placement order
}

type sim struct {
	r      *rand.Rand
	w      *world
	n, q   int
	seats  []*simSeat
	obs    *peer
	queue  []simMsg
	data   *ent
	now    int64
	commit []int64
}

func newSim(t testing.TB, seed uint64, n int, gaps []int, offline int) *sim {
	s := &sim{r: rand.New(rand.NewPCG(seed, 11)), w: newWorld(), n: n, q: Quorum(Majority, n)}
	cfg := cfg0(s.w, Majority, seq(n)...)
	for k := range n {
		st := &simSeat{key: k, gap: gaps[k], online: k != offline, lastRound: -1, lastAt: -1 << 40, waitFrom: map[uint64]int64{},
			newest: map[int][]*ent{}}
		st.p = newPeer(t, s.w, cfg, nil, rules{})
		s.seats = append(s.seats, st)
	}
	s.obs = newPeer(t, s.w, cfg, nil, rules{})
	return s
}

func (s *sim) deliver(st *simSeat, e *ent) {
	placed, err := st.p.d.Add(st.p.w.node(e))
	if err != nil {
		st.p.fail(err)
		return
	}
	for _, pl := range placed {
		x := s.w.byID[pl.ID]
		st.newest[x.strand] = append(st.newest[x.strand], x)
		if x.pulse {
			if err := st.p.s.Add(x.asPulse()); err != nil {
				st.p.fail(err)
			}
		}
	}
	st.p.advance()
}

func (s *sim) publish(from int, e *ent) {
	before := len(s.obs.pivots)
	s.obs.feed(e)
	for range len(s.obs.pivots) - before {
		s.commit = append(s.commit, s.now)
	}
	for i, st := range s.seats {
		if i == from {
			s.deliver(st, e)
			continue
		}
		if st.online {
			s.queue = append(s.queue, simMsg{at: s.now + 10 + int64(s.r.IntN(simMaxDelay-9)), to: i, e: e})
		}
	}
}

func union(cs ...dag.Clock) dag.Clock {
	var u dag.Clock
	for _, c := range cs {
		for i, v := range c {
			for len(u) <= i {
				u = append(u, 0)
			}
			u[i] = max(u[i], v)
		}
	}
	return u
}

// nextPivotRound is the seat's next pivot round above its last pulse round.
func (s *sim) nextPivotRound(st *simSeat) uint64 {
	r := uint64(0)
	if st.lastRound >= 0 {
		r = uint64(st.lastRound) + 1
	}
	for {
		if r%2 == 0 {
			if ps, _ := st.p.s.PivotSeat(r); ps == st.key {
				return r
			}
		}
		r++
	}
}

// links applies rules 1 and 2: per strand the newest placed entry, held back below the seat's next pivot round.
func (s *sim) links(st *simSeat, hold uint64) ([]*ent, dag.Clock) {
	var out []*ent
	var cs []dag.Clock
	if st.head != nil {
		c, _ := st.p.d.Clock(st.head.id)
		cs = append(cs, c)
	}
	strands := make([]int, 0, len(st.newest))
	for k := range st.newest {
		strands = append(strands, k)
	}
	slices.Sort(strands)
	for _, k := range strands {
		if k == st.key {
			continue
		}
		es := st.newest[k]
		for i := len(es) - 1; i >= 0; i-- {
			c, _ := st.p.d.Clock(es[i].id)
			if h, ok := st.p.s.Highest(c); !ok || h < hold {
				out = append(out, es[i])
				cs = append(cs, c)
				break
			}
		}
	}
	return out, union(cs...)
}

func (s *sim) work(st *simSeat) bool {
	if s.data == nil {
		return false
	}
	last, ok := st.p.s.Last()
	if !ok {
		return true
	}
	c, _ := st.p.d.Clock(last.ID)
	return !st.p.d.Contains(c, s.data.id)
}

func (s *sim) step(st *simSeat) {
	hold := s.nextPivotRound(st)
	links, u := s.links(st, hold)
	rho := st.p.s.Round(u)
	pivot := rho == hold
	if !pivot {
		if !s.work(st) || (st.lastRound >= 0 && int64(rho) <= st.lastRound) || s.now < st.lastAt+int64(st.gap) {
			return
		}
		if rho%2 == 1 {
			ps, _ := st.p.s.PivotSeat(rho - 1)
			v, held := st.p.s.Vertex(ps, rho-1)
			if !held {
				if _, ok := st.waitFrom[rho]; !ok {
					st.waitFrom[rho] = s.now
				}
				if s.now-st.waitFrom[rho] < simPivotWait {
					return
				}
			} else if ve := s.w.byID[v]; ve.key != st.key {
				links = append(links, ve)
			}
		}
	}
	e := s.w.pulse(st.key, st.head, 0, nil, links...)
	st.head, st.lastAt = e, s.now
	s.publish(st.key, e)
	if r := st.p.s.byID[e.id]; r != nil {
		st.lastRound = int64(r.round)
	}
}

func (s *sim) run(ms int64) {
	nextData := int64(0)
	for s.now = 0; s.now < ms; s.now += simTick {
		if s.now >= nextData {
			s.data = s.w.add(99, 99, s.data)
			s.publish(-1, s.data)
			nextData = s.now + simDataGap
		}
		slices.SortStableFunc(s.queue, func(a, b simMsg) int { return int(a.at - b.at) })
		k := 0
		for k < len(s.queue) && s.queue[k].at <= s.now {
			s.deliver(s.seats[s.queue[k].to], s.queue[k].e)
			k++
		}
		s.queue = s.queue[k:]
		for _, st := range s.seats {
			if st.online {
				s.step(st)
			}
		}
	}
}

// check reports the online pivot rounds, those directly committed at the observer, and the longest gap between commits.
func (s *sim) check() (online, committed int, gap int64, err error) {
	cfg := s.obs.s.Config()
	rf := newRef(s.w, s.w.all(), Majority, seq(s.n), staticRoster(Proposal{Rule: Majority, Seats: cfg.Seats}))
	for r := 0; r+2 <= rf.maxRound(rf.D); r += 2 {
		ps := rf.pivotSeat(r)
		if !s.seats[ps].online {
			continue
		}
		online++
		if rf.vertex(rf.D, ps, r) == nil {
			return online, committed, 0, fmt.Errorf("online pivot round %d has no vertex", r)
		}
		if rf.direct(r) == nil {
			return online, committed, 0, fmt.Errorf("online pivot round %d not directly committed", r)
		}
		committed++
	}
	for i := 1; i < len(s.commit); i++ {
		gap = max(gap, s.commit[i]-s.commit[i-1])
	}
	for _, st := range append(slices.Clone(s.seats), &simSeat{p: s.obs}) {
		if st.p.err != nil {
			return online, committed, gap, st.p.err
		}
	}
	return online, committed, gap, nil
}

// TestModelScopeG is scope g: liveness under the link and pulse rules with mixed PulseGap, delays of 10 to 150 ms, a slow
// pivot and one seat offline.
func TestModelScopeG(t *testing.T) {
	runs, ms, limit := 1, int64(15_000), time.Minute
	if *fullModel {
		runs, ms, limit = 20, 60_000, 15*time.Minute
	}
	sc := newScope(t, "g", limit)
	gapsFrom := []int{10, 100, 500, 1000}
	total := 0
	for _, n := range []int{3, 4, 5} {
		q := Quorum(Majority, n)
		for _, offline := range []int{-1, n - 1} {
			var worst int64
			on, com := 0, 0
			for run := range runs {
				seed := uint64(n*1000 + run*10 + offline + 1)
				r := rand.New(rand.NewPCG(seed, 3))
				gaps := make([]int, n)
				for k := range gaps {
					gaps[k] = gapsFrom[r.IntN(len(gapsFrom))]
				}
				gaps[0] = 1000 // the slow pivot
				s := newSim(t, seed, n, gaps, offline)
				s.run(ms)
				o, c, gap, err := s.check()
				if err != nil {
					t.Fatalf("scope g n=%d offline %d run %d gaps %v: %v", n, offline, run, gaps, err)
				}
				if c == 0 {
					t.Fatalf("scope g n=%d offline %d run %d: nothing committed", n, offline, run)
				}
				g := 0
				for k, v := range gaps {
					if k != offline {
						g = max(g, v)
					}
				}
				bound := int64(2*(n-q+1)) * int64(g+simPivotWait+simMaxDelay+simTick)
				if gap > bound {
					t.Fatalf("scope g n=%d offline %d run %d: gap %d ms over Theorem 3's bound %d ms", n, offline, run, gap, bound)
				}
				worst = max(worst, gap)
				on += o
				com += c
				total++
			}
			t.Logf("scope g n=%d q=%d offline seat %d: %d runs of %d s, online pivot rounds %d, directly committed %d, longest gap %d ms",
				n, q, offline, runs, ms/1000, on, com, worst)
		}
	}
	sc.done("%d runs", total)
}

// TestModelScopeH is scope h: a non-settler writer signs two histories, settlers 0 and 1 get A first and 2 and 3 get B;
// every peer can place both, and a settler stops linking the writer once its past holds both.
func TestModelScopeH(t *testing.T) {
	runs, limit := 200, time.Minute
	if raceEnabled {
		runs = 20
	}
	if *fullModel {
		runs, limit = 10_000, 15*time.Minute
	}
	sc := newScope(t, "h", limit)
	type res struct{ pivots, ideals int }
	results := make([]res, runs)
	jobs, ideals, err := parallel(sc, func(yield func(int) bool) {
		for i := range runs {
			if !yield(i) {
				return
			}
		}
	}, func(i int) (int, error) {
		pv, n, err := forkedWriter(t, uint64(i))
		results[i] = res{pv, n}
		return n, err
	})
	if err != nil {
		t.Fatalf("scope h after %d runs: %v", jobs, err)
	}
	sum := 0
	for _, r := range results[:jobs] {
		sum += r.pivots
	}
	mean := float64(sum) / float64(jobs)
	if mean < 3 {
		t.Fatalf("scope h: mean committed pivots %.2f per run: the fork stalls settlement", mean)
	}
	sc.done("%d runs of 60 steps, mean committed pivots %.2f, %d ideals consistent", jobs, mean, ideals)
}

func forkedWriter(t testing.TB, seed uint64) (int, int, error) {
	r := rand.New(rand.NewPCG(seed, 8))
	w := newWorld()
	const M = 9
	side := []int{0, 0, 1, 1}
	m0 := w.add(M, M, nil)
	hist := [2][]*ent{{w.add(M, M, m0)}, {w.add(M, M, m0)}}
	var mark [2]bitset
	mark[0].set(hist[0][0].n)
	mark[1].set(hist[1][0].n)
	holds := func(x *ent, h int) bool {
		for i, m := range mark[h] {
			if i < len(x.past) && x.past[i]&m != 0 {
				return true
			}
		}
		return false
	}
	heads := map[int]*ent{}
	for k := range 4 {
		heads[k] = w.pulse(k, nil, 0, nil, hist[side[k]][0])
	}
	for range 60 {
		if r.IntN(4) == 0 {
			h := r.IntN(2)
			e := w.add(M, M, hist[h][len(hist[h])-1])
			hist[h] = append(hist[h], e)
			mark[h].set(e.n)
			continue
		}
		k := r.IntN(4)
		var links []*ent
		for o := range 4 {
			if o != k {
				links = append(links, heads[o])
			}
		}
		x := closure(append(slices.Clone(links), heads[k])...)
		probe := &ent{past: x}
		// a settler whose past holds both histories has evidence and stops linking the writer
		if !holds(probe, 0) || !holds(probe, 1) {
			links = append(links, hist[side[k]][len(hist[side[k]])-1])
		}
		heads[k] = w.pulse(k, heads[k], 0, nil, links...)
	}
	cfg := cfg0(w, Majority, seq(4)...)
	all := w.all()
	full := refIDs(newRef(w, all, Majority, seq(4), staticRoster(Proposal{Rule: Majority, Seats: cfg.Seats})).committed())
	n := 0
	for range 40 {
		var tips []*ent
		for _, es := range w.byStrand {
			if k := r.IntN(len(es) + 1); k > 0 {
				tips = append(tips, es[k-1])
			}
		}
		n++
		if err := checkSet(w, closure(tips...), cfg, full, static(cfg), nil); err != nil {
			return 0, n, err
		}
	}
	n++
	if err := checkSet(w, all, cfg, full, static(cfg), shuffled(w.ents, seed)); err != nil {
		return 0, n, err
	}
	return len(full), n, nil
}
