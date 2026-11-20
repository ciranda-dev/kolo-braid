package settle

import (
	"bytes"
	"errors"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/ciranda-dev/kolo-braid/internal/dag"
)

// restart saves p's settler, builds a new DAG with the entries placed so far and loads the state into it; every pulse is
// added again, as a restart that re-reads its tail would.
func restart(t *testing.T, p *peer, placed []*ent) *peer {
	t.Helper()
	b := p.s.Save()
	d, err := dag.New(dag.Config{MaxLinks: 63, MaxPendingBytes: 1 << 30})
	if err != nil {
		t.Fatal(err)
	}
	q := &peer{t: t, w: p.w, d: d}
	q.r = newRoster(p.w, d, p.r.base)
	q.r.committed = slices.Clone(p.r.committed)
	q.pivots = slices.Clone(p.pivots)
	q.s, err = Load(d, q.r.fn, b)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if err := q.s.RestoreMarks(p.s.Marks()); err != nil {
		t.Fatalf("restore marks: %v", err)
	}
	if !bytes.Equal(q.s.Save(), b) {
		t.Fatal("Save after Load differs")
	}
	q.place(placed...)
	q.advance()
	if len(q.pivots) != len(p.pivots) {
		t.Fatalf("re-adding the placed pulses committed %d more pivots", len(q.pivots)-len(p.pivots))
	}
	return q
}

func TestSaveLoad(t *testing.T) {
	worlds := map[string]func() (*world, Config){
		"lockstep": func() (*world, Config) {
			w := newWorld()
			w.lockstep(seq(4), map[int]*ent{}, 16, 0, nil)
			return w, cfg0(w, Majority, seq(4)...)
		},
		"grow":   func() (*world, Config) { return dCase{true, 1, 1, 0, 4, -1, -1, dNone}.build(t) },
		"shrink": func() (*world, Config) { return dCase{false, 2, 2, -1, -1, -1, -1, dNone}.build(t) },
		"closure": func() (*world, Config) {
			w, _, _, _ := closureWorld(scenario{4, Majority}, 2)
			return w, cfg0(w, Majority, seq(4)...)
		},
	}
	for name, mk := range worlds {
		w, cfg := mk()
		ref := newPeer(t, w, cfg, nil, rules{})
		ref.feed(w.ents...)
		if len(ref.pivots) == 0 {
			t.Fatalf("%s: nothing committed", name)
		}
		for cut := 0; cut <= len(w.ents); cut += 3 {
			p := newPeer(t, w, cfg, nil, rules{})
			p.feed(w.ents[:cut]...)
			q := restart(t, p, w.ents[:cut])
			q.feed(w.ents[cut:]...)
			if !slices.Equal(q.ids(), ref.ids()) {
				t.Fatalf("%s cut %d: %v after a restart, %v without", name, cut, q.rounds(), ref.rounds())
			}
		}
	}
}

func TestSaveLoadKeepsEverything(t *testing.T) {
	// anchor, evidence, pending, readiness, future pulses and a halt survive a restart
	w, _, fake, real10 := closureWorld(scenario{3, Majority}, 1)
	cfg := cfg0(w, Majority, seq(3)...)
	p := newPeer(t, w, cfg, &Anchor{ID: real10.id}, rules{})
	p.s.Equivocated(strandID(40))
	p.feed(w.ents[:30]...)
	q := restart(t, p, w.ents[:30])
	if q.s.anchor == nil || !q.s.evid[strandID(40)] || q.s.certFloor != 1 {
		t.Fatal("anchor or evidence lost")
	}
	q.feed(w.ents[30:]...)
	if !slices.Equal(roundsOf(q, 0), oldList) {
		t.Fatalf("anchored joiner after a restart: %v", roundsOf(q, 0))
	}
	_ = fake
	// a stopped settler stays stopped
	b := newPeer(t, w, cfg, &Anchor{ID: w.byStrand[0][1].id}, rules{})
	b.feed(w.ents...)
	if b.s.Halted() == nil {
		t.Fatal("bad anchor did not stop")
	}
	r := restart(t, b, w.ents)
	if !errors.Is(r.s.Halted(), ErrHalted) {
		t.Fatal("halt lost in a restart")
	}
}

func TestLoadRejects(t *testing.T) {
	w := newWorld()
	w.lockstep(seq(3), map[int]*ent{}, 6, 0, nil)
	p := newPeer(t, w, cfg0(w, Majority, seq(3)...), nil, rules{})
	p.feed(w.ents...)
	good := p.s.Save()
	d, _ := dag.New(dag.Config{MaxLinks: 63, MaxPendingBytes: 1 << 20})
	if _, err := Load(nil, p.r.fn, good); !errors.Is(err, ErrInvalid) {
		t.Fatal("nil dag")
	}
	if _, err := Load(d, nil, good); !errors.Is(err, ErrInvalid) {
		t.Fatal("nil roster")
	}
	for i := range good {
		if _, err := Load(d, p.r.fn, good[:i]); !errors.Is(err, ErrMalformed) {
			t.Fatalf("truncated at %d: %v", i, err)
		}
	}
	if _, err := Load(d, p.r.fn, append(slices.Clone(good), 0)); !errors.Is(err, ErrMalformed) {
		t.Fatal("trailing byte")
	}
	for i := range good {
		b := slices.Clone(good)
		b[i] ^= 0xff
		s, err := Load(d, p.r.fn, b)
		if err == nil && !bytes.Equal(s.Save(), b) {
			t.Fatalf("flip at %d: loads but saves differently", i)
		}
	}
}

// TestRoundsOfALateSecondHistory: after old rounds are pruned, a seat's second history that forks at an old index still
// gets the rounds the definition gives, through the kept round marks, also across a restart.
func TestRoundsOfALateSecondHistory(t *testing.T) {
	w := newWorld()
	keys := seq(3)
	w.lockstep(keys, map[int]*ent{}, 12, 0, nil)
	honest := len(w.ents)
	at := func(k, i int) *ent { return w.byStrand[k][i] }
	f4 := w.pulse(2, at(2, 3), 0, nil, at(0, 3), at(1, 3))
	f5 := w.pulse(2, f4, 0, nil, at(0, 4))
	g5 := w.pulse(1, at(1, 4), 0, nil, f4)
	f6 := w.pulse(2, f5, 0, nil, g5)
	p := newPeer(t, w, cfg0(w, Majority, keys...), nil, rules{})
	p.feed(w.ents[:honest]...)
	if p.s.seats[0].old[0] == nil {
		t.Fatal("nothing was pruned")
	}
	q := restart(t, p, w.ents[:honest])
	rf := newRef(w, w.all(), Majority, keys, staticRoster(Proposal{Rule: Majority, Seats: p.s.cfg.Seats}))
	for _, pp := range []*peer{p, q} {
		pp.feed(w.ents[honest:]...)
		for _, e := range []*ent{f4, f5, g5, f6} {
			if r := pp.s.byID[e.id]; r == nil || int(r.round) != rf.roundOf(e) {
				t.Fatalf("entry %d: round %v, the definition gives %d", e.n, r, rf.roundOf(e))
			}
		}
	}
}

// TestOldStateTagRefused: a state saved under the tag before the pending config's clock was added refuses to load.
func TestOldStateTagRefused(t *testing.T) {
	w := newWorld()
	w.lockstep(seq(3), map[int]*ent{}, 4, 0, nil)
	p := newPeer(t, w, cfg0(w, Majority, seq(3)...), nil, rules{})
	p.feed(w.ents...)
	old := bytes.Replace(p.s.Save(), []byte(stateTag), []byte("kolo-braid/settle/v0"), 1)
	if _, err := Load(p.d, p.r.fn, old); !errors.Is(err, ErrMalformed) || !strings.Contains(err.Error(), "tag") {
		t.Fatalf("a state with the old tag: %v", err)
	}
}

// TestMarksKeptOutsideTheState: the state Save encodes does not grow with the rounds of a config; the round marks are handed
// out once each (TakeMarks) for the caller to store, and a restart that restores them gives the same marks.
func TestMarksKeptOutsideTheState(t *testing.T) {
	size := map[int]int{}
	for _, steps := range []int{100, 2000} {
		w := newWorld()
		w.lockstep(seq(4), map[int]*ent{}, steps, 0, nil)
		p := newPeer(t, w, cfg0(w, Majority, seq(4)...), nil, rules{})
		var taken []Mark
		for i, e := range w.ents {
			p.feed(e)
			if i%50 == 0 {
				taken = append(taken, p.s.TakeMarks()...)
			}
		}
		taken = append(taken, p.s.TakeMarks()...)
		slices.SortFunc(taken, func(a, b Mark) int {
			if a.Seat != b.Seat {
				return a.Seat - b.Seat
			}
			if a.Seg != b.Seg {
				return a.Seg - b.Seg
			}
			return int(a.Index) - int(b.Index)
		})
		size[steps] = len(p.s.Save())
		all := p.s.Marks()
		if len(all) < steps || !slices.Equal(taken, all) || len(p.s.TakeMarks()) != 0 {
			t.Fatalf("%d rounds: %d marks taken, %d held", steps, len(taken), len(all))
		}
		q := restart(t, p, w.ents)
		if !slices.Equal(q.s.Marks(), all) {
			t.Fatal("the restored marks differ")
		}
	}
	if size[2000] > size[100]+256 {
		t.Fatalf("state grew from %d to %d bytes between 100 and 2000 rounds", size[100], size[2000])
	}
}

// TestStateWithInlineMarksLoads: a state saved while round marks were part of it (testdata, 4 seats after 60 rounds) loads,
// hands its marks out once for the caller to store, and saves without them.
func TestStateWithInlineMarksLoads(t *testing.T) {
	old, err := os.ReadFile("testdata/state-v1-inline-marks.bin")
	if err != nil {
		t.Fatal(err)
	}
	d, err := dag.New(dag.Config{MaxLinks: 63, MaxPendingBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	roster := func(_ uint64, ps []Pivot) ([]Proposal, error) { return make([]Proposal, len(ps)), nil }
	s, err := Load(d, roster, old)
	if err != nil {
		t.Fatal(err)
	}
	marks := s.Marks()
	if taken := s.TakeMarks(); len(marks) == 0 || len(taken) != len(marks) || len(s.TakeMarks()) != 0 {
		t.Fatalf("%d marks held, %d handed out", len(marks), len(taken))
	}
	saved := s.Save()
	if len(saved) >= len(old) {
		t.Fatalf("saved without marks: %d bytes, before %d", len(saved), len(old))
	}
	s2, err := Load(d, roster, saved)
	if err != nil {
		t.Fatal(err)
	}
	if err := s2.RestoreMarks(marks); err != nil || !slices.Equal(s2.Marks(), marks) {
		t.Fatalf("restored: %v", err)
	}
}

// TestRestoreMarksTwice: restoring marks a settler already holds keeps them as they are (a defensive skip: the root never
// restores into a settler that holds marks).
func TestRestoreMarksTwice(t *testing.T) {
	w := newWorld()
	w.lockstep(seq(3), map[int]*ent{}, 30, 0, nil)
	p := newPeer(t, w, cfg0(w, Majority, seq(3)...), nil, rules{})
	p.feed(w.ents...)
	marks := p.s.Marks()
	if len(marks) == 0 {
		t.Fatal("no marks")
	}
	for range 2 {
		if err := p.s.RestoreMarks(marks); err != nil || !slices.Equal(p.s.Marks(), marks) {
			t.Fatalf("restored again: %v", err)
		}
	}
}

// TestMarksOfAnEarlierConfigNotHandedOut: marks made before a config activates and not taken by then are dropped with the
// config (a defensive reset: the root takes the marks after every Advance).
func TestMarksOfAnEarlierConfigNotHandedOut(t *testing.T) {
	w := newWorld()
	heads := w.lockstep(seq(4), map[int]*ent{}, 10, 0, nil)
	set := w.settlersEntry(70, 70, nil, Majority, []int{0, 1, 2})
	w.lockstep(seq(4), heads, 4, 0, nil, set)
	p := newPeer(t, w, cfg0(w, Majority, seq(4)...), nil, rules{})
	had := false
	for _, e := range w.ents {
		p.feed(e)
		had = had || (p.s.Config().Number == 0 && len(p.s.Marks()) > 0)
	}
	if p.s.Config().Number != 1 || !had {
		t.Fatalf("config %d, marks in config 0 %v", p.s.Config().Number, had)
	}
	for _, m := range p.s.TakeMarks() {
		if m.Config != 1 {
			t.Fatalf("a mark of config %d handed out in config 1", m.Config)
		}
	}
}
