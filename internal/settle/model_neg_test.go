package settle

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"
)

// selfConsistent checks production alone under dev: the list of every ideal must be a prefix of the full DAG's list.
func selfConsistent(l layered, rule Rule, dev rules, ideals func(yield func([]int))) (int, error) {
	w, ents := l.world()
	cfg := cfg0(w, rule, seq(l.n)...)
	fp, err := newPeerE(w, cfg, nil, dev)
	if err != nil {
		return 0, err
	}
	fp.feed(w.ents...)
	full := fp.ids()
	n := 0
	ideals(func(k []int) {
		if err != nil {
			return
		}
		var tips []*ent
		for s, v := range k {
			if v > 0 {
				tips = append(tips, ents[s][v-1])
			}
		}
		p, e := newPeerE(w, cfg, nil, dev)
		if e != nil {
			err = e
			return
		}
		p.feed(w.in(closure(tips...))...)
		n++
		if !isPrefix(p.ids(), full) {
			err = fmt.Errorf("ideal %v: %v against the full DAG's %v", k, p.rounds(), fp.rounds())
		}
	})
	return n, err
}

// TestModelNegativeControls: a deliberately wrong rule must be caught by the model checks.
func TestModelNegativeControls(t *testing.T) {
	sc := newScope(t, "negative controls", time.Minute)
	// pivots on every round: scope a's enumeration finds conflicting ideals
	found := ""
	for _, z := range []struct{ n, R int }{{3, 3}, {3, 4}} {
		q := Quorum(Majority, z.n)
		_, ideals, err := parallel(sc, func(yield func(layered) bool) {
			enumerate(z.n, q, z.R, 1, func(l layered) bool { return yield(l) })
		}, func(l layered) (int, error) {
			return selfConsistent(l, Majority, rules{everyRound: true}, l.ideals)
		})
		if err != nil && !errors.Is(err, errIncomplete) {
			found = fmt.Sprintf("n=%d rounds 0..%d after %d ideals: %v", z.n, z.R, ideals, err)
			break
		}
	}
	if found == "" {
		// sampled DAGs of scope a' as a second chance
		r := rand.New(rand.NewPCG(1, 2))
		for i := 0; i < 20000 && found == ""; i++ {
			l := randomLayered(r, 4, 3, 6)
			if _, err := selfConsistent(l, Majority, rules{everyRound: true}, l.ideals); err != nil {
				found = fmt.Sprintf("random n=4 DAG %d: %v", i, err)
			}
		}
	}
	if found == "" {
		t.Fatal("pivots on every round were not caught")
	}
	t.Logf("pivots on every round caught: %s", found)
	// the other wrong rules are caught where their scope runs: counting by key (scope c, TestSeatRule), f equivocating
	// seats with 2q - n = f (scope b: n = 3 and n = 5 Majority conflict), and settling a forked config with the walk taking
	// a seat's first vertex in the whole DAG (scope b', [0 2 6 8 10])
	sc.done("every-round pivots caught")
}
