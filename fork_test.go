package kolobraid

import (
	"crypto/ed25519"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"
)

// heldFrom returns the entries of a peer's own strand from index from on, as the sync layer would deliver a second history.
func heldFrom(t testing.TB, p *peer, from uint64) []heldEntry {
	t.Helper()
	l := p.b.own
	var out []heldEntry
	for i := from; i < l.Len(); i++ {
		rec, err := l.Record(i)
		if err != nil {
			t.Fatal(err)
		}
		v, err := l.Get(i)
		if err != nil {
			t.Fatal(err)
		}
		pr, err := l.Record(i - 1)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, heldEntry{Strand: l.ID(), Record: rec, Value: v, Prev: entryIDOf(pr.Encode())})
	}
	return out
}

// fork makes peer w sign two histories after its current length: history one is shown to the peers in one, history two to those
// in two. It returns both histories as second-history entries.
func fork(t testing.TB, w *peer, one, two []*peer) ([]heldEntry, []heldEntry) {
	t.Helper()
	base := w.b.own.Len()
	backup := copyBackend(t, w.db)
	mustAppend(t, w.b, "h1=1")
	for _, p := range one {
		pull(t, w, p, 0)
	}
	h1 := heldFrom(t, w, base)
	w.close()
	clone := &peer{t: t, name: w.name + "-clone", id: w.id, db: backup}
	clone.opts = testOptions(backup, appApply)
	clone.opts.AppendUnconfirmed = true
	clone.reopen()
	mustAppend(t, clone.b, "h2=2")
	for _, p := range two {
		pull(t, clone, p, 0)
	}
	h2 := heldFrom(t, clone, base)
	clone.close()
	return h1, h2
}

func deliverAll(t testing.TB, peers []*peer, hs ...[]heldEntry) {
	t.Helper()
	for _, p := range peers {
		for _, h := range hs {
			for _, e := range h {
				if err := p.b.deliver(e); err != nil {
					t.Fatalf("%s: deliver: %v", p.name, err)
				}
			}
		}
	}
}

// TestWriterForkKeepsSettling: a writer that is not a settler signs two histories and shows one to each half of four settlers;
// once both histories are placeable everywhere, the writer is fenced by evidence and settlement goes on with one order.
func TestWriterForkKeepsSettling(t *testing.T) {
	nt, ps := group(t, 5, GroupConfig{}, appApply, false)
	seats := ps[:4]
	setSettlers(t, nt, ps, seats)
	w := ps[4]
	mustAppend(t, w.b, "base=1")
	nt.settle(nil, 20*time.Second, func() bool { return quiet(ps...) && settledGet(t, ps[0].b, "base") == "1" })
	h1, h2 := fork(t, w, seats[:2], seats[2:])
	rest := newNet(t, seats...)
	for _, p := range seats {
		mustAppend(t, p.b, "during="+p.name)
	}
	rest.round(nil, 0)
	deliverAll(t, seats, h1, h2)
	rest.settle(nil, 30*time.Second, func() bool {
		if !quiet(seats...) {
			return false
		}
		for _, p := range seats {
			if hasMember(p.b, keyOf(w), true) {
				return false
			}
		}
		return true
	})
	ref := mustAppend(t, seats[0].b, "later=1")
	b0 := seats[0].b
	b0.mu.Lock()
	n, err := b0.readEntry(ref.Strand, ref.Index, ref.ID)
	b0.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	found := true
	for _, l := range n.links {
		if l.Strand == h1[0].Strand {
			t.Fatal("an entry links a strand this peer holds evidence against")
		}
	}
	if !found {
		t.Fatal("own entry not in the order")
	}
	rest.settle(nil, 20*time.Second, func() bool { return quiet(seats...) && settledGet(t, seats[3].b, "later") == "1" })
	checkSame(t, seats)
	for _, p := range seats {
		if p.b.Status().SettlementHalted != nil {
			t.Fatalf("%s: a fork of a writer that is not a seat halted settlement", p.name)
		}
	}
}

func setSettlers(t testing.TB, nt *net, all, seats []*peer) {
	t.Helper()
	var keys []ed25519.PublicKey
	for _, p := range seats {
		keys = append(keys, keyOf(p))
	}
	c0 := all[0].b.Status().Config
	if _, err := all[0].b.SetSettlers(ctx10(t), keys, Majority); err != nil {
		t.Fatal(err)
	}
	nt.settle(nil, 30*time.Second, func() bool {
		s := all[0].b.Status()
		// a config whose added seats are ready activates without another entry
		return quiet(all...) && ((s.PendingConfig != nil && len(s.AwaitingReadiness) == 0) || s.Config > c0)
	})
	mustAppend(t, all[0].b, "settlers=set")
	nt.settle(nil, 30*time.Second, func() bool {
		for _, p := range all {
			if p.b.Status().Seats != len(seats) {
				return false
			}
		}
		return quiet(all...)
	})
}

// TestSeatForkHalts: with TwoThirds and four seats, a seat that signs two histories halts settlement on every peer that holds
// both, and no two peers settle conflicting prefixes.
func TestSeatForkHalts(t *testing.T) {
	nt, ps := group(t, 4, GroupConfig{Quorum: TwoThirds}, appApply, true)
	w := ps[3]
	ws := w.b.meta.OwnStrand
	honest := ps[:3]
	h1, h2 := fork(t, w, honest[:1], honest[1:])
	rest := newNet(t, honest...)
	for _, p := range honest {
		mustAppend(t, p.b, "during="+p.name)
	}
	rest.round(nil, 0)
	deliverAll(t, honest, h1, h2)
	rest.settle(nil, 20*time.Second, func() bool {
		for _, p := range honest {
			if p.b.Status().SettlementHalted == nil {
				return false
			}
		}
		return true
	})
	for _, p := range honest {
		ev := waitEvent(t, p.b, EventSettlementHalted, 5*time.Second)
		if string(ev.Member) != string(keyOf(w)) {
			t.Fatalf("%s: halt names another key", p.name)
		}
	}
	var lists [][]EntryID
	for _, p := range honest {
		lists = append(lists, settledIDs(t, p.b))
	}
	for i := range lists {
		for j := range lists {
			a, b := lists[i], lists[j]
			if len(a) > len(b) {
				a, b = b, a
			}
			if !slices.Equal(a, b[:len(a)]) {
				t.Fatal("two peers settled conflicting prefixes")
			}
		}
	}
	// tentative application and new entries go on
	mustAppend(t, honest[0].b, "still=1")
	rest.round(nil, 0)
	waitUntil(t, 5*time.Second, func() bool {
		s := honest[1].b.View()
		defer s.Close()
		return get(t, s, "still") == "1"
	})
	_ = nt
	if err := honest[0].b.addEvidence(evidenceBody{}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("empty evidence: %v", err)
	}
	// after the halt a seat's strand is still fetched MaxPullAhead past its newest pulse, so a closure certificate can arrive,
	// and no further
	p := honest[0]
	st, ok := p.b.syncState()
	if !ok {
		t.Fatal("no sync state")
	}
	p.b.mu.Lock()
	bound := max(p.b.strands[ws].floor, p.b.newestPulse[ws]+1) + uint64(p.b.lim.MaxPullAhead)
	_, hasPulse := p.b.newestPulse[ws]
	p.b.mu.Unlock()
	if need := st.strands[ws].need; !hasPulse || need != max(bound, st.strands[ws].held) {
		t.Fatalf("a seat's strand is fetched to %d after the halt, window %d", need, bound)
	}
	// a halted seat writes no vote, but still a pulse after pulseSpan own entries, which keeps its strand reachable
	for i := range 2 * pulseSpan {
		mustAppend(t, p.b, fmt.Sprintf("halted%d=1", i))
	}
	if n := longestRun(t, p); n > pulseSpan {
		t.Fatalf("a halted seat wrote %d entries in a row without a pulse", n)
	}
}
