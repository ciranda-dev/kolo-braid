package kolobraid

import (
	"bytes"
	"crypto/ed25519"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/ciranda-dev/kolo-braid/internal/dag"
	"github.com/ciranda-dev/kolo-braid/internal/settle"
)

// TestStalledSpamIsBounded: while settlement waits for a seat that is gone, a member that keeps writing makes a peer fetch at
// most MaxPullAhead entries of its strand past what is settled, and placed entries keep no data body in memory.
func TestStalledSpamIsBounded(t *testing.T) {
	nt, ps := group(t, 3, GroupConfig{}, appApply, false)
	setSettlers(t, nt, ps, ps[:2])
	v, spam := ps[0], ps[2]
	nt.partition([]int{1}, []int{0, 2})
	nt.partition([]int{0}, []int{2})
	for i := range 300 {
		mustAppend(t, spam.b, fmt.Sprintf("s%d=%d", i%10, i))
	}
	ss := spam.b.meta.OwnStrand
	st, ok := v.b.syncState()
	if !ok {
		t.Fatal("no sync state")
	}
	v.b.mu.Lock()
	floor, limit := v.b.strands[ss].floor, uint64(v.b.lim.MaxPullAhead)
	v.b.mu.Unlock()
	if need := st.strands[ss].need; need > floor+limit {
		t.Fatalf("a spamming member's strand is fetched to %d with %d settled and a bound of %d", need, floor, limit)
	}
	// placed while settlement waits, the entries keep no data body in memory
	nt.heal()
	nt.partition([]int{1}, []int{0, 2})
	nt.round(nil, 0)
	v.b.mu.Lock()
	held, bodies := v.b.strands[ss].length(), 0
	for _, n := range v.b.nodes {
		if n.kind == KindData {
			bodies += len(n.body)
		}
	}
	settled := v.b.settledNext
	v.b.mu.Unlock()
	if held < 300 || bodies != 0 {
		t.Fatalf("%d entries placed, %d bytes of bodies in memory", held, bodies)
	}
	if got := v.b.Status(); got.Tail == 0 || got.Settled > settled {
		t.Fatalf("status while stalled: %+v", got)
	}
	nt.heal()
	nt.settle(nil, 30*time.Second, func() bool { return quiet(ps...) && settledGet(t, v.b, "s9") == "299" })
	checkSame(t, ps)
}

// TestNetSeatBurstOfManySeats: with four seats, a seat writes more than MaxPullAhead entries while a member is away; the
// member fetches the seats' strands whole, since the pulses that settle the burst lie past any bound, and settles everything
// once back.
func TestNetSeatBurstOfManySeats(t *testing.T) {
	m := newMesh(t)
	ps := grow(t, m, 4)
	fourSeats(t, ps)
	w := ps[3]
	m.isolate(w)
	// entries large enough that one sync message holds fewer than MaxPullAhead of them, so the bound holds per message
	for range 150 {
		appendBig(t, ps[0])
	}
	waitNet(t, 60*time.Second, ps[:3], func() bool { return synced(ps[:3]...) })
	m.full(ps...)
	waitNet(t, 60*time.Second, ps, func() bool { return synced(ps...) && counter(t, w) == counter(t, ps[0]) })
	checkSame(t, ps)
}

// TestNetMemberBurstAfterAbsence: a member that is not a settler writes more than MaxPullAhead entries while another member is
// away; the settlers' pulses link the end of the burst, so the member that comes back fetches past its bound and settles it.
func TestNetMemberBurstAfterAbsence(t *testing.T) {
	m := newMesh(t)
	ps := grow(t, m, 5)
	fourSeats(t, ps)
	var x, w *peer
	for _, p := range ps {
		if _, _, seat := p.b.mySeat(); !seat && x == nil {
			x = p
		} else if w == nil && p != ps[0] {
			w = p
		}
	}
	if x == nil {
		t.Fatal("no member without a seat")
	}
	m.isolate(w)
	for range 150 {
		appendBig(t, x)
	}
	var rest []*peer
	for _, p := range ps {
		if p != w {
			rest = append(rest, p)
		}
	}
	waitNet(t, 60*time.Second, rest, func() bool { return synced(rest...) })
	m.full(ps...)
	waitNet(t, 60*time.Second, ps, func() bool { return synced(ps...) && counter(t, w) == counter(t, ps[0]) })
	checkSame(t, ps)
}

// appendBig appends a 64 KiB data entry: about 60 of them fill one sync message, under the test MaxPullAhead of 64.
func appendBig(t testing.TB, p *peer) {
	t.Helper()
	if _, err := p.b.Append(ctx10(t), append([]byte("big="), bytes.Repeat([]byte{'x'}, 64<<10)...)); err != nil {
		t.Fatal(err)
	}
}

// TestNetJoinWhileFounderAway: four seats, the founder offline, a seat writes more than MaxPullAhead large entries, and a new
// member joins through another seat: what its inviter's AddMember entry links is fetched past the bound, so it places its
// welcome anchor and settles.
func TestNetJoinWhileFounderAway(t *testing.T) {
	m := newMesh(t)
	ps := grow(t, m, 4)
	fourSeats(t, ps)
	m.isolate(ps[0])
	for range 200 {
		appendBig(t, ps[2])
	}
	waitNet(t, 60*time.Second, ps[1:], func() bool { return synced(ps[1:]...) })
	j := m.join(ps[1], "j", nil)
	on := []*peer{ps[1], ps[2], ps[3], j}
	m.full(on...)
	waitNet(t, 60*time.Second, on, func() bool {
		return !waiting(j) && j.b.Status().Settled >= ps[1].b.Status().Settled && hasMember(j.b, keyOf(j), true)
	})
}

// TestNetBurstWhenEverySeatCounts: two seats under Majority (q = n), pulses paced 2 s apart: a seat that writes more than
// MaxPullAhead entries pulses every pulseSpan of them, so the other seat, which needs its pulses, fetches past the burst.
func TestNetBurstWhenEverySeatCounts(t *testing.T) {
	m := newMesh(t)
	ps := grow(t, m, 3)
	var keys []ed25519.PublicKey
	for _, p := range ps[:2] {
		keys = append(keys, keyOf(p))
	}
	if _, err := ps[0].b.SetSettlers(ctx10(t), keys, Majority); err != nil {
		t.Fatal(err)
	}
	waitNet(t, 60*time.Second, ps, func() bool {
		if !synced(ps...) {
			return false
		}
		mustAppend(t, ps[0].b, "grow=1")
		for _, p := range ps {
			if p.b.Status().Seats != 2 {
				return false
			}
		}
		return true
	})
	for _, p := range ps {
		p.b.mu.Lock()
		p.b.lim.PulseGap = 2 * time.Second
		p.b.mu.Unlock()
	}
	for range 200 {
		appendBig(t, ps[1])
	}
	waitNet(t, 60*time.Second, ps, func() bool { return synced(ps...) && counter(t, ps[2]) == counter(t, ps[1]) })
	checkSame(t, ps)
}

func trailingRun(t testing.TB, p *peer) int {
	t.Helper()
	p.b.mu.Lock()
	defer p.b.mu.Unlock()
	n := 0
	for i := p.b.own.Len(); i > 0; i-- {
		nd, err := p.b.readEntry(p.b.meta.OwnStrand, i-1, entryIDOf(mustRecord(t, p, i-1)))
		if err != nil || nd.kind == KindPulse {
			break
		}
		n++
	}
	return n
}

// longestRun returns the most own entries in a row without a pulse.
func longestRun(t testing.TB, p *peer) int {
	t.Helper()
	p.b.mu.Lock()
	defer p.b.mu.Unlock()
	run, most := 0, 0
	for i := uint64(0); i < p.b.own.Len(); i++ {
		nd, err := p.b.readEntry(p.b.meta.OwnStrand, i, entryIDOf(mustRecord(t, p, i)))
		if err != nil {
			t.Fatal(err)
		}
		if nd.kind == KindPulse {
			run = 0
			continue
		}
		run++
		most = max(most, run)
	}
	return most
}

// TestSpanPulseCountsAcrossRestart: a settler that cannot raise its round still pulses after pulseSpan of its own entries,
// counting the ones it wrote before a restart.
func TestSpanPulseCountsAcrossRestart(t *testing.T) {
	nt, ps := group(t, 3, GroupConfig{}, appApply, false)
	setSettlers(t, nt, ps, ps[:2])
	f := ps[0]
	nt.partition([]int{1}, []int{0, 2})
	for i := range 20 {
		mustAppend(t, f.b, fmt.Sprintf("a%d=1", i))
	}
	// reopened and not confirmed (the other seat is cut off), it writes nothing; the count of own entries since its last
	// pulse is restored from its strand
	f.reopen()
	f.b.mu.Lock()
	got := f.b.sincePulse
	f.b.mu.Unlock()
	if trailing := trailingRun(t, f); got != min(trailing, pulseSpan) {
		t.Fatalf("own entries since the last pulse after a restart: %d, the strand ends with %d", got, trailing)
	}
	// the test writes unconfirmed from here
	f.opts.AppendUnconfirmed = true
	f.reopen()
	for i := range 40 {
		mustAppend(t, f.b, fmt.Sprintf("b%d=1", i))
	}
	if n := longestRun(t, f); n > pulseSpan {
		t.Fatalf("%d own entries in a row without a pulse, at most %d", n, pulseSpan)
	}
}

// TestNetPendingSeatBurst: a settler added by SetSettlers writes its readiness pulse and then more than MaxPullAhead large
// entries while it is a seat of the pending config only; it pulses every pulseSpan of them, so once the config with it
// activates and q = n needs its pulses, the other seats reach them.
func TestNetPendingSeatBurst(t *testing.T) {
	m := newMesh(t)
	ps := grow(t, m, 3)
	set := func(a, c *peer) {
		if _, err := ps[0].b.SetSettlers(ctx10(t), []ed25519.PublicKey{keyOf(a), keyOf(c)}, Majority); err != nil {
			t.Fatal(err)
		}
	}
	set(ps[0], ps[1])
	waitNet(t, 60*time.Second, ps, func() bool {
		if !synced(ps...) {
			return false
		}
		mustAppend(t, ps[0].b, "grow=1")
		for _, p := range ps {
			if p.b.Status().Seats != 2 {
				return false
			}
		}
		return true
	})
	cfg := ps[0].b.Status().Config
	set(ps[0], ps[2])
	mustAppend(t, ps[0].b, "grow=1")
	w := ps[2]
	waitUntil(t, 60*time.Second, func() bool {
		w.b.mu.Lock()
		defer w.b.mu.Unlock()
		return w.b.ps.hasReady
	})
	for range 300 {
		appendBig(t, w)
	}
	waitNet(t, 90*time.Second, ps, func() bool {
		return synced(ps...) && counter(t, ps[0]) == counter(t, w) && ps[0].b.Status().Config > cfg
	})
	checkSame(t, ps)
}

// TestSpanPulseIsNoVote: in a round whose vote waits for a gone pivot seat (rule 4), a span pulse is written at once and is a
// readiness pulse, so it is no vote of that round and the seat's vote state is unchanged.
func TestSpanPulseIsNoVote(t *testing.T) {
	nt, ps := group(t, 3, GroupConfig{}, appApply, true)
	for _, p := range ps {
		p.b.mu.Lock()
		p.b.lim.PivotWait = time.Hour
		p.b.mu.Unlock()
	}
	nt.partition([]int{2}, []int{0, 1})
	a := ps[0]
	isolateAtGonePivot(t, nt, a, ps[2])
	a.b.mu.Lock()
	n, before := a.b.own.Len(), a.b.ps
	a.b.spanPulse()
	after := a.b.ps
	a.b.mu.Unlock()
	if ownLen(a) != n+1 {
		t.Fatal("no span pulse while the vote waits for the pivot")
	}
	nd := readOwn(t, a, n)
	body, ok := nd.dec.(pulseBody)
	if nd.kind != KindPulse || !ok || body.Readiness == nil || body.Claim != nil {
		t.Fatalf("the span pulse is a vote: %+v", nd.dec)
	}
	if before.has != after.has || before.round != after.round || before.cfg != after.cfg {
		t.Fatal("a span pulse changed the own vote state")
	}
}

func readOwn(t testing.TB, p *peer, i uint64) *node {
	t.Helper()
	p.b.mu.Lock()
	defer p.b.mu.Unlock()
	nd, err := p.b.readEntry(p.b.meta.OwnStrand, i, entryIDOf(mustRecord(t, p, i)))
	if err != nil {
		t.Fatal(err)
	}
	return nd
}

// isolateAtGonePivot drives a until its next counting pulse is of an odd round whose pivot seat, gone, has no vertex: the state
// in which rule 4 makes a vote wait.
func isolateAtGonePivot(t *testing.T, nt *net, a, gone *peer) {
	t.Helper()
	step := 0
	waitUntil(t, 60*time.Second, func() bool {
		step++
		a.b.mu.Lock()
		since := a.b.sincePulse
		a.b.mu.Unlock()
		if since < pulseSpan-2 {
			mustAppend(t, a.b, fmt.Sprintf("w%d=1", step))
		}
		nt.round(nil, 0)
		a.b.mu.Lock()
		defer a.b.mu.Unlock()
		rr := a.b.st.Round(a.b.pastOf(a.b.links()))
		pivot, _ := a.b.st.PivotSeat(rr - 1)
		cs, _ := a.b.st.SeatOf(key32(keyOf(gone)), dag.StrandID(gone.b.meta.OwnStrand))
		_, held := a.b.st.Vertex(pivot, rr-1)
		return rr%2 == 1 && pivot == cs && !held
	})
}

// TestGonePivotHoldsNoWrites: a seat whose round waits for a gone pivot seat's vertex still writes at full speed, offline and
// connected: span pulses are no votes, so rule 4 holds back no writing call; it still pulses every pulseSpan entries.
func TestGonePivotHoldsNoWrites(t *testing.T) {
	nt, ps := group(t, 3, GroupConfig{}, appApply, true)
	for _, p := range ps {
		p.b.mu.Lock()
		p.b.lim.PivotWait = 3 * time.Second
		p.b.mu.Unlock()
	}
	nt.partition([]int{2}, []int{0, 1})
	a, gone := ps[0], ps[2]
	isolateAtGonePivot(t, nt, a, gone)
	start := time.Now()
	for i := range 5 * pulseSpan {
		mustAppend(t, a.b, fmt.Sprintf("local%d=1", i))
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("%d local appends of a seat waiting for a gone pivot took %v", 5*pulseSpan, d)
	}
	if n := longestRun(t, a); n > pulseSpan {
		t.Fatalf("%d own entries in a row without a pulse", n)
	}
	start = time.Now()
	for i := range 20 * pulseSpan {
		mustAppend(t, a.b, fmt.Sprintf("c%d=1", i))
		if i%8 == 0 {
			nt.round(nil, 0)
		}
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("%d connected appends with a gone seat took %v", 20*pulseSpan, d)
	}
}

// TestSpanPulseAfterCrash: a seat whose 32nd own entry was stored without its span pulse (a crash between the two appends)
// writes that pulse before its next entry, so no more than pulseSpan own entries ever follow one another without a pulse.
func TestSpanPulseAfterCrash(t *testing.T) {
	f := newFounder(t, "f", GroupConfig{}, appApply)
	waitUntil(t, 10*time.Second, func() bool { return quiet(f) })
	f.b.mu.Lock()
	f.b.sincePulse = -1 << 30
	for i := range pulseSpan {
		if _, err := f.b.appendLocked(KindData, fmt.Appendf(nil, "x%d=1", i)); err != nil {
			f.b.mu.Unlock()
			t.Fatal(err)
		}
	}
	f.b.countSincePulse() // what Open does
	_, err := f.b.appendLocked(KindData, []byte("next=1"))
	f.b.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if n := longestRun(t, f); n > pulseSpan {
		t.Fatalf("%d own entries in a row without a pulse after a restart", n)
	}
}

// TestReadinessSpamIsBounded: readiness pulses of a plain member that name no pending config are dropped by settlement once
// they settle, so the settlement state saved with every settled batch stays flat while the member writes 40 per round. The
// newest 40, which no entry links yet, are still kept.
func TestReadinessSpamIsBounded(t *testing.T) {
	nt, ps := group(t, 3, GroupConfig{}, appApply, false)
	f, m := ps[0], ps[2]
	saveLen := func(p *peer) int {
		p.b.mu.Lock()
		defer p.b.mu.Unlock()
		return len(p.b.st.Save())
	}
	base := map[*peer]int{}
	for round := range 10 {
		m.b.mu.Lock()
		cfg := m.b.st.Config()
		for i := range 40 {
			var cid configID
			copy(cid[:], fmt.Sprintf("junk-%d-%d", round, i))
			body, err := encodeBody(pulseBody{Config: cfg.Number, Activating: EntryID(cfg.Activating), Readiness: &cid}, m.b.bounds)
			if err == nil {
				_, err = m.b.appendLocked(KindPulse, body)
			}
			if err != nil {
				m.b.mu.Unlock()
				t.Fatal(err)
			}
		}
		m.b.mu.Unlock()
		k := fmt.Sprintf("r%d", round)
		mustAppend(t, f.b, k+"=1")
		nt.settle(nil, 20*time.Second, func() bool { return quiet(ps...) && settledGet(t, f.b, k) == "1" })
		for _, p := range ps {
			if round == 0 {
				base[p] = saveLen(p)
			}
		}
	}
	for _, p := range ps {
		if got := saveLen(p); got > base[p]+512 {
			t.Fatalf("%s: settlement state grew from %d to %d bytes over 360 more readiness pulses of a member", p.name, base[p], got)
		}
	}
}

// TestMarksStoredPerBatch: the braid's released state does not grow with the rounds of a config; each settled batch writes
// only its new round marks, and a reopened braid holds every mark it had.
func TestMarksStoredPerBatch(t *testing.T) {
	f := newFounder(t, "f", GroupConfig{}, appApply)
	released := func() int {
		v, err := getOpt(f.b.db, []byte(keyReleased))
		if err != nil {
			t.Fatal(err)
		}
		return len(v)
	}
	stored := func() int {
		n := 0
		if err := scanPrefix(f.b.db, []byte(keyMark), func(_, _ []byte) error { n++; return nil }); err != nil {
			t.Fatal(err)
		}
		return n
	}
	write := func(from, to int) {
		for i := from; i < to; i++ {
			mustAppend(t, f.b, fmt.Sprintf("k%d=1", i))
			waitUntil(t, 10*time.Second, func() bool { return quiet(f) })
		}
	}
	held := func() []settle.Mark {
		f.b.mu.Lock()
		defer f.b.mu.Unlock()
		return f.b.st.Marks()
	}
	write(0, 50)
	small := released()
	write(50, 350)
	marks := held()
	if big := released(); big > small+128 || len(marks) < 100 || stored() != len(marks) {
		t.Fatalf("released state %d then %d bytes; %d marks held, %d stored", small, big, len(marks), stored())
	}
	f.reopen()
	if again := held(); !slices.Equal(again, marks) {
		t.Fatalf("%d marks after reopening, %d before", len(again), len(marks))
	}
	// a config change (a rule change adds no seat, so it activates at once) drops the stored marks of the old one
	if _, err := f.b.SetSettlers(ctx10(t), []ed25519.PublicKey{keyOf(f)}, TwoThirds); err != nil {
		t.Fatal(err)
	}
	write(350, 450)
	if c, n := f.b.Status().Config, len(held()); c != 1 || n == 0 || stored() != n {
		t.Fatalf("config %d: %d marks held, %d stored", c, n, stored())
	}
}
