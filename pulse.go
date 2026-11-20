package kolobraid

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"slices"
	"time"

	kololog "github.com/ciranda-dev/kolo-log"

	"github.com/ciranda-dev/kolo-braid/internal/dag"
	"github.com/ciranda-dev/kolo-braid/internal/roster"
	"github.com/ciranda-dev/kolo-braid/internal/settle"
)

// pulseSpan is the most own entries a seat writes without a pulse: half the least MaxPullAhead, so a peer that fetches a
// seat's strand MaxPullAhead past its newest pulse always reaches the next one.
const pulseSpan = minMaxPullAhead / 2

// spanPulse writes the pulse due after pulseSpan own entries without one, whatever PulseGap, so other peers can fetch past a
// burst of this seat. It is a readiness pulse (no rule holds it back and it counts for nothing): for the pending config from a
// seat that only it adds, else for the current one. The caller holds b.mu.
func (b *Braid) spanPulse() {
	cfg := b.st.Config()
	me := settle.Seat{Key: key32(b.key), Strand: dag.StrandID(b.meta.OwnStrand)}
	pend, pending := b.st.Pending()
	cid := configID(cfg.ID())
	switch {
	case slices.Contains(cfg.Seats, me):
	case pending && slices.Contains(pend.Seats, me):
		cid = configID(settle.Config{Number: cfg.Number + 1, Rule: pend.Rule, Seats: pend.Seats}.ID())
	default:
		return
	}
	body, err := encodeBody(pulseBody{Config: cfg.Number, Activating: EntryID(cfg.Activating), Readiness: &cid}, b.bounds)
	if err == nil {
		_, _ = b.appendLocked(KindPulse, body)
	}
}

// pulseState is what the writer loop remembers of the own pulses.
type pulseState struct {
	// last is when the own last pulse was written; cfg and round are those of the own last counting pulse.
	last  time.Time
	has   bool
	cfg   uint64
	round uint64
	// waiting is set while a vote waits up to PivotWait for the pivot of round waitRound-1.
	waiting   bool
	waitRound uint64
	waitSince time.Time
	// ready is the config ID of the own last readiness pulse.
	ready    configID
	hasReady bool
}

// notePulse records an own pulse that was placed; settle has not seen it yet.
func (b *Braid) notePulse(n *node, p pulseBody) {
	if p.Readiness != nil {
		b.ps.ready, b.ps.hasReady = *p.Readiness, true
		return
	}
	cfg := b.st.Config()
	if p.Config != cfg.Number {
		return
	}
	c, ok := b.d.Clock(dag.ID(n.id))
	seg, _, placed := b.d.Pos(dag.ID(n.id))
	if !ok || !placed {
		return
	}
	// the past without the pulse itself, which a restored settlement state may already hold
	c = slices.Clone(c)
	c[seg] = n.index
	r := b.st.Round(c)
	if !b.ps.has || b.ps.cfg != p.Config || r > b.ps.round {
		b.ps.has, b.ps.cfg, b.ps.round = true, p.Config, r
	}
}

func (b *Braid) mySeat() (int, settle.Config, bool) {
	cfg := b.st.Config()
	i, ok := b.st.SeatOf(key32(b.key), dag.StrandID(b.meta.OwnStrand))
	return i, cfg, ok
}

// nextPivotRound returns the own next pivot round: the lowest even round of this seat above its last pulse round and the last
// committed round.
func (b *Braid) nextPivotRound(seat int, cfg settle.Config) uint64 {
	n := uint64(len(cfg.Seats))
	var lower uint64
	if b.ps.has && b.ps.cfg == cfg.Number {
		lower = b.ps.round + 1
	}
	if last, ok := b.st.Last(); ok && last.Round+1 > lower {
		lower = last.Round + 1
	}
	j := (lower + 1) / 2
	j += (uint64(seat) + n - j%n) % n
	return 2 * j
}

// holdBack returns the pending own pivot round that links must stay below (link rule 2), ok false when none applies.
func (b *Braid) holdBack() (uint64, bool) {
	seat, cfg, ok := b.mySeat()
	if !ok || b.st.Halted() != nil {
		return 0, false
	}
	r := b.nextPivotRound(seat, cfg)
	if _, has := b.st.Vertex(seat, r); has {
		return 0, false
	}
	return r, true
}

// settledKey reports whether the settled roster admitted the strand under the key.
func (b *Braid) settledKey(id kololog.LogID, key ed25519.PublicKey) bool {
	k, ok := b.settled.Used(roster.StrandID(id))
	return ok && bytes.Equal(k[:], key)
}

// links returns the links of the next own entry: for each other active strand without evidence against it, its newest placed
// entry, or with a pending own pivot round r the newest whose past holds no counting pulse of round r or more.
func (b *Braid) links() []Link {
	r, hold := b.holdBack()
	var out []Link
	// a seat removed in the tentative order is still linked: its pulses count until a config without it activates
	ids := map[kololog.LogID]bool{}
	for _, w := range b.tent.Members() {
		ids[kololog.LogID(w.Strand)] = true
	}
	seats := b.st.Config().Seats
	if p, ok := b.st.Pending(); ok {
		seats = append(slices.Clone(seats), p.Seats...)
	}
	for _, st := range seats {
		ids[kololog.LogID(st.Strand)] = true
	}
	for id := range ids {
		if id == b.meta.OwnStrand || b.evidence[id] != nil {
			continue
		}
		s := b.strands[id]
		// a strand is linked only under the key the settled roster gives it: one followed under another key may hold entries that
		// are not the strand's, which an own entry could never link
		if s == nil || !s.hasTop || !b.settledKey(id, s.key) {
			continue
		}
		l, ok := s.top, true
		if hold {
			l, ok = b.below(s, r)
		}
		if ok {
			out = append(out, Link{Strand: id, Index: l.Index, ID: EntryID(l.ID)})
		}
	}
	slices.SortFunc(out, func(a, c Link) int { return bytes.Compare(a.Strand[:], c.Strand[:]) })
	return out
}

// below walks back from a strand's newest placed entry to the newest whose past holds no counting pulse of round r or more.
func (b *Braid) below(s *strand, r uint64) (dag.Link, bool) {
	l := s.top
	for {
		n := b.nodes[EntryID(l.ID)]
		if n == nil {
			// released: inside the settled prefix, whose pulses are below every pending pivot round
			return l, true
		}
		c, ok := b.d.Clock(l.ID)
		if !ok {
			return dag.Link{}, false
		}
		if h, has := b.st.Highest(c); !has || h < r {
			return l, true
		}
		if l.Index == 0 {
			return dag.Link{}, false
		}
		l = dag.Link{Strand: l.Strand, Index: l.Index - 1, ID: dag.ID(n.prev)}
	}
}

// pastOf returns the clock of the past an own entry with these links would have, itself excluded.
func (b *Braid) pastOf(links []Link) dag.Clock {
	var c dag.Clock
	merge := func(x dag.Clock) {
		for i, v := range x {
			if i >= len(c) {
				c = append(c, make(dag.Clock, i+1-len(c))...)
			}
			c[i] = max(c[i], v)
		}
	}
	clockOf := func(l dag.Link) {
		if x, ok := b.d.Clock(l.ID); ok {
			merge(x)
			return
		}
		if _, x, ok, err := b.lk.find(l); err == nil && ok {
			merge(x)
		}
	}
	if s := b.strands[b.meta.OwnStrand]; s != nil && s.hasTop {
		clockOf(s.top)
	}
	for _, l := range links {
		clockOf(dag.Link{Strand: dag.StrandID(l.Strand), Index: l.Index, ID: dag.ID(l.ID)})
	}
	return c
}

// writeLoop writes the entries the protocol asks of this peer: Evidence entries, readiness pulses and pulses.
func (b *Braid) writeLoop(ctx context.Context) {
	defer b.wg.Done()
	t := time.NewTimer(time.Hour)
	defer t.Stop()
	for {
		b.mu.Lock()
		wait := b.writeStep(time.Now())
		b.mu.Unlock()
		if !t.Stop() {
			select {
			case <-t.C:
			default:
			}
		}
		if wait > 0 {
			t.Reset(wait)
		}
		select {
		case <-ctx.Done():
			return
		case <-b.wake:
		case <-t.C:
		}
	}
}

// writeStep writes what is due now and returns how long until something may be due, 0 for nothing.
func (b *Braid) writeStep(now time.Time) time.Duration {
	if b.closed || b.fatal != nil || b.conf.lost {
		return 0
	}
	wait := b.confirmTimer(now)
	if !b.writable() {
		return wait
	}
	member := b.mayWrite(false) == nil
	// a seat counts until a config without it activates, so it pulses on also when the tentative order removed it
	if _, _, seat := b.mySeat(); !member && (!seat || b.erased != nil) {
		return wait
	}
	if member {
		b.writeEvidence()
		b.writeReadiness()
	}
	if d := b.writePulse(now); d > 0 && (wait == 0 || d < wait) {
		wait = d
	}
	return wait
}

// writeEvidence appends one Evidence entry per strand that this peer holds evidence against and whose member is still active
// with that strand.
func (b *Braid) writeEvidence() {
	for _, id := range b.sortedEvidence() {
		h := b.evidence[id]
		if h.appended {
			continue
		}
		w, ok := b.tent.ByStrand(dag.StrandID(id))
		if !ok || w.Key != key32(h.ev.Key) {
			continue
		}
		body, err := encodeBody(h.ev, b.bounds)
		if err != nil {
			continue
		}
		if _, err := b.appendLocked(KindEvidence, body); err == nil {
			b.markAppended(id)
		}
	}
}

func (b *Braid) sortedEvidence() []kololog.LogID {
	ids := make([]kololog.LogID, 0, len(b.evidence))
	for id := range b.evidence {
		ids = append(ids, id)
	}
	slices.SortFunc(ids, func(a, c kololog.LogID) int { return bytes.Compare(a[:], c[:]) })
	return ids
}

// writeReadiness writes the readiness pulse of a pending config that adds the own seat, once it is known from the settled prefix.
func (b *Braid) writeReadiness() {
	pend, ok := b.st.Pending()
	if !ok {
		return
	}
	cfg := b.st.Config()
	me := settle.Seat{Key: key32(b.key), Strand: dag.StrandID(b.meta.OwnStrand)}
	if !slices.Contains(pend.Seats, me) || slices.Contains(cfg.Seats, me) || !slices.Contains(b.st.Awaiting(), me) {
		return
	}
	// awaited, so no own readiness pulse counts: an older one naming this ID is held by the pivot before the change settled
	cid := configID(settle.Config{Number: cfg.Number + 1, Rule: pend.Rule, Seats: pend.Seats}.ID())
	body, err := encodeBody(pulseBody{Config: cfg.Number, Activating: EntryID(cfg.Activating), Readiness: &cid}, b.bounds)
	if err != nil {
		return
	}
	if _, err := b.appendLocked(KindPulse, body); err == nil {
		b.ps.ready, b.ps.hasReady = cid, true
	}
}

// writePulse writes the own next pulse when the pulse rules call for one, and returns how long to wait when one is only paced.
func (b *Braid) writePulse(now time.Time) time.Duration {
	seat, cfg, ok := b.mySeat()
	if !ok || b.st.Halted() != nil || (b.work <= 0 && !b.readyPending()) {
		return 0
	}
	past := b.pastOf(b.links())
	if !b.hasWork(past) {
		return 0
	}
	r := b.nextPivotRound(seat, cfg)
	_, hasVertex := b.st.Vertex(seat, r)
	rr := b.st.Round(past)
	if b.ps.has && b.ps.cfg == cfg.Number && rr <= b.ps.round {
		return 0
	}
	pivot := !hasVertex && rr == r
	if !pivot && !b.ps.last.IsZero() {
		if d := b.lim.PulseGap - now.Sub(b.ps.last); d > 0 {
			return d
		}
	}
	if rr%2 == 1 {
		ps, _ := b.st.PivotSeat(rr - 1)
		if _, held := b.st.Vertex(ps, rr-1); !held {
			if !b.ps.waiting || b.ps.waitRound != rr {
				b.ps.waiting, b.ps.waitRound, b.ps.waitSince = true, rr, now
			}
			if d := b.lim.PivotWait - now.Sub(b.ps.waitSince); d > 0 {
				return d
			}
		}
	}
	p := pulseBody{Config: cfg.Number, Claim: b.myClaim}
	if cfg.Number > 0 {
		p.Activating = EntryID(cfg.Activating)
	}
	body, err := encodeBody(p, b.bounds)
	if err != nil {
		return 0
	}
	if _, err := b.appendLocked(KindPulse, body); err != nil {
		return 0
	}
	b.ps.last, b.ps.waiting = now, false
	if p.Claim != nil {
		b.clearMyClaim()
	}
	return 0
}

// hasWork reports whether the past of the own next entry holds an unsettled entry other than a pulse, or a readiness pulse that
// the pending config waits for. Entries no own entry would link, such as those of a removed strand, are no work.
func (b *Braid) hasWork(past dag.Clock) bool {
	var cid configID
	pend, ready := b.st.Pending()
	if ready = ready && b.readyPending(); ready {
		cfg := b.st.Config()
		cid = configID(settle.Config{Number: cfg.Number + 1, Rule: pend.Rule, Seats: pend.Seats}.ID())
	}
	for id, n := range b.nodes {
		if !n.placed || !b.d.Contains(past, dag.ID(id)) {
			continue
		}
		if n.bad || n.kind != KindPulse {
			return true
		}
		// a readiness pulse is work until a committed pivot holds it, which activates the config
		if p, ok := n.dec.(pulseBody); ready && ok && p.Readiness != nil && *p.Readiness == cid &&
			slices.Contains(pend.Seats, settle.Seat{Key: key32(n.writer), Strand: dag.StrandID(n.strand)}) {
			return true
		}
	}
	return false
}

// readyPending reports whether a pending config adds seats and every one of them is ready.
func (b *Braid) readyPending() bool {
	pend, ok := b.st.Pending()
	if !ok || len(b.st.Awaiting()) > 0 {
		return false
	}
	cfg := b.st.Config()
	return slices.ContainsFunc(pend.Seats, func(s settle.Seat) bool { return !slices.Contains(cfg.Seats, s) })
}
