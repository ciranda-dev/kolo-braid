package kolobraid

import (
	"bytes"
	"crypto/ed25519"
	"encoding/binary"
	"errors"
	"fmt"
	"iter"
	"slices"

	kololog "github.com/ciranda-dev/kolo-log"
	kolostore "github.com/ciranda-dev/kolo-store"

	"github.com/ciranda-dev/kolo-braid/internal/dag"
	"github.com/ciranda-dev/kolo-braid/internal/roster"
	"github.com/ciranda-dev/kolo-braid/internal/settle"
)

// maxReorgMarks bounds the reorg positions kept for Entries; an iteration older than all of them ends with ErrReorged.
const maxReorgMarks = 256

// job is work for the applier goroutine, done in order.
type job interface{ isJob() }

// batchJob applies one settled batch: steps are its entries with their roster.
type batchJob struct {
	index  uint64
	steps  []applyStep
	config uint64
}

// verifyJob compares this peer's digest at a checkpoint with a certified one.
type verifyJob struct {
	pos uint64
	d   Digest
}

func (*batchJob) isJob()  {}
func (*verifyJob) isJob() {}

// tailPlan is the tentative tail to apply on top of base settled batches; steps stop at MaxTail.
type tailPlan struct {
	gen    uint64
	base   uint64
	config uint64
	start  uint64
	steps  []applyStep
}

type reorgMark struct{ gen, pos uint64 }

// rosterView is a frozen roster at one position, as apply, Tx and snapshots read it; cfg is the settler config whose seats the
// Seat flags mark.
type rosterView struct {
	ws  []Member
	idx map[string]int
	cfg settle.Config
}

func (v *rosterView) Member(key ed25519.PublicKey) (Member, bool) {
	if i, ok := v.idx[string(key)]; ok {
		return v.ws[i], true
	}
	return Member{}, false
}

func (v *rosterView) Members() []Member { return v.ws }

func freeze(r *roster.Roster, cfg settle.Config) *rosterView {
	ws := r.Members()
	seats := seatSet(cfg.Seats)
	v := &rosterView{ws: make([]Member, len(ws)), idx: make(map[string]int, len(ws)), cfg: cfg}
	for i, w := range ws {
		v.ws[i] = Member{Key: ed25519.PublicKey(slices.Clone(w.Key[:])), Strand: kololog.LogID(w.Strand), Roles: Role(w.Roles),
			Seat: seats[settle.Seat{Key: settle.Key(w.Key), Strand: w.Strand}]}
		v.idx[string(w.Key[:])] = i
	}
	return v
}

func seatSet(seats []settle.Seat) map[settle.Seat]bool {
	m := make(map[settle.Seat]bool, len(seats))
	for _, s := range seats {
		m[s] = true
	}
	return m
}

// libWrites turns roster records into library writes under the roster prefix.
func libWrites(ws []roster.Write) []viewWrite {
	if len(ws) == 0 {
		return nil
	}
	out := make([]viewWrite, len(ws))
	for i, w := range ws {
		k := append([]byte{rosterPrefix}, w.Key...)
		if w.Delete {
			out[i] = viewWrite{Op: opDelete, A: k}
		} else {
			out[i] = viewWrite{Op: opPut, A: k, B: slices.Clone(w.Value)}
		}
	}
	return out
}

// step evaluates n at pos on r and returns its apply step; rv is the frozen roster before it, replaced when n changes r.
func step(n *node, r *roster.Roster, pos uint64, rv *rosterView, cfg settle.Config) (applyStep, *rosterView, roster.Result) {
	e := n.rosterEntry()
	var res roster.Result
	if n.membership() {
		res = r.Apply(pos, &e)
	} else {
		res.Reason = r.Check(&e)
	}
	if len(res.Writes) > 0 {
		rv = freeze(r, cfg)
	}
	return applyStep{Entry: n.entry(), Roster: rv, Lib: libWrites(res.Writes), Void: !res.Effective(), lazy: n.lazy}, rv, res
}

// fill reads the body of a lazy step back from its strand; the caller does not hold b.mu.
func (b *Braid) fill(s applyStep) (applyStep, error) {
	if !s.lazy {
		return s, nil
	}
	b.mu.Lock()
	n, err := b.readEntry(s.Entry.Strand, s.Entry.Index, s.Entry.ID)
	b.mu.Unlock()
	if err != nil {
		return s, err
	}
	s.Entry.Body, s.lazy = n.body, false
	return s, nil
}

func (b *Braid) filled(steps []applyStep) iter.Seq2[applyStep, error] {
	return func(yield func(applyStep, error) bool) {
		for _, s := range steps {
			if !yield(b.fill(s)) {
				return
			}
		}
	}
}

// progress settles what the held pulses allow, plans the tail and wakes the goroutines.
func (b *Braid) progress() error {
	for {
		if err := b.settleAll(); err != nil {
			return b.fail(err)
		}
		if err := b.buildPlan(); err != nil {
			return b.fail(err)
		}
		grew, err := b.reread()
		if err != nil {
			return b.fail(err)
		}
		if !grew {
			break
		}
	}
	b.noteJoined()
	b.checkConfirm()
	b.cond.Broadcast()
	b.signal()
	b.sync.signal()
	return nil
}

// reread reads the strands that the order just added, which may already hold entries, and the strands the pending bound
// stopped, whose waiting entry may be placeable now. It reports whether anything was read.
func (b *Braid) reread() (bool, error) {
	ids := b.fresh
	b.fresh = nil
	for id, s := range b.strands {
		if s.full {
			ids = append(ids, id)
		}
	}
	slices.SortFunc(ids, func(a, c kololog.LogID) int { return bytes.Compare(a[:], c[:]) })
	grew := false
	for _, id := range slices.Compact(ids) {
		s := b.strands[id]
		if s == nil || s.next >= s.length() {
			continue
		}
		next := s.next
		if err := b.ingest(s); err != nil {
			return false, err
		}
		grew = grew || s.next > next
	}
	return grew, nil
}

func (b *Braid) noteHalt() {
	err := b.st.Halted()
	switch {
	case err != nil && !b.halted:
		b.halted = true
		var he *settle.HaltError
		ev := Event{Kind: EventSettlementHalted, Position: b.settledNext, Err: fmt.Errorf("%w: %w", ErrEquivocation, err)}
		if errors.As(err, &he) && he.Key != (settle.Key{}) {
			ev.Member = ed25519.PublicKey(slices.Clone(he.Key[:]))
		}
		b.ev.push(ev)
	case err == nil && b.halted:
		b.halted = false
	}
}

// settleAll commits every pivot the held pulses allow; each Advance result is stored in one batch with the state that follows it.
func (b *Braid) settleAll() error {
	for {
		cfg := b.st.Config()
		c0 := b.st.Count()
		ps, err := b.st.Advance()
		b.noteHalt()
		if err != nil {
			return err
		}
		if len(ps) == 0 {
			return nil
		}
		if c0 != b.pivots {
			return fmt.Errorf("%w: settlement at pivot %d, batches at %d", ErrCorrupt, c0, b.pivots)
		}
		bt := b.db.NewBatch()
		jobs := make([]job, 0, len(ps))
		for _, p := range ps {
			j, err := b.buildBatch(p, cfg, bt)
			if err != nil {
				return err
			}
			jobs = append(jobs, j)
		}
		if ps[len(ps)-1].Activates {
			nc := b.st.Config()
			rec := configRec{Rule: QuorumRule(nc.Rule), Activating: EntryID(nc.Activating)}
			for _, s := range nc.Seats {
				rec.Seats = append(rec.Seats, seatRec{Key: ed25519.PublicKey(slices.Clone(s.Key[:])), Strand: kololog.LogID(s.Strand)})
			}
			b.configs[nc.Number] = rec
			bt.Put(be8(keyConfig, nc.Number), rec.encode())
			b.ps.has = false
			// the round marks of earlier configs are read no more
			bt.DeleteRange([]byte(keyMark), be8(keyMark, nc.Number))
		}
		// only the new round marks: the stored ones never change
		for _, m := range b.st.TakeMarks() {
			bt.Put(markKey(m), binary.AppendUvarint(nil, m.Round))
		}
		bt.Put([]byte(keyReleased), b.releasedState().encode())
		if err := bt.Commit(); err != nil {
			return err
		}
		if b.viewHalt == nil {
			b.jobs = append(b.jobs, jobs...)
		}
	}
}

func (b *Braid) releasedState() released {
	fl := make(map[kololog.LogID]uint64, len(b.strands))
	for id, s := range b.strands {
		if s.floor > 0 {
			fl[id] = s.floor
		}
	}
	return released{Pivots: b.pivots, Next: b.settledNext, Last: b.lastPivot, Frontier: b.frontier, Segments: b.d.Segments(),
		Floors: fl, Settle: b.st.Save()}
}

// buildBatch orders the batch of pivot p, evaluates the roster over it and releases it from memory.
func (b *Braid) buildBatch(p settle.Pivot, cfg settle.Config, bt kolostore.Batch) (*batchJob, error) {
	pc, ok := b.d.Clock(p.ID)
	if !ok {
		return nil, fmt.Errorf("%w: pivot not held", ErrCorrupt)
	}
	var ids []EntryID
	if err := b.d.Order(b.frontier, pc, [32]byte(p.ID), func(id dag.ID) error {
		ids = append(ids, EntryID(id))
		return nil
	}); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrCorrupt, err)
	}
	r := b.settled
	rv := b.settledEnd
	if rv == nil {
		rv = freeze(r, cfg)
	}
	pos := b.settledNext
	steps := make([]applyStep, 0, len(ids))
	for _, id := range ids {
		n := b.nodes[id]
		if n == nil {
			return nil, fmt.Errorf("%w: batch entry not held", ErrCorrupt)
		}
		if n.bad || n.kind != KindPulse {
			b.work--
		}
		if !n.positioned() {
			continue
		}
		b.joinSeen = b.joinSeen || id == b.joinID
		if id == b.joinID && b.welcome == nil {
			// the join is settled, effective or void: a restart must not wait for it again
			bt.Delete([]byte(keyWelcome))
		}
		var s applyStep
		var res roster.Result
		s, rv, res = step(n, r, pos, rv, cfg)
		steps = append(steps, s)
		b.settledEvents(n, res, pos)
		if err := b.followAdded(n, res, bt, true); err != nil {
			return nil, err
		}
		bt.Put(be8(keyPos, pos), posRec{Strand: n.strand, Index: n.index, ID: n.id, Void: s.Void}.encode())
		if pos >= b.prevAt && pos-b.prevAt < uint64(len(b.prevTail)) && b.prevTail[pos-b.prevAt] != id {
			b.reorgAt(pos)
		}
		pos++
	}
	if err := b.d.Release(pc); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrCorrupt, err)
	}
	b.release(ids)
	b.frontier, b.settledNext, b.lastPivot = pc, pos, EntryID(p.ID)
	rec := pivotRec{ID: EntryID(p.ID), Config: p.Config, Round: p.Round, Activates: p.Activates, Clock: pc, Next: pos}
	bt.Put(be8(keyPivot, b.pivots), rec.encode())
	b.pivots++
	b.settledEnd = rv
	if p.Activates {
		b.settledEnd = nil
	}
	return &batchJob{index: b.pivots - 1, steps: steps, config: p.Config}, nil
}

// followAdded follows the strand that an effective AddMember entry adds. A void one is never followed: a log ID does not bind a
// key, so only the order says which key a strand is read under. A settled entry that names a strand followed under another
// key (only a tentative order settlement did not keep can cause it) stops the braid; Open reads the strand again under that key.
func (b *Braid) followAdded(n *node, res roster.Result, bt kolostore.Batch, settled bool) error {
	nm, ok := n.dec.(NewMember)
	if !ok || (res.Change != roster.Added && res.Change != roster.Replaced) {
		return nil
	}
	err := b.follow(nm.Strand, nm.Key, nm.Header, bt)
	if !errors.Is(err, errStrandKey) {
		return err
	}
	if !settled {
		b.logf("kolobraid: an AddMember entry names a followed strand under another key")
		return nil
	}
	rec := append([]byte(keyStrand), nm.Strand[:]...)
	if err := b.db.Put(rec, strandRec{Key: nm.Key, Header: nm.Header}.encode()); err != nil {
		return err
	}
	return fmt.Errorf("%w: a strand was read under a key that the settled order does not name: open again", ErrCorrupt)
}

func (b *Braid) settledEvents(n *node, res roster.Result, pos uint64) {
	w := ed25519.PublicKey(slices.Clone(res.Member.Key[:]))
	own := dag.StrandID(b.meta.OwnStrand)
	switch res.Change {
	case roster.Founded, roster.Added:
		b.ev.push(Event{Kind: EventMemberAdded, Position: pos, Member: w})
	case roster.Replaced:
		b.ev.push(Event{Kind: EventMemberAdded, Position: pos, Member: w})
		if res.Old == own {
			b.ev.push(Event{Kind: EventRemoved, Position: pos, Member: w})
		}
	case roster.Removed, roster.Fenced:
		b.ev.push(Event{Kind: EventMemberRemoved, Position: pos, Member: w})
		if res.Change == roster.Fenced {
			b.ev.push(Event{Kind: EventEquivocation, Position: pos, Member: w})
		}
		if res.Member.Strand == own {
			b.ev.push(Event{Kind: EventRemoved, Position: pos, Member: w})
		}
	}
}

func (b *Braid) reorgAt(pos uint64) {
	if b.reorgPending == nil || pos < *b.reorgPending {
		p := pos
		b.reorgPending = &p
	}
}

// buildPlan orders the tentative tail as the predicted batches, then the rest under the group ID, and evaluates the tentative roster.
// Entries placed since the last plan that all hold the whole last tail in their past extend it (see extends), so a stalled
// settlement costs no work per entry that grows with the tail.
func (b *Braid) buildPlan() error {
	pred := b.st.Predicted()
	var ids []EntryID
	add := func(id dag.ID) error {
		ids = append(ids, EntryID(id))
		return nil
	}
	cfg := b.st.Config()
	var r *roster.Roster
	var rv *rosterView
	var plan *tailPlan
	var pos uint64
	order := b.prevTail
	if b.extends(pred) {
		if err := b.d.Order(b.planEnds, nil, [32]byte(b.meta.Group), add); err != nil {
			return fmt.Errorf("%w: %w", ErrCorrupt, err)
		}
		r, rv, pos = b.tent, b.planRV, b.prevAt+uint64(len(b.prevTail))
		plan = &tailPlan{base: b.pivots, config: cfg.Number, start: b.settledNext, steps: slices.Clone(b.plan.steps)}
	} else {
		front := b.frontier
		for _, p := range pred {
			pc, ok := b.d.Clock(p)
			if !ok {
				break
			}
			if err := b.d.Order(front, pc, [32]byte(p), add); err != nil {
				return fmt.Errorf("%w: %w", ErrCorrupt, err)
			}
			front = pc
		}
		if err := b.d.Order(front, nil, [32]byte(b.meta.Group), add); err != nil {
			return fmt.Errorf("%w: %w", ErrCorrupt, err)
		}
		r = b.settled.Clone()
		rv = freeze(r, cfg)
		pos = b.settledNext
		plan = &tailPlan{base: b.pivots, config: cfg.Number, start: pos}
		order = make([]EntryID, 0, len(ids))
	}
	var bt kolostore.Batch
	for _, id := range ids {
		n := b.nodes[id]
		if n == nil {
			return fmt.Errorf("%w: tail entry not held", ErrCorrupt)
		}
		if !n.positioned() {
			continue
		}
		b.joinSeen = b.joinSeen || id == b.joinID
		// past MaxTail only membership entries matter: they make the tentative roster
		if len(plan.steps) < b.lim.MaxTail || n.membership() {
			var s applyStep
			var res roster.Result
			s, rv, res = step(n, r, pos, rv, cfg)
			if res.Change == roster.Added || res.Change == roster.Replaced {
				if bt == nil {
					bt = b.db.NewBatch()
				}
				if err := b.followAdded(n, res, bt, false); err != nil {
					return err
				}
			}
			if len(plan.steps) < b.lim.MaxTail {
				plan.steps = append(plan.steps, s)
			}
		}
		if i := pos - b.prevAt; pos >= b.prevAt && i < uint64(len(b.prevTail)) && b.prevTail[i] != id {
			b.reorgAt(pos)
		}
		order = append(order, id)
		pos++
	}
	if bt != nil {
		if err := bt.Commit(); err != nil {
			return err
		}
	}
	b.tent, b.planRV, b.planPred, b.planEnds, b.placedSince = r, rv, pred, b.d.Ends(), nil
	b.prevTail, b.prevAt = order, b.settledNext
	if b.reorgPending != nil {
		b.reorgGen++
		b.reorgs = append(b.reorgs, reorgMark{gen: b.reorgGen, pos: *b.reorgPending})
		if len(b.reorgs) > maxReorgMarks {
			b.reorgs = slices.Delete(b.reorgs, 0, len(b.reorgs)-maxReorgMarks)
		}
		b.ev.push(Event{Kind: EventReorg, Position: *b.reorgPending})
		b.reorgPending = nil
	}
	b.planGen++
	plan.gen = b.planGen
	b.plan = plan
	return nil
}

// extends reports whether the entries placed since the last plan can extend it: the same settled base and predicted pivots,
// and each of them holds every entry of the last plan in its past, so Kahn orders them after all of it.
func (b *Braid) extends(pred []dag.ID) bool {
	if b.plan == nil || b.plan.base != b.pivots || b.prevAt != b.settledNext || !slices.Equal(pred, b.planPred) {
		return false
	}
	for _, id := range b.placedSince {
		c, ok := b.d.Clock(dag.ID(id))
		if !ok {
			return false
		}
		for seg, end := range b.planEnds {
			if end > b.frontier.At(seg) && c.At(seg) < end {
				return false
			}
		}
	}
	return true
}

// reorgedSince returns the lowest position a reorg changed after generation gen, ok false when none did.
func (b *Braid) reorgedSince(gen uint64) (uint64, bool) {
	if len(b.reorgs) > 0 && b.reorgs[0].gen > gen+1 {
		return 0, true
	}
	var low uint64
	found := false
	for _, m := range b.reorgs {
		if m.gen > gen && (!found || m.pos < low) {
			low, found = m.pos, true
		}
	}
	return low, found
}

// proposals is settlement's roster: the settler config at the end of each pivot's batch, computed on a copy of the settled roster.
func (b *Braid) proposals(from uint64, pivots []settle.Pivot) ([]settle.Proposal, error) {
	if from != b.pivots {
		return nil, fmt.Errorf("%w: roster asked from pivot %d at %d", ErrCorrupt, from, b.pivots)
	}
	r := b.settled.Clone()
	front, pos := b.frontier, b.settledNext
	out := make([]settle.Proposal, 0, len(pivots))
	for _, p := range pivots {
		pc, ok := b.d.Clock(p.ID)
		if !ok {
			return nil, fmt.Errorf("%w: pivot not held", ErrCorrupt)
		}
		if err := b.d.Order(front, pc, [32]byte(p.ID), func(id dag.ID) error {
			n := b.nodes[EntryID(id)]
			switch {
			case n == nil:
				return fmt.Errorf("%w: batch entry not held", ErrCorrupt)
			case !n.positioned():
				return nil
			case n.membership():
				e := n.rosterEntry()
				r.Apply(pos, &e)
			}
			pos++
			return nil
		}); err != nil {
			return nil, err
		}
		front = pc
		rule, _ := r.Settlers()
		out = append(out, settle.Proposal{Rule: settle.Rule(rule), Seats: settleSeats(r.Seats())})
	}
	return out, nil
}

func settleSeats(ss []roster.Seat) []settle.Seat {
	out := make([]settle.Seat, len(ss))
	for i, s := range ss {
		out[i] = settle.Seat{Key: settle.Key(s.Key), Strand: s.Strand}
	}
	return out
}

// applyLoop drives the view: seeds first in a successor braid, then settled batches in order, then the newest tail plan.
func (b *Braid) applyLoop() {
	defer b.wg.Done()
	b.mu.Lock()
	defer b.mu.Unlock()
	for {
		for !b.closed && !b.applyReady() {
			b.cond.Wait()
		}
		if b.closed {
			return
		}
		switch {
		case b.needSeed():
			b.mu.Unlock()
			err := b.seedView()
			b.mu.Lock()
			if err != nil {
				b.haltView(err)
			}
		case len(b.jobs) > 0:
			j := b.jobs[0]
			b.jobs[0] = nil
			b.jobs = b.jobs[1:]
			b.busy = true
			b.mu.Unlock()
			b.runJob(j)
			b.mu.Lock()
			b.busy = false
		default:
			p := b.plan
			b.busy = true
			b.mu.Unlock()
			n, applied := b.runPlan(p)
			b.mu.Lock()
			b.busy = false
			b.appliedGen = p.gen
			if applied > 0 {
				b.ev.push(Event{Kind: EventApplied, Position: p.start + uint64(n) - 1})
			}
		}
	}
}

func (b *Braid) applyReady() bool {
	if b.viewHalt != nil || b.fatal != nil {
		return false
	}
	if b.needSeed() {
		s := b.strands[b.meta.FounderStrand]
		return s != nil && s.next > b.cont.Seeds
	}
	return len(b.jobs) > 0 || (b.plan != nil && b.plan.gen != b.appliedGen)
}

func (b *Braid) needSeed() bool { return b.cont != nil && b.cont.Seeds > 0 && !b.seeded }

func (b *Braid) haltView(err error) {
	if b.viewHalt != nil {
		return
	}
	b.viewHalt = err
	b.jobs = nil
	b.ev.push(Event{Kind: EventHalted, Position: b.view.Progress().Next, Err: err})
}

func (b *Braid) runJob(j job) {
	switch j := j.(type) {
	case *batchJob:
		b.runBatch(j)
	case *verifyJob:
		known, err := b.view.Verify(j.pos, j.d)
		b.mu.Lock()
		defer b.mu.Unlock()
		switch {
		case errors.Is(err, ErrDiverged):
			b.ev.push(Event{Kind: EventDiverged, Position: j.pos, Err: err})
			b.haltView(err)
		case err != nil:
			b.haltView(err)
		case known:
			delete(b.certs, j.pos)
			b.ev.push(Event{Kind: EventCertified, Position: j.pos})
		}
	}
}

func (b *Braid) runBatch(j *batchJob) {
	p := b.view.Progress()
	if p.Batches > j.index {
		return
	}
	if p.Batches < j.index || p.Offset > uint64(len(j.steps)) {
		b.mu.Lock()
		b.haltView(fmt.Errorf("%w: view at batch %d, batch %d next", ErrCorrupt, p.Batches, j.index))
		b.mu.Unlock()
		return
	}
	var err error
	if b.appliedConfig != j.config {
		// the tail was applied under another config's seats: nothing of it may be promoted into this batch
		err = b.view.DropTail(0)
		b.appliedConfig = j.config
	}
	var res settleResult
	if err == nil {
		res, err = b.view.Settle(j.index, b.filled(j.steps[p.Offset:]))
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if err != nil {
		b.haltView(err)
		return
	}
	if res.Progress.Next > 0 {
		b.ev.push(Event{Kind: EventSettled, Position: res.Progress.Next - 1})
	}
	if res.Checkpoint != nil {
		b.onCheckpoint(*res.Checkpoint)
	}
}

// runPlan makes the view's tail equal to the plan: it keeps the longest prefix already applied and applies the rest until the tail
// is full or an entry fails. It returns the tail length and how many entries it applied.
func (b *Braid) runPlan(p *tailPlan) (int, int) {
	if b.view.Progress().Batches != p.base {
		return b.view.Tail(), 0
	}
	n := b.view.Tail()
	k := 0
	if b.appliedConfig == p.config {
		for k < n && k < len(p.steps) {
			ti, _ := b.view.TailEntry(k)
			if ti.ID != p.steps[k].Entry.ID {
				break
			}
			k++
		}
	}
	if k < n {
		if err := b.view.DropTail(k); err != nil {
			return 0, 0
		}
	}
	b.appliedConfig = p.config
	applied := 0
	for _, s := range p.steps[k:] {
		s, err := b.fill(s)
		if err != nil {
			b.mu.Lock()
			b.haltView(err)
			b.mu.Unlock()
			break
		}
		res, err := b.view.ApplyTail(s)
		if err != nil || res.Paused {
			break
		}
		applied++
	}
	return k + applied, applied
}

func (b *Braid) genesis() roster.Genesis {
	g := roster.Genesis{Group: dag.ID(b.meta.Group), Founder: key32(b.meta.FounderKey), Strand: dag.StrandID(b.meta.FounderStrand),
		Rule: roster.Rule(b.cfg.Quorum), MaxMembers: b.cfg.MaxMembers, MaxSettlers: b.cfg.MaxSettlers}
	if b.cont != nil {
		g.Seeds = b.cont.Seeds
	}
	return g
}

// restoreConfigs reads the stored settler configs, and stores config 0 when the braid has none.
func (b *Braid) restoreConfigs() error {
	if err := scanPrefix(b.db, []byte(keyConfig), func(k, v []byte) error {
		c, err := decodeConfigRec(v)
		if err != nil {
			return err
		}
		if len(k) != len(keyConfig)+8 {
			return fmt.Errorf("%w: config record key", ErrCorrupt)
		}
		b.configs[beUint(k[len(keyConfig):])] = c
		return nil
	}); err != nil {
		return err
	}
	if _, ok := b.configs[0]; !ok {
		c := configRec{Rule: b.cfg.Quorum, Seats: []seatRec{{Key: slices.Clone(b.meta.FounderKey), Strand: b.meta.FounderStrand}}}
		b.configs[0] = c
		if err := b.db.Put(be8(keyConfig, 0), c.encode()); err != nil {
			return err
		}
	}
	return nil
}

// restoreRoster loads the settled roster from its stored records.
func (b *Braid) restoreRoster(g roster.Genesis) error {
	var err error
	var recs []struct{ k, v []byte }
	if err := scanPrefix(b.db, []byte{rosterPrefix}, func(k, v []byte) error {
		recs = append(recs, struct{ k, v []byte }{k[1:], v})
		return nil
	}); err != nil {
		return err
	}
	if len(recs) == 0 {
		b.settled, err = roster.New(g)
	} else {
		b.settled, err = roster.Load(g, func(yield func([]byte, []byte) bool) {
			for _, r := range recs {
				if !yield(r.k, r.v) {
					return
				}
			}
		})
	}
	if err != nil {
		return fmt.Errorf("%w: %w", ErrCorrupt, err)
	}
	return nil
}

// replayStored applies the stored batches that the view has not applied.
func (b *Braid) replayStored() error {
	p := b.view.Progress()
	halted := false
	for i := p.Batches; i < b.pivots; i++ {
		off := uint64(0)
		if i == p.Batches {
			off = p.Offset
		}
		if err := b.replayBatch(i, off, !halted); err != nil {
			if !errors.Is(err, ErrHalted) {
				return err
			}
			halted = true
			b.viewHalt = err
		}
	}
	if !halted && b.pivots > 0 && b.view.Progress().Next != b.settledNext {
		return fmt.Errorf("%w: view at position %d, settlement at %d", ErrCorrupt, b.view.Progress().Next, b.settledNext)
	}
	return nil
}

// restore loads the settled state at Open and replays into the view the batches it has not applied.
func (b *Braid) restore() error {
	g := b.genesis()
	var rs released
	if v, err := getOpt(b.db, []byte(keyReleased)); err != nil {
		return err
	} else if v != nil {
		if rs, err = decodeReleased(v); err != nil {
			return err
		}
	}
	if err := b.restoreConfigs(); err != nil {
		return err
	}
	seeded, err := getOpt(b.db, []byte(keySeeded))
	if err != nil {
		return err
	}
	b.seeded = seeded != nil
	if b.opts.Rebuild {
		if err := b.view.Reset(viewWrite{Op: opDeleteRange, A: []byte{rosterPrefix}, B: []byte{rosterPrefix + 1}}); err != nil {
			return err
		}
		if err := b.db.Delete([]byte(keySeeded)); err != nil {
			return err
		}
		b.seeded = false
	}
	if err := b.restoreRoster(g); err != nil {
		return err
	}
	b.lk = &lookup{db: b.db, segs: func() int { return b.segCount }, cache: map[dag.Link]lookupHit{}}
	b.segCount = len(rs.Segments)
	b.d, err = dag.New(dag.Config{MaxLinks: b.bounds.links(), MaxPendingBytes: b.lim.MaxPendingBytes,
		MaxPendingPerStrand: b.lim.MaxPendingPerStrand, MaxForks: b.lim.MaxForks, Lookup: b.lk.find,
		Segments: rs.Segments})
	if err != nil {
		return fmt.Errorf("%w: %w", ErrCorrupt, err)
	}
	if rs.Settle != nil {
		if b.st, err = settle.Load(b.d, b.proposals, rs.Settle); err == nil {
			var ms []settle.Mark
			if ms, err = storedMarks(b.db, b.st.Config().Number); err == nil {
				err = b.st.RestoreMarks(ms)
			}
		}
	} else {
		var anchor *settle.Anchor
		if a := b.meta.Anchor; a != nil {
			anchor = &settle.Anchor{ID: dag.ID(a.Pivot), Config: a.Config}
		}
		b.st, err = settle.New(b.d, b.proposals, settle.Config{Rule: settle.Rule(b.cfg.Quorum),
			Seats: []settle.Seat{{Key: key32(b.meta.FounderKey), Strand: dag.StrandID(b.meta.FounderStrand)}}}, anchor)
	}
	if err != nil {
		return fmt.Errorf("%w: %w", ErrCorrupt, err)
	}
	b.pivots, b.frontier, b.lastPivot = rs.Pivots, rs.Frontier, rs.Last
	b.settledNext = b.view.cfg.Next
	if rs.Settle != nil {
		b.settledNext = rs.Next
	}
	for id, f := range rs.Floors {
		s := b.strands[id]
		if s == nil || f > s.length() {
			return fmt.Errorf("%w: strand floor beyond the strand", ErrCorrupt)
		}
		s.next, s.floor = f, f
		rec, err := s.recordAt(f - 1)
		if err != nil {
			return err
		}
		s.last = entryIDOf(rec.Encode())
	}
	if b.needSeed() && b.pivots > 0 {
		if err := b.seedView(); err != nil {
			return err
		}
	}
	// the stored roster is the one at the view's position: the view commits the roster's records with its own writes
	cfg, err := b.viewBatchConfig()
	if err != nil {
		return err
	}
	b.view.setRoster(freeze(b.settled, cfg))
	if err := b.replayStored(); err != nil {
		return err
	}
	b.appliedConfig = b.st.Config().Number
	b.tent = b.settled.Clone()
	if err := b.loadEvidence(); err != nil {
		return err
	}
	return b.loadClaims()
}

// viewBatchConfig returns the config of the batch that holds the view's last applied entry, config 0 when it holds none.
func (b *Braid) viewBatchConfig() (settle.Config, error) {
	p := b.view.Progress()
	i := p.Batches
	if p.Offset == 0 {
		if i == 0 {
			return b.source().config(0)
		}
		i--
	}
	rec, err := b.pivotAt(i)
	if err != nil {
		return settle.Config{}, err
	}
	return b.source().config(rec.Config)
}

func beUint(b []byte) uint64 {
	var v uint64
	for _, c := range b {
		v = v<<8 | uint64(c)
	}
	return v
}

func (b *Braid) pivotAt(i uint64) (pivotRec, error) {
	v, err := b.db.Get(be8(keyPivot, i))
	if err != nil {
		if errors.Is(err, kolostore.ErrNotFound) {
			return pivotRec{}, fmt.Errorf("%w: pivot %d not stored", ErrCorrupt, i)
		}
		return pivotRec{}, err
	}
	return decodePivotRec(v)
}

// replayBatch applies stored batch i again from the entry index, skipping the first off entries, which the view and the stored
// roster already hold. With toView false only the roster advances.
func (b *Braid) replayBatch(i, off uint64, toView bool) error {
	_, err := b.replayInto(b.source(), b.view, b.settled, i, off, noStop, toView)
	return err
}

// noStop is the stop position of a replay of whole batches.
const noStop = ^uint64(0)

// replaySource is what a replay reads of the braid besides its stored records; stop, when set, ends the replay with its error
// before any entry is read or applied, and after each batch.
type replaySource struct {
	segs    []dag.Segment
	configs map[uint64]configRec
	read    func(kololog.LogID, uint64, EntryID) (*node, error)
	stop    func() error
}

// steps yields the steps of a replay with their bodies, checking stop before each.
func (b *Braid) steps(src replaySource, steps []applyStep) iter.Seq2[applyStep, error] {
	if src.stop == nil {
		return b.filled(steps)
	}
	return func(yield func(applyStep, error) bool) {
		for _, s := range steps {
			if err := src.stop(); err != nil {
				yield(applyStep{}, err)
				return
			}
			if !yield(b.fill(s)) {
				return
			}
		}
	}
}

// source returns the braid's replay source; the caller holds b.mu or runs before the goroutines start.
func (b *Braid) source() replaySource {
	return replaySource{segs: b.d.Segments(), configs: b.configs, read: b.readEntry}
}

// config returns stored settler config n.
func (src replaySource) config(n uint64) (settle.Config, error) {
	cr, ok := src.configs[n]
	if !ok {
		return settle.Config{}, fmt.Errorf("%w: config %d not stored", ErrCorrupt, n)
	}
	c := settle.Config{Number: n, Rule: settle.Rule(cr.Rule), Activating: dag.ID(cr.Activating)}
	for _, s := range cr.Seats {
		c.Seats = append(c.Seats, settle.Seat{Key: key32(s.Key), Strand: dag.StrandID(s.Strand)})
	}
	return c, nil
}

// replayInto replays stored batch i into v and r, as replayBatch does into the braid's own view and settled roster, and returns
// the roster after the last entry it applied. Entries at stop and after it are read but not applied.
func (b *Braid) replayInto(src replaySource, v *view, r *roster.Roster, i, off, stop uint64, toView bool) (*rosterView, error) {
	rec, err := b.pivotAt(i)
	if err != nil {
		return nil, err
	}
	var prev dag.Clock
	pos := v.cfg.Next
	if i > 0 {
		pr, err := b.pivotAt(i - 1)
		if err != nil {
			return nil, err
		}
		prev, pos = pr.Clock, pr.Next
	}
	cfg, err := src.config(rec.Config)
	if err != nil {
		return nil, err
	}
	segs := src.segs
	runs := make([]dag.Run, len(segs))
	for si, sg := range segs {
		lo, hi := max(sg.Start, prev.At(si)), max(sg.Start, rec.Clock.At(si))
		if lo > hi {
			return nil, fmt.Errorf("%w: pivot clocks not nested", ErrCorrupt)
		}
		idx := lo
		runs[si] = dag.Run{Lo: lo, Hi: hi, Next: func() (dag.Item, error) {
			v, err := b.db.Get(segKey(si, idx))
			if err != nil {
				return dag.Item{}, fmt.Errorf("%w: entry index: %w", ErrCorrupt, err)
			}
			se, err := decodeSegEntry(v)
			if err != nil {
				return dag.Item{}, err
			}
			it := dag.Item{ID: dag.ID(se.ID), Class: se.Class, Seg: si, Index: idx}
			if idx == sg.Start && sg.Parent >= 0 {
				it.Waits = append(it.Waits, dag.Pos{Seg: sg.Parent, Index: idx - 1})
			}
			for _, c := range se.Record.Comps {
				it.Waits = append(it.Waits, dag.Pos{Seg: c.Seg, Index: c.Value - 1})
			}
			idx++
			return it, nil
		}}
	}
	rv := freeze(r, cfg)
	var steps []applyStep
	skip := off
	if err := dag.Merge(runs, [32]byte(rec.ID), func(it dag.Item) error {
		n, err := src.read(kololog.LogID(segs[it.Seg].Strand), it.Index, EntryID(it.ID))
		if err != nil {
			return err
		}
		if !n.positioned() {
			return nil
		}
		if skip > 0 {
			skip--
			pos++
			return nil
		}
		if pos >= stop {
			pos++
			return nil
		}
		var s applyStep
		s, rv, _ = step(n, r, pos, rv, cfg)
		if toView {
			steps = append(steps, s)
		}
		pos++
		return nil
	}); err != nil {
		return nil, err
	}
	if pos != rec.Next {
		return nil, fmt.Errorf("%w: replayed batch %d ends at %d, stored %d", ErrCorrupt, i, pos, rec.Next)
	}
	if !toView {
		return rv, nil
	}
	_, err = v.Settle(i, b.steps(src, steps))
	if err == nil && rec.Activates {
		err = v.DropTail(0)
	}
	if err != nil && v.Halted() != nil {
		return nil, fmt.Errorf("%w: %w", ErrHalted, err)
	}
	if err == nil && src.stop != nil {
		err = src.stop()
	}
	if err != nil {
		return nil, err
	}
	return rv, nil
}

// seedView writes the pairs of the seed entries of a successor braid into the view and checks their content digest.
func (b *Braid) seedView() error {
	b.mu.Lock()
	s := b.strands[b.meta.FounderStrand]
	m := b.cont.Seeds
	b.mu.Unlock()
	for i := uint64(1); i <= m; i++ {
		v, err := s.value(i)
		if err != nil {
			return err
		}
		c, body, err := decodeContent(v, s.id, i, b.bounds)
		if err != nil || c.Kind != KindSeed {
			return fmt.Errorf("%w: seed entry %d", ErrBadSeed, i)
		}
		if err := b.view.SeedPairs(body.(seedBody).Pairs); err != nil {
			return err
		}
	}
	d, err := b.view.ContentDigest()
	if err != nil {
		return err
	}
	if d != b.cont.Content {
		return ErrBadSeed
	}
	if err := b.db.Put([]byte(keySeeded), []byte{1}); err != nil {
		return err
	}
	b.mu.Lock()
	b.seeded = true
	b.cond.Broadcast()
	b.mu.Unlock()
	return nil
}
