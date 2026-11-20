package kolobraid

import (
	"iter"
	"slices"
)

// Ordered is one entry at its position in the order.
type Ordered struct {
	Entry
	// Settled is set for an entry of the settled prefix; Void for one that takes no effect at its position; Rejected for one that
	// apply refused (known for settled entries once applied, and for applied tentative ones).
	Settled, Void, Rejected bool
}

// Entries yields the order from position from on, as of the call: the settled prefix, then the planned tentative tail up to
// MaxTail entries. When a reorg changes a position it already yielded, it ends with ErrReorged.
func (b *Braid) Entries(from uint64) iter.Seq2[Ordered, error] {
	return func(yield func(Ordered, error) bool) {
		b.mu.Lock()
		if err := b.usable(); err != nil {
			b.mu.Unlock()
			yield(Ordered{}, err)
			return
		}
		// a joiner that waited for the genesis replaces the view and the limits under b.mu
		end, gen, v, maxTail := b.settledNext, b.reorgGen, b.view, b.lim.MaxTail
		var tail []applyStep
		start := end
		if b.plan != nil {
			tail, start = slices.Clone(b.plan.steps), b.plan.start
		}
		b.mu.Unlock()
		var last uint64
		yielded := false
		reorged := func() bool {
			b.mu.Lock()
			defer b.mu.Unlock()
			low, ok := b.reorgedSince(gen)
			return ok && yielded && low <= last
		}
		for pos := max(from, v.cfg.Next); pos < end; pos++ {
			o, err := b.settledAt(v, pos)
			if err != nil {
				yield(Ordered{}, err)
				return
			}
			last, yielded = pos, true
			if !yield(o, nil) {
				return
			}
		}
		for i, s := range tail {
			pos := start + uint64(i)
			if pos < from || pos < end {
				continue
			}
			if reorged() {
				yield(Ordered{}, ErrReorged)
				return
			}
			s, err := b.fill(s)
			if err != nil {
				yield(Ordered{}, err)
				return
			}
			o := Ordered{Entry: s.Entry, Void: s.Void}
			o.Entry.Position = pos
			o.Rejected = tentativeRejected(v, maxTail, pos, s.Entry.ID)
			last, yielded = pos, true
			if !yield(o, nil) {
				return
			}
		}
		if reorged() {
			yield(Ordered{}, ErrReorged)
		}
	}
}

func (b *Braid) settledAt(v *view, pos uint64) (Ordered, error) {
	rec, err := b.db.Get(be8(keyPos, pos))
	if err != nil {
		return Ordered{}, err
	}
	pr, err := decodePosRec(rec)
	if err != nil {
		return Ordered{}, err
	}
	b.mu.Lock()
	n, err := b.readEntry(pr.Strand, pr.Index, pr.ID)
	b.mu.Unlock()
	if err != nil {
		return Ordered{}, err
	}
	o := Ordered{Entry: n.entry(), Settled: true, Void: pr.Void}
	o.Entry.Position = pos
	if rej, err := v.Rejected(pos); err == nil {
		o.Rejected = rej
	}
	return o, nil
}

func tentativeRejected(v *view, maxTail int, pos uint64, id EntryID) bool {
	start := v.Progress().Next
	if pos < start {
		return false
	}
	ti, ok := v.TailEntry(int(min(pos-start, uint64(maxTail))))
	return ok && ti.ID == id && ti.Status == statusRejected
}
