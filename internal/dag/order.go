package dag

import (
	"bytes"
	"container/heap"
	"fmt"

	kolocodec "github.com/ciranda-dev/kolo-codec"
	"golang.org/x/crypto/blake2b"
)

const orderTag = "kolo-braid/order/v0"

// Pos is a position in a chain segment.
type Pos struct {
	Seg   int
	Index uint64
}

// Item is one entry of a batch as Merge sees it.
type Item struct {
	ID    ID
	Class Class
	Seg   int
	Index uint64
	// Waits are the positions that must be ordered first, besides the entry before it in its segment: for each clock
	// component the entry raises over its predecessor, the highest entry of that segment in its past, and the predecessor
	// itself when the entry starts a segment of a forked strand.
	Waits []Pos
}

// Run is the part of one segment inside a batch: the entries at indexes Lo to Hi-1, which Next yields in index order.
// Entries of the segment below Lo are ordered before the batch.
type Run struct {
	Lo, Hi uint64
	Next   func() (Item, error)
}

type cand struct {
	Item
	key  [32]byte
	wait int
}

type readyHeap []*cand

func (h readyHeap) Len() int { return len(h) }
func (h readyHeap) Less(i, j int) bool {
	a, b := h[i], h[j]
	if a.Class != b.Class {
		return a.Class < b.Class
	}
	if c := bytes.Compare(a.key[:], b.key[:]); c != 0 {
		return c < 0
	}
	if c := bytes.Compare(a.ID[:], b.ID[:]); c != 0 {
		return c < 0
	}
	return a.Seg < b.Seg
}
func (h readyHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *readyHeap) Push(x any)   { *h = append(*h, x.(*cand)) }
func (h *readyHeap) Pop() any {
	old := *h
	c := old[len(old)-1]
	*h = old[:len(old)-1]
	return c
}

// waiter is a candidate of segment seg parked until its wait index is ordered.
type waiter struct {
	index uint64
	seg   int
}

type waitHeap []waiter

func (h waitHeap) Len() int { return len(h) }
func (h waitHeap) Less(i, j int) bool {
	return h[i].index < h[j].index || h[i].index == h[j].index && h[i].seg < h[j].seg
}
func (h waitHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *waitHeap) Push(x any)   { *h = append(*h, x.(waiter)) }
func (h *waitHeap) Pop() any {
	old := *h
	w := old[len(old)-1]
	*h = old[:len(old)-1]
	return w
}

type merger struct {
	runs   []Run
	cursor []uint64
	cand   []*cand
	waitq  []waitHeap
	ready  readyHeap
	buf    []byte
	prefix int
}

// Merge emits the entries of a batch in Kahn order under seed: each step takes the smallest ready entry by (class, order
// key, entry ID). runs has one Run per segment, indexed by segment number. At most one candidate per segment is held, so
// memory is O(segments) whatever the batch size. A source must not change an Item's Waits before Merge emits the item.
func Merge(runs []Run, seed [32]byte, emit func(Item) error) error {
	m := &merger{runs: runs, cursor: make([]uint64, len(runs)), cand: make([]*cand, len(runs)), waitq: make([]waitHeap, len(runs))}
	var e kolocodec.Encoder
	e.PutString(orderTag)
	e.PutFixed(seed[:])
	m.prefix = len(e.Bytes())
	m.buf = append(e.Bytes(), make([]byte, len(ID{}))...)
	for s, r := range runs {
		if r.Lo > r.Hi || r.Lo < r.Hi && r.Next == nil {
			return fmt.Errorf("%w: run %d", ErrInvalid, s)
		}
		m.cursor[s] = r.Lo
	}
	for s := range runs {
		if err := m.load(s); err != nil {
			return err
		}
	}
	for m.ready.Len() > 0 {
		c := heap.Pop(&m.ready).(*cand)
		if err := emit(c.Item); err != nil {
			return err
		}
		s := c.Seg
		m.cand[s] = nil
		m.cursor[s]++
		for q := &m.waitq[s]; q.Len() > 0 && (*q)[0].index < m.cursor[s]; {
			if err := m.check(heap.Pop(q).(waiter).seg); err != nil {
				return err
			}
		}
		if err := m.load(s); err != nil {
			return err
		}
	}
	for s, r := range runs {
		if m.cursor[s] < r.Hi {
			return fmt.Errorf("%w: segment %d stops at index %d", ErrStuck, s, m.cursor[s])
		}
	}
	return nil
}

func (m *merger) load(s int) error {
	if m.cursor[s] >= m.runs[s].Hi {
		return nil
	}
	it, err := m.runs[s].Next()
	if err != nil {
		return err
	}
	if it.Seg != s || it.Index != m.cursor[s] || it.Class > Other {
		return fmt.Errorf("%w: item at segment %d index %d", ErrInvalid, s, m.cursor[s])
	}
	c := &cand{Item: it}
	copy(m.buf[m.prefix:], it.ID[:])
	c.key = blake2b.Sum256(m.buf)
	m.cand[s] = c
	return m.check(s)
}

// check parks the candidate of segment s on its first wait that is not ordered yet, or makes it ready.
func (m *merger) check(s int) error {
	c := m.cand[s]
	for ; c.wait < len(c.Waits); c.wait++ {
		w := c.Waits[c.wait]
		if w.Seg < 0 || w.Seg >= len(m.runs) {
			return fmt.Errorf("%w: wait on segment %d", ErrInvalid, w.Seg)
		}
		if m.cursor[w.Seg] > w.Index {
			continue
		}
		if w.Seg == s {
			return fmt.Errorf("%w: entry at segment %d index %d waits on its own segment", ErrStuck, s, c.Index)
		}
		heap.Push(&m.waitq[w.Seg], waiter{index: w.Index, seg: s})
		return nil
	}
	heap.Push(&m.ready, c)
	return nil
}

// Order emits the placed entries of P(to) \ P(from) in Kahn order under seed. A nil from is the empty set, a nil to every
// placed entry. Every entry of the range must be held in memory.
func (d *DAG) Order(from, to Clock, seed [32]byte, emit func(ID) error) error {
	if len(from) > len(d.segs) || len(to) > len(d.segs) {
		return fmt.Errorf("%w: clock names unknown segments", ErrInvalid)
	}
	runs := make([]Run, len(d.segs))
	for si, s := range d.segs {
		lo := max(s.start, from.At(si))
		hi := s.end()
		if to != nil {
			hi = max(s.start, to.At(si))
		}
		switch {
		case lo > hi:
			return fmt.Errorf("%w: from is not inside to at segment %d", ErrInvalid, si)
		case hi > s.end():
			return fmt.Errorf("%w: segment %d up to index %d", ErrNotHeld, si, hi)
		case lo < hi && lo < s.released:
			return fmt.Errorf("%w: segment %d from index %d is released", ErrNotHeld, si, lo)
		}
		runs[si] = Run{Lo: lo, Hi: hi, Next: d.items(si, lo)}
	}
	return Merge(runs, seed, func(it Item) error { return emit(it.ID) })
}

func (d *DAG) items(si int, lo uint64) func() (Item, error) {
	s := d.segs[si]
	i := lo
	return func() (Item, error) {
		e := &s.ents[i-s.released]
		it := Item{ID: e.id, Class: e.class, Seg: si, Index: i, Waits: make([]Pos, 0, len(e.delta)+1)}
		if i == s.start && s.parent >= 0 {
			it.Waits = append(it.Waits, Pos{Seg: s.parent, Index: i - 1})
		}
		for _, p := range e.delta {
			it.Waits = append(it.Waits, Pos{Seg: p.Seg, Index: p.Value - 1})
		}
		i++
		return it, nil
	}
}

// Batch emits the settled batch of pivot: P(pivot) \ P(prev) under the pivot's ID; a zero prev is the first batch.
func (d *DAG) Batch(prev, pivot ID, emit func(ID) error) error {
	var from Clock
	if prev != (ID{}) {
		c, ok := d.Clock(prev)
		if !ok {
			return fmt.Errorf("%w: previous pivot", ErrNotHeld)
		}
		from = c
	}
	to, ok := d.Clock(pivot)
	if !ok {
		return fmt.Errorf("%w: pivot", ErrNotHeld)
	}
	return d.Order(from, to, pivot, emit)
}

// Tail returns the tentative tail: the entries outside P(settled), ordered as the batches of the predicted pivots, each in
// the past of the next, then the rest under the group ID. A zero settled is the empty settled prefix.
func (d *DAG) Tail(settled ID, predicted []ID, group [32]byte) ([]ID, error) {
	var out []ID
	add := func(id ID) error { out = append(out, id); return nil }
	prev := settled
	for _, p := range predicted {
		if err := d.Batch(prev, p, add); err != nil {
			return nil, err
		}
		prev = p
	}
	var from Clock
	if prev != (ID{}) {
		c, ok := d.Clock(prev)
		if !ok {
			return nil, fmt.Errorf("%w: settled pivot", ErrNotHeld)
		}
		from = c
	}
	if err := d.Order(from, nil, group, add); err != nil {
		return nil, err
	}
	return out, nil
}

// FirstChange returns the reorg boundary: the first position where two orders differ, or the shorter length when one is a
// prefix of the other.
func FirstChange(a, b []ID) int {
	n := min(len(a), len(b))
	for i := range n {
		if a[i] != b[i] {
			return i
		}
	}
	return n
}
