// Package dag holds the causal DAG of a braid and computes its deterministic total order.
//
// Each strand is a chain by index; a strand whose writer signed two histories is a tree of chain segments. Clocks are
// derived from links and kept per entry as the components it raises over its strand predecessor, with a full clock every
// 256 entries of a strand. The order of a batch is Kahn's topological sort run as a merge of segments, picking the
// smallest ready entry by (class, order key under the batch seed, entry ID).
package dag

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"slices"
)

// Errors of this package.
var (
	ErrInvalid     = errors.New("kolobraid: invalid dag input")
	ErrConflict    = errors.New("kolobraid: entry ID held with other fields")
	ErrPendingFull = errors.New("kolobraid: pending entries over their bound")
	ErrNotHeld     = errors.New("kolobraid: entry or range not held in memory")
	ErrStuck       = errors.New("kolobraid: batch entry can never be ready")
	ErrForkLimit   = errors.New("kolobraid: strand holds the most forks")
	ErrMalformed   = errors.New("kolobraid: malformed index record")
	// ErrOutsideTable is a record that names a segment the segment table does not hold, as one of an entry placed after the last
	// stored table does.
	ErrOutsideTable = errors.New("kolobraid: segment not in the table")
)

const (
	// checkpointEvery keeps a full clock at strand indexes that are multiples of 256, so a clock is rebuilt from at most 255 deltas.
	checkpointEvery = 256
	// MaxLinksLimit is the largest MaxLinks: MaxMembers is at most 1024.
	MaxLinksLimit = 1023
	// pendingNodeCost and pendingLinkCost are the measured heap bytes of one pending node and of each of its links, rounded up.
	pendingNodeCost = 512
	pendingLinkCost = 256
)

// ID is an entry ID.
type ID [32]byte

// StrandID is the kolo-log log ID of a strand.
type StrandID [32]byte

// Link names the entry at Index of Strand with the given ID.
type Link struct {
	Strand StrandID
	Index  uint64
	ID     ID
}

// Class is the first key of the order: Membership entries go before Other entries that are ready at the same time.
type Class uint8

const (
	// Membership is the class of well-formed AddMember, RemoveMember, Settlers and Evidence entries.
	Membership Class = 0
	// Other is every other entry, malformed ones included.
	Other Class = 1
)

// Node is what the DAG needs of one decoded entry.
type Node struct {
	ID     ID
	Strand StrandID
	Index  uint64
	// Prev is the ID of the strand predecessor at Index-1; zero at index 0.
	Prev ID
	// Links are at most MaxLinks, strictly ascending by strand, none on Strand; a malformed entry passes none.
	Links []Link
	Class Class
	// Size is the decoded size of the entry, counted against MaxPendingBytes while it waits.
	Size int
}

// Clock maps a segment number to 1 + the highest index of that segment in an entry's past, 0 when none is.
type Clock []uint64

// At returns c[seg], 0 beyond its length.
func (c Clock) At(seg int) uint64 {
	if seg < 0 || seg >= len(c) {
		return 0
	}
	return c[seg]
}

// Comp is one clock component.
type Comp struct {
	Seg   int
	Value uint64
}

// Lookup finds a placed entry that Release took out of memory: its segment and clock, ok false when it is not held.
type Lookup func(l Link) (seg int, c Clock, ok bool, err error)

// Segment describes one chain segment for a restart: entries below Released are only reachable through Lookup.
type Segment struct {
	Strand StrandID
	// Start is the index of the segment's first entry.
	Start uint64
	// Parent is the segment of the entry at Start-1, -1 for a segment that starts at index 0.
	Parent int
	// Released is the first index not released; Base is the clock of the entry at Released-1, nil when Released is Start.
	Released uint64
	Base     Clock
	// First is the ID of the segment's first entry, zero when unknown: a restored segment with nothing released is used
	// again when that entry is added, so segment numbers stay the same across a restart.
	First ID
}

// Config sets the bounds of a DAG.
type Config struct {
	// MaxLinks is MaxMembers - 1 of the group, 1 to MaxLinksLimit.
	MaxLinks int
	// MaxPendingBytes bounds the nodes that wait for a predecessor.
	MaxPendingBytes int64
	// MaxPendingPerStrand bounds the pending bytes of one strand, so one strand cannot take the whole of MaxPendingBytes; a
	// strand with nothing pending may always park one entry. 0 for no bound.
	MaxPendingPerStrand int64
	// MaxForks is how many chain segments a strand may hold besides its first, the entries of more are refused; 0 for no bound.
	MaxForks int
	// Lookup is needed once entries are released; nil otherwise.
	Lookup Lookup
	// Segments restores the table that Segments returned before a restart; nil for a new DAG.
	Segments []Segment
}

// Placed reports one entry that became placed.
type Placed struct {
	ID    ID
	Seg   int
	Index uint64
	// Record is the entry's encoded index record for durable storage; see DecodeRecord.
	Record []byte
}

type ent struct {
	id    ID
	class Class
	// delta holds the components raised over the predecessor's clock, the own segment excluded.
	delta []Comp
	// full is set at checkpoints and on the first entry of a segment.
	full Clock
}

type segment struct {
	strand   StrandID
	first    ID
	start    uint64
	parent   int
	released uint64
	base     Clock
	ents     []ent
	head     Clock
}

func (s *segment) end() uint64 { return s.released + uint64(len(s.ents)) }

type ref struct {
	seg   int
	index uint64
}

type pend struct {
	n       Node
	missing int
	cost    int64
}

// DAG is the placed entries of a braid with their clocks, and the entries that wait for a predecessor. It is not safe for
// concurrent use.
type DAG struct {
	cfg          Config
	segs         []*segment
	byStrand     map[StrandID][]int
	byID         map[ID]ref
	pending      map[ID]*pend
	waiters      map[Link][]ID
	pendingBytes int64
	strandBytes  map[StrandID]int64
	refused      []ID
}

// New returns an empty DAG, or one with the segment table of an earlier run.
func New(cfg Config) (*DAG, error) {
	if cfg.MaxLinks < 1 || cfg.MaxLinks > MaxLinksLimit || cfg.MaxPendingBytes < 1 || cfg.MaxPendingPerStrand < 0 || cfg.MaxForks < 0 {
		return nil, fmt.Errorf("%w: config", ErrInvalid)
	}
	d := &DAG{cfg: cfg, byStrand: map[StrandID][]int{}, byID: map[ID]ref{}, pending: map[ID]*pend{}, waiters: map[Link][]ID{},
		strandBytes: map[StrandID]int64{}}
	for i, s := range cfg.Segments {
		if err := checkSegment(i, len(cfg.Segments), s); err != nil {
			return nil, err
		}
		if s.Released > s.Start && cfg.Lookup == nil {
			return nil, fmt.Errorf("%w: released segment without lookup", ErrInvalid)
		}
		base := slices.Clone(s.Base)
		d.segs = append(d.segs, &segment{strand: s.Strand, first: s.First, start: s.Start, parent: s.Parent, released: s.Released, base: base, head: base})
		d.byStrand[s.Strand] = append(d.byStrand[s.Strand], i)
	}
	d.cfg.Segments = nil
	return d, nil
}

func checkSegment(i, n int, s Segment) error {
	ok := s.Parent >= -1 && s.Parent < i && (s.Parent == -1) == (s.Start == 0) && s.Released >= s.Start &&
		s.Released < math.MaxUint64 && (s.Released == s.Start) == (s.Base == nil)
	if ok && s.Base != nil {
		ok = len(s.Base) <= n && s.Base.At(i) == s.Released
	}
	if !ok {
		return fmt.Errorf("%w: segment %d", ErrInvalid, i)
	}
	return nil
}

// Segments returns the segment table for a restart: what Release has taken out of memory, nothing that is still held only
// in memory.
func (d *DAG) Segments() []Segment {
	out := make([]Segment, len(d.segs))
	for i, s := range d.segs {
		out[i] = Segment{Strand: s.strand, Start: s.start, Parent: s.parent, Released: s.released, Base: slices.Clone(s.base), First: s.first}
	}
	return out
}

func (d *DAG) check(n *Node) error {
	var why string
	switch {
	case n.ID == ID{}:
		why = "zero entry ID"
	case n.Class > Other:
		why = "unknown class"
	case n.Index == math.MaxUint64:
		why = "index out of range"
	case (n.Index == 0) != (n.Prev == ID{}):
		why = "prev is zero only at index 0"
	case len(n.Links) > d.cfg.MaxLinks:
		why = "too many links"
	case n.Size < 0:
		why = "negative size"
	}
	for i, l := range n.Links {
		if why != "" {
			break
		}
		if l.Strand == n.Strand {
			why = "link to the entry's own strand"
		} else if i > 0 && bytes.Compare(n.Links[i-1].Strand[:], l.Strand[:]) >= 0 {
			why = "links not strictly ascending by strand"
		}
	}
	if why != "" {
		return fmt.Errorf("%w: %s", ErrInvalid, why)
	}
	return nil
}

func preds(n *Node) []Link {
	p := make([]Link, 0, len(n.Links)+1)
	if n.Index > 0 {
		p = append(p, Link{Strand: n.Strand, Index: n.Index - 1, ID: n.Prev})
	}
	return append(p, n.Links...)
}

// Add stores n. It returns the entries that became placed, n first when its predecessors are all placed, then entries that
// waited for it; none when n waits or is already held. An entry linking an entry that is never held is never placed. An
// entry that would open more segments of its strand than MaxForks allows is not placed and not kept: TakeRefused returns
// it. After a Lookup error the entries placed so far are returned.
func (d *DAG) Add(n Node) ([]Placed, error) {
	if err := d.check(&n); err != nil {
		return nil, err
	}
	if dup, err := d.held(&n); dup || err != nil {
		return nil, err
	}
	var missing []Link
	for _, l := range preds(&n) {
		_, _, ok, err := d.resolve(l)
		if err != nil {
			return nil, err
		}
		if !ok {
			missing = append(missing, l)
		}
	}
	if len(missing) > 0 {
		cost := int64(n.Size) + pendingNodeCost + int64(len(n.Links))*pendingLinkCost
		if d.pendingBytes+cost > d.cfg.MaxPendingBytes {
			return nil, ErrPendingFull
		}
		if q, have := d.cfg.MaxPendingPerStrand, d.strandBytes[n.Strand]; q > 0 && have > 0 && have+cost > q {
			return nil, ErrPendingFull
		}
		n.Links = slices.Clone(n.Links)
		d.pending[n.ID] = &pend{n: n, missing: len(missing), cost: cost}
		d.pendingBytes += cost
		d.strandBytes[n.Strand] += cost
		for _, l := range missing {
			d.waiters[l] = append(d.waiters[l], n.ID)
		}
		return nil, nil
	}
	var out []Placed
	queue := []Node{n}
	for len(queue) > 0 {
		m := queue[0]
		queue = queue[1:]
		p, err := d.place(&m)
		if errors.Is(err, ErrForkLimit) {
			d.refused = append(d.refused, m.ID)
			continue
		}
		if err != nil {
			return out, err
		}
		out = append(out, p)
		key := Link{Strand: m.Strand, Index: m.Index, ID: m.ID}
		for _, w := range d.waiters[key] {
			pw := d.pending[w]
			if pw.missing--; pw.missing == 0 {
				delete(d.pending, w)
				d.pendingBytes -= pw.cost
				if d.strandBytes[pw.n.Strand] -= pw.cost; d.strandBytes[pw.n.Strand] == 0 {
					delete(d.strandBytes, pw.n.Strand)
				}
				queue = append(queue, pw.n)
			}
		}
		delete(d.waiters, key)
	}
	return out, nil
}

// held reports whether n is already held, placed or pending, and refuses another entry with its ID.
func (d *DAG) held(n *Node) (bool, error) {
	if r, ok := d.byID[n.ID]; ok {
		s := d.segs[r.seg]
		if s.strand != n.Strand || r.index != n.Index || s.ents[r.index-s.released].class != n.Class {
			return false, ErrConflict
		}
		return true, nil
	}
	if p, ok := d.pending[n.ID]; ok {
		if p.n.Strand != n.Strand || p.n.Index != n.Index || p.n.Prev != n.Prev || p.n.Class != n.Class || !slices.Equal(p.n.Links, n.Links) {
			return false, ErrConflict
		}
		return true, nil
	}
	_, _, ok, err := d.released(Link{Strand: n.Strand, Index: n.Index, ID: n.ID})
	return ok, err
}

func (d *DAG) resolve(l Link) (int, Clock, bool, error) {
	if r, ok := d.byID[l.ID]; ok {
		if d.segs[r.seg].strand != l.Strand || r.index != l.Index {
			return 0, nil, false, nil
		}
		return r.seg, d.clockAt(r.seg, r.index), true, nil
	}
	return d.released(l)
}

func (d *DAG) released(l Link) (int, Clock, bool, error) {
	if d.cfg.Lookup == nil {
		return 0, nil, false, nil
	}
	found := false
	for _, si := range d.byStrand[l.Strand] {
		if s := d.segs[si]; l.Index >= s.start && l.Index < s.released {
			found = true
		}
	}
	if !found {
		return 0, nil, false, nil
	}
	seg, c, ok, err := d.cfg.Lookup(l)
	if err != nil || !ok {
		return 0, nil, false, err
	}
	if seg < 0 || seg >= len(d.segs) || len(c) > len(d.segs) || d.segs[seg].strand != l.Strand || c.At(seg) != l.Index+1 {
		return 0, nil, false, fmt.Errorf("%w: lookup answer does not fit the segment table", ErrInvalid)
	}
	// a stored record of an entry that is not released is stale after a restart: that entry is added again
	if l.Index < d.segs[seg].start || l.Index >= d.segs[seg].released {
		return 0, nil, false, nil
	}
	return seg, slices.Clone(c), true, nil
}

// clockAt rebuilds the clock of the held entry at index of segment si from the nearest full clock.
func (d *DAG) clockAt(si int, index uint64) Clock {
	s := d.segs[si]
	if index+1 == s.end() {
		return slices.Clone(s.head)
	}
	off := int(index - s.released)
	k := off
	for k >= 0 && s.ents[k].full == nil {
		k--
	}
	var c Clock
	if k >= 0 {
		c = slices.Clone(s.ents[k].full)
	} else {
		c = slices.Clone(s.base)
	}
	for j := k + 1; j <= off; j++ {
		for _, p := range s.ents[j].delta {
			c = raise(c, p.Seg, p.Value)
		}
		c = raise(c, si, s.released+uint64(j)+1)
	}
	return c
}

// raise returns c with c[seg] at least v; c is owned by the caller.
func raise(c Clock, seg int, v uint64) Clock {
	if seg >= len(c) {
		if v == 0 {
			return c
		}
		c = append(c, make(Clock, seg+1-len(c))...)
	}
	if c[seg] < v {
		c[seg] = v
	}
	return c
}

// place adds n, whose predecessors are all placed, to its segment: the one of its predecessor when n extends it, else a new one.
func (d *DAG) place(n *Node) (Placed, error) {
	var prev Clock
	si, parent := -1, -1
	if n.Index > 0 {
		ps, pc, ok, err := d.resolve(Link{Strand: n.Strand, Index: n.Index - 1, ID: n.Prev})
		if err != nil {
			return Placed{}, err
		}
		if !ok {
			return Placed{}, fmt.Errorf("%w: predecessor no longer held", ErrInvalid)
		}
		prev = pc
		if r := d.restored(n, ps); r >= 0 {
			si, parent = -1, ps
		} else if d.segs[ps].end() == n.Index {
			si = ps
		} else {
			parent = ps
		}
	}
	c := slices.Clone(prev)
	for _, l := range n.Links {
		_, lc, ok, err := d.resolve(l)
		if err != nil {
			return Placed{}, err
		}
		if !ok {
			return Placed{}, fmt.Errorf("%w: link target no longer held", ErrInvalid)
		}
		for seg, v := range lc {
			c = raise(c, seg, v)
		}
	}
	first := si < 0
	if first {
		si = d.restored(n, parent)
		if si < 0 {
			if d.ForkFull(n.Strand) {
				return Placed{}, ErrForkLimit
			}
			si = len(d.segs)
			d.segs = append(d.segs, &segment{strand: n.Strand, first: n.ID, start: n.Index, parent: parent, released: n.Index})
			d.byStrand[n.Strand] = append(d.byStrand[n.Strand], si)
		}
	}
	c = raise(c, si, n.Index+1)
	var delta []Comp
	for seg, v := range c {
		if seg != si && v > prev.At(seg) {
			delta = append(delta, Comp{Seg: seg, Value: v})
		}
	}
	e := ent{id: n.ID, class: n.Class, delta: delta}
	full := first || n.Index%checkpointEvery == 0
	if full {
		e.full = c
	}
	s := d.segs[si]
	s.ents = append(s.ents, e)
	s.head = c
	d.byID[n.ID] = ref{seg: si, index: n.Index}
	rec := Record{Seg: si, Index: n.Index, Full: full, Comps: delta}
	if full {
		rec.Comps = comps(c, si)
	}
	return Placed{ID: n.ID, Seg: si, Index: n.Index, Record: rec.Encode()}, nil
}

// ForkFull reports whether a strand holds all the segments MaxForks allows, so an entry that needs another is refused.
func (d *DAG) ForkFull(strand StrandID) bool {
	return d.cfg.MaxForks > 0 && len(d.byStrand[strand]) > d.cfg.MaxForks
}

// TakeRefused returns the entries that Add refused with ErrForkLimit since the last call, in the order they were refused.
func (d *DAG) TakeRefused() []ID {
	out := d.refused
	d.refused = nil
	return out
}

// restored returns the empty restored segment that n starts, -1 when there is none.
func (d *DAG) restored(n *Node, parent int) int {
	for _, si := range d.byStrand[n.Strand] {
		s := d.segs[si]
		if s.first == n.ID && s.start == n.Index && s.parent == parent && s.released == s.start && len(s.ents) == 0 {
			return si
		}
	}
	return -1
}

func comps(c Clock, own int) []Comp {
	var out []Comp
	for seg, v := range c {
		if seg != own && v > 0 {
			out = append(out, Comp{Seg: seg, Value: v})
		}
	}
	return out
}

// Clock returns the clock of a placed entry held in memory.
func (d *DAG) Clock(id ID) (Clock, bool) {
	r, ok := d.byID[id]
	if !ok {
		return nil, false
	}
	return d.clockAt(r.seg, r.index), true
}

// Pos returns the segment and index of a placed entry held in memory.
func (d *DAG) Pos(id ID) (seg int, index uint64, ok bool) {
	r, ok := d.byID[id]
	return r.seg, r.index, ok
}

// Contains reports whether the placed entry id, held in memory, is in the past that c describes.
func (d *DAG) Contains(c Clock, id ID) bool {
	r, ok := d.byID[id]
	return ok && c.At(r.seg) > r.index
}

// Forked reports whether a strand has more than one chain segment that holds or held an entry: two histories.
func (d *DAG) Forked(strand StrandID) bool {
	n := 0
	for _, si := range d.byStrand[strand] {
		if s := d.segs[si]; s.end() > s.start {
			n++
		}
	}
	return n > 1
}

// Len returns the number of placed entries held in memory.
func (d *DAG) Len() int { return len(d.byID) }

// Pending returns the number of entries that wait and the bytes counted for them.
func (d *DAG) Pending() (int, int64) { return len(d.pending), d.pendingBytes }

// Unresolved returns the entries that pending entries wait for and that are not held at all, ascending by strand, index and ID.
func (d *DAG) Unresolved() []Link {
	var out []Link
	for l := range d.waiters {
		if _, ok := d.pending[l.ID]; !ok {
			out = append(out, l)
		}
	}
	slices.SortFunc(out, func(a, b Link) int {
		if c := bytes.Compare(a.Strand[:], b.Strand[:]); c != 0 {
			return c
		}
		if a.Index != b.Index {
			if a.Index < b.Index {
				return -1
			}
			return 1
		}
		return bytes.Compare(a.ID[:], b.ID[:])
	})
	return out
}

// Ends returns, per segment, 1 + the highest index placed in it, 0 for a segment with nothing placed: the clock of the set of
// every placed entry.
func (d *DAG) Ends() Clock {
	out := make(Clock, len(d.segs))
	for i, s := range d.segs {
		out[i] = s.end()
	}
	return out
}

// WaitingStrands returns the strands of the pending entries that wait for l, ascending and without repeats.
func (d *DAG) WaitingStrands(l Link) []StrandID {
	var out []StrandID
	for _, id := range d.waiters[l] {
		if p, ok := d.pending[id]; ok {
			out = append(out, p.n.Strand)
		}
	}
	slices.SortFunc(out, func(a, b StrandID) int { return bytes.Compare(a[:], b[:]) })
	return slices.Compact(out)
}

// Release takes the placed entries in the past that frontier describes out of memory: the settled prefix. frontier must be
// downward-closed and held; entries released earlier stay released. Their clocks come from Lookup from then on.
func (d *DAG) Release(frontier Clock) error {
	if d.cfg.Lookup == nil {
		return fmt.Errorf("%w: release without lookup", ErrInvalid)
	}
	if len(frontier) > len(d.segs) {
		return fmt.Errorf("%w: frontier names unknown segments", ErrInvalid)
	}
	upto := make([]uint64, len(d.segs))
	for si, s := range d.segs {
		r := max(s.released, frontier.At(si))
		if r > s.end() {
			return fmt.Errorf("%w: frontier beyond segment %d", ErrNotHeld, si)
		}
		upto[si] = r
		if r > s.released {
			for seg, v := range d.clockAt(si, r-1) {
				if v > frontier.At(seg) && v > d.segs[seg].released {
					return fmt.Errorf("%w: frontier is not downward-closed", ErrInvalid)
				}
			}
		}
	}
	for si, s := range d.segs {
		k := int(upto[si] - s.released)
		if k == 0 {
			continue
		}
		s.base = d.clockAt(si, upto[si]-1)
		for _, e := range s.ents[:k] {
			delete(d.byID, e.id)
		}
		s.ents = slices.Clone(s.ents[k:])
		s.released = upto[si]
	}
	return nil
}
