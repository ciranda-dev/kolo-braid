package settle

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/bits"
	"slices"
	"testing"

	"github.com/ciranda-dev/kolo-braid/internal/dag"
	"golang.org/x/crypto/blake2b"
)

// bitset is a set of entry numbers.
type bitset []uint64

func (b bitset) has(i int) bool { return i/64 < len(b) && b[i/64]&(1<<(i%64)) != 0 }

func (b *bitset) set(i int) {
	for len(*b) <= i/64 {
		*b = append(*b, 0)
	}
	(*b)[i/64] |= 1 << (i % 64)
}

func (b *bitset) or(o bitset) {
	for len(*b) < len(o) {
		*b = append(*b, 0)
	}
	for i, v := range o {
		(*b)[i] |= v
	}
}

func (b bitset) count() int {
	n := 0
	for _, v := range b {
		n += bits.OnesCount64(v)
	}
	return n
}

// ent is one entry of a test world.
type ent struct {
	n        int
	id       dag.ID
	key      int
	strand   int
	index    uint64
	prev     *ent
	links    []*ent
	pulse    bool
	cfg      uint64
	act      *ent
	ready    *[32]byte
	settlers []int
	rule     Rule
	past     bitset
	fake     bool
}

// world is a set of entries; keys and strands are small numbers, and a key's seat strand is its number unless strandOf says
// otherwise.
type world struct {
	ents     []*ent
	byStrand map[int][]*ent
	strandOf map[int]int
	byID     map[dag.ID]*ent
}

func newWorld() *world {
	return &world{byStrand: map[int][]*ent{}, strandOf: map[int]int{}, byID: map[dag.ID]*ent{}}
}

func keyOf(k int) Key {
	var key Key
	binary.BigEndian.PutUint16(key[:], uint16(k+1))
	key[31] = 0x4b
	return key
}

func strandID(s int) dag.StrandID {
	var id dag.StrandID
	binary.BigEndian.PutUint16(id[:], uint16(s+1))
	id[31] = 0x53
	return id
}

func (w *world) seatStrand(k int) int {
	if s, ok := w.strandOf[k]; ok {
		return s
	}
	return k
}

func (w *world) seat(k int) Seat { return Seat{Key: keyOf(k), Strand: strandID(w.seatStrand(k))} }

func (w *world) seats(keys ...int) []Seat {
	out := make([]Seat, 0, len(keys))
	for _, k := range keys {
		out = append(out, w.seat(k))
	}
	slices.SortFunc(out, func(a, b Seat) int { return bytes.Compare(a.Key[:], b.Key[:]) })
	return out
}

// add appends an entry on strand after prev (nil: index 0), linking links (at most one per other strand is kept: the
// highest index).
func (w *world) add(key, strand int, prev *ent, links ...*ent) *ent {
	e := &ent{n: len(w.ents), key: key, strand: strand, prev: prev}
	if prev != nil {
		e.index = prev.index + 1
	}
	var h [8]byte
	binary.BigEndian.PutUint64(h[:], uint64(e.n))
	e.id = blake2b.Sum256(append([]byte("settle-test"), h[:]...))
	best := map[int]*ent{}
	for _, l := range links {
		if l == nil || l.strand == strand {
			continue
		}
		if b := best[l.strand]; b == nil || l.index > b.index {
			best[l.strand] = l
		}
	}
	for _, l := range best {
		e.links = append(e.links, l)
	}
	slices.SortFunc(e.links, func(a, b *ent) int {
		sa, sb := strandID(a.strand), strandID(b.strand)
		return bytes.Compare(sa[:], sb[:])
	})
	e.past.set(e.n)
	if prev != nil {
		e.past.or(prev.past)
	}
	for _, l := range e.links {
		e.past.or(l.past)
	}
	w.ents = append(w.ents, e)
	w.byStrand[strand] = append(w.byStrand[strand], e)
	w.byID[e.id] = e
	return e
}

// pulse adds a pulse of config cfg (activating pivot act) on key's seat strand.
func (w *world) pulse(key int, prev *ent, cfg uint64, act *ent, links ...*ent) *ent {
	return w.pulseOn(key, w.seatStrand(key), prev, cfg, act, links...)
}

func (w *world) pulseOn(key, strand int, prev *ent, cfg uint64, act *ent, links ...*ent) *ent {
	e := w.add(key, strand, prev, links...)
	e.pulse, e.cfg, e.act = true, cfg, act
	return e
}

func (w *world) readiness(key int, prev *ent, cfg uint64, act *ent, id [32]byte, links ...*ent) *ent {
	e := w.pulse(key, prev, cfg, act, links...)
	e.ready = &id
	return e
}

func (w *world) hasSettlers() bool {
	for _, e := range w.ents {
		if e.settlers != nil {
			return true
		}
	}
	return false
}

func (w *world) settlersEntry(key, strand int, prev *ent, rule Rule, keys []int, links ...*ent) *ent {
	e := w.add(key, strand, prev, links...)
	e.settlers, e.rule = keys, rule
	return e
}

func (w *world) node(e *ent) dag.Node {
	n := dag.Node{ID: e.id, Strand: strandID(e.strand), Index: e.index, Class: dag.Other}
	if e.settlers != nil {
		n.Class = dag.Membership
	}
	if e.prev != nil {
		n.Prev = e.prev.id
	}
	for _, l := range e.links {
		n.Links = append(n.Links, dag.Link{Strand: strandID(l.strand), Index: l.index, ID: l.id})
	}
	return n
}

func (e *ent) asPulse() Pulse {
	p := Pulse{ID: e.id, Key: keyOf(e.key), Strand: strandID(e.strand), Config: e.cfg, Ready: e.ready}
	if e.act != nil {
		p.Activating = e.act.id
	}
	return p
}

func (w *world) all() bitset {
	var b bitset
	for _, e := range w.ents {
		b.set(e.n)
	}
	return b
}

// closure returns the union of the pasts of tips.
func closure(tips ...*ent) bitset {
	var b bitset
	for _, t := range tips {
		if t != nil {
			b.or(t.past)
		}
	}
	return b
}

// in returns the entries of D in creation order, which is a linear extension.
func (w *world) in(D bitset) []*ent {
	var out []*ent
	for _, e := range w.ents {
		if D.has(e.n) {
			out = append(out, e)
		}
	}
	return out
}

// roster is the test roster: the proposal is the last Settlers entry in the settled order, or base when there is none.
type roster struct {
	w         *world
	d         *dag.DAG
	base      Proposal
	committed []Pivot
	byID      map[dag.ID]*ent
	calls     int
}

func newRoster(w *world, d *dag.DAG, base Proposal) *roster {
	return &roster{w: w, d: d, base: base, byID: w.byID}
}

func (r *roster) fn(from uint64, pivots []Pivot) ([]Proposal, error) {
	r.calls++
	if from > uint64(len(r.committed)) {
		return nil, fmt.Errorf("roster: from %d beyond %d returned pivots", from, len(r.committed))
	}
	out := make([]Proposal, 0, len(pivots))
	if !r.w.hasSettlers() {
		for range pivots {
			out = append(out, r.base)
		}
		return out, nil
	}
	list := append(slices.Clone(r.committed[:from]), pivots...)
	cur := r.base
	var prev dag.ID
	for i, p := range list {
		err := r.d.Batch(prev, p.ID, func(id dag.ID) error {
			if e := r.byID[id]; e != nil && e.settlers != nil {
				cur = Proposal{Rule: e.rule, Seats: r.w.seats(e.settlers...)}
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		prev = p.ID
		if uint64(i) >= from {
			out = append(out, cur)
		}
	}
	return out, nil
}

// peer is one production settler fed with entries. With t nil, the first error is kept in err and later feeds do nothing.
type peer struct {
	t      testing.TB
	w      *world
	d      *dag.DAG
	s      *Settler
	r      *roster
	pivots []Pivot
	err    error
}

func newPeer(t testing.TB, w *world, cfg Config, anchor *Anchor, dev rules) *peer {
	p, err := newPeerE(w, cfg, anchor, dev)
	if err != nil {
		t.Fatal(err)
	}
	p.t = t
	return p
}

func newPeerE(w *world, cfg Config, anchor *Anchor, dev rules) (*peer, error) {
	d, err := dag.New(dag.Config{MaxLinks: 63, MaxPendingBytes: 1 << 30})
	if err != nil {
		return nil, err
	}
	p := &peer{w: w, d: d}
	p.r = newRoster(w, d, Proposal{Rule: cfg.Rule, Seats: cfg.Seats})
	p.s, err = New(d, p.r.fn, cfg, anchor)
	if err != nil {
		return nil, err
	}
	p.s.dev = dev
	return p, nil
}

func (p *peer) fail(err error) {
	if p.t != nil {
		p.t.Helper()
		p.t.Fatal(err)
	}
	if p.err == nil {
		p.err = err
	}
}

// place adds entries to the DAG and their placed pulses to the settler, without advancing.
func (p *peer) place(es ...*ent) {
	for _, e := range es {
		if p.err != nil {
			return
		}
		placed, err := p.d.Add(p.w.node(e))
		if err != nil {
			p.fail(fmt.Errorf("dag add %d: %w", e.n, err))
			return
		}
		for _, pl := range placed {
			if x := p.w.byID[pl.ID]; x.pulse {
				if err := p.s.Add(x.asPulse()); err != nil {
					p.fail(fmt.Errorf("settle add %d: %w", x.n, err))
					return
				}
			}
		}
	}
}

// feed adds entries in the given order (any order: the DAG waits for missing predecessors) and advances after each.
func (p *peer) feed(es ...*ent) {
	for _, e := range es {
		p.place(e)
		p.advance()
	}
}

func (p *peer) advance() {
	for p.err == nil {
		ps, err := p.s.Advance()
		p.pivots = append(p.pivots, ps...)
		p.r.committed = append(p.r.committed, ps...)
		if err != nil {
			p.fail(err)
			return
		}
		if len(ps) == 0 {
			return
		}
	}
}

func (p *peer) ids() []dag.ID {
	out := make([]dag.ID, len(p.pivots))
	for i, x := range p.pivots {
		out[i] = x.ID
	}
	return out
}

func (p *peer) rounds() []uint64 {
	out := make([]uint64, len(p.pivots))
	for i, x := range p.pivots {
		out[i] = x.Round
	}
	return out
}

func isPrefix(a, b []dag.ID) bool {
	if len(a) > len(b) {
		a, b = b, a
	}
	return slices.Equal(a, b[:len(a)])
}

// ref is the reference settlement, written from the record's definitions over explicit pasts.
type ref struct {
	w      *world
	D      bitset
	roster func(list []dag.ID) Proposal
	rule   Rule
	seats  []refSeat
	cfgN   uint64
	act    *ent
	round  map[int]int
	step   int
}

type refSeat struct{ key, strand int }

type refPivot struct {
	e     *ent
	cfg   uint64
	round int
}

func newRef(w *world, D bitset, rule Rule, keys []int, roster func([]dag.ID) Proposal) *ref {
	r := &ref{w: w, D: D, roster: roster, rule: rule, step: 2}
	r.setSeats(keys)
	return r
}

func (r *ref) setSeats(keys []int) {
	r.seats = nil
	for _, k := range keys {
		r.seats = append(r.seats, refSeat{k, r.w.seatStrand(k)})
	}
	slices.SortFunc(r.seats, func(a, b refSeat) int { return a.key - b.key })
	r.round = map[int]int{}
}

func (r *ref) q() int { return Quorum(r.rule, len(r.seats)) }

func (r *ref) seatOf(e *ent) int {
	for i, s := range r.seats {
		if s.key == e.key && s.strand == e.strand {
			return i
		}
	}
	return -1
}

func (r *ref) counts(e *ent) bool {
	return e.pulse && e.ready == nil && e.cfg == r.cfgN && e.act == r.act && r.seatOf(e) >= 0 && (r.act == nil || e.past.has(r.act.n))
}

// roundOf follows the definition: 1 + the highest r such that the past without e holds round-r pulses of q seats.
func (r *ref) roundOf(e *ent) int {
	if v, ok := r.round[e.n]; ok {
		return v
	}
	by := map[int]map[int]bool{}
	for _, x := range r.w.ents {
		if x == e || !e.past.has(x.n) || !r.counts(x) {
			continue
		}
		rx := r.roundOf(x)
		if by[rx] == nil {
			by[rx] = map[int]bool{}
		}
		by[rx][r.seatOf(x)] = true
	}
	best := -1
	for rr, ss := range by {
		if len(ss) >= r.q() && rr > best {
			best = rr
		}
	}
	r.round[e.n] = best + 1
	return best + 1
}

// vertex is the seat's first counting pulse of round rr inside set.
func (r *ref) vertex(set bitset, seat, rr int) *ent {
	var best *ent
	for _, x := range r.w.ents {
		if !set.has(x.n) || !r.counts(x) || r.seatOf(x) != seat || r.roundOf(x) != rr {
			continue
		}
		if best == nil || x.index < best.index || (x.index == best.index && bytes.Compare(x.id[:], best.id[:]) < 0) {
			best = x
		}
	}
	return best
}

func (r *ref) pivotSeat(rr int) int { return (rr / r.step) % len(r.seats) }

func (r *ref) maxRound(set bitset) int {
	m := -1
	for _, x := range r.w.ents {
		if set.has(x.n) && r.counts(x) && r.roundOf(x) > m {
			m = r.roundOf(x)
		}
	}
	return m
}

func (r *ref) direct(rr int) *ent {
	v := r.vertex(r.D, r.pivotSeat(rr), rr)
	if v == nil {
		return nil
	}
	votes := 0
	for s := range r.seats {
		if x := r.vertex(r.D, s, rr+1); x != nil && x.past.has(v.n) {
			votes++
		}
	}
	if votes >= r.q() {
		return v
	}
	return nil
}

// walk returns the pivots inside P(t) from round 0 up to t.
func (r *ref) walk(t *ent) []*ent {
	out := []*ent{t}
	base := t
	for rr := r.roundOf(t) - r.step; rr >= 0; rr -= r.step {
		if v := r.vertex(base.past, r.pivotSeat(rr), rr); v != nil {
			out = append(out, v)
			base = v
		}
	}
	slices.Reverse(out)
	return out
}

// activates follows the record: a pending config that adds no seat activates at once; one that adds seats activates at
// the first pivot whose past holds a readiness pulse naming it from every added seat, none in the past of since, the last
// pivot before the config became pending.
func (r *ref) activates(p Proposal, c, since *ent) bool {
	cur := make([]Seat, 0, len(r.seats))
	for _, s := range r.seats {
		cur = append(cur, Seat{Key: keyOf(s.key), Strand: strandID(s.strand)})
	}
	if p.Rule == r.rule && slices.Equal(p.Seats, cur) || len(p.Seats) == 0 {
		return false
	}
	cid := configID(r.cfgN+1, p.Rule, p.Seats)
	for _, st := range p.Seats {
		if slices.Contains(cur, st) {
			continue
		}
		ok := false
		for _, x := range r.w.ents {
			if c.past.has(x.n) && x.ready != nil && *x.ready == cid && keyOf(x.key) == st.Key && strandID(x.strand) == st.Strand &&
				(r.act == nil || !r.act.past.has(x.n)) && (since == nil || !since.past.has(x.n)) {
				ok = true
			}
		}
		if !ok {
			return false
		}
	}
	return true
}

// pendingOf is the pending config a proposal makes: nil when it is the current config.
func (r *ref) pendingOf(p Proposal) *Proposal {
	cur := make([]Seat, 0, len(r.seats))
	for _, s := range r.seats {
		cur = append(cur, Seat{Key: keyOf(s.key), Strand: strandID(s.strand)})
	}
	if p.Rule == r.rule && slices.Equal(p.Seats, cur) {
		return nil
	}
	return &p
}

func keysOf(seats []Seat) []int {
	var out []int
	for _, s := range seats {
		out = append(out, int(binary.BigEndian.Uint16(s.Key[:]))-1)
	}
	return out
}

// committed is the static committed list of D: per config, the walk from the highest directly committed pivot, then the
// activation rules over it.
func (r *ref) committed() []refPivot {
	var out []refPivot
	var ids []dag.ID
	for {
		var top *ent
		for rr := r.maxRound(r.D); rr >= 0; rr-- {
			if rr%r.step != 0 {
				continue
			}
			if v := r.direct(rr); v != nil {
				top = v
				break
			}
		}
		if top == nil {
			return out
		}
		next := false
		var pend *Proposal
		since, before := (*ent)(nil), r.act
		for _, c := range r.walk(top) {
			out = append(out, refPivot{e: c, cfg: r.cfgN, round: r.roundOf(c)})
			ids = append(ids, c.id)
			p := r.roster(ids)
			if cur := r.pendingOf(p); !sameProposal(cur, pend) {
				pend, since = cur, before
			}
			before = c
			if r.activates(p, c, since) {
				r.cfgN++
				r.act = c
				r.rule = p.Rule
				r.setSeats(keysOf(p.Seats))
				next = true
				break
			}
		}
		if !next {
			return out
		}
	}
}

func refIDs(l []refPivot) []dag.ID {
	out := make([]dag.ID, len(l))
	for i, p := range l {
		out[i] = p.e.id
	}
	return out
}

// staticRoster is a roster that never changes the config.
func staticRoster(p Proposal) func([]dag.ID) Proposal { return func([]dag.ID) Proposal { return p } }

// dagRoster orders the batches with the production DAG, as roster.fn does.
func dagRoster(t testing.TB, w *world, D bitset, base Proposal) func([]dag.ID) Proposal {
	d, err := dag.New(dag.Config{MaxLinks: 63, MaxPendingBytes: 1 << 30})
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range w.in(D) {
		if _, err := d.Add(w.node(e)); err != nil {
			t.Fatal(err)
		}
	}
	r := newRoster(w, d, base)
	return func(ids []dag.ID) Proposal {
		var ps []Pivot
		for _, id := range ids {
			ps = append(ps, Pivot{ID: id})
		}
		out, err := r.fn(0, ps)
		if err != nil {
			t.Fatal(err)
		}
		return out[len(out)-1]
	}
}

func cfg0(w *world, rule Rule, keys ...int) Config {
	return Config{Number: 0, Rule: rule, Seats: w.seats(keys...)}
}

func seq(n int) []int {
	out := make([]int, n)
	for i := range out {
		out[i] = i
	}
	return out
}

// lockstep writes rounds of pulses: in each step every seat in order writes one pulse linking the newest pulse of every
// other seat from the previous step. It returns the heads after the last step.
func (w *world) lockstep(keys []int, heads map[int]*ent, steps int, cfg uint64, act *ent, extra ...*ent) map[int]*ent {
	for range steps {
		prev := map[int]*ent{}
		for k, v := range heads {
			prev[k] = v
		}
		for _, k := range keys {
			var links []*ent
			for _, o := range keys {
				if o != k && prev[o] != nil {
					links = append(links, prev[o])
				}
			}
			links = append(links, extra...)
			heads[k] = w.pulse(k, prev[k], cfg, act, links...)
		}
		extra = nil
	}
	return heads
}
