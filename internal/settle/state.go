package settle

import (
	"bytes"
	"fmt"
	"math"
	"slices"

	"github.com/ciranda-dev/kolo-braid/internal/dag"
	kolocodec "github.com/ciranda-dev/kolo-codec"
)

const stateTag = "kolo-braid/settle/v1"

// Save encodes the state a restart needs: the current config, the committed count, the pulses later decisions read and the
// pending config. Load with the restored DAG continues from it; pulses added again after Load are no-ops.
func (s *Settler) Save() []byte {
	var e kolocodec.Encoder
	e.PutString(stateTag)
	e.PutUvarint(s.count)
	putConfig(&e, s.cfg)
	e.PutBool(s.act != nil)
	if s.act != nil {
		putPos(&e, s.act)
		putClock(&e, s.act.clock)
	}
	e.PutBool(s.last != nil)
	if s.last != nil {
		e.PutFixed(s.last.id[:])
	}
	for _, st := range s.seats {
		e.PutUvarint(uint64(len(st.recs)))
		for _, r := range st.recs {
			e.PutFixed(r.id[:])
			putPos(&e, r)
			e.PutUvarint(r.round)
			e.PutBool(r.clock != nil)
			if r.clock != nil {
				putClock(&e, r.clock)
			}
		}
		segs := make([]int, 0, len(st.floor))
		for seg := range st.floor {
			segs = append(segs, seg)
		}
		slices.Sort(segs)
		e.PutUvarint(uint64(len(segs)))
		for _, seg := range segs {
			e.PutUvarint(uint64(seg))
			e.PutUvarint(st.floor[seg])
			// round marks are stored by the caller (TakeMarks); a state saved before carries them here
			e.PutUvarint(0)
		}
	}
	e.PutUvarint(uint64(len(s.future)))
	for _, r := range s.future {
		putPulse(&e, r)
		e.PutUvarint(r.cfg)
		e.PutFixed(r.act[:])
		putClock(&e, r.clock)
	}
	e.PutUvarint(uint64(len(s.ready)))
	for _, r := range s.ready {
		putPulse(&e, r)
		e.PutFixed(r.ready[:])
	}
	e.PutBool(s.pending != nil)
	if s.pending != nil {
		e.PutUvarint(uint64(s.pending.Rule))
		putSeats(&e, s.pending.Seats)
		putClock(&e, s.pendAt)
	}
	ev := make([]dag.StrandID, 0, len(s.evid))
	for st := range s.evid {
		ev = append(ev, st)
	}
	slices.SortFunc(ev, func(a, b dag.StrandID) int { return bytes.Compare(a[:], b[:]) })
	e.PutUvarint(uint64(len(ev)))
	for _, st := range ev {
		e.PutFixed(st[:])
	}
	e.PutBool(s.anchor != nil)
	if s.anchor != nil {
		e.PutFixed(s.anchor.ID[:])
		e.PutUvarint(s.anchor.Config)
	}
	e.PutUvarint(s.certFloor)
	e.PutBool(s.stop != nil)
	if s.stop != nil {
		e.PutUvarint(uint64(s.stop.Reason))
		e.PutUvarint(s.stop.Config)
		e.PutFixed(s.stop.Key[:])
	}
	return e.Bytes()
}

func putConfig(e *kolocodec.Encoder, c Config) {
	e.PutUvarint(c.Number)
	e.PutUvarint(uint64(c.Rule))
	putSeats(e, c.Seats)
	e.PutFixed(c.Activating[:])
}

func putSeats(e *kolocodec.Encoder, seats []Seat) {
	e.PutUvarint(uint64(len(seats)))
	for _, st := range seats {
		e.PutFixed(st.Key[:])
		e.PutFixed(st.Strand[:])
	}
}

func putPos(e *kolocodec.Encoder, r *rec) {
	e.PutUvarint(uint64(r.seg))
	e.PutUvarint(r.index)
}

func putPulse(e *kolocodec.Encoder, r *rec) {
	e.PutFixed(r.id[:])
	e.PutFixed(r.key[:])
	e.PutFixed(r.strand[:])
	putPos(e, r)
}

func putClock(e *kolocodec.Encoder, c dag.Clock) {
	e.PutUvarint(uint64(len(c)))
	for _, v := range c {
		e.PutUvarint(v)
	}
}

type reader struct {
	d   *kolocodec.Decoder
	err error
}

func (r *reader) fail(why string) {
	if r.err == nil {
		r.err = fmt.Errorf("%w: %s", ErrMalformed, why)
	}
}

func (r *reader) uvarint() uint64 {
	if r.err != nil {
		return 0
	}
	v, err := r.d.Uvarint()
	if err != nil {
		r.fail(err.Error())
	}
	return v
}

// count reads a length that each element of at least min bytes must fit in the rest of the input.
func (r *reader) count(min int) int {
	v := r.uvarint()
	if r.err == nil && v > uint64(r.d.Len()/min) {
		r.fail("count over the input")
		return 0
	}
	return int(v)
}

func (r *reader) small(max uint64) int {
	v := r.uvarint()
	if v > max {
		r.fail("value out of range")
		return 0
	}
	return int(v)
}

func (r *reader) boolean() bool {
	if r.err != nil {
		return false
	}
	v, err := r.d.Bool()
	if err != nil {
		r.fail(err.Error())
	}
	return v
}

func (r *reader) fixed(dst []byte) {
	if r.err != nil {
		return
	}
	b, err := r.d.Fixed(len(dst))
	if err != nil {
		r.fail(err.Error())
		return
	}
	copy(dst, b)
}

func (r *reader) seats() []Seat {
	n := r.count(64)
	seats := make([]Seat, n)
	for i := range seats {
		r.fixed(seats[i].Key[:])
		r.fixed(seats[i].Strand[:])
	}
	return seats
}

func (r *reader) pos(x *rec) {
	x.seg = r.small(math.MaxInt32)
	x.index = r.uvarint()
}

func (r *reader) pulse() *rec {
	x := &rec{seat: -1}
	r.fixed(x.id[:])
	r.fixed(x.key[:])
	r.fixed(x.strand[:])
	r.pos(x)
	return x
}

func (r *reader) clock() dag.Clock {
	n := r.count(1)
	c := make(dag.Clock, n)
	for i := range c {
		c[i] = r.uvarint()
	}
	return c
}

// loadSeat reads the pulses, floors and round marks of seat i.
func (r *reader) loadSeat(s *Settler, i int, st *seatState, cfg Config) {
	n := r.count(36)
	for j := 0; j < n && r.err == nil; j++ {
		x := &rec{key: cfg.Seats[i].Key, strand: cfg.Seats[i].Strand, cfg: cfg.Number, act: cfg.Activating, seat: i}
		r.fixed(x.id[:])
		r.pos(x)
		x.round = r.uvarint()
		if r.boolean() {
			x.clock = r.clock()
		}
		if _, dup := s.byID[x.id]; dup {
			r.fail("pulse saved twice")
		}
		if len(st.recs) > 0 && st.recs[0].seg != x.seg {
			st.forked = true
		}
		st.recs = append(st.recs, x)
		st.rounds[x.round] = append(st.rounds[x.round], x)
		s.byID[x.id] = x
		if !s.hasRound || x.round > s.maxRound {
			s.maxRound, s.hasRound = x.round, true
		}
	}
	nf := r.count(2)
	prev := -1
	for j := 0; j < nf && r.err == nil; j++ {
		seg := r.small(math.MaxInt32)
		if seg <= prev {
			r.fail("floors not ascending")
		}
		prev = seg
		st.floor[seg] = r.uvarint()
		nm := r.count(2)
		var l []mark
		for k := 0; k < nm && r.err == nil; k++ {
			m := mark{index: r.uvarint(), round: r.uvarint()}
			if k > 0 && (m.index <= l[k-1].index || m.round <= l[k-1].round) {
				r.fail("round marks not ascending")
			}
			l = append(l, m)
		}
		if len(l) > 0 {
			st.old[seg] = l
		}
		// a state saved before marks were stored apart: hand them to the caller to store
		for _, m := range l {
			s.fresh = append(s.fresh, Mark{Config: cfg.Number, Seat: i, Seg: seg, Index: m.index, Round: m.round})
		}
	}
}

// loadProposal reads the pending proposal that follows a true flag.
func (r *reader) loadProposal(s *Settler) {
	p := &Proposal{Rule: Rule(r.small(255))}
	p.Seats = r.seats()
	s.pendAt = r.clock()
	if r.err == nil {
		if err := checkRule(p.Rule); err != nil {
			r.fail(err.Error())
		} else if err := checkSeats(p.Seats); err != nil {
			r.fail(err.Error())
		} else if s.proposalIf(*p) == nil {
			r.fail("pending proposal equals the config")
		}
	}
	s.pending = p
}

// loadWaiting reads the pulses of later configs and the readiness pulses.
func (r *reader) loadWaiting(s *Settler, cfg Config) {
	nf := r.count(64 + 32 + 32 + 4)
	for j := 0; j < nf && r.err == nil; j++ {
		x := r.pulse()
		x.cfg = r.uvarint()
		r.fixed(x.act[:])
		x.clock = r.clock()
		if _, dup := s.byID[x.id]; dup {
			r.fail("pulse saved twice")
		}
		if r.err == nil && x.cfg <= cfg.Number {
			r.fail("future pulse of a config that is not later")
		}
		s.future = append(s.future, x)
		s.byID[x.id] = x
	}
	nr := r.count(32*4 + 2)
	for j := 0; j < nr && r.err == nil; j++ {
		x := r.pulse()
		var cid [32]byte
		r.fixed(cid[:])
		x.ready = &cid
		if _, dup := s.byID[x.id]; dup {
			r.fail("pulse saved twice")
		}
		s.ready = append(s.ready, x)
		s.byID[x.id] = x
	}
}

// loadTail reads the evidence strands, the anchor, the certificate floor and the halt.
func (r *reader) loadTail(s *Settler) {
	ne := r.count(32)
	var prevStrand *dag.StrandID
	for j := 0; j < ne && r.err == nil; j++ {
		var st dag.StrandID
		r.fixed(st[:])
		if prevStrand != nil && bytes.Compare(prevStrand[:], st[:]) >= 0 {
			r.fail("evidence strands not ascending")
		}
		prevStrand = &st
		s.evid[st] = true
	}
	if r.boolean() {
		s.anchor = &Anchor{}
		r.fixed(s.anchor.ID[:])
		s.anchor.Config = r.uvarint()
	}
	s.certFloor = r.uvarint()
	if r.boolean() {
		h := &HaltError{Reason: Reason(r.small(uint64(Conflict)))}
		h.Config = r.uvarint()
		r.fixed(h.Key[:])
		if r.err == nil && h.Reason == 0 {
			r.fail("halt reason")
		}
		s.stop = h
	}
}

// Load restores a settler from Save's output; d is the DAG restored with the same segment table.
func Load(d *dag.DAG, roster Roster, b []byte) (*Settler, error) {
	if d == nil || roster == nil {
		return nil, fmt.Errorf("%w: nil dag or roster", ErrInvalid)
	}
	r := &reader{d: kolocodec.NewDecoder(b)}
	if tag, err := r.d.String(len(stateTag)); err != nil || tag != stateTag {
		return nil, fmt.Errorf("%w: tag", ErrMalformed)
	}
	s := &Settler{d: d, roster: roster, byID: map[dag.ID]*rec{}, evid: map[dag.StrandID]bool{}}
	s.count = r.uvarint()
	var cfg Config
	cfg.Number = r.uvarint()
	cfg.Rule = Rule(r.small(255))
	cfg.Seats = r.seats()
	r.fixed(cfg.Activating[:])
	if r.err == nil {
		if err := checkConfig(cfg); err != nil {
			r.fail(err.Error())
		}
	}
	if r.err != nil {
		return nil, r.err
	}
	var act *rec
	if r.boolean() {
		act = &rec{seat: -1}
		r.pos(act)
		act.clock = r.clock()
		act.id = cfg.Activating
	}
	if r.err == nil && (act != nil) != (cfg.Number > 0) {
		r.fail("activating pivot position")
	}
	s.setConfig(cfg, act)
	hasLast := r.boolean()
	var lastID dag.ID
	if hasLast {
		r.fixed(lastID[:])
	}
	for i, st := range s.seats {
		r.loadSeat(s, i, st, cfg)
	}
	if hasLast && r.err == nil {
		s.last = s.byID[lastID]
		if s.last == nil {
			r.fail("last pivot not among the saved pulses")
		}
	}
	r.loadWaiting(s, cfg)
	if r.boolean() {
		r.loadProposal(s)
	}
	r.loadTail(s)
	if r.err == nil {
		if err := r.d.Done(); err != nil {
			r.fail(err.Error())
		}
	}
	if r.err != nil {
		return nil, r.err
	}
	for _, x := range s.future {
		if x.cfg == cfg.Number+1 {
			s.claim(x)
		}
	}
	return s, nil
}
