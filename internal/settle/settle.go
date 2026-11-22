// Package settle decides the settled prefix of a braid: rounds of pulses, pivots on even rounds, votes, direct commits and
// the commit walk, config changes with readiness, closure certificates, settling inside trusted pivots, and the halt when a
// seat of the current config signs two histories.
package settle

import (
	"bytes"
	"cmp"
	"errors"
	"fmt"
	"slices"

	"github.com/ciranda-dev/kolo-braid/internal/dag"
	kolocodec "github.com/ciranda-dev/kolo-codec"
	"golang.org/x/crypto/blake2b"
)

// Errors of this package.
var (
	ErrInvalid   = errors.New("kolobraid: invalid settlement input")
	ErrNotPlaced = errors.New("kolobraid: pulse is not placed in memory")
	ErrHalted    = errors.New("kolobraid: settlement halted")
	ErrMalformed = errors.New("kolobraid: malformed settlement state")
)

const configTag = "kolo-braid/config/v0"

// Key is a settler's public key.
type Key [32]byte

// Rule is a quorum rule; the values are those of the group config.
type Rule uint8

const (
	// Majority needs floor(n/2) + 1 seats.
	Majority Rule = 1
	// TwoThirds needs floor(2n/3) + 1 seats.
	TwoThirds Rule = 2
)

// Quorum returns q for n seats under rule.
func Quorum(rule Rule, n int) int {
	if rule == TwoThirds {
		return 2*n/3 + 1
	}
	return n/2 + 1
}

// Seat is a settler key with the strand that counts for it in a config.
type Seat struct {
	Key    Key
	Strand dag.StrandID
}

// Config is a settler config: config 0 is the founder's seat, every later one starts after its activating pivot's batch.
type Config struct {
	Number uint64
	Rule   Rule
	// Seats are strictly ascending by key.
	Seats []Seat
	// Activating is the pivot after whose batch the config starts; zero for config 0.
	Activating dag.ID
}

// Quorum returns q of the config.
func (c Config) Quorum() int { return Quorum(c.Rule, len(c.Seats)) }

// ID returns the config ID that readiness pulses name.
func (c Config) ID() [32]byte { return configID(c.Number, c.Rule, c.Seats) }

func configID(n uint64, rule Rule, seats []Seat) [32]byte {
	var e kolocodec.Encoder
	e.PutString(configTag)
	e.PutUvarint(n)
	e.PutUvarint(uint64(rule))
	for _, s := range seats {
		e.PutFixed(s.Key[:])
		e.PutFixed(s.Strand[:])
	}
	return blake2b.Sum256(e.Bytes())
}

// Proposal is the config the roster gives at the end of a settled batch: the settler keys of the last effective Settlers
// entry with their active strands and its rule. Seats may be empty: such a config never activates.
type Proposal struct {
	Rule  Rule
	Seats []Seat
}

// Pulse is what settlement needs of a placed pulse entry.
type Pulse struct {
	ID     dag.ID
	Key    Key
	Strand dag.StrandID
	// Config and Activating are the body's claims; Activating is zero for config 0.
	Config     uint64
	Activating dag.ID
	// Ready is the config ID of a readiness pulse, nil for any other pulse.
	Ready *[32]byte
}

// Pivot is a committed pivot.
type Pivot struct {
	ID     dag.ID
	Config uint64
	Round  uint64
	// Activates is set when config Config+1 starts after this pivot's batch.
	Activates bool
}

// Anchor is a pivot trusted to be committed, with its config number: the welcome's last committed pivot of the inviter.
type Anchor struct {
	ID     dag.ID
	Config uint64
}

// Roster returns the proposal at the end of the batch of each pivot. from is the number of pivots committed before
// pivots[0]; pivots extend that list. It must be a pure function of the committed list: settle also calls it for pivots it
// checks before committing them.
type Roster func(from uint64, pivots []Pivot) ([]Proposal, error)

// Reason says why settlement halted.
type Reason uint8

const (
	// SeatFork is a seat of the current config that signed two histories.
	SeatFork Reason = iota + 1
	// TwoCertificates is two closure certificates of one config that name different pivots.
	TwoCertificates
	// BadTrust is an anchor whose pivots do not follow the rules: not a pivot, a broken chain or another activation.
	BadTrust
	// Conflict is a walk that would not keep the committed list as its prefix.
	Conflict
)

// HaltError reports a halt and the key that caused it.
type HaltError struct {
	Reason Reason
	Config uint64
	Key    Key
}

// Error describes the halt.
func (e *HaltError) Error() string {
	why := map[Reason]string{SeatFork: "a seat signed two histories", TwoCertificates: "two closure certificates name different pivots",
		BadTrust: "a trusted pivot does not follow the rules", Conflict: "a walk conflicts with the committed pivots"}[e.Reason]
	return fmt.Sprintf("%v: config %d: %s", ErrHalted, e.Config, why)
}

// Unwrap returns ErrHalted.
func (e *HaltError) Unwrap() error { return ErrHalted }

// rules holds deliberate deviations used only by the negative controls of the model checks; the zero value is the rule.
type rules struct {
	everyRound  bool // a pivot on every round
	byKey       bool // pulses count by key, whatever their strand
	noHalt      bool // keep settling a config whose seat signed two histories
	globalWalk  bool // the walk takes a seat's first vertex in the whole DAG instead of inside the base's past
	certNoVotes bool // a closure certificate needs no votes of the config it closes
}

type rec struct {
	id     dag.ID
	key    Key
	strand dag.StrandID
	seg    int
	index  uint64
	clock  dag.Clock
	cfg    uint64
	act    dag.ID
	ready  *[32]byte
	round  uint64
	seat   int
}

func in(c dag.Clock, r *rec) bool { return c.At(r.seg) > r.index }

type seatState struct {
	recs   []*rec
	rounds map[uint64][]*rec
	// floor holds, per segment, the index below which records were pruned; old keeps, per segment and ascending, the
	// index of the first pruned pulse of each round, which is what a later pulse on a second history may still need.
	floor  map[int]uint64
	old    map[int][]mark
	forked bool
}

type mark struct{ index, round uint64 }

// Mark is a round mark of the current config: the index of the first pulse of a pruned round on one segment of a seat.
// Marks grow with the rounds of a config and are not part of Save: the caller stores each one TakeMarks hands out and gives
// them back with RestoreMarks after Load.
type Mark struct {
	Config       uint64
	Seat, Seg    int
	Index, Round uint64
}

// TakeMarks returns the round marks made since the last call, in the order they were made.
func (s *Settler) TakeMarks() []Mark {
	out := s.fresh
	s.fresh = nil
	return out
}

// Marks returns every round mark of the current config, by seat, segment and index.
func (s *Settler) Marks() []Mark {
	var out []Mark
	for i, st := range s.seats {
		segs := make([]int, 0, len(st.old))
		for seg := range st.old {
			segs = append(segs, seg)
		}
		slices.Sort(segs)
		for _, seg := range segs {
			for _, m := range st.old[seg] {
				out = append(out, Mark{Config: s.cfg.Number, Seat: i, Seg: seg, Index: m.index, Round: m.round})
			}
		}
	}
	return out
}

// RestoreMarks adds stored round marks of the current config after Load; one already held is skipped.
func (s *Settler) RestoreMarks(ms []Mark) error {
	for _, m := range ms {
		if m.Config != s.cfg.Number || m.Seat < 0 || m.Seat >= len(s.seats) || m.Seg < 0 {
			return fmt.Errorf("%w: round mark of config %d seat %d", ErrMalformed, m.Config, m.Seat)
		}
		st := s.seats[m.Seat]
		l := st.old[m.Seg]
		byIndex := func(x mark, i uint64) int { return cmp.Compare(x.index, i) }
		if k, ok := slices.BinarySearchFunc(l, m.Index, byIndex); ok && l[k].round == m.Round {
			continue
		}
		if n := len(l); n > 0 && (m.Index <= l[n-1].index || m.Round <= l[n-1].round) {
			return fmt.Errorf("%w: round marks not ascending", ErrMalformed)
		}
		st.old[m.Seg] = append(l, mark{m.Index, m.Round})
	}
	return nil
}

// oldHighest is the round of the seat's latest pruned pulse in the past c describes.
func (st *seatState) oldHighest(c dag.Clock) (uint64, bool) {
	var m uint64
	found := false
	for seg, l := range st.old {
		k, _ := slices.BinarySearchFunc(l, c.At(seg), func(x mark, b uint64) int {
			if x.index < b {
				return -1
			}
			return 1
		})
		if k > 0 && (!found || l[k-1].round > m) {
			m, found = l[k-1].round, true
		}
	}
	return m, found
}

func (st *seatState) oldHas(c dag.Clock, r uint64) bool {
	for seg, l := range st.old {
		k, ok := slices.BinarySearchFunc(l, r, func(x mark, b uint64) int {
			if x.round < b {
				return -1
			}
			if x.round > b {
				return 1
			}
			return 0
		})
		if ok && c.At(seg) > l[k].index {
			return true
		}
	}
	return false
}

type cand struct {
	claims  []*rec
	dirty   bool
	checked bool
	at      uint64
	ok      bool
	path    []*rec
	props   []Proposal
	next    Config
}

// Settler holds the settlement state of one braid. It is not safe for concurrent use.
type Settler struct {
	d      *dag.DAG
	roster Roster
	dev    rules

	cfg      Config
	q        int
	act      *rec
	seatOf   map[Seat]int
	keyOf    map[Key]int
	seats    []*seatState
	maxRound uint64
	hasRound bool
	count    uint64
	last     *rec
	byID     map[dag.ID]*rec
	future   []*rec
	ready    []*rec
	pending  *Proposal
	// pendAt is the clock of the last pivot committed before the pending config became pending: readiness in its past is
	// older than the config and never counts for it.
	pendAt dag.Clock
	evid   map[dag.StrandID]bool
	anchor *Anchor
	// certFloor is the lowest config number for which closure certificates are used.
	certFloor uint64
	cands     map[dag.ID]*cand
	// fresh holds the round marks made since the last TakeMarks.
	fresh []Mark
	stop  *HaltError
	out   []Pivot
}

// New returns a settler that starts at cfg with no pivot committed in it: config 0 for a new braid or a joiner. A joiner
// passes its welcome anchor; it then commits nothing before the anchor's pivots are placed, and settles every config
// through the anchor inside the anchor's past.
func New(d *dag.DAG, roster Roster, cfg Config, anchor *Anchor) (*Settler, error) {
	if d == nil || roster == nil {
		return nil, fmt.Errorf("%w: nil dag or roster", ErrInvalid)
	}
	if err := checkConfig(cfg); err != nil {
		return nil, err
	}
	if cfg.Number > 0 {
		return nil, fmt.Errorf("%w: a settler starts at config 0 or from a saved state", ErrInvalid)
	}
	s := &Settler{d: d, roster: roster, byID: map[dag.ID]*rec{}, evid: map[dag.StrandID]bool{}}
	s.setConfig(cloneConfig(cfg), nil)
	if anchor != nil {
		a := *anchor
		s.anchor = &a
		s.certFloor = a.Config + 1
	}
	return s, nil
}

func cloneConfig(c Config) Config {
	c.Seats = slices.Clone(c.Seats)
	return c
}

func checkSeats(seats []Seat) error {
	strands := map[dag.StrandID]bool{}
	for i, st := range seats {
		if i > 0 && bytes.Compare(seats[i-1].Key[:], st.Key[:]) >= 0 {
			return fmt.Errorf("%w: seats not strictly ascending by key", ErrInvalid)
		}
		if strands[st.Strand] {
			return fmt.Errorf("%w: two seats on one strand", ErrInvalid)
		}
		strands[st.Strand] = true
	}
	return nil
}

func checkRule(r Rule) error {
	if r != Majority && r != TwoThirds {
		return fmt.Errorf("%w: quorum rule %d", ErrInvalid, r)
	}
	return nil
}

func checkConfig(c Config) error {
	if err := checkRule(c.Rule); err != nil {
		return err
	}
	if len(c.Seats) == 0 {
		return fmt.Errorf("%w: config without seats", ErrInvalid)
	}
	if (c.Number == 0) != (c.Activating == dag.ID{}) {
		return fmt.Errorf("%w: activating pivot is zero exactly in config 0", ErrInvalid)
	}
	return checkSeats(c.Seats)
}

func (s *Settler) setConfig(c Config, act *rec) {
	s.cfg, s.q, s.act = c, c.Quorum(), act
	s.seatOf, s.keyOf = map[Seat]int{}, map[Key]int{}
	s.seats = make([]*seatState, len(c.Seats))
	for i, st := range c.Seats {
		s.seatOf[st] = i
		s.keyOf[st.Key] = i
		s.seats[i] = &seatState{rounds: map[uint64][]*rec{}, floor: map[int]uint64{}, old: map[int][]mark{}}
	}
	s.maxRound, s.hasRound, s.last = 0, false, nil
	s.cands = map[dag.ID]*cand{}
	s.fresh = nil
}

// Config returns the current config.
func (s *Settler) Config() Config { return cloneConfig(s.cfg) }

// Count returns the number of committed pivots, every config included.
func (s *Settler) Count() uint64 { return s.count }

// Last returns the last committed pivot of the current config; ok is false when none is.
func (s *Settler) Last() (Pivot, bool) {
	if s.last == nil {
		return Pivot{}, false
	}
	return Pivot{ID: s.last.id, Config: s.cfg.Number, Round: s.last.round}, true
}

// Pending returns the pending config's proposal; ok is false when none is pending.
func (s *Settler) Pending() (Proposal, bool) {
	if s.pending == nil {
		return Proposal{}, false
	}
	return Proposal{Rule: s.pending.Rule, Seats: slices.Clone(s.pending.Seats)}, true
}

// Awaiting returns the seats a pending config adds without a readiness pulse naming it that counts; those that cannot count
// are dropped with the batch that makes it pending.
func (s *Settler) Awaiting() []Seat {
	if s.pending == nil {
		return nil
	}
	cid := configID(s.cfg.Number+1, s.pending.Rule, s.pending.Seats)
	var out []Seat
	for _, st := range s.pending.Seats {
		if _, ok := s.seatOf[st]; ok {
			continue
		}
		seen := false
		for _, r := range s.ready {
			seen = seen || (r.key == st.Key && r.strand == st.Strand && *r.ready == cid)
		}
		if !seen {
			out = append(out, st)
		}
	}
	return out
}

// Equivocated records evidence against a strand: two verified objects signed by its key.
func (s *Settler) Equivocated(strand dag.StrandID) { s.evid[strand] = true }

// Halted returns the reason settlement stopped, nil while it goes on. A seat fork of the current config clears when a
// closure certificate for that config is used, and stops nothing while an anchor is pending, since the anchor settles it.
func (s *Settler) Halted() error {
	if s.stop != nil {
		return s.stop
	}
	if st, ok := s.seatFork(); ok && s.anchor == nil {
		return &HaltError{Reason: SeatFork, Config: s.cfg.Number, Key: st.Key}
	}
	return nil
}

func (s *Settler) seatFork() (Seat, bool) {
	if s.dev.noHalt {
		return Seat{}, false
	}
	for _, st := range s.cfg.Seats {
		if s.evid[st.Strand] || s.d.Forked(st.Strand) {
			return st, true
		}
	}
	return Seat{}, false
}

// Add records a placed pulse. Every placed pulse must be added before the next Advance; adding one twice is a no-op.
func (s *Settler) Add(p Pulse) error {
	if _, ok := s.byID[p.ID]; ok {
		return nil
	}
	seg, index, ok := s.d.Pos(p.ID)
	if !ok {
		return ErrNotPlaced
	}
	c, _ := s.d.Clock(p.ID)
	r := &rec{id: p.ID, key: p.Key, strand: p.Strand, seg: seg, index: index, clock: c, cfg: p.Config, act: p.Activating, seat: -1}
	before := s.act != nil && in(s.act.clock, r)
	if p.Ready != nil {
		// an honest readiness pulse for a later config is written after the current config's activating pivot
		if before {
			return nil
		}
		cid := *p.Ready
		r.ready = &cid
		if s.last != nil && in(s.last.clock, r) && (!s.keepReady(r) || slices.ContainsFunc(s.ready, func(x *rec) bool {
			return x.key == r.key && x.strand == r.strand && in(s.last.clock, x)
		})) {
			return nil
		}
		s.ready = append(s.ready, r)
		s.byID[r.id] = r
		return nil
	}
	settled := before || (s.last != nil && in(s.last.clock, r))
	switch {
	case p.Config < s.cfg.Number:
	case p.Config > s.cfg.Number:
		// a pulse of a later config in the settled prefix can never count: its activating pivot would be settled before it
		if !settled {
			s.future = append(s.future, r)
			s.byID[r.id] = r
			if p.Config == s.cfg.Number+1 {
				s.claim(r)
			}
		}
	default:
		s.addCurrent(r)
	}
	return nil
}

func (s *Settler) seatIndex(key Key, strand dag.StrandID) (int, bool) {
	if s.dev.byKey {
		i, ok := s.keyOf[key]
		return i, ok
	}
	i, ok := s.seatOf[Seat{Key: key, Strand: strand}]
	return i, ok
}

func (s *Settler) addCurrent(r *rec) {
	i, ok := s.seatIndex(r.key, r.strand)
	if !ok || r.act != s.cfg.Activating || (s.act != nil && !in(r.clock, s.act)) {
		delete(s.byID, r.id)
		return
	}
	st := s.seats[i]
	if f, ok := st.floor[r.seg]; ok && r.index < f {
		delete(s.byID, r.id)
		return
	}
	r.seat = i
	r.round = s.roundOf(r.clock)
	if len(st.recs) > 0 && st.recs[0].seg != r.seg {
		st.forked = true
	}
	st.recs = append(st.recs, r)
	st.rounds[r.round] = append(st.rounds[r.round], r)
	s.byID[r.id] = r
	if !s.hasRound || r.round > s.maxRound {
		s.maxRound, s.hasRound = r.round, true
	}
	// a vote can complete a certificate whose claims are all held
	for _, c := range s.cands {
		c.dirty = true
	}
}

// highest returns the highest round of a counting pulse in the past c describes.
func (s *Settler) highest(c dag.Clock) (uint64, bool) {
	var m uint64
	found := false
	for _, st := range s.seats {
		seen := false
		for i := len(st.recs) - 1; i >= 0; i-- {
			x := st.recs[i]
			if !in(c, x) {
				continue
			}
			seen = true
			if !found || x.round > m {
				m, found = x.round, true
			}
			// rounds never decrease along one history
			if !st.forked {
				break
			}
		}
		if !seen || st.forked {
			if r, ok := st.oldHighest(c); ok && (!found || r > m) {
				m, found = r, true
			}
		}
	}
	return m, found
}

func (s *Settler) quorumAt(c dag.Clock, r uint64) bool {
	n := 0
	for _, st := range s.seats {
		has := false
		for _, x := range st.rounds[r] {
			if in(c, x) {
				has = true
				break
			}
		}
		if has || st.oldHas(c, r) {
			n++
		}
	}
	return n >= s.q
}

// roundOf is round(p) for a pulse whose past without itself is c: a pulse of round m in that past already gives q seats at
// m-1, so the answer is m or m+1.
func (s *Settler) roundOf(c dag.Clock) uint64 {
	m, ok := s.highest(c)
	if !ok {
		return 0
	}
	if s.quorumAt(c, m) {
		return m + 1
	}
	return m
}

// Round returns the round that a counting pulse of the current config would get with the past c describes, itself
// excluded.
func (s *Settler) Round(c dag.Clock) uint64 { return s.roundOf(c) }

// Highest returns the highest round of a counting pulse of the current config in the past c describes.
func (s *Settler) Highest(c dag.Clock) (uint64, bool) { return s.highest(c) }

// SeatOf returns the seat number of a key and strand in the current config.
func (s *Settler) SeatOf(key Key, strand dag.StrandID) (int, bool) { return s.seatIndex(key, strand) }

func (s *Settler) step() uint64 {
	if s.dev.everyRound {
		return 1
	}
	return 2
}

// PivotSeat returns the seat number of round r's pivot seat; false when r is not a pivot round.
func (s *Settler) PivotSeat(r uint64) (int, bool) {
	if r%s.step() != 0 {
		return 0, false
	}
	return s.pivotSeat(r), true
}

func (s *Settler) pivotSeat(r uint64) int { return int((r / s.step()) % uint64(len(s.seats))) }

// Vertex returns a seat's round-r vertex: its first counting pulse of round r in the current config.
func (s *Settler) Vertex(seat int, r uint64) (dag.ID, bool) {
	if seat < 0 || seat >= len(s.seats) {
		return dag.ID{}, false
	}
	if v := s.vertex(seat, r); v != nil {
		return v.id, true
	}
	return dag.ID{}, false
}

func (s *Settler) vertex(seat int, r uint64) *rec {
	if l := s.seats[seat].rounds[r]; len(l) > 0 {
		return l[0]
	}
	return nil
}

// vertexIn is the seat's first round-r pulse inside the past c describes; on a forked strand the lowest index, then ID.
func (s *Settler) vertexIn(c dag.Clock, seat int, r uint64) *rec {
	var best *rec
	for _, x := range s.seats[seat].rounds[r] {
		if !in(c, x) {
			continue
		}
		if best == nil || x.index < best.index || (x.index == best.index && bytes.Compare(x.id[:], best.id[:]) < 0) {
			best = x
		}
	}
	return best
}

func (s *Settler) votes(v *rec, r uint64) int {
	n := 0
	for i := range s.seats {
		if x := s.vertex(i, r+1); x != nil && in(x.clock, v) {
			n++
		}
	}
	return n
}

// highestDirect returns the highest directly committed pivot above the last committed round.
func (s *Settler) highestDirect() *rec {
	if !s.hasRound || s.maxRound == 0 {
		return nil
	}
	st := s.step()
	r := s.maxRound - 1
	r -= r % st
	for {
		if s.last != nil && r <= s.last.round {
			return nil
		}
		if v := s.vertex(s.pivotSeat(r), r); v != nil && s.votes(v, r) >= s.q {
			return v
		}
		if r < st {
			return nil
		}
		r -= st
	}
}

// inside returns, ascending, the pivots of the current config that the walk from t inside P(t) commits above the last
// committed one, t last; ok is false when t is not a pivot vertex or the walk would not keep the committed list.
func (s *Settler) inside(t *rec) ([]*rec, bool) {
	if t.seat < 0 || t.ready != nil || t.cfg != s.cfg.Number || t.round%s.step() != 0 || s.pivotSeat(t.round) != t.seat ||
		s.vertexIn(t.clock, t.seat, t.round) != t || (s.last != nil && t.round <= s.last.round) {
		return nil, false
	}
	st := s.step()
	path, base := []*rec{t}, t
	for r := t.round; r >= st; {
		r -= st
		if s.last != nil && r <= s.last.round {
			break
		}
		v := s.vertexIn(base.clock, s.pivotSeat(r), r)
		if s.dev.globalWalk {
			if v = s.vertex(s.pivotSeat(r), r); v != nil && !in(base.clock, v) {
				v = nil
			}
		}
		if v != nil {
			path = append(path, v)
			base = v
		}
	}
	if s.last != nil && s.vertexIn(base.clock, s.last.seat, s.last.round) != s.last {
		return nil, false
	}
	slices.Reverse(path)
	return path, true
}

func (s *Settler) pivots(path []*rec) []Pivot {
	out := make([]Pivot, len(path))
	for i, p := range path {
		out[i] = Pivot{ID: p.id, Config: s.cfg.Number, Round: p.round}
	}
	return out
}

func (s *Settler) propose(path []*rec) ([]Proposal, error) {
	props, err := s.roster(s.count, s.pivots(path))
	if err != nil {
		return nil, err
	}
	if len(props) != len(path) {
		return nil, fmt.Errorf("%w: roster gave %d proposals for %d pivots", ErrInvalid, len(props), len(path))
	}
	for _, p := range props {
		if err := checkRule(p.Rule); err != nil {
			return nil, err
		}
		if err := checkSeats(p.Seats); err != nil {
			return nil, err
		}
	}
	return props, nil
}

func (s *Settler) proposalIf(p Proposal) *Proposal {
	if p.Rule == s.cfg.Rule && slices.Equal(p.Seats, s.cfg.Seats) {
		return nil
	}
	return &Proposal{Rule: p.Rule, Seats: slices.Clone(p.Seats)}
}

// replay runs the pending config rules over path and returns the index of the pivot the next config activates at, -1
// when none, with the pending proposal after each pivot and its pendAt.
func (s *Settler) replay(path []*rec, props []Proposal) (int, []*Proposal, []dag.Clock) {
	pends := make([]*Proposal, len(path))
	ats := make([]dag.Clock, len(path))
	cur, at := s.pending, s.pendAt
	var before dag.Clock
	if s.last != nil {
		before = s.last.clock
	} else if s.act != nil {
		before = s.act.clock
	}
	for i, p := range path {
		pends[i] = s.proposalIf(props[i])
		if !sameProposal(pends[i], cur) {
			cur, at = pends[i], before
		}
		if cur == nil {
			at = nil
		}
		ats[i] = at
		if s.activates(p, pends[i], at) {
			return i, pends, ats
		}
		before = p.clock
	}
	return -1, pends, ats
}

func sameProposal(a, b *Proposal) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Rule == b.Rule && slices.Equal(a.Seats, b.Seats)
}

// keepReady reports whether a settled readiness pulse can still count: it names the pending config, comes from a seat that
// config adds and is not older than the config (pendAt).
func (s *Settler) keepReady(r *rec) bool {
	if s.pending == nil || in(s.pendAt, r) {
		return false
	}
	st := Seat{Key: r.key, Strand: r.strand}
	if _, cur := s.seatOf[st]; cur || !slices.Contains(s.pending.Seats, st) {
		return false
	}
	return *r.ready == configID(s.cfg.Number+1, s.pending.Rule, s.pending.Seats)
}

// activates reports whether pend activates at p: it adds no seat, or p's past holds a readiness pulse naming it from every
// seat it adds, outside since, the past of the last pivot before it became pending.
func (s *Settler) activates(p *rec, pend *Proposal, since dag.Clock) bool {
	if pend == nil || len(pend.Seats) == 0 {
		return false
	}
	cid := configID(s.cfg.Number+1, pend.Rule, pend.Seats)
	for _, st := range pend.Seats {
		if _, ok := s.seatOf[st]; ok {
			continue
		}
		ok := false
		for _, r := range s.ready {
			if r.key == st.Key && r.strand == st.Strand && *r.ready == cid && in(p.clock, r) && !in(since, r) {
				ok = true
				break
			}
		}
		if !ok {
			return false
		}
	}
	return true
}

type trust uint8

const (
	direct trust = iota
	// closes: the next config activates exactly at the last pivot of the path.
	closes
	// reaches: the last pivot of the path is committed, so no activation comes before it.
	reaches
)

// commit commits path in order under the activation rules, which a trusted path must agree with.
func (s *Settler) commit(path []*rec, props []Proposal, t trust) {
	at, pends, ats := s.replay(path, props)
	n := len(path)
	if at >= 0 {
		n = at + 1
	}
	if (t == closes && at != len(path)-1) || (t == reaches && at >= 0 && at != len(path)-1) {
		s.halt(BadTrust, path[len(path)-1].key)
		return
	}
	for i := 0; i < n; i++ {
		p := path[i]
		s.count++
		s.last = p
		s.pending, s.pendAt = pends[i], ats[i]
		s.out = append(s.out, Pivot{ID: p.id, Config: s.cfg.Number, Round: p.round, Activates: i == at})
	}
	if at >= 0 {
		s.activate(path[at], *pends[at])
		return
	}
	s.prune()
}

func (s *Settler) halt(r Reason, key Key) {
	s.stop = &HaltError{Reason: r, Config: s.cfg.Number, Key: key}
}

func (s *Settler) activate(p *rec, pend Proposal) {
	old := s.seats
	next := Config{Number: s.cfg.Number + 1, Rule: pend.Rule, Seats: slices.Clone(pend.Seats), Activating: p.id}
	s.pending, s.pendAt = nil, nil
	for _, st := range old {
		for _, r := range st.recs {
			delete(s.byID, r.id)
		}
	}
	act := *p
	s.setConfig(next, &act)
	s.ready = slices.DeleteFunc(s.ready, func(r *rec) bool {
		drop := in(p.clock, r)
		if drop {
			delete(s.byID, r.id)
		}
		return drop
	})
	fut := s.future
	s.future = nil
	for _, r := range fut {
		switch {
		case in(p.clock, r):
			delete(s.byID, r.id)
		case r.cfg == next.Number:
			s.addCurrent(r)
		default:
			s.future = append(s.future, r)
			if r.cfg == next.Number+1 {
				s.claim(r)
			}
		}
	}
}

// prune drops pulses no vote or walk reads again: rounds below the last committed one and below every seat's latest round.
// Their round marks stay, so a pulse of a second history that arrives later still gets its exact round.
func (s *Settler) prune() {
	if s.last == nil {
		return
	}
	thr := s.last.round
	for _, st := range s.seats {
		if len(st.recs) == 0 {
			thr = 0
		} else if r := st.recs[len(st.recs)-1].round; r < thr {
			thr = r
		}
	}
	for i, st := range s.seats {
		if st.forked {
			continue
		}
		k := 0
		for k < len(st.recs) && st.recs[k].round < thr {
			r := st.recs[k]
			if r.index+1 > st.floor[r.seg] {
				st.floor[r.seg] = r.index + 1
			}
			if l := st.old[r.seg]; len(l) == 0 || l[len(l)-1].round != r.round {
				st.old[r.seg] = append(l, mark{r.index, r.round})
				s.fresh = append(s.fresh, Mark{Config: s.cfg.Number, Seat: i, Seg: r.seg, Index: r.index, Round: r.round})
			}
			delete(s.byID, r.id)
			delete(st.rounds, r.round)
			k++
		}
		st.recs = slices.Delete(st.recs, 0, k)
		for _, r := range st.recs {
			if r.round+1 < s.last.round {
				r.clock = nil
			}
		}
	}
	s.future = slices.DeleteFunc(s.future, func(r *rec) bool {
		drop := in(s.last.clock, r)
		if drop {
			delete(s.byID, r.id)
		}
		return drop
	})
	// a settled claim names a settled pivot, which no certificate can close
	for id, c := range s.cands {
		if c.claims = slices.DeleteFunc(c.claims, func(r *rec) bool { return in(s.last.clock, r) }); len(c.claims) == 0 {
			delete(s.cands, id)
		}
	}
	// settled readiness that cannot count again; one that can per added seat is enough, since every later pivot holds it
	kept := map[Seat]bool{}
	s.ready = slices.DeleteFunc(s.ready, func(r *rec) bool {
		if !in(s.last.clock, r) {
			return false
		}
		st := Seat{Key: r.key, Strand: r.strand}
		if kept[st] || !s.keepReady(r) {
			delete(s.byID, r.id)
			return true
		}
		kept[st] = true
		return false
	})
}

func (s *Settler) claim(r *rec) {
	c := s.cands[r.act]
	if c == nil {
		c = &cand{}
		s.cands[r.act] = c
	}
	c.claims = append(c.claims, r)
	c.dirty = true
}

// Advance commits the next pivots that the pulses now held allow and returns them in order; call it again until it returns
// none, so that every Roster call sees the pivots returned before it. A Roster error is returned as is.
func (s *Settler) Advance() ([]Pivot, error) {
	for {
		ok, err := s.advance()
		if err != nil || !ok || len(s.out) > 0 {
			out := s.out
			s.out = nil
			return out, err
		}
	}
}

func (s *Settler) advance() (bool, error) {
	if s.stop != nil {
		return false, nil
	}
	if s.anchor != nil {
		return s.trusted()
	}
	if s.cfg.Number >= s.certFloor {
		c, err := s.certified()
		if err != nil || s.stop != nil {
			return false, err
		}
		if c != nil {
			s.commit(c.path, c.props, closes)
			return s.stop == nil, nil
		}
	}
	if _, ok := s.seatFork(); ok {
		return false, nil
	}
	v := s.highestDirect()
	if v == nil {
		return false, nil
	}
	path, ok := s.inside(v)
	if !ok {
		s.halt(Conflict, v.key)
		return false, nil
	}
	props, err := s.propose(path)
	if err != nil {
		return false, err
	}
	s.commit(path, props, direct)
	return true, nil
}

// trusted settles the current config inside the anchor's chain of activating pivots.
func (s *Settler) trusted() (bool, error) {
	t := s.byID[s.anchor.ID]
	if t == nil {
		if _, _, ok := s.d.Pos(s.anchor.ID); ok {
			s.halt(BadTrust, Key{})
		}
		return false, nil
	}
	if t.cfg != s.anchor.Config || t.ready != nil {
		s.halt(BadTrust, t.key)
		return false, nil
	}
	cur := t
	for cur.cfg > s.cfg.Number {
		nx := s.byID[cur.act]
		if nx == nil {
			if _, _, ok := s.d.Pos(cur.act); ok {
				s.halt(BadTrust, cur.key)
			}
			return false, nil
		}
		if nx.ready != nil || nx.cfg+1 != cur.cfg {
			s.halt(BadTrust, nx.key)
			return false, nil
		}
		cur = nx
	}
	closing := cur != t
	if cur == s.last && !closing {
		s.anchor = nil
		return true, nil
	}
	path, ok := s.inside(cur)
	if !ok {
		s.halt(BadTrust, cur.key)
		return false, nil
	}
	props, err := s.propose(path)
	if err != nil {
		return false, err
	}
	mode := reaches
	if closing {
		mode = closes
	}
	s.commit(path, props, mode)
	if !closing {
		s.anchor = nil
	}
	return s.stop == nil, nil
}

// certified returns the one closure certificate of the current config, nil when there is none; two that name different
// pivots stop settlement.
func (s *Settler) certified() (*cand, error) {
	ids := make([]dag.ID, 0, len(s.cands))
	for id := range s.cands {
		ids = append(ids, id)
	}
	slices.SortFunc(ids, func(a, b dag.ID) int { return bytes.Compare(a[:], b[:]) })
	var valid []*cand
	var pivots []*rec
	for _, id := range ids {
		c := s.cands[id]
		if c.dirty || !c.checked || c.at != s.count {
			if err := s.check(id, c); err != nil {
				return nil, err
			}
		}
		if c.ok {
			valid = append(valid, c)
			pivots = append(pivots, c.path[len(c.path)-1])
		}
	}
	switch len(valid) {
	case 0:
		return nil, nil
	case 1:
		return valid[0], nil
	}
	key := pivots[1].key
	for _, p := range pivots {
		if s.d.Forked(p.strand) {
			key = p.key
			break
		}
	}
	s.halt(TwoCertificates, key)
	return nil, nil
}

// check decides whether the claims naming pivot id certify the current config closed at it: the walk inside the pivot keeps
// the committed list, the next config activates exactly at it, and q of the next config's seats name it with it in their
// past.
func (s *Settler) check(id dag.ID, c *cand) error {
	c.dirty, c.ok = false, false
	if !c.checked || c.at != s.count {
		c.checked, c.at, c.path, c.props = true, s.count, nil, nil
		t := s.byID[id]
		if t == nil {
			// placed later: its claims hold it in their past, so it is placed before them
			c.checked = false
			return nil
		}
		path, ok := s.inside(t)
		if !ok {
			return nil
		}
		props, err := s.propose(path)
		if err != nil {
			return err
		}
		if at, pends, _ := s.replay(path, props); at == len(path)-1 {
			c.path, c.props = path, props
			c.next = Config{Number: s.cfg.Number + 1, Rule: pends[at].Rule, Seats: pends[at].Seats, Activating: id}
		}
	}
	if c.path == nil {
		return nil
	}
	t := c.path[len(c.path)-1]
	seats := map[Seat]bool{}
	keys := map[Key]bool{}
	for _, st := range c.next.Seats {
		seats[st] = true
		keys[st.Key] = true
	}
	got := map[Key]bool{}
	for _, r := range c.claims {
		if r.act == id && in(r.clock, t) && (seats[Seat{Key: r.key, Strand: r.strand}] || (s.dev.byKey && keys[r.key])) {
			got[r.key] = true
		}
	}
	c.ok = len(got) >= c.next.Quorum() && (s.dev.certNoVotes || s.voted(t))
	return nil
}

// voted reports whether q seats of the current config vote for a pivot whose walk passes through t. The claims of a
// certificate are signed by keys that t's own past names, so only these votes tie t to the config it closes.
func (s *Settler) voted(t *rec) bool {
	st := s.step()
	for r := t.round; r < s.maxRound; r += st {
		for _, v := range s.seats[s.pivotSeat(r)].rounds[r] {
			n := 0
			for _, seat := range s.seats {
				if slices.ContainsFunc(seat.rounds[r+1], func(x *rec) bool { return in(x.clock, v) }) {
					n++
				}
			}
			if n < s.q {
				continue
			}
			if path, ok := s.inside(v); ok && slices.Contains(path, t) {
				return true
			}
		}
	}
	return false
}

// Predicted returns the pivots that the walk from the newest pivot candidate would commit if it were committed, ascending;
// the tentative tail is ordered as their batches. It is empty while an anchor is pending or settlement is halted.
func (s *Settler) Predicted() []dag.ID {
	if s.anchor != nil || s.Halted() != nil || !s.hasRound {
		return nil
	}
	st := s.step()
	r := s.maxRound - s.maxRound%st
	for {
		if s.last != nil && r <= s.last.round {
			return nil
		}
		if v := s.vertex(s.pivotSeat(r), r); v != nil {
			path, ok := s.inside(v)
			if !ok {
				return nil
			}
			out := make([]dag.ID, len(path))
			for i, p := range path {
				out[i] = p.id
			}
			return out
		}
		if r < st {
			return nil
		}
		r -= st
	}
}
