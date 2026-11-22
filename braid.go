package kolobraid

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"

	koloid "github.com/ciranda-dev/kolo-id"
	kololog "github.com/ciranda-dev/kolo-log"
	kolostore "github.com/ciranda-dev/kolo-store"

	"github.com/ciranda-dev/kolo-braid/internal/dag"
	"github.com/ciranda-dev/kolo-braid/internal/prefixdb"
	"github.com/ciranda-dev/kolo-braid/internal/roster"
	"github.com/ciranda-dev/kolo-braid/internal/settle"
)

// Braid is an open braid: the strands of its members merged into one order, applied to a view. It is safe for concurrent use.
type Braid struct {
	id      *koloid.Identity
	key     ed25519.PublicKey
	opts    Options
	lim     Limits
	cfg     GroupConfig
	bounds  formatBounds
	db      kolostore.Backend
	meta    meta
	cont    *continuation
	logger  *slog.Logger
	ev      *events
	view    *view
	hist    histories
	created bool

	// mu guards what follows. Lock order: mu before view.mu and events.mu; syncer.mu is never held together with mu.
	mu      sync.Mutex
	cond    *sync.Cond
	closed  bool
	fatal   error
	stores  map[string]*kolostore.Store
	strands map[kololog.LogID]*strand
	// fresh lists the strands followed since progress last read them.
	fresh    []kololog.LogID
	own      *kolostore.Log
	d        *dag.DAG
	lk       *lookup
	segCount int
	st       *settle.Settler
	nodes    map[EntryID]*node
	work     int
	// newestPulse is the index of the newest pulse held of each strand; sincePulse counts the own entries since the own last
	// pulse.
	newestPulse map[kololog.LogID]uint64
	sincePulse  int

	settled     *roster.Roster
	settledEnd  *rosterView
	settledNext uint64
	pivots      uint64
	frontier    dag.Clock
	lastPivot   EntryID
	configs     map[uint64]configRec

	tent *roster.Roster
	plan *tailPlan
	// planRV, planPred and planEnds are the roster view, the predicted pivots and the segment ends of the last plan;
	// placedSince lists the entries placed after it.
	planRV       *rosterView
	planPred     []dag.ID
	planEnds     dag.Clock
	placedSince  []EntryID
	planGen      uint64
	prevTail     []EntryID
	prevAt       uint64
	reorgs       []reorgMark
	reorgGen     uint64
	reorgPending *uint64

	jobs          []job
	seeded        bool
	appliedConfig uint64
	busy          bool
	viewHalt      error
	appliedGen    uint64

	evidence map[kololog.LogID]*heldEvidence
	// early holds Evidence entries placed before the strand they accuse is followed, a few per strand by key.
	early      map[kololog.LogID][]evidenceBody
	earlyBytes int64
	halted     bool
	erased     error
	temp       *kolostore.Store

	claims  map[uint64]map[[32]byte]Digest
	certs   map[uint64]Digest
	myClaim *pulseClaim

	conf confirmState
	ps   pulseState

	// waiting is set while a joiner waits for the founder strand's genesis entry; welcome is the AddMember entry to check.
	waiting bool
	welcome *welcomeRec
	// joinID is the welcome's AddMember entry and joinInviter the inviter's strand: joinSeen is set once the order holds the
	// entry, joined once writing may start.
	joinID      EntryID
	joinInviter kololog.LogID
	joinSeen    bool
	joined      bool
	sync        *syncer
	// admitting serializes Admit, so the default roles of each joiner count the entries of the one before.
	admitting chan struct{}
	at        settledAtState

	wake   chan struct{}
	life   context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// node is a decoded strand entry that the DAG holds, placed or waiting.
type node struct {
	id     EntryID
	strand kololog.LogID
	index  uint64
	prev   EntryID
	writer ed25519.PublicKey
	record []byte
	kind   Kind
	bad    bool
	body   []byte
	links  []Link
	dec    entryBody
	placed bool
	// lazy is set for a data or seed entry held without its body, which is read back from its strand when needed, so placed
	// entries that wait for settlement hold no body in memory.
	lazy bool
}

func (n *node) entry() Entry {
	return Entry{ID: n.id, Writer: n.writer, Strand: n.strand, Index: n.index, Kind: n.kind, Body: n.body, Links: n.links}
}

// positioned reports whether the entry takes a position: every entry but a seed does.
func (n *node) positioned() bool { return n.bad || n.kind != KindSeed }

// membership reports whether the entry can change the roster.
func (n *node) membership() bool {
	return !n.bad && (n.kind == KindGenesis || (n.kind >= KindAddMember && n.kind <= KindEvidence))
}

func (n *node) class() dag.Class { return roster.Class(roster.Kind(n.kind), n.bad) }

func key32(k ed25519.PublicKey) (out [32]byte) {
	copy(out[:], k)
	return out
}

func (n *node) rosterEntry() roster.Entry {
	e := roster.Entry{ID: dag.ID(n.id), Strand: dag.StrandID(n.strand), Index: n.index, Kind: roster.Kind(n.kind), Malformed: n.bad}
	switch b := n.dec.(type) {
	case NewMember:
		e.Add = roster.Add{Key: key32(b.Key), Strand: dag.StrandID(b.Strand), Roles: roster.Role(b.Roles)}
	case removeBody:
		e.Target = key32(b.Key)
	case settlersBody:
		e.Settlers.Rule = roster.Rule(b.Rule)
		for _, k := range b.Keys {
			e.Settlers.Keys = append(e.Settlers.Keys, key32(k))
		}
	case evidenceBody:
		e.Accused = roster.Seat{Key: key32(b.Key), Strand: dag.StrandID(b.Strand)}
	}
	return e
}

// Create makes a new braid with id as its founder: its strand and the genesis entry that fixes cfg. The founder is the only
// admin and settler, and its strand is confirmed at once. An error after the genesis is stored leaves the braid to Open.
func Create(ctx context.Context, id *koloid.Identity, cfg GroupConfig, opts Options) (*Braid, error) {
	return create(ctx, id, cfg, opts, nil, nil)
}

// create writes the founder strand: the genesis entry and, for a successor, its seed entries.
func create(ctx context.Context, id *koloid.Identity, cfg GroupConfig, opts Options, cont *continuation, seeds func(func([]byte) error) error) (*Braid, error) {
	if id == nil {
		return nil, fmt.Errorf("%w: nil identity", ErrInvalidArgument)
	}
	if err := opts.check(); err != nil {
		return nil, err
	}
	cfg = cfg.WithDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if cfg.App != opts.App {
		return nil, ErrWrongApp
	}
	if _, err := opts.limits(cfg.MaxBody); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := claimPrefix(opts.Backend, opts.Prefix); err != nil {
		return nil, err
	}
	db := PrefixBackend(opts.Backend, opts.Prefix)
	if v, err := getOpt(db, []byte(keyMeta)); err != nil {
		return nil, err
	} else if v != nil {
		return nil, fmt.Errorf("%w: a braid is already stored under this prefix", ErrInvalidArgument)
	}
	key := id.PublicKey()
	st := kolostore.New(prefixdb.New(db, append([]byte{storePrefix}, key...)))
	l, err := st.Create(id, kololog.WithValueEncoding(kololog.Bytes))
	if err != nil {
		st.Close()
		return nil, err
	}
	strandID := l.ID()
	gb, err := encodeBody(genesisBody{Config: cfg, Continues: cont}, cfg.bounds())
	if err != nil {
		st.Close()
		return nil, err
	}
	gc, err := encodeContent(entryContent{Kind: KindGenesis, Body: gb}, strandID, 0, cfg.bounds())
	if err != nil {
		st.Close()
		return nil, err
	}
	if _, err := l.Append(gc); err != nil {
		st.Close()
		return nil, err
	}
	rec, err := l.Record(0)
	if err != nil {
		st.Close()
		return nil, err
	}
	group := GroupID(entryIDOf(rec.Encode()))
	if seeds != nil {
		prev := EntryID(group)
		err := seeds(func(body []byte) error {
			i := l.Len()
			c, err := encodeContent(entryContent{Group: group, Kind: KindSeed, Prev: prev, Body: body}, strandID, i, cfg.bounds())
			if err != nil {
				return err
			}
			if _, err := l.Append(c); err != nil {
				return err
			}
			r, err := l.Record(i)
			if err != nil {
				return err
			}
			prev = entryIDOf(r.Encode())
			return nil
		})
		if err != nil {
			st.Close()
			return nil, err
		}
	}
	m := meta{Group: group, FounderKey: key, FounderStrand: strandID, FounderHeader: l.Header().Encode(), OwnStrand: strandID}
	bt := db.NewBatch()
	bt.Put([]byte(keyMeta), m.encode())
	bt.Put(append([]byte(keyStrand), strandID[:]...), strandRec{Key: key, Header: m.FounderHeader}.encode())
	if err := bt.Commit(); err != nil {
		st.Close()
		return nil, err
	}
	b := &Braid{created: true}
	b.stores = map[string]*kolostore.Store{string(key): st}
	b.own = l
	if err := b.start(id, opts, db, m); err != nil {
		b.shutdown()
		return nil, err
	}
	return b, nil
}

// Open opens the braid stored in opts.Backend under opts.Prefix. After every Open the own strand is unconfirmed until a member, a
// pin or, for a braid that never had another member, its own storage confirms it; writing calls wait for that.
func Open(ctx context.Context, id *koloid.Identity, opts Options) (*Braid, error) {
	return open(ctx, id, opts, false)
}

// open opens a stored braid; created is set when the own strand was made by this process and has no older copy anywhere, as for
// a joiner that completes its admission.
func open(ctx context.Context, id *koloid.Identity, opts Options, created bool) (*Braid, error) {
	if id == nil {
		return nil, fmt.Errorf("%w: nil identity", ErrInvalidArgument)
	}
	if err := opts.check(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := claimPrefix(opts.Backend, opts.Prefix); err != nil {
		return nil, err
	}
	db := PrefixBackend(opts.Backend, opts.Prefix)
	v, err := getOpt(db, []byte(keyMeta))
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, fmt.Errorf("%w: no braid is stored under this prefix", ErrNotFound)
	}
	m, err := decodeMeta(v)
	if err != nil {
		return nil, err
	}
	b := &Braid{stores: map[string]*kolostore.Store{}, created: created}
	if err := b.start(id, opts, db, m); err != nil {
		b.shutdown()
		return nil, err
	}
	return b, nil
}

// start opens everything below the meta record and starts the braid's goroutines.
func (b *Braid) start(id *koloid.Identity, opts Options, db kolostore.Backend, m meta) error {
	b.id, b.key, b.opts, b.db, b.meta = id, id.PublicKey(), opts, db, m
	b.admitting = make(chan struct{}, 1)
	b.opts.Pins = slices.Clone(opts.Pins)
	b.logger = opts.Logger
	b.cond = sync.NewCond(&b.mu)
	b.strands = map[kololog.LogID]*strand{}
	b.nodes = map[EntryID]*node{}
	b.newestPulse = map[kololog.LogID]uint64{}
	b.configs = map[uint64]configRec{}
	b.evidence = map[kololog.LogID]*heldEvidence{}
	b.early = map[kololog.LogID][]evidenceBody{}
	b.claims = map[uint64]map[[32]byte]Digest{}
	b.certs = map[uint64]Digest{}
	b.wake = make(chan struct{}, 1)
	b.life, b.cancel = context.WithCancel(context.Background())
	b.at.init()
	b.sync = newSyncer(b)
	if b.hist == nil {
		b.hist = storedHistories{db: db}
	}
	if b.own == nil {
		l, err := b.storeFor(b.key).Open(id, m.OwnStrand)
		if err != nil {
			return err
		}
		b.own = l
	}
	if err := clearPrefix(db, []byte{tempPrefix}); err != nil {
		return err
	}
	if err := b.follow(m.FounderStrand, m.FounderKey, m.FounderHeader, nil); err != nil {
		return err
	}
	if err := b.loadWelcome(); err != nil {
		return err
	}
	// a joiner, also one that restarted, waits for the founder strand's genesis entry
	if b.strands[m.FounderStrand].length() == 0 {
		return b.wait()
	}
	if err := b.begin(); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.run()
}

func (b *Braid) followStored() error {
	return scanPrefix(b.db, []byte(keyStrand), func(k, v []byte) error {
		rec, err := decodeStrandRec(v)
		if err != nil {
			return err
		}
		if len(k) != len(keyStrand)+32 {
			return fmt.Errorf("%w: strand record key", ErrCorrupt)
		}
		return b.follow(kololog.LogID(k[len(keyStrand):]), rec.Key, rec.Header, nil)
	})
}

// begin reads the genesis entry and opens what depends on it: the view, the stored strands and the settlement state.
func (b *Braid) begin() error {
	opts, db, m := b.opts, b.db, b.meta
	g, err := b.readGenesis()
	if err != nil {
		return err
	}
	if g.Config.App != opts.App {
		return ErrWrongApp
	}
	b.cfg, b.bounds, b.cont = g.Config, g.Config.bounds(), g.Continues
	if b.lim, err = opts.limits(b.cfg.MaxBody); err != nil {
		return err
	}
	// a replay that a crash cut short, or a SettledAt snapshot never closed, left its view behind
	if err := clearPrefix(db, []byte{scratchPrefix}); err != nil {
		return err
	}
	if opts.Predecessor != nil {
		if b.cont == nil {
			return fmt.Errorf("%w: the braid continues no other", ErrWrongGroup)
		}
		err := checkPredecessor(opts.Predecessor, m.FounderKey, b.cont, PrefixBackend(db, []byte{scratchPrefix}))
		if cerr := clearPrefix(db, []byte{scratchPrefix}); err == nil {
			err = cerr
		}
		if err != nil {
			return err
		}
	}
	if b.ev == nil {
		b.ev = newEvents(b.lim.MaxEvents)
	}
	vc := viewConfig{MaxBody: b.cfg.MaxBody, CheckpointEvery: b.cfg.CheckpointEvery, MaxTail: b.lim.MaxTail,
		MaxTailBytes: b.lim.MaxTailBytes, Initial: initialDigest(m.Group)}
	if b.cont != nil {
		vc.Next = b.cont.Position + 1
	}
	v, err := openView(db, vc, opts.Apply)
	if err != nil {
		return err
	}
	if b.view != nil {
		b.view.Close()
	}
	b.view = v
	if err := b.followStored(); err != nil {
		return err
	}
	return b.restore()
}

// run places the held entries and starts the braid's goroutines; the caller holds b.mu.
func (b *Braid) run() error {
	b.waiting = false
	b.countSincePulse()
	err := b.ingestAll()
	if err == nil {
		err = b.progress()
	}
	if err == nil {
		err = b.checkWelcome()
	}
	b.initConfirm()
	if err != nil {
		return err
	}
	b.wg.Add(2)
	go b.applyLoop()
	go b.writeLoop(b.life)
	if !b.sync.started {
		b.sync.started = true
		b.wg.Add(1)
		go b.sync.loop(b.life)
	}
	return nil
}

// countSincePulse counts the own entries after the own last pulse, at most pulseSpan; the caller holds b.mu.
func (b *Braid) countSincePulse() {
	b.sincePulse = 0
	for i := b.own.Len(); i > 0 && b.sincePulse < pulseSpan; i-- {
		rec, err := b.own.Record(i - 1)
		if err != nil {
			return
		}
		n, err := b.readEntry(b.meta.OwnStrand, i-1, entryIDOf(rec.Encode()))
		if err != nil || (!n.bad && n.kind == KindPulse) {
			return
		}
		b.sincePulse++
	}
}

// wait starts a joiner that does not hold the founder strand's genesis entry yet: only the sync layer runs, and writing calls
// wait, until a pull brings that entry.
func (b *Braid) wait() error {
	lim, err := b.opts.limits(defaultMaxBody)
	if err != nil {
		return err
	}
	b.lim, b.bounds = lim, genesisBounds()
	b.ev = newEvents(lim.MaxEvents)
	vc := viewConfig{MaxBody: defaultMaxBody, CheckpointEvery: defaultCheckpoint, MaxTail: lim.MaxTail,
		MaxTailBytes: lim.MaxTailBytes, Initial: initialDigest(b.meta.Group)}
	if b.view, err = openView(PrefixBackend(kolostore.NewMemory(), nil), vc, b.opts.Apply); err != nil {
		return err
	}
	if err := b.followStored(); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.waiting = true
	b.conf.changed()
	b.sync.started = true
	b.wg.Add(1)
	go b.sync.loop(b.life)
	return nil
}

// beginWaiting finishes a waiting joiner's start once the genesis entry is held; the caller holds b.mu.
func (b *Braid) beginWaiting() error {
	if !b.waiting || b.closed || b.strands[b.meta.FounderStrand].length() == 0 {
		return nil
	}
	if err := b.begin(); err != nil {
		b.waiting = false
		return b.fail(err)
	}
	if err := b.run(); err != nil {
		return b.fail(err)
	}
	return nil
}

// readGenesis reads and checks the genesis entry of the founder strand.
func (b *Braid) readGenesis() (genesisBody, error) {
	s := b.strands[b.meta.FounderStrand]
	if s.length() == 0 {
		return genesisBody{}, fmt.Errorf("%w: genesis entry not held", ErrNotFound)
	}
	rec, err := s.recordAt(0)
	if err != nil {
		return genesisBody{}, err
	}
	if GroupID(entryIDOf(rec.Encode())) != b.meta.Group {
		return genesisBody{}, ErrWrongGroup
	}
	v, err := s.value(0)
	if err != nil {
		return genesisBody{}, err
	}
	c, body, err := decodeContent(v, b.meta.FounderStrand, 0, genesisBounds())
	if err != nil || c.Kind != KindGenesis {
		return genesisBody{}, fmt.Errorf("%w: genesis entry", ErrCorrupt)
	}
	return body.(genesisBody), nil
}

func clearPrefix(db kolostore.Backend, prefix []byte) error {
	bt := db.NewBatch()
	bt.DeleteRange(prefix, prefixdb.Successor(prefix))
	return bt.Commit()
}

// shutdown releases what start opened; the backend stays open.
func (b *Braid) shutdown() error {
	var first error
	if b.view != nil {
		first = b.view.Close()
	}
	for _, s := range b.stores {
		if err := s.Close(); err != nil && first == nil {
			first = err
		}
	}
	if b.temp != nil {
		if err := b.temp.Close(); err != nil && first == nil {
			first = err
		}
	}
	if b.ev != nil {
		b.ev.close()
	}
	return first
}

// Close stops the braid and closes its strands and view; the backend stays open. A second Close returns nil.
func (b *Braid) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	b.cond.Broadcast()
	b.conf.wakeAll()
	b.mu.Unlock()
	if b.cancel != nil {
		b.cancel()
	}
	b.wg.Wait()
	b.closeSettledAt()
	return b.shutdown()
}

// GroupID returns the group ID: the entry ID of the genesis entry.
func (b *Braid) GroupID() GroupID { return b.meta.Group }

// PublicKey returns the key of the identity this peer runs the braid with.
func (b *Braid) PublicKey() ed25519.PublicKey { return slices.Clone(b.key) }

// Topic returns the kolo-swarm topic of the braid.
func (b *Braid) Topic() [32]byte { return topicOf(b.meta.Group) }

// usable returns why calls must fail, or nil.
func (b *Braid) usable() error {
	switch {
	case b.closed:
		return ErrClosed
	case b.fatal != nil:
		return b.fatal
	}
	return nil
}

// fail stops the braid after an error that leaves its state unknown; the next Open continues from what is stored.
func (b *Braid) fail(err error) error {
	if b.fatal == nil {
		b.fatal = fmt.Errorf("%w: %w", ErrHalted, err)
		b.ev.push(Event{Kind: EventHalted, Position: b.settledNext, Err: b.fatal})
		b.cond.Broadcast()
	}
	return b.fatal
}

// Append writes a data entry to the own strand and returns its reference; whether it takes effect is decided at its position.
// After every Open it first waits, as long as ctx allows, until the own strand is confirmed (ErrUnconfirmed when ctx ends). A
// braid entry can never be erased: every peer keeps every entry and its key.
func (b *Braid) Append(ctx context.Context, body []byte) (EntryRef, error) {
	if err := b.waitConfirmed(ctx); err != nil {
		return EntryRef{}, err
	}
	if len(body) > b.maxBody() {
		return EntryRef{}, fmt.Errorf("%w: body of %d bytes", ErrTooLarge, len(body))
	}
	return b.write(ctx, KindData, slices.Clone(body), false)
}

// AddMember appends an AddMember entry for w, or changes w's roles or strand when its key is active. The caller must be an
// admin; the header must be w's signed log header for w.Strand.
func (b *Braid) AddMember(ctx context.Context, w NewMember) (EntryRef, error) {
	if err := b.waitConfirmed(ctx); err != nil {
		return EntryRef{}, err
	}
	body, err := encodeBody(w, b.groupBounds())
	if err != nil {
		return EntryRef{}, err
	}
	return b.write(ctx, KindAddMember, body, true)
}

// RemoveMember appends a RemoveMember entry for key. The caller must be an admin. Entries of that member ordered before the
// removal keep their effect; those ordered after it are void.
func (b *Braid) RemoveMember(ctx context.Context, key ed25519.PublicKey) (EntryRef, error) {
	if len(key) != ed25519.PublicKeySize {
		return EntryRef{}, fmt.Errorf("%w: key of %d bytes", ErrInvalidArgument, len(key))
	}
	if err := b.waitConfirmed(ctx); err != nil {
		return EntryRef{}, err
	}
	body, err := encodeBody(removeBody{Key: key}, b.groupBounds())
	if err != nil {
		return EntryRef{}, err
	}
	return b.write(ctx, KindRemoveMember, body, true)
}

// SetSettlers appends a Settlers entry. Under Majority, 3 or 5 settlers give f = 0: one settler that signs two histories can
// split settled prefixes; 4 under Majority, or TwoThirds from 4, do not. A change that adds settlers activates once they are
// ready, and the settlers it removes keep their seats until then: retire an old device after Status.Config changes.
func (b *Braid) SetSettlers(ctx context.Context, keys []ed25519.PublicKey, rule QuorumRule) (EntryRef, error) {
	if err := b.waitConfirmed(ctx); err != nil {
		return EntryRef{}, err
	}
	body, err := b.encodeSettlers(keys, rule)
	if err != nil {
		return EntryRef{}, err
	}
	return b.write(ctx, KindSettlers, body, true)
}

func (b *Braid) encodeSettlers(keys []ed25519.PublicKey, rule QuorumRule) ([]byte, error) {
	ks := slices.Clone(keys)
	slices.SortFunc(ks, func(a, c ed25519.PublicKey) int { return bytes.Compare(a, c) })
	return encodeBody(settlersBody{Rule: rule, Keys: ks}, b.groupBounds())
}

// write checks the tentative roster and appends one entry; the caller has waited for the own strand.
func (b *Braid) write(ctx context.Context, kind Kind, body []byte, admin bool) (EntryRef, error) {
	if err := ctx.Err(); err != nil {
		return EntryRef{}, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.usable(); err != nil {
		return EntryRef{}, err
	}
	if err := b.mayWrite(admin); err != nil {
		return EntryRef{}, err
	}
	return b.appendLocked(kind, body)
}

// mayWrite checks the own key in the tentative roster.
func (b *Braid) mayWrite(admin bool) error {
	switch {
	case b.conf.lost:
		return ErrStrandLost
	case b.erased != nil:
		return b.erased
	}
	w, ok := b.tent.ByStrand(dag.StrandID(b.meta.OwnStrand))
	if !ok {
		if _, used := b.tent.Used(dag.StrandID(b.meta.OwnStrand)); used {
			return ErrRemoved
		}
		return ErrNotMember
	}
	if admin && w.Roles&roster.Admin == 0 {
		return ErrNotAdmin
	}
	return nil
}

// appendLocked writes one entry on the own strand with the links of the link rules and places it.
func (b *Braid) appendLocked(kind Kind, body []byte) (EntryRef, error) {
	// a span pulse still due, after a crash between an entry and its pulse, comes first
	if kind != KindPulse && b.sincePulse >= pulseSpan {
		b.spanPulse()
	}
	s := b.strands[b.meta.OwnStrand]
	if err := b.ingest(s); err != nil {
		return EntryRef{}, b.fail(err)
	}
	i := b.own.Len()
	if s.next != i || s.stop != nil {
		return EntryRef{}, fmt.Errorf("%w: the own strand cannot be read back", ErrErased)
	}
	c := entryContent{Group: b.meta.Group, Kind: kind, Prev: s.last, Links: b.links(), Body: body}
	enc, err := encodeContent(c, b.meta.OwnStrand, i, b.bounds)
	if err != nil {
		return EntryRef{}, err
	}
	idx, err := b.own.Append(enc)
	if err != nil {
		return EntryRef{}, err
	}
	rec, err := b.own.Record(idx)
	if err != nil {
		return EntryRef{}, b.fail(err)
	}
	ref := EntryRef{Strand: b.meta.OwnStrand, Index: idx, ID: entryIDOf(rec.Encode())}
	if err := b.ingest(s); err != nil {
		return ref, b.fail(err)
	}
	if err := b.progress(); err != nil {
		return ref, err
	}
	if kind == KindPulse {
		b.sincePulse = 0
	} else if b.sincePulse++; b.sincePulse >= pulseSpan {
		b.spanPulse()
	}
	return ref, nil
}

// View returns a snapshot of the view as of now: settled plus tentative entries. Close it when done.
func (b *Braid) View() *Snapshot {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.view.View()
}

// Settled returns a snapshot of the settled view. Close it when done.
func (b *Braid) Settled() *Snapshot {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.view.Settled()
}

func (b *Braid) maxBody() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.cfg.MaxBody
}

func (b *Braid) maxUnknownHeads() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.lim.MaxUnknownHeads
}

func (b *Braid) groupBounds() formatBounds {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.bounds
}

// Next returns the next event, waiting as long as ctx allows. EventRemoved, EventSettlementHalted, EventStrandLost, EventHalted and
// EventDiverged are never dropped; other events past MaxEvents are dropped oldest first and counted in Status.EventsDropped.
func (b *Braid) Next(ctx context.Context) (Event, error) { return b.ev.next(ctx) }

// Status describes the braid's state.
type Status struct {
	// Settled and Tentative are the next position after the settled entries and after the tentative ones.
	Settled, Tentative uint64
	// Tail is the number of tentative positions, pulses included: a pulse takes a position but is never applied, so an idle
	// braid can keep a tail of pulses.
	Tail int
	// Config is the current settler config number; Seats and Quorum are its seat count and q.
	Config        uint64
	Seats, Quorum int
	// SingleSettler is set when one seat settles a braid with more than one member: losing that device stops settlement.
	SingleSettler bool
	// PendingConfig holds the seats of a pending config, nil when none.
	PendingConfig []Member
	// AwaitingReadiness holds the seats a pending config adds without a readiness pulse that counts: one that the last pivot
	// committed before the config became pending does not hold.
	// Once none is awaited, the online current settlers pulse until the config activates, with no other entry needed.
	AwaitingReadiness []Member
	// Unconfirmed is set until the own strand is confirmed after Open.
	Unconfirmed bool
	// StrandLost is set when a history of the own strand conflicts with ours: writes are refused.
	StrandLost bool
	// SeededFrom is set in a successor braid: the braid it continues.
	SeededFrom *GroupID
	// SettlementHalted says why the current config settles no more, nil while it settles.
	SettlementHalted error
	// Halted says why apply stopped on this peer, nil while it runs.
	Halted error
	// EventsDropped counts the events dropped past MaxEvents.
	EventsDropped uint64
}

// Status returns the braid's state.
func (b *Braid) Status() Status {
	b.mu.Lock()
	defer b.mu.Unlock()
	p := b.view.Progress()
	s := Status{Settled: p.Next, Tail: b.view.Tail(), Unconfirmed: (!b.conf.confirmed || b.conf.behind) && !b.conf.lost, StrandLost: b.conf.lost,
		EventsDropped: b.ev.droppedCount()}
	s.Tentative = p.Next + uint64(s.Tail)
	if b.st != nil {
		c := b.st.Config()
		s.Config, s.Seats, s.Quorum = c.Number, len(c.Seats), c.Quorum()
		s.SettlementHalted = b.st.Halted()
		if pend, ok := b.st.Pending(); ok {
			s.PendingConfig = b.seatMembers(pend.Seats)
			if s.PendingConfig == nil {
				s.PendingConfig = []Member{}
			}
			s.AwaitingReadiness = b.seatMembers(b.st.Awaiting())
		}
		s.SingleSettler = len(c.Seats) == 1 && b.tent != nil && b.tent.Len() > 1
	}
	if b.cont != nil {
		g := b.cont.Group
		s.SeededFrom = &g
	}
	switch {
	case b.viewHalt != nil:
		s.Halted = b.viewHalt
	case b.fatal != nil:
		s.Halted = b.fatal
	default:
		s.Halted = b.view.Halted()
	}
	return s
}

// seatMembers describes seats as members, with the roles the tentative roster gives them.
func (b *Braid) seatMembers(seats []settle.Seat) []Member {
	var out []Member
	for _, st := range seats {
		w := Member{Key: ed25519.PublicKey(slices.Clone(st.Key[:])), Strand: kololog.LogID(st.Strand)}
		if b.tent != nil {
			if rw, ok := b.tent.Member(roster.Key(st.Key)); ok {
				w.Roles = Role(rw.Roles)
			}
		}
		out = append(out, w)
	}
	return out
}

func (b *Braid) signal() {
	select {
	case b.wake <- struct{}{}:
	default:
	}
}

func (b *Braid) logf(msg string, args ...any) {
	if b.logger != nil {
		b.logger.Info(msg, args...)
	}
}

var errNotHeld = errors.New("kolobraid: entry not held")
