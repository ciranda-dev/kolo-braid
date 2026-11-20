package kolobraid

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"fmt"
	"maps"
	"slices"

	kolocodec "github.com/ciranda-dev/kolo-codec"
	koloid "github.com/ciranda-dev/kolo-id"
	kololog "github.com/ciranda-dev/kolo-log"
	kolostore "github.com/ciranda-dev/kolo-store"

	"github.com/ciranda-dev/kolo-braid/internal/roster"
	"github.com/ciranda-dev/kolo-braid/internal/settle"
)

// Succeed starts a successor of old, for a braid whose settler quorum is lost: a new braid whose genesis names old, its settled
// position p and the content digest of its settled view at p, followed by seed entries that copy that view. Positions continue
// at p + 1. The caller must be an admin of old's settled roster at p. Members join the successor again; tentative entries of
// old are not carried.
func Succeed(ctx context.Context, id *koloid.Identity, old *Braid, cfg GroupConfig, opts Options) (*Braid, error) {
	if old == nil || id == nil {
		return nil, fmt.Errorf("%w: nil braid or identity", ErrInvalidArgument)
	}
	old.mu.Lock()
	if err := old.usable(); err != nil {
		old.mu.Unlock()
		return nil, err
	}
	next := old.view.Progress().Next
	if next == 0 {
		old.mu.Unlock()
		return nil, fmt.Errorf("%w: the old braid has nothing settled", ErrInvalidArgument)
	}
	snap := old.view.Settled()
	group := old.meta.Group
	old.mu.Unlock()
	defer snap.Close()
	if w, ok := snapMember(snap, id.PublicKey()); !ok || w.Roles&RoleAdmin == 0 {
		return nil, ErrNotAdmin
	}
	p := next - 1
	if snap.Position() != p {
		return nil, fmt.Errorf("%w: settled view moved", ErrInvalidArgument)
	}
	c := cfg.WithDefaults()
	limit := max(c.MaxBody, seedPairMax)
	// a storage error must not end a scan early: the digest and the seeds would cover a truncated view
	var scanErr error
	keep := func(err error) {
		if scanErr == nil {
			scanErr = err
		}
	}
	content := contentDigest(p, snap.scan(nil, nil, keep))
	if scanErr != nil {
		return nil, scanErr
	}
	var m uint64
	if err := packSeeds(snap, limit, keep, func([]byte) error { m++; return nil }); err != nil {
		return nil, err
	}
	if scanErr != nil {
		return nil, scanErr
	}
	cont := &continuation{Group: group, Position: p, Content: content, Seeds: m}
	return create(ctx, id, cfg, opts, cont, func(put func([]byte) error) error {
		var n uint64
		err := packSeeds(snap, limit, keep, func(body []byte) error {
			n++
			if n > m {
				return fmt.Errorf("%w: the settled view changed while it was copied", ErrBadSeed)
			}
			return put(body)
		})
		if err == nil && scanErr != nil {
			err = scanErr
		}
		if err == nil && n != m {
			err = fmt.Errorf("%w: the settled view changed while it was copied", ErrBadSeed)
		}
		return err
	})
}

// snapMember returns key's member in the roster of s.
func snapMember(s *Snapshot, key ed25519.PublicKey) (Member, bool) {
	if s.roster == nil {
		return Member{}, false
	}
	return s.roster.Member(key)
}

// checkPredecessor checks a successor's genesis against old, the braid it continues as this peer holds it: the founder was an
// admin of old's settled roster at the old position and old's settled view there has the content digest the genesis names.
func checkPredecessor(old *Braid, founder ed25519.PublicKey, c *continuation, scratch kolostore.Backend) error {
	if old.GroupID() != c.Group {
		return fmt.Errorf("%w: the successor continues another braid", ErrWrongGroup)
	}
	rv, d, err := old.stateAt(c.Position, scratch)
	if err != nil {
		return err
	}
	if w, ok := rv.Member(founder); !ok || w.Roles&RoleAdmin == 0 {
		return fmt.Errorf("%w: the successor's founder was not an admin at position %d", ErrNotAdmin, c.Position)
	}
	if d != c.Content {
		return fmt.Errorf("%w: the old view at position %d has another content digest", ErrBadSeed, c.Position)
	}
	return nil
}

// stateAt replays the settled prefix through position p, which must end a settled batch, into a scratch view, and returns the
// roster and the content digest there.
func (b *Braid) stateAt(p uint64, scratch kolostore.Backend) (*rosterView, Digest, error) {
	b.mu.Lock()
	first := b.view.cfg.Next
	b.mu.Unlock()
	if p < first {
		return nil, Digest{}, fmt.Errorf("%w: position %d is before the old braid", ErrBadSeed, p)
	}
	v, rv, err := b.replayTo(context.Background(), p, scratch, true)
	if err != nil {
		return nil, Digest{}, err
	}
	defer v.Close()
	snap := v.Settled()
	defer snap.Close()
	var scanErr error
	d := contentDigest(p, snap.scan(nil, nil, func(err error) { scanErr = err }))
	if scanErr != nil {
		return nil, Digest{}, scanErr
	}
	return rv, d, nil
}

// replayInput is what a replay of the settled order reads of the braid, copied under b.mu.
type replayInput struct {
	src     replaySource
	vc      viewConfig
	apply   ApplyFunc
	g       roster.Genesis
	cont    *continuation
	bounds  formatBounds
	founder kololog.LogID
}

// replayInputs checks that p is a settled position and copies what a replay through it reads. It holds b.mu only for that:
// what the replay reads is settled and so fixed.
func (b *Braid) replayInputs(ctx context.Context, p uint64) (replayInput, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.usable(); err != nil {
		return replayInput{}, err
	}
	if p < b.view.cfg.Next {
		return replayInput{}, fmt.Errorf("%w: position %d is before the braid's first position %d", ErrInvalidArgument, p, b.view.cfg.Next)
	}
	if p >= b.settledNext {
		return replayInput{}, fmt.Errorf("%w: position %d is not settled yet", ErrNotFound, p)
	}
	stop := func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		b.mu.Lock()
		defer b.mu.Unlock()
		return b.usable()
	}
	// each read takes b.mu: the braid goes on running, and a catch-up replaces a strand's log under it
	src := replaySource{segs: b.d.Segments(), configs: maps.Clone(b.configs), stop: stop,
		read: func(id kololog.LogID, i uint64, e EntryID) (*node, error) {
			if err := stop(); err != nil {
				return nil, err
			}
			b.mu.Lock()
			defer b.mu.Unlock()
			return b.readFrom(b.strands, nil, id, i, e)
		}}
	return replayInput{src: src, apply: b.opts.Apply, g: b.genesis(), cont: b.cont, bounds: b.bounds, founder: b.meta.FounderStrand,
		vc: viewConfig{MaxBody: b.cfg.MaxBody, CheckpointEvery: b.cfg.CheckpointEvery, MaxTail: b.lim.MaxTail,
			MaxTailBytes: b.lim.MaxTailBytes, Initial: initialDigest(b.meta.Group), Next: b.view.cfg.Next}}, nil
}

// replayTo replays the settled prefix through position p into a new view on scratch, along the stored pivots, and returns the
// view and the roster after p; batchEnd requires p to end a settled batch, which every peer agrees on.
func (b *Braid) replayTo(ctx context.Context, p uint64, scratch kolostore.Backend, batchEnd bool) (*view, *rosterView, error) {
	in, err := b.replayInputs(ctx, p)
	if err != nil {
		return nil, nil, err
	}
	v, err := openView(scratch, in.vc, in.apply)
	if err != nil {
		return nil, nil, err
	}
	rv, err := b.replayBatches(in, v, p, batchEnd)
	if err != nil {
		v.Close()
		return nil, nil, err
	}
	return v, rv, nil
}

// replayBatches fills v for replayTo: the seed pairs of a successor, then the stored batches through position p.
func (b *Braid) replayBatches(in replayInput, v *view, p uint64, batchEnd bool) (*rosterView, error) {
	r, err := b.seedReplay(in, v)
	if err != nil {
		return nil, err
	}
	for i := uint64(0); ; i++ {
		rec, err := b.pivotAt(i)
		if err != nil {
			return nil, err
		}
		if batchEnd && rec.Next > p+1 {
			return nil, fmt.Errorf("%w: position %d is not the end of a settled batch", ErrBadSeed, p)
		}
		rv, err := b.replayInto(in.src, v, r, i, 0, p+1, true)
		if err != nil {
			return nil, err
		}
		if rec.Next > p {
			return rv, nil
		}
	}
}

// seedReplay writes the seed pairs of a successor into v and returns the genesis roster a replay starts from.
func (b *Braid) seedReplay(in replayInput, v *view) (*roster.Roster, error) {
	if in.cont != nil {
		for i := uint64(1); i <= in.cont.Seeds; i++ {
			if err := in.src.stop(); err != nil {
				return nil, err
			}
			b.mu.Lock()
			val, err := b.strands[in.founder].value(i)
			b.mu.Unlock()
			if err != nil {
				return nil, err
			}
			c, body, err := decodeContent(val, in.founder, i, in.bounds)
			if err != nil || c.Kind != KindSeed {
				return nil, fmt.Errorf("%w: seed entry %d", ErrBadSeed, i)
			}
			if err := v.SeedPairs(body.(seedBody).Pairs); err != nil {
				return nil, err
			}
		}
	}
	return roster.New(in.g)
}

// packSeeds encodes the snapshot's pairs, in key order, into seed bodies of at most limit bytes; onErr gets a storage error.
func packSeeds(s *Snapshot, limit int, onErr func(error), emit func([]byte) error) error {
	var pairs []seedPair
	size := 0
	flush := func() error {
		if len(pairs) == 0 {
			return nil
		}
		var e kolocodec.Encoder
		seedBody{Pairs: pairs}.encode(&e)
		pairs, size = nil, 0
		return emit(e.Bytes())
	}
	for k, v := range s.scan(nil, nil, onErr) {
		cost := uvarintLen(uint64(len(k))) + len(k) + uvarintLen(uint64(len(v))) + len(v)
		if len(pairs) > 0 && uvarintLen(uint64(len(pairs)+1))+size+cost > limit {
			if err := flush(); err != nil {
				return err
			}
		}
		pairs = append(pairs, seedPair{Key: k, Value: v})
		size += cost
	}
	return flush()
}

func uvarintLen(v uint64) int {
	n := 1
	for v >= 0x80 {
		v >>= 7
		n++
	}
	return n
}

// admitDefaults returns what Admit appends for a joiner admitted with GrowSettlers: its roles, and the keys of a Settlers entry, nil
// when none is due. Seats are counted as the pending config if one is pending, else the current one.
func (b *Braid) admitDefaults(joiner [32]byte) (Role, [][32]byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	cfg := b.st.Config()
	seats := cfg.Seats
	if p, ok := b.st.Pending(); ok {
		seats = p.Seats
	}
	keys := make([]roster.Key, len(seats))
	for i, s := range seats {
		keys[i] = roster.Key(s.Key)
	}
	role, list := b.tent.AdmitDefaults(roster.Key(joiner), keys, b.cfg.MinSettlers)
	if list == nil {
		return Role(role), nil
	}
	out := make([][32]byte, len(list))
	for i, k := range list {
		out[i] = [32]byte(k)
	}
	return Role(role), out
}

// welcomeAnchor returns the last committed pivot and its config number, the anchor a welcome carries; nil when none is committed.
func (b *Braid) welcomeAnchor() *anchorRec {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.pivots == 0 {
		return nil
	}
	p, err := b.pivotAt(b.pivots - 1)
	if err != nil {
		return nil
	}
	return &anchorRec{Pivot: p.ID, Config: p.Config}
}

// isSeatKey reports whether k is the key of a seat of the current or the pending config; the caller holds b.mu.
func (b *Braid) isSeatKey(k [32]byte) bool {
	if b.st == nil || b.waiting {
		return false
	}
	seats := b.st.Config().Seats
	if p, ok := b.st.Pending(); ok {
		seats = append(slices.Clone(seats), p.Seats...)
	}
	return slices.ContainsFunc(seats, func(st settle.Seat) bool { return st.Key == settle.Key(k) })
}

// peerAccess is what a peer may fetch of the braid's strands.
type peerAccess uint8

const (
	accessNone peerAccess = iota
	accessCiphertext
	accessKeys
)

// access answers the sync layer's authorize from the tentative roster: entry keys for active members, ciphertext for pins,
// nothing for anyone else, from the first request after this peer holds a removal.
func (b *Braid) access(peer ed25519.PublicKey) peerAccess {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(peer) == ed25519.PublicKeySize && b.tent != nil && b.tent.Access(rosterKey(peer)) {
		return accessKeys
	}
	// a seat whose removal is only ordered still settles until a config without it activates, and needs the entries for that
	if len(peer) == ed25519.PublicKeySize && b.isSeatKey(key32(peer)) {
		return accessKeys
	}
	for _, p := range b.opts.Pins {
		if bytes.Equal(p, peer) {
			return accessCiphertext
		}
	}
	return accessNone
}
