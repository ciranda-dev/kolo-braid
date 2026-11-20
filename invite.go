package kolobraid

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"fmt"
	"math"
	"slices"
	"sync"

	kolocodec "github.com/ciranda-dev/kolo-codec"
	koloid "github.com/ciranda-dev/kolo-id"
	kololog "github.com/ciranda-dev/kolo-log"
	kolostore "github.com/ciranda-dev/kolo-store"

	"github.com/ciranda-dev/kolo-braid/internal/dag"
	"github.com/ciranda-dev/kolo-braid/internal/prefixdb"
)

// keyWelcome holds the AddMember entry a joiner's welcome names, until the joiner has checked it on the inviter's strand;
// keyJoin holds the strand of a join that is not complete, so a restart before Complete finds it again.
const (
	keyWelcome = "mbw"
	keyJoin    = "mbj"
)

// welcomeRec is the AddMember entry of a welcome: the inviter strand, its index there and its ID.
type welcomeRec struct {
	Strand kololog.LogID
	Index  uint64
	ID     EntryID
}

func (w welcomeRec) encode() []byte {
	var e kolocodec.Encoder
	e.PutFixed(w.Strand[:])
	e.PutUvarint(w.Index)
	e.PutFixed(w.ID[:])
	return e.Bytes()
}

func decodeWelcomeRec(b []byte) (welcomeRec, error) {
	r := newReader(b, ErrCorrupt)
	w := welcomeRec{Strand: r.array(), Index: r.uvarint(math.MaxUint64), ID: r.array()}
	return w, r.end()
}

// Joining is a strand made for a braid this peer has not joined yet: its join request goes to an admin, whose welcome
// completes the join.
type Joining struct {
	id     *koloid.Identity
	opts   Options
	strand kololog.LogID
	header []byte

	// mu makes concurrent Complete calls see each other's meta record.
	mu sync.Mutex
}

// NewJoin creates a new strand for id in opts.Backend under opts.Prefix, for a braid to join; nothing else may be stored under
// that prefix. When a join of id is pending there, from a NewJoin before a restart, it returns that join again, whose
// request and welcome stay valid.
func NewJoin(ctx context.Context, id *koloid.Identity, opts Options) (*Joining, error) {
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
	if v, err := getOpt(db, []byte(keyMeta)); err != nil {
		return nil, err
	} else if v != nil {
		return nil, fmt.Errorf("%w: a braid is already stored under this prefix", ErrInvalidArgument)
	}
	j := &Joining{id: id, opts: opts}
	j.opts.Pins = slices.Clone(opts.Pins)
	if v, err := getOpt(db, []byte(keyJoin)); err != nil {
		return nil, err
	} else if v != nil {
		rec, err := decodeStrandRec(v)
		if err != nil {
			return nil, err
		}
		h, err := kololog.DecodeLogHeader(rec.Header)
		if err != nil {
			return nil, fmt.Errorf("%w: pending join: %w", ErrCorrupt, err)
		}
		if !bytes.Equal(rec.Key, id.PublicKey()) {
			return nil, fmt.Errorf("%w: a join of another key is pending under this prefix", ErrInvalidArgument)
		}
		j.strand, j.header = h.LogID, rec.Header
		return j, nil
	}
	st := kolostore.New(prefixdb.New(db, append([]byte{storePrefix}, id.PublicKey()...)))
	defer st.Close()
	l, err := st.Create(id, kololog.WithValueEncoding(kololog.Bytes))
	if err != nil {
		return nil, err
	}
	j.strand, j.header = l.ID(), l.Header().Encode()
	if err := l.Close(); err != nil {
		return nil, err
	}
	if err := db.Put([]byte(keyJoin), strandRec{Key: id.PublicKey(), Header: j.header}.encode()); err != nil {
		return nil, err
	}
	return j, nil
}

// Request returns the join request to send to an admin: the new strand, its signed log header and appData, which the admin's
// application reads.
func (j *Joining) Request(appData []byte) ([]byte, error) {
	return encodeJoinRequest(JoinRequest{Strand: j.strand, Header: j.header, AppData: appData})
}

// Complete opens the braid a welcome names. The returned braid syncs through ServeConn: once a member serves the founder strand's
// genesis entry it starts, and it checks the welcome's AddMember entry when it holds the inviter's strand. Until then, and
// until its own AddMember entry is ordered, writing calls wait, and the view is empty. An error after the welcome is stored
// leaves the join to Open.
func (j *Joining) Complete(ctx context.Context, welcome []byte) (*Braid, error) {
	w, err := parseWelcome(welcome)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	switch {
	case w.FounderStrand == j.strand || w.InviterStrand == j.strand:
		return nil, fmt.Errorf("%w: the welcome names the own strand as another member's", ErrInvalidArgument)
	case bytes.Equal(w.InviterKey, j.id.PublicKey()):
		return nil, fmt.Errorf("%w: the welcome names this peer as its inviter", ErrInvalidArgument)
	case w.FounderStrand == w.InviterStrand && !bytes.Equal(w.FounderKey, w.InviterKey):
		return nil, fmt.Errorf("%w: founder and inviter strands differ in key", ErrInvalidArgument)
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	db := PrefixBackend(j.opts.Backend, j.opts.Prefix)
	if v, err := getOpt(db, []byte(keyMeta)); err != nil {
		return nil, err
	} else if v != nil {
		return nil, fmt.Errorf("%w: the join is already complete", ErrInvalidArgument)
	}
	m := meta{Group: w.Group, FounderKey: w.FounderKey, FounderStrand: w.FounderStrand, FounderHeader: w.FounderHeader,
		OwnStrand: j.strand, Anchor: w.Anchor}
	bt := db.NewBatch()
	bt.Put([]byte(keyMeta), m.encode())
	bt.Put(append([]byte(keyStrand), w.InviterStrand[:]...), strandRec{Key: w.InviterKey, Header: w.InviterHeader}.encode())
	bt.Put(append([]byte(keyStrand), j.strand[:]...), strandRec{Key: j.id.PublicKey(), Header: j.header}.encode())
	bt.Put([]byte(keyWelcome), welcomeRec{Strand: w.InviterStrand, Index: w.AddIndex, ID: w.AddID}.encode())
	bt.Delete([]byte(keyJoin))
	if err := bt.Commit(); err != nil {
		return nil, err
	}
	return open(ctx, j.id, j.opts, true)
}

func (b *Braid) loadWelcome() error {
	v, err := getOpt(b.db, []byte(keyWelcome))
	if err != nil || v == nil {
		return err
	}
	w, err := decodeWelcomeRec(v)
	if err != nil {
		return err
	}
	b.welcome, b.joinID, b.joinInviter = &w, w.ID, w.Strand
	return nil
}

// joining reports whether a joiner waits for its own AddMember entry to be ordered; writing calls wait for that, as for the
// genesis entry, so ErrNotMember means a join that the order made void. The caller holds b.mu.
func (b *Braid) joining() bool { return b.joinID != (EntryID{}) && !b.joined }

// noteJoined ends the wait of joining once the welcome is checked and its AddMember entry is ordered; the caller holds b.mu.
func (b *Braid) noteJoined() {
	if !b.joining() || b.welcome != nil || b.tent == nil {
		return
	}
	if _, used := b.tent.Used(dag.StrandID(b.meta.OwnStrand)); used || b.joinSeen {
		b.joined = true
		b.conf.changed()
		// the welcome record keeps the wait across a restart until here; a settled join deletes it with its batch
		if err := b.db.Delete([]byte(keyWelcome)); err != nil {
			b.logf("kolobraid: the welcome record stays", "error", err.Error())
		}
	}
}

// checkWelcome checks the welcome's AddMember entry once the inviter's strand holds it: its ID and that it adds this member's key
// and strand. A welcome that names another entry halts the braid. The caller holds b.mu.
func (b *Braid) checkWelcome() error {
	w := b.welcome
	if w == nil || b.waiting {
		return nil
	}
	s := b.strands[w.Strand]
	if s == nil {
		return b.fail(fmt.Errorf("%w: the welcome's inviter strand is not followed", ErrCorrupt))
	}
	if s.length() <= w.Index {
		return nil
	}
	rec, err := s.recordAt(w.Index)
	if err != nil {
		return b.fail(err)
	}
	if entryIDOf(rec.Encode()) != w.ID {
		return b.fail(fmt.Errorf("%w: the welcome names an entry the inviter's strand does not hold", ErrNotMember))
	}
	val, err := s.value(w.Index)
	if err != nil {
		// without its key yet; a later pull brings it
		return nil
	}
	c, body, err := decodeContent(val, s.id, w.Index, b.bounds)
	if err == nil && c.Kind == KindAddMember {
		if nm := body.(NewMember); bytes.Equal(nm.Key, b.key) && nm.Strand == b.meta.OwnStrand {
			b.welcome = nil
			b.noteJoined()
			return nil
		}
	}
	return b.fail(fmt.Errorf("%w: the welcome's entry does not add this member", ErrNotMember))
}

// AdmitOptions change what Admit appends.
type AdmitOptions struct {
	// Roles are the joiner's roles; nil makes it a plain member that is neither admin nor settler, or, with Replace, keeps the
	// roles of the active key.
	Roles *Role
	// GrowSettlers makes the joiner an admin while the braid has fewer than MinSettlers - 1 admins and a settler while it has
	// fewer than MinSettlers seats, and appends the Settlers entry when one is due. An admin can add and remove anyone, and a
	// settler that signs two histories halts settlement for good, so use it only for joiners you trust that far. It cannot be
	// combined with Roles.
	GrowSettlers bool
	// Replace allows a key that is already active, with a new strand.
	Replace bool
}

// Admit appends an AddMember entry for the joiner whose key and join request are given, and with GrowSettlers a Settlers entry
// when one is due, and returns the welcome to seal to the joiner. Without Roles or GrowSettlers the joiner is a plain member:
// it writes entries but is no admin and no settler. The caller must be an admin (ErrNotAdmin, ErrRemoved). It returns
// ErrActiveKey for an active key without opts.Replace, ErrKnownStrand for a strand that is used or known and
// ErrInvalidArgument for Roles together with GrowSettlers. When the welcome is not delivered, or Admit fails after the AddMember
// entry, remove the joiner with RemoveMember. Calls run one at a time.
func (b *Braid) Admit(ctx context.Context, joiner ed25519.PublicKey, request []byte, opts *AdmitOptions) ([]byte, error) {
	if len(joiner) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("%w: key of %d bytes", ErrInvalidArgument, len(joiner))
	}
	jr, err := ParseJoinRequest(request)
	if err != nil {
		return nil, err
	}
	if h, err := kololog.DecodeLogHeader(jr.Header); err != nil || !bytes.Equal(h.LogKey, joiner) {
		return nil, fmt.Errorf("%w: the join request's strand is not the joiner's", ErrInvalidArgument)
	}
	if opts == nil {
		opts = &AdmitOptions{}
	}
	if opts.Roles != nil && opts.GrowSettlers {
		return nil, fmt.Errorf("%w: Roles and GrowSettlers together", ErrInvalidArgument)
	}
	select {
	case b.admitting <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	defer func() { <-b.admitting }()
	if err := b.waitConfirmed(ctx); err != nil {
		return nil, err
	}
	b.mu.Lock()
	err = b.usable()
	if err == nil {
		err = b.mayWrite(true)
	}
	var kept Role
	if err == nil {
		m, active := b.tent.Member(rosterKey(joiner))
		if active && !opts.Replace {
			err = ErrActiveKey
		}
		kept = Role(m.Roles)
	}
	if err == nil {
		_, used := b.tent.Used(dag.StrandID(jr.Strand))
		if _, followed := b.strands[jr.Strand]; used || followed {
			err = ErrKnownStrand
		}
	}
	rule, ownHeader := QuorumRule(0), []byte(nil)
	if err == nil {
		rule, ownHeader = QuorumRule(b.st.Config().Rule), b.own.Header().Encode()
	}
	b.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if b.sync.pendingHead(jr.Strand, joiner) {
		return nil, ErrKnownStrand
	}
	nm := NewMember{Key: slices.Clone(joiner), Strand: jr.Strand, Header: jr.Header}
	var seats [][32]byte
	switch {
	case opts.Roles != nil:
		nm.Roles = *opts.Roles
	case opts.GrowSettlers:
		nm.Roles, seats = b.admitDefaults(key32(joiner))
	default:
		nm.Roles = kept
	}
	// both bodies are encoded first, so only a failed write can leave the AddMember entry alone
	var settlers []byte
	if seats != nil {
		keys := make([]ed25519.PublicKey, len(seats))
		for i, k := range seats {
			keys[i] = ed25519.PublicKey(slices.Clone(k[:]))
		}
		if settlers, err = b.encodeSettlers(keys, rule); err != nil {
			return nil, err
		}
	}
	ref, err := b.AddMember(ctx, nm)
	if err != nil {
		return nil, err
	}
	if settlers != nil {
		if _, err := b.write(ctx, KindSettlers, settlers, true); err != nil {
			return nil, fmt.Errorf("kolobraid: the AddMember entry is written, not the Settlers entry: %w", err)
		}
	}
	w := welcomeMsg{Group: b.meta.Group, FounderKey: b.meta.FounderKey, FounderStrand: b.meta.FounderStrand,
		FounderHeader: b.meta.FounderHeader, InviterKey: b.key, InviterStrand: b.meta.OwnStrand,
		InviterHeader: ownHeader, AddIndex: ref.Index, AddID: ref.ID, Anchor: b.welcomeAnchor()}
	return encodeWelcome(w)
}

// CheckAdmit reports, without writing anything, what Admit would refuse now for joiner and request: ErrNotAdmin, ErrRemoved or
// ErrNotMember for this peer, ErrActiveKey, ErrKnownStrand (a used, followed or pending strand), and ErrGroupFull, which Admit
// itself does not return: it appends an AddMember entry that the roster then leaves void. It does not wait for the own strand to
// be confirmed, as Admit does.
func (b *Braid) CheckAdmit(joiner ed25519.PublicKey, request []byte) error {
	if len(joiner) != ed25519.PublicKeySize {
		return fmt.Errorf("%w: key of %d bytes", ErrInvalidArgument, len(joiner))
	}
	jr, err := ParseJoinRequest(request)
	if err != nil {
		return err
	}
	if h, err := kololog.DecodeLogHeader(jr.Header); err != nil || !bytes.Equal(h.LogKey, joiner) {
		return fmt.Errorf("%w: the join request's strand is not the joiner's", ErrInvalidArgument)
	}
	b.mu.Lock()
	err = b.usable()
	if err == nil && b.tent == nil {
		err = ErrNotMember
	}
	if err == nil {
		err = b.mayWrite(true)
	}
	if err == nil {
		if _, active := b.tent.Member(rosterKey(joiner)); active {
			err = ErrActiveKey
		} else if b.tent.Len() >= b.cfg.MaxMembers {
			err = ErrGroupFull
		}
	}
	if err == nil {
		_, used := b.tent.Used(dag.StrandID(jr.Strand))
		if _, followed := b.strands[jr.Strand]; used || followed {
			err = ErrKnownStrand
		}
	}
	b.mu.Unlock()
	if err == nil && b.sync.pendingHead(jr.Strand, joiner) {
		err = ErrKnownStrand
	}
	return err
}

// WelcomeEntry returns the AddMember entry a welcome names: the strand of the admitting peer, the index there and the entry ID.
func WelcomeEntry(welcome []byte) (EntryRef, error) {
	w, err := parseWelcome(welcome)
	if err != nil {
		return EntryRef{}, err
	}
	return EntryRef{Strand: w.InviterStrand, Index: w.AddIndex, ID: w.AddID}, nil
}
