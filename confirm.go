package kolobraid

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"slices"
	"time"

	kololog "github.com/ciranda-dev/kolo-log"
	kolostore "github.com/ciranda-dev/kolo-store"

	"github.com/ciranda-dev/kolo-braid/internal/prefixdb"
)

// confirmState follows the own strand after Open: unconfirmed, confirmed, behind or lost.
type confirmState struct {
	confirmed bool
	lost      bool
	// behind is set when a peer showed a longer history of the own strand or an entry links past its end: it must catch up,
	// and nothing is written until it has. at is the own strand's length when it was confirmed.
	behind bool
	at     uint64
	// heard is set when a member or a pin showed a consistent head of the own strand that is shorter than it.
	heard bool
	// wake is closed and replaced at every change, for the writing calls that wait.
	wake     chan struct{}
	opened   time.Time
	answered map[[32]byte]bool
}

func (c *confirmState) changed() {
	if c.wake != nil {
		close(c.wake)
	}
	c.wake = make(chan struct{})
}

func (c *confirmState) wakeAll() { c.changed() }

// initConfirm sets the state after Open: a new braid, or one that never had another member and has no pin, is confirmed at once.
func (b *Braid) initConfirm() {
	b.conf.opened = time.Now()
	b.conf.answered = map[[32]byte]bool{}
	b.conf.changed()
	if b.created || b.singleMember() {
		b.confirm()
		return
	}
	b.checkConfirm()
}

// singleMember reports whether no other strand was ever active and no pin is configured: nobody can hold a newer copy.
func (b *Braid) singleMember() bool {
	return len(b.opts.Pins) == 0 && b.tent.UsedCount() == 1 && b.settled.UsedCount() <= 1 && len(b.strands) == 1
}

func (b *Braid) confirm() {
	if b.conf.confirmed || b.conf.lost {
		return
	}
	b.conf.confirmed, b.conf.at = true, b.own.Len()
	b.conf.changed()
	if h, err := b.own.Head(); err == nil {
		_ = b.db.Put([]byte(keyHead), h.Encode())
	}
	b.signal()
}

// writable reports whether the own strand may be written now: confirmed and not behind, or AppendUnconfirmed.
func (b *Braid) writable() bool {
	return b.opts.AppendUnconfirmed || (b.conf.confirmed && !b.conf.behind)
}

// longer takes proof that a history of the own strand is longer than the local one. Once the strand wrote after its
// confirmation, only a second running copy can have written past it, so the strand is lost; else it must catch up first.
func (b *Braid) longer() {
	if b.conf.confirmed && b.own.Len() > b.conf.at {
		b.lose()
		return
	}
	if !b.conf.behind {
		b.conf.behind = true
		b.conf.changed()
	}
}

// lose marks the own strand lost: a peer holds a history of it that conflicts with ours. Writes are refused from now on.
func (b *Braid) lose() {
	if b.conf.lost {
		return
	}
	b.conf.lost, b.conf.confirmed = true, false
	b.conf.changed()
	b.ev.push(Event{Kind: EventStrandLost, Member: b.key, Err: ErrStrandLost})
}

// otherSeats returns the keys of the current config's seats other than the own one, and whether the own strand is a seat.
func (b *Braid) otherSeats() ([][32]byte, bool) {
	_, cfg, mine := b.mySeat()
	var out [][32]byte
	for _, s := range cfg.Seats {
		if s.Key != key32(b.key) {
			out = append(out, s.Key)
		}
	}
	return out, mine
}

// checkConfirm confirms a seat once every other seat answered, or after ConfirmWait once at least q - 1 did, and a device that
// is no seat after ConfirmWait once a shorter head was shown.
func (b *Braid) checkConfirm() {
	if b.conf.confirmed || b.conf.lost || b.conf.behind || b.conf.answered == nil {
		return
	}
	others, mine := b.otherSeats()
	if !mine {
		if b.conf.heard && time.Since(b.conf.opened) >= b.lim.ConfirmWait {
			b.confirm()
		}
		return
	}
	n := 0
	for _, k := range others {
		if b.conf.answered[k] {
			n++
		}
	}
	q := b.st.Config().Quorum()
	if n == len(others) || (time.Since(b.conf.opened) >= b.lim.ConfirmWait && n >= q-1) {
		b.confirm()
	}
}

// confirmTimer checks the seat rule when ConfirmWait ends and returns how long until then.
func (b *Braid) confirmTimer(now time.Time) time.Duration {
	if b.conf.confirmed || b.conf.lost {
		return 0
	}
	b.checkConfirm()
	if d := b.lim.ConfirmWait - now.Sub(b.conf.opened); d > 0 && !b.conf.confirmed {
		return d
	}
	return 0
}

func (b *Braid) waitConfirmed(ctx context.Context) error {
	for {
		b.mu.Lock()
		switch {
		case b.closed:
			b.mu.Unlock()
			return ErrClosed
		case b.fatal != nil:
			err := b.fatal
			b.mu.Unlock()
			return err
		case b.conf.lost:
			b.mu.Unlock()
			return ErrStrandLost
		case b.waiting, b.joining():
		case b.writable():
			b.mu.Unlock()
			return nil
		case b.tent != nil:
			// no confirmation makes a strand that the order holds no member of writable, and no member answers for it
			if err := b.mayWrite(false); errors.Is(err, ErrNotMember) || errors.Is(err, ErrRemoved) {
				b.mu.Unlock()
				return err
			}
		}
		w := b.conf.wake
		b.mu.Unlock()
		select {
		case <-w:
		case <-ctx.Done():
			return fmt.Errorf("%w: %w", ErrUnconfirmed, ctx.Err())
		}
	}
}

// member reports whether a peer key may confirm the own strand: an active member or a pin.
func (b *Braid) member(k ed25519.PublicKey) bool {
	if bytes.Equal(k, b.key) {
		return false
	}
	if _, ok := b.tent.Member(rosterKey(k)); ok {
		return true
	}
	return slices.ContainsFunc(b.opts.Pins, func(p ed25519.PublicKey) bool { return bytes.Equal(p, k) })
}

// ownHead takes a head of the own strand that peer from holds, for every head the sync layer fetched or was announced.
// An inconsistent head loses the strand, a longer one makes it behind, an equal one confirms it and a shorter one counts toward
// confirming it after ConfirmWait.
func (b *Braid) ownHead(from ed25519.PublicKey, h kololog.Head) error {
	vh, err := kololog.VerifyHead(b.key, b.meta.OwnStrand, h)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidArgument, err)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.usable(); err != nil {
		return err
	}
	if b.waiting || b.conf.lost || !b.member(from) {
		return nil
	}
	switch b.consistent(vh) {
	case headConflicts:
		b.lose()
		return ErrStrandLost
	case headLonger:
		b.longer()
		if b.conf.lost {
			return ErrStrandLost
		}
		return nil
	}
	b.answer(from, vh.Size())
	return nil
}

// ownNone takes the answer of a peer that holds no head of the own strand. Only an own strand that is still empty counts it as
// consistent, as a head of size 0 would be: a member that never appended has nothing a peer could show it.
func (b *Braid) ownNone(from ed25519.PublicKey) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.usable() != nil || b.waiting || b.conf.lost || !b.member(from) || b.own.Len() != 0 {
		return
	}
	b.answer(from, 0)
}

// answer counts a consistent answer of peer from, a head of size; the caller holds b.mu. A device that is no seat is confirmed
// at once by a head as long as its strand, and by a shorter one only after ConfirmWait, since a shorter head may come from a
// member as far behind as a restored backup.
func (b *Braid) answer(from ed25519.PublicKey, size uint64) {
	if b.conf.confirmed {
		return
	}
	others, mine := b.otherSeats()
	if !mine {
		if b.conf.behind {
			return
		}
		if size >= b.own.Len() {
			b.confirm()
			return
		}
		b.conf.heard = true
		b.checkConfirm()
		return
	}
	if k := key32(from); slices.Contains(others, k) {
		b.conf.answered[k] = true
	}
	b.checkConfirm()
}

type headCheck uint8

const (
	headConsistent headCheck = iota
	headConflicts
	headLonger
)

// consistent compares a verified head of the own strand with the local log.
func (b *Braid) consistent(vh kololog.VerifiedHead) headCheck {
	n := b.own.Len()
	lh, err := b.own.Head()
	if err != nil {
		return headConflicts
	}
	lv, err := kololog.VerifyHead(b.key, b.meta.OwnStrand, lh)
	if err != nil {
		return headConflicts
	}
	switch {
	case vh.Size() > n:
		return headLonger
	case vh.Size() == n:
		if vh.Root() == lv.Root() {
			return headConsistent
		}
		return headConflicts
	}
	p, err := b.own.ConsistencyProof(vh.Size(), n)
	if err != nil || !kololog.VerifyConsistency(vh, lv, p) {
		return headConflicts
	}
	return headConsistent
}

// tempStore returns the Store of temporary replicas, which Open clears.
func (b *Braid) tempStore() *kolostore.Store {
	if b.temp == nil {
		b.temp = kolostore.New(prefixdb.New(b.db, []byte{tempPrefix}))
	}
	return b.temp
}

// catchUpReplica returns a temporary replica of the own strand for the sync layer to fill from a peer with a longer history.
func (b *Braid) catchUpReplica() (*kolostore.Replica, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.usable(); err != nil {
		return nil, err
	}
	if b.waiting {
		return nil, ErrUnconfirmed
	}
	return b.tempStore().OpenReplica(b.key, b.meta.OwnStrand, b.own.Header().Encode())
}

// adoptOwn replaces the own strand with rep, a complete replica of it from catchUpReplica whose history extends the local one, and
// reads the entries that were missing. A replica that does not extend the local log makes the strand lost.
func (b *Braid) adoptOwn(rep *kolostore.Replica) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.usable(); err != nil {
		return err
	}
	if rep == nil || rep.ID() != b.meta.OwnStrand || !bytes.Equal(rep.Header().LogKey, b.key) {
		return fmt.Errorf("%w: not a replica of the own strand", ErrInvalidArgument)
	}
	n := b.own.Len()
	rh, err := rep.Head()
	if err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidArgument, err)
	}
	rv, err := kololog.VerifyHead(b.key, b.meta.OwnStrand, rh)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidArgument, err)
	}
	if rv.Size() <= n {
		if b.consistent(rv) == headConflicts {
			b.lose()
			return ErrStrandLost
		}
		return nil
	}
	lh, err := b.own.Head()
	if err != nil {
		return err
	}
	lv, err := kololog.VerifyHead(b.key, b.meta.OwnStrand, lh)
	if err != nil {
		return err
	}
	p, err := rep.ConsistencyProof(n, rv.Size())
	if err != nil || !kololog.VerifyConsistency(lv, rv, p) {
		b.lose()
		return ErrStrandLost
	}
	st := b.storeFor(b.key)
	if err := b.own.Close(); err != nil {
		return b.fail(err)
	}
	l, err := st.Adopt(b.id, rep)
	if err != nil {
		if old, oerr := st.Open(b.id, b.meta.OwnStrand); oerr == nil {
			b.own = old
			b.strands[b.meta.OwnStrand].log = old
			return err
		}
		return b.fail(err)
	}
	b.own = l
	s := b.strands[b.meta.OwnStrand]
	s.log, s.stop = l, nil
	if err := rep.Close(); err == nil {
		_ = b.tempStore().Delete(rep.ID())
	}
	b.conf.behind = false
	if b.conf.confirmed {
		b.conf.at = l.Len()
	}
	b.ev.push(Event{Kind: EventStrandCaughtUp, Position: l.Len(), Member: b.key})
	if err := b.ingest(s); err != nil {
		return b.fail(err)
	}
	// the adopted entries may end with a run without a pulse
	b.countSincePulse()
	for _, x := range b.nodes {
		for _, lk := range x.links {
			if x.placed && x.strand != b.meta.OwnStrand && lk.Strand == b.meta.OwnStrand && lk.Index >= l.Len() {
				b.conf.behind = true
			}
		}
	}
	b.conf.changed()
	return b.progress()
}
