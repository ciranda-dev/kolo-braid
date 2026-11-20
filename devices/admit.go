package devices

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"time"

	koloid "github.com/ciranda-dev/kolo-id"
	kololog "github.com/ciranda-dev/kolo-log"

	kolobraid "github.com/ciranda-dev/kolo-braid"
)

// ctxAdmit is the signing context of a device's proof for a join request.
const ctxAdmit = "kolo-braid/device-admit/v0"

// recordPoll is how often a record is looked for in the view after it was appended.
const recordPoll = 2 * time.Millisecond

// proofMessage is what a device signs for its join request: the person, the group and the strand of the request.
func proofMessage(person []byte, group kolobraid.GroupID, strand kololog.LogID) []byte {
	b := append(bytes.Clone(person), group[:]...)
	return append(b, strand[:]...)
}

// NewJoinRequest returns the join request of j with a proof that id holds the key cert certifies, bound to group, which the
// application tells the joiner (for example in its invite). appData comes back through AppData.
func NewJoinRequest(j *kolobraid.Joining, id *koloid.Identity, cert *koloid.DeviceCert, group kolobraid.GroupID, appData []byte) ([]byte, error) {
	if j == nil || id == nil || cert == nil || id.PublicKey() == nil {
		return nil, fmt.Errorf("%w: nil join, identity or certificate", kolobraid.ErrInvalidArgument)
	}
	if !bytes.Equal(id.PublicKey(), cert.Device()) {
		return nil, fmt.Errorf("%w: the certificate is for another key", kolobraid.ErrInvalidArgument)
	}
	first, err := j.Request(nil)
	if err != nil {
		return nil, err
	}
	jr, err := kolobraid.ParseJoinRequest(first)
	if err != nil {
		return nil, err
	}
	sig, err := id.Sign(ctxAdmit, proofMessage(cert.Person(), group, jr.Strand))
	if err != nil {
		return nil, err
	}
	return j.Request(proof{cert: cert, sig: sig, app: appData}.encode())
}

func requestProof(request []byte) (kolobraid.JoinRequest, proof, error) {
	jr, err := kolobraid.ParseJoinRequest(request)
	if err != nil {
		return jr, proof{}, err
	}
	p, err := decodeProof(jr.AppData)
	if err != nil {
		return jr, proof{}, fmt.Errorf("%w: %w", ErrNotProven, err)
	}
	return jr, p, nil
}

// AppData returns the application data NewJoinRequest carried in request; ErrNotProven when it carries no device proof.
func AppData(request []byte) ([]byte, error) {
	_, p, err := requestProof(request)
	return p.app, err
}

// Certificate returns the certificate NewJoinRequest carried in request, not verified; ErrNotProven when it carries none.
func Certificate(request []byte) (*koloid.DeviceCert, error) {
	_, p, err := requestProof(request)
	return p.cert, err
}

// Record appends the certificate c as a roster entry from this peer, which must be an admin, and returns once the roster holds
// it anchored. An admin's record anchors a person: use it for the first device of a person, after the admin decided to admit it.
// It returns ErrNotRecorded when the entry was applied or rejected without an anchored certificate, or the context ended.
func (s *Store) Record(ctx context.Context, b *kolobraid.Braid, c *koloid.DeviceCert) error {
	if b == nil || c == nil {
		return fmt.Errorf("%w: nil braid or certificate", kolobraid.ErrInvalidArgument)
	}
	if err := c.Verify(&koloid.VerifyOptions{Person: c.Person(), Now: epoch}); err != nil {
		return fmt.Errorf("%w: %w", ErrNotProven, err)
	}
	if err := s.acquire(ctx); err != nil {
		return err
	}
	defer s.release()
	if err := adminOf(ctx, b); err != nil {
		return err
	}
	return s.record(ctx, b, c)
}

// adminOf returns an error unless this peer is an active admin of b. A view whose genesis is not applied yet has no members, so it
// waits for that, as long as ctx allows.
func adminOf(ctx context.Context, b *kolobraid.Braid) error {
	t := time.NewTimer(recordPoll)
	defer t.Stop()
	for {
		v := b.View()
		ms := v.Members()
		v.Close()
		for _, m := range ms {
			if bytes.Equal(m.Key, b.PublicKey()) {
				if m.Roles&kolobraid.RoleAdmin == 0 {
					return kolobraid.ErrNotAdmin
				}
				return nil
			}
		}
		if len(ms) > 0 {
			return kolobraid.ErrNotMember
		}
		t.Reset(recordPoll)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
}

// findEntry looks for the entry id from position from on, and from start when it is not there: a reorg can move it before from.
func findEntry(b *kolobraid.Braid, id kolobraid.EntryID, from, start uint64) (kolobraid.Ordered, bool) {
	for _, f := range []uint64{from, start} {
		for o, err := range b.Entries(f) {
			if err != nil {
				break // ErrReorged: look again at the next poll
			}
			if o.ID == id {
				return o, true
			}
		}
		if from <= start {
			break
		}
	}
	return kolobraid.Ordered{}, false
}

// record appends c and waits, as long as ctx allows, until the roster holds a certificate of the device anchored and at least as
// new as c; if the entry settles without that (rejected, void, or applied unanchored) it returns ErrNotRecorded.
func (s *Store) record(ctx context.Context, b *kolobraid.Braid, c *koloid.DeviceCert) error {
	body, err := CertBody(c)
	if err != nil {
		return err
	}
	start := b.Status().Settled // the entry is ordered at this position or later
	ref, err := b.Append(ctx, body)
	if err != nil {
		return err
	}
	from := start
	t := time.NewTimer(recordPoll)
	defer t.Stop()
	held := func(v *kolobraid.Snapshot) bool {
		defer v.Close()
		got, st, err := s.cert(v, c.Person(), c.Device())
		return err == nil && st.anchored && got.Serial() >= c.Serial()
	}
	for {
		if held(b.View()) {
			return nil
		}
		if o, ok := findEntry(b, ref.ID, from, start); ok {
			from = o.Position
			if o.Settled && b.Status().Settled > o.Position {
				if held(b.Settled()) {
					return nil
				}
				return ErrNotRecorded
			}
		}
		t.Reset(recordPoll)
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: %w", ErrNotRecorded, ctx.Err())
		case <-t.C:
		}
	}
}

// Admit admits the device whose key and join request are given when the request proves a valid certificate of the joiner's key
// for a person an admin already anchored in the group, and returns the welcome as Braid.Admit does; the checks and their order are
// in the guide. The caller must be an admin. Every wait ends with ctx.
func (s *Store) Admit(ctx context.Context, b *kolobraid.Braid, joiner ed25519.PublicKey, request []byte) ([]byte, error) {
	if b == nil || len(joiner) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("%w: nil braid or a key of %d bytes", kolobraid.ErrInvalidArgument, len(joiner))
	}
	jr, p, err := requestProof(request)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(p.cert.Device(), joiner) {
		return nil, fmt.Errorf("%w: the certificate is for another key", ErrNotProven)
	}
	if err := s.acquire(ctx); err != nil {
		return nil, err
	}
	defer s.release()
	recorded, err := s.check(b, jr, p, request)
	if err != nil {
		return nil, err
	}
	if !recorded {
		if err := s.record(ctx, b, p.cert); err != nil {
			return nil, err
		}
	}
	start := b.Status().Settled
	ordinary := kolobraid.Role(0)
	welcome, err := b.Admit(ctx, joiner, request, &kolobraid.AdmitOptions{Roles: &ordinary})
	if err != nil {
		return nil, err
	}
	if err := s.waitSeat(ctx, b, welcome, joiner, start); err != nil {
		return nil, err
	}
	return welcome, nil
}

// waitSeat waits until the view shows joiner as a member, so the next admission under the lock counts it, or the AddMember entry the
// welcome names settles without a member (ErrNotAdmitted: void, rejected or removed again), or ctx ends.
func (s *Store) waitSeat(ctx context.Context, b *kolobraid.Braid, welcome []byte, joiner ed25519.PublicKey, start uint64) error {
	ref, err := kolobraid.WelcomeEntry(welcome)
	if err != nil {
		return err
	}
	from := start
	t := time.NewTimer(recordPoll)
	defer t.Stop()
	member := func(v *kolobraid.Snapshot) bool {
		defer v.Close()
		for _, m := range v.Members() {
			if bytes.Equal(m.Key, joiner) {
				return true
			}
		}
		return false
	}
	for {
		if member(b.View()) {
			return nil
		}
		if o, ok := findEntry(b, ref.ID, from, start); ok {
			from = o.Position
			if o.Settled && b.Status().Settled > o.Position {
				if member(b.Settled()) {
					return nil
				}
				return ErrNotAdmitted
			}
		}
		t.Reset(recordPoll)
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: %w", ErrNotAdmitted, ctx.Err())
		case <-t.C:
		}
	}
}

// belongs reports ErrUnknownPerson unless an active member of v holds an anchored certificate of person that verifies at now.
func (s *Store) belongs(v *kolobraid.Snapshot, person ed25519.PublicKey, now time.Time) error {
	for _, m := range v.Members() {
		c, st, err := s.cert(v, person, m.Key)
		if errors.Is(err, kolobraid.ErrNotFound) || (err == nil && !st.anchored) {
			continue
		}
		if err != nil {
			return err
		}
		revs, err := s.revocationsOf(v, v, person, c)
		if err != nil {
			return err
		}
		if c.Verify(&koloid.VerifyOptions{Person: person, Now: now, Revocations: revs}) == nil {
			return nil
		}
	}
	return ErrUnknownPerson
}

// check runs the checks of Admit on the braid's snapshots, which it closes before it returns; recorded is true when the roster
// already holds the certificate, anchored.
func (s *Store) check(b *kolobraid.Braid, jr kolobraid.JoinRequest, p proof, request []byte) (recorded bool, err error) {
	person, joiner, now := p.cert.Person(), p.cert.Device(), s.now()
	// what Braid.Admit would refuse, and a full group, before anything is written
	if err := b.CheckAdmit(joiner, request); err != nil {
		return false, err
	}
	settled, view := b.Settled(), b.View()
	defer settled.Close()
	defer view.Close()
	if err := s.belongs(settled, person, now); err != nil {
		return false, err
	}
	have, st, err := s.cert(view, person, joiner)
	switch {
	case err == nil && have.Serial() > p.cert.Serial():
		return false, fmt.Errorf("%w: a newer certificate of this key is in the roster", ErrNotProven)
	case err == nil && have.Serial() == p.cert.Serial() && !bytes.Equal(have.Encode(), p.cert.Encode()):
		return false, fmt.Errorf("%w: another certificate with this serial is in the roster", ErrNotProven)
	case err == nil:
		recorded = st.anchored && bytes.Equal(have.Encode(), p.cert.Encode())
	case !errors.Is(err, kolobraid.ErrNotFound):
		return false, err
	}
	// approver-signed revocations count only once settled: every peer then agrees, whatever order they arrive in
	revs, err := s.revocationsOf(view, settled, person, p.cert)
	if err != nil {
		return false, err
	}
	opts := &koloid.VerifyOptions{Person: person, Now: now, Revocations: revs}
	if err := koloid.VerifyDevice(p.cert, opts, ctxAdmit, proofMessage(person, b.GroupID(), jr.Strand), p.sig); err != nil {
		return false, fmt.Errorf("%w: %w", ErrNotProven, err)
	}
	if s.max > 0 {
		// a seat is a member with an anchored certificate of the person, revoked or not: a revoked device keeps its seat until an
		// admin removes it
		n := 0
		for _, m := range view.Members() {
			_, st, err := s.cert(view, person, m.Key)
			if errors.Is(err, kolobraid.ErrNotFound) || (err == nil && !st.anchored) {
				continue
			}
			if err != nil {
				return false, err
			}
			n++
		}
		if n >= s.max {
			return false, ErrTooManyDevices
		}
	}
	return recorded, nil
}
