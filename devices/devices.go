// Package devices keeps the device certificates and revocations of a group's persons in a braid's view and admits a certified
// device into a group its person already belongs to. The rules are in docs/guide.md.
package devices

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"iter"
	"slices"
	"time"

	kolocodec "github.com/ciranda-dev/kolo-codec"
	koloid "github.com/ciranda-dev/kolo-id"

	kolobraid "github.com/ciranda-dev/kolo-braid"
)

const (
	// DefaultMaxCertsPerPerson is the default of Options.MaxCertsPerPerson.
	DefaultMaxCertsPerPerson = 1024
	// DefaultMaxRevocationsPerDevice is the default of Options.MaxRevocationsPerDevice.
	DefaultMaxRevocationsPerDevice = 64
	// maxCertsLimit and maxRevocationsLimit bound the options: one scan of the view is at most that long.
	maxCertsLimit       = 1 << 20
	maxRevocationsLimit = 1 << 16
)

var (
	// ErrTooManyDevices is returned by Admit when Options.MaxDevices is set and the person already has that many devices.
	ErrTooManyDevices = errors.New("kolobraid: too many devices of this person in the group")
	// ErrNotProven is returned by Admit and Record when the certificate or the proof of the joiner does not hold.
	ErrNotProven = errors.New("kolobraid: device certificate not proven")
	// ErrUnknownPerson is returned by Admit when no admin has anchored a device of the person in the group.
	ErrUnknownPerson = errors.New("kolobraid: person has no anchored device in the group")
	// ErrUnknownDevice is returned by Verify for a device without a stored certificate of the person.
	ErrUnknownDevice = errors.New("kolobraid: no certificate for this device")
	// ErrNotRecorded is returned when the roster applied or rejected the record without holding it anchored, or when the caller's
	// context ended first; in that case the entry may still apply later, anchored or not, and nothing else was written.
	ErrNotRecorded = errors.New("kolobraid: the roster did not take the record")
	// ErrNotAdmitted is returned by Admit when the AddMember entry was written but the group did not take the member (it is void,
	// for example in a full group), or when the caller's context ended first, in which case the entry may still apply later:
	// remove the joiner with RemoveMember if no welcome reaches it.
	ErrNotAdmitted = errors.New("kolobraid: the group did not take the member")
)

// Options are the choices of a Store. The three bounds of apply decide which entries are rejected, so every peer of a group must use
// the same values, as it must run the same apply function.
type Options struct {
	// MaxDevices, when positive, is the most seats of one person that Admit lets in: the members that hold an anchored certificate
	// of it, revoked or not. Zero, the default, is no ceiling: the group's MaxMembers seats are the only bound. Local to the
	// admitting peer.
	MaxDevices int
	// MaxCertsPerPerson is the most device certificates a view holds for one person; the next is rejected. Default
	// DefaultMaxCertsPerPerson, range 1 to 2^20: it bounds the scan of one apply.
	MaxCertsPerPerson int
	// PersonCertReserve of those slots are kept for certificates the person key issued, so a stolen approver cannot fill the
	// roster: approver-issued certificates stop at MaxCertsPerPerson - PersonCertReserve. Default an eighth of
	// MaxCertsPerPerson, at least 1 when MaxCertsPerPerson is above 1; range 0 to MaxCertsPerPerson; zero stands for the default,
	// and -1 reserves nothing.
	PersonCertReserve int
	// MaxRevocationsPerDevice is the most approver-signed revocations kept for one device key, one per certificate it had; the
	// next is rejected. Default DefaultMaxRevocationsPerDevice, range 1 to 2^16: an arbitrary safety bound.
	MaxRevocationsPerDevice int
	// Now is the clock of Admit; nil means time.Now.
	Now func() time.Time
}

// Store reads and writes the roster in a view under its key prefix; no other view key may start with the prefix.
type Store struct {
	prefix []byte
	max    int
	now    func() time.Time

	sem chan struct{} // one Admit or Record at a time; a caller waits for it only as long as its context allows

	certCap, reserve, revCap int
}

// acquire takes the store's lock, giving up when ctx ends; release gives it back.
func (s *Store) acquire(ctx context.Context) error {
	select {
	case s.sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Store) release() { <-s.sem }

// Key kinds under the prefix: 'c' person device, 'r' person device, 'v' person device target.
const (
	kindCert    = 'c' // the certificate with the highest serial of a device
	kindSelf    = 'r' // the highest revocation by serial of a device, signed by the person or the device itself
	kindApprove = 'v' // one approver-signed revocation of a device, by target hash
)

// New returns a store whose records live in the view under prefix, which may be empty.
func New(prefix []byte, o *Options) (*Store, error) {
	var opts Options
	if o != nil {
		opts = *o
	}
	if opts.MaxCertsPerPerson == 0 {
		opts.MaxCertsPerPerson = DefaultMaxCertsPerPerson
	}
	switch opts.PersonCertReserve {
	case 0:
		opts.PersonCertReserve = opts.MaxCertsPerPerson / 8
		if opts.MaxCertsPerPerson > 1 {
			opts.PersonCertReserve = max(1, opts.PersonCertReserve)
		}
	case -1:
		opts.PersonCertReserve = 0
	}
	if opts.MaxRevocationsPerDevice == 0 {
		opts.MaxRevocationsPerDevice = DefaultMaxRevocationsPerDevice
	}
	var why string
	switch {
	case opts.MaxDevices < 0:
		why = "MaxDevices must not be negative"
	case opts.MaxCertsPerPerson < 1 || opts.MaxCertsPerPerson > maxCertsLimit:
		why = "MaxCertsPerPerson must be 1 to 2^20"
	case opts.PersonCertReserve < 0 || opts.PersonCertReserve > opts.MaxCertsPerPerson:
		why = "PersonCertReserve must be 0 to MaxCertsPerPerson"
	case opts.MaxRevocationsPerDevice < 1 || opts.MaxRevocationsPerDevice > maxRevocationsLimit:
		why = "MaxRevocationsPerDevice must be 1 to 2^16"
	case len(prefix) > 1024:
		why = fmt.Sprintf("prefix of %d bytes", len(prefix))
	}
	if why != "" {
		return nil, fmt.Errorf("%w: %s", kolobraid.ErrInvalidArgument, why)
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	return &Store{prefix: slices.Clone(prefix), max: opts.MaxDevices, now: opts.Now, sem: make(chan struct{}, 1),
		certCap: opts.MaxCertsPerPerson, reserve: opts.PersonCertReserve, revCap: opts.MaxRevocationsPerDevice}, nil
}

func (s *Store) key(kind byte, parts ...[]byte) []byte {
	k := append(slices.Clone(s.prefix), kind)
	for _, p := range parts {
		k = append(k, p...)
	}
	return k
}

// upper returns the smallest key above every key that starts with p.
func upper(p []byte) []byte {
	for i := len(p) - 1; i >= 0; i-- {
		if p[i] != 0xff {
			return append(slices.Clone(p[:i]), p[i]+1)
		}
	}
	return nil
}

func badKeys(keys ...ed25519.PublicKey) error {
	for _, k := range keys {
		if len(k) != ed25519.PublicKeySize {
			return fmt.Errorf("%w: a key of %d bytes", kolobraid.ErrInvalidArgument, len(k))
		}
	}
	return nil
}

// stored is a record in the view: where its entry was ordered, whether an admin wrote it, whether an approver issued it, and
// its encoding.
type stored struct {
	position uint64
	anchored bool
	approver bool
	raw      []byte
}

func (s stored) encode() []byte {
	var e kolocodec.Encoder
	e.PutUvarint(s.position)
	e.PutBool(s.anchored)
	e.PutBool(s.approver)
	e.PutBytes(s.raw)
	return e.Bytes()
}

func decodeStored(b []byte) (stored, error) {
	d := kolocodec.NewDecoder(b)
	var s stored
	var err error
	if s.position, err = d.Uvarint(); err == nil {
		if s.anchored, err = d.Bool(); err == nil {
			if s.approver, err = d.Bool(); err == nil {
				s.raw, err = d.Bytes(maxCertBytes)
			}
		}
	}
	if err == nil {
		err = d.Done()
	}
	if err != nil {
		return stored{}, fmt.Errorf("%w: roster record: %w", kolobraid.ErrCorrupt, err)
	}
	return s, nil
}

// epoch is the clock of apply: a certificate is checked for structure and signatures only, since a view has no clock.
var epoch = time.Unix(0, 0)

// Apply applies e when it is a roster entry and reports false for every other entry; see the guide for what it rejects.
func (s *Store) Apply(tx *kolobraid.Tx, e *kolobraid.Entry) (handled bool, err error) {
	if e.Kind != kolobraid.KindData || !isRecord(e.Body) {
		return false, nil
	}
	r, err := decodeRecord(e.Body)
	if err != nil {
		return true, kolobraid.Reject(err.Error())
	}
	if r.op == opCert {
		return true, s.applyCert(tx, e, r.cert)
	}
	return true, s.applyRevocation(tx, e, r)
}

func reject(why string, err error) error {
	if err != nil {
		return kolobraid.Reject(why + ": " + err.Error())
	}
	return kolobraid.Reject(why)
}

// writerIsAdmin reports whether the writer of e is an admin at e's position.
func writerIsAdmin(tx *kolobraid.Tx, e *kolobraid.Entry) bool {
	m, ok := tx.Member(e.Writer)
	return ok && m.Roles&kolobraid.RoleAdmin != 0
}

// countCerts counts the certificates of person other than device's: all of them and those an approver issued.
func (s *Store) countCerts(tx *kolobraid.Tx, person, device []byte) (total, approver int, err error) {
	start := s.key(kindCert, person)
	for k, v := range tx.Scan(start, upper(start)) {
		if bytes.Equal(k[len(k)-ed25519.PublicKeySize:], device) {
			continue
		}
		st, err := decodeStored(v)
		if err != nil {
			return 0, 0, err
		}
		total++
		if st.approver {
			approver++
		}
		if total >= s.certCap {
			break
		}
	}
	return total, approver, nil
}

func (s *Store) applyCert(tx *kolobraid.Tx, e *kolobraid.Entry, c *koloid.DeviceCert) error {
	person, device := c.Person(), c.Device()
	if err := c.Verify(&koloid.VerifyOptions{Person: person, Now: epoch}); err != nil {
		return reject("certificate", err)
	}
	viaApprover := !bytes.Equal(c.Issuer(), person)
	if viaApprover {
		// an approver the person or the device already revoked signs no more certificates
		if _, err := tx.Get(s.key(kindSelf, person, c.Issuer())); err == nil {
			return reject("certificate of a retired approver", nil)
		} else if !errors.Is(err, kolobraid.ErrNotFound) {
			return err
		}
	}
	admin := writerIsAdmin(tx, e)
	key := s.key(kindCert, person, device)
	enc := c.Encode()
	old, err := tx.Get(key)
	anchored := admin
	switch {
	case err == nil:
		st, err := decodeStored(old)
		if err != nil {
			return err
		}
		if bytes.Equal(st.raw, enc) {
			if admin && !st.anchored {
				st.anchored = true
				return tx.Put(key, st.encode())
			}
			return nil
		}
		prev, err := koloid.DecodeDeviceCert(st.raw)
		if err != nil {
			return fmt.Errorf("%w: stored certificate: %w", kolobraid.ErrCorrupt, err)
		}
		if c.Serial() <= prev.Serial() {
			return reject("a different certificate whose serial is not higher", nil)
		}
		anchored = admin || st.anchored
	case !errors.Is(err, kolobraid.ErrNotFound):
		return err
	}
	total, approver, err := s.countCerts(tx, person, device)
	if err != nil {
		return err
	}
	if total >= s.certCap || (viaApprover && approver >= s.certCap-s.reserve) {
		return reject("too many devices of one person", nil)
	}
	return tx.Put(key, stored{position: e.Position, anchored: anchored, approver: viaApprover, raw: enc}.encode())
}

func (s *Store) applyRevocation(tx *kolobraid.Tx, e *kolobraid.Entry, rec record) error {
	r := rec.rev
	person := r.Person()
	approver := r.TargetHash() != nil
	if approver != (rec.target != nil) {
		return reject("a target goes with an approver-signed revocation and with no other", nil)
	}
	if approver {
		if err := rec.target.Verify(&koloid.VerifyOptions{Person: person, Now: epoch}); err != nil {
			return reject("target certificate", err)
		}
	}
	if err := r.Verify(&koloid.VerifyOptions{Person: person}, rec.target); err != nil {
		return reject("revocation", err)
	}
	if approver {
		return s.putApprover(tx, e, r)
	}
	return s.putSelf(tx, e, r)
}

func (s *Store) putApprover(tx *kolobraid.Tx, e *kolobraid.Entry, r *koloid.Revocation) error {
	key := s.key(kindApprove, r.Person(), r.Device(), r.TargetHash())
	if _, err := tx.Get(key); err == nil {
		return nil
	} else if !errors.Is(err, kolobraid.ErrNotFound) {
		return err
	}
	n := 0
	start := s.key(kindApprove, r.Person(), r.Device())
	for range tx.Scan(start, upper(start)) {
		if n++; n >= s.revCap {
			return reject("too many revocations of one device", nil)
		}
	}
	return tx.Put(key, stored{position: e.Position, raw: r.Encode()}.encode())
}

// putSelf keeps the revocation by serial with the highest UpToSerial and the position of the first one: the position is when
// the person or the device first said it is revoked.
func (s *Store) putSelf(tx *kolobraid.Tx, e *kolobraid.Entry, r *koloid.Revocation) error {
	key := s.key(kindSelf, r.Person(), r.Device())
	first := e.Position
	old, err := tx.Get(key)
	switch {
	case err == nil:
		st, err := decodeStored(old)
		if err != nil {
			return err
		}
		prev, err := koloid.DecodeRevocation(st.raw)
		if err != nil {
			return fmt.Errorf("%w: stored revocation: %w", kolobraid.ErrCorrupt, err)
		}
		if r.UpToSerial() <= prev.UpToSerial() {
			return nil
		}
		first = st.position
	case !errors.Is(err, kolobraid.ErrNotFound):
		return err
	}
	return tx.Put(key, stored{position: first, raw: r.Encode()}.encode())
}

// Cert returns the certificate of device in the roster of person at v, the one with the highest serial, not verified; it
// returns kolobraid.ErrNotFound for a device with none.
func (s *Store) Cert(v *kolobraid.Snapshot, person, device ed25519.PublicKey) (*koloid.DeviceCert, error) {
	c, _, err := s.cert(v, person, device)
	return c, err
}

// Anchored reports whether an admin recorded the certificate of device, which is what lets Admit trust the person.
func (s *Store) Anchored(v *kolobraid.Snapshot, person, device ed25519.PublicKey) (bool, error) {
	_, st, err := s.cert(v, person, device)
	return st.anchored, err
}

// CertTx is Cert inside apply: it reads the roster at the position of the entry that tx applies, with the writes tx made so far.
func (s *Store) CertTx(tx *kolobraid.Tx, person, device ed25519.PublicKey) (*koloid.DeviceCert, error) {
	c, _, err := s.cert(tx, person, device)
	return c, err
}

// reader is a view as a Store reads it: a *kolobraid.Snapshot, or inside apply a *kolobraid.Tx.
type reader interface {
	Get(key []byte) ([]byte, error)
	Scan(start, end []byte) iter.Seq2[[]byte, []byte]
}

// scan is v.Scan with the error that cut that scan short. A Tx has none to report: such an error halts the view.
func scan(v reader, start, end []byte) (iter.Seq2[[]byte, []byte], func() error) {
	if sn, ok := v.(*kolobraid.Snapshot); ok {
		return sn.ScanErr(start, end)
	}
	return v.Scan(start, end), func() error { return nil }
}

func (s *Store) cert(v reader, person, device ed25519.PublicKey) (*koloid.DeviceCert, stored, error) {
	if err := badKeys(person, device); err != nil {
		return nil, stored{}, err
	}
	raw, err := v.Get(s.key(kindCert, person, device))
	if err != nil {
		return nil, stored{}, err
	}
	return decodeCert(raw)
}

func decodeCert(raw []byte) (*koloid.DeviceCert, stored, error) {
	st, err := decodeStored(raw)
	if err != nil {
		return nil, stored{}, err
	}
	c, err := koloid.DecodeDeviceCert(st.raw)
	if err != nil {
		return nil, stored{}, fmt.Errorf("%w: stored certificate: %w", kolobraid.ErrCorrupt, err)
	}
	return c, st, nil
}

// Devices returns the certificates of person at v in device key order, not verified.
func (s *Store) Devices(v *kolobraid.Snapshot, person ed25519.PublicKey) ([]*koloid.DeviceCert, error) {
	if err := badKeys(person); err != nil {
		return nil, err
	}
	start := s.key(kindCert, person)
	var out []*koloid.DeviceCert
	pairs, scanErr := scan(v, start, upper(start))
	for _, val := range pairs {
		c, _, err := decodeCert(val)
		if err != nil {
			return nil, err
		}
		if out = append(out, c); len(out) >= s.certCap {
			break
		}
	}
	if err := scanErr(); err != nil {
		return nil, err
	}
	return out, nil
}

// Revocations returns the revocations of the device key that apply to person at v, for koloid.VerifyOptions; see the guide for
// the retirement of approvers. A certificate an approver issued is also void when the approver is revoked: RevocationsOf
// returns both, as Verify uses them.
func (s *Store) Revocations(v *kolobraid.Snapshot, person, device ed25519.PublicKey) ([]*koloid.Revocation, error) {
	if err := badKeys(person, device); err != nil {
		return nil, err
	}
	return s.collect(v, person, device, true, true)
}

// RevocationsTx is Revocations inside apply: it reads the roster at the position of the entry that tx applies, with the writes tx
// made so far. Like Revocations it leaves out the revocations of the certificate's issuer: RevocationsOfTx has them.
func (s *Store) RevocationsTx(tx *kolobraid.Tx, person, device ed25519.PublicKey) ([]*koloid.Revocation, error) {
	if err := badKeys(person, device); err != nil {
		return nil, err
	}
	return s.collect(tx, person, device, true, true)
}

// RevocationsOf returns the revocations at v that can void c for person, the set Verify passes to koloid.VerifyDevice: those of
// c's device and, for a certificate an approver issued, those of the approver.
func (s *Store) RevocationsOf(v *kolobraid.Snapshot, person ed25519.PublicKey, c *koloid.DeviceCert) ([]*koloid.Revocation, error) {
	return s.revocationsOfChecked(v, person, c)
}

// RevocationsOfTx is RevocationsOf inside apply, at the position of the entry that tx applies: with koloid.DeviceCert.Verify it
// tells whether c was revoked at or before that position.
func (s *Store) RevocationsOfTx(tx *kolobraid.Tx, person ed25519.PublicKey, c *koloid.DeviceCert) ([]*koloid.Revocation, error) {
	return s.revocationsOfChecked(tx, person, c)
}

func (s *Store) revocationsOfChecked(v reader, person ed25519.PublicKey, c *koloid.DeviceCert) ([]*koloid.Revocation, error) {
	if err := badKeys(person); err != nil {
		return nil, err
	}
	if c == nil {
		return nil, fmt.Errorf("%w: no certificate", kolobraid.ErrInvalidArgument)
	}
	return s.revocationsOf(v, v, person, c)
}

// collect returns the revocations by serial when self is set and the approver-signed ones when approver is set, with the
// approver records that a retired approver signed after its retirement left out.
func (s *Store) collect(v reader, person, device []byte, self, approver bool) ([]*koloid.Revocation, error) {
	var out []*koloid.Revocation
	if self {
		if raw, err := v.Get(s.key(kindSelf, person, device)); err == nil {
			r, _, err := decodeRevocation(raw)
			if err != nil {
				return nil, err
			}
			out = append(out, r)
		} else if !errors.Is(err, kolobraid.ErrNotFound) {
			return nil, err
		}
	}
	if !approver {
		return out, nil
	}
	start := s.key(kindApprove, person, device)
	retired := map[string]uint64{}
	pairs, scanErr := scan(v, start, upper(start))
	for _, val := range pairs {
		r, pos, err := decodeRevocation(val)
		if err != nil {
			return nil, err
		}
		signer := string(r.Signer())
		at, known := retired[signer]
		if !known {
			at = noRetirement
			if raw, err := v.Get(s.key(kindSelf, person, r.Signer())); err == nil {
				if _, at, err = decodeRevocation(raw); err != nil {
					return nil, err
				}
			} else if !errors.Is(err, kolobraid.ErrNotFound) {
				return nil, err
			}
			retired[signer] = at
		}
		if pos > at {
			continue
		}
		out = append(out, r)
	}
	if err := scanErr(); err != nil {
		return nil, err
	}
	return out, nil
}

// noRetirement is the position of an approver that was never retired.
const noRetirement = ^uint64(0)

func decodeRevocation(raw []byte) (*koloid.Revocation, uint64, error) {
	st, err := decodeStored(raw)
	if err != nil {
		return nil, 0, err
	}
	r, err := koloid.DecodeRevocation(st.raw)
	if err != nil {
		return nil, 0, fmt.Errorf("%w: stored revocation: %w", kolobraid.ErrCorrupt, err)
	}
	return r, st.position, nil
}

// revocationsOf returns the revocations that can void c: those of its device and, for a certificate an approver issued, those
// of the approver. Approver-signed records are read from approverView, the others from selfView.
func (s *Store) revocationsOf(selfView, approverView reader, person []byte, c *koloid.DeviceCert) ([]*koloid.Revocation, error) {
	keys := [][]byte{c.Device()}
	if issuer := c.Issuer(); !bytes.Equal(issuer, person) {
		keys = append(keys, issuer)
	}
	var out []*koloid.Revocation
	for _, k := range keys {
		own, err := s.collect(selfView, person, k, true, false)
		if err != nil {
			return nil, err
		}
		approver, err := s.collect(approverView, person, k, false, true)
		if err != nil {
			return nil, err
		}
		out = append(append(out, own...), approver...)
	}
	return out, nil
}

// Verify checks that sig is a signature of msg under context by device, a device of person, at time now (zero means now), with
// koloid.VerifyDevice on the stored certificate and revocations. The person is the caller's, never read from a certificate.
func (s *Store) Verify(v *kolobraid.Snapshot, person, device ed25519.PublicKey, now time.Time, context string, msg, sig []byte) error {
	c, _, err := s.cert(v, person, device)
	if errors.Is(err, kolobraid.ErrNotFound) {
		return ErrUnknownDevice
	}
	if err != nil {
		return err
	}
	revs, err := s.revocationsOf(v, v, person, c)
	if err != nil {
		return err
	}
	return koloid.VerifyDevice(c, &koloid.VerifyOptions{Person: person, Now: now, Revocations: revs}, context, msg, sig)
}
