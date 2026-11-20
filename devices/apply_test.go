package devices_test

import (
	"bytes"
	"errors"
	"testing"
	"time"

	kolocodec "github.com/ciranda-dev/kolo-codec"
	koloid "github.com/ciranda-dev/kolo-id"

	kolobraid "github.com/ciranda-dev/kolo-braid"
	"github.com/ciranda-dev/kolo-braid/devices"
	"github.com/ciranda-dev/kolo-braid/kolobraidtest"
)

const msgCtx = "devices/test/msg/v0"

// check verifies a signature by dev of the person in the tentative view of b.
func check(t testing.TB, s *devices.Store, b *kolobraid.Braid, person, dev *koloid.Identity, now time.Time) error {
	t.Helper()
	v := b.View()
	defer v.Close()
	msg := []byte("hello")
	return s.Verify(v, person.PublicKey(), dev.PublicKey(), now, msgCtx, msg, signAs(t, dev, msgCtx, msg))
}

func TestCertificateIsStoredAndVerified(t *testing.T) {
	s := newStore(t, nil)
	person, laptop, phone := generate(t), generate(t), generate(t)
	b := found(t, s, generate(t), kolobraid.GroupConfig{})
	lc := certify(t, person, laptop, 1, true)
	pc := certifyBy(t, laptop, lc, phone, 1)
	mustRecord(t, b, certBody(t, lc))
	mustRecord(t, b, certBody(t, pc))
	v := view(t, b)
	got, err := s.Cert(v, person.PublicKey(), phone.PublicKey())
	if err != nil || !bytes.Equal(got.Encode(), pc.Encode()) {
		t.Fatalf("Cert = %v, %v", got, err)
	}
	devs, err := s.Devices(v, person.PublicKey())
	if err != nil || len(devs) != 2 {
		t.Fatalf("Devices = %d, %v", len(devs), err)
	}
	for _, d := range []*koloid.Identity{laptop, phone} {
		if err := check(t, s, b, person, d, time.Time{}); err != nil {
			t.Fatalf("Verify: %v", err)
		}
	}
	// A device of another person, an unknown device and another person's key are not verified.
	other := generate(t)
	if _, err := s.Cert(v, other.PublicKey(), phone.PublicKey()); !errors.Is(err, kolobraid.ErrNotFound) {
		t.Fatalf("Cert of another person = %v", err)
	}
	if err := check(t, s, b, person, generate(t), time.Time{}); !errors.Is(err, devices.ErrUnknownDevice) {
		t.Fatalf("unknown device = %v", err)
	}
	if err := check(t, s, b, other, phone, time.Time{}); !errors.Is(err, devices.ErrUnknownDevice) {
		t.Fatalf("another person = %v", err)
	}
	if devs, _ := s.Devices(v, other.PublicKey()); len(devs) != 0 {
		t.Fatalf("Devices of another person = %d", len(devs))
	}
	// A signature of another device, and one under another context, do not verify.
	msg := []byte("hello")
	if err := s.Verify(v, person.PublicKey(), phone.PublicKey(), time.Time{}, msgCtx, msg, signAs(t, laptop, msgCtx, msg)); !errors.Is(err, koloid.ErrCertSignature) {
		t.Fatalf("signature of another device = %v", err)
	}
	if err := s.Verify(v, person.PublicKey(), phone.PublicKey(), time.Time{}, "other/v0", msg, signAs(t, phone, msgCtx, msg)); !errors.Is(err, koloid.ErrCertSignature) {
		t.Fatalf("signature under another context = %v", err)
	}
	if err := s.Verify(v, person.PublicKey(), phone.PublicKey(), time.Time{}, "kolo-id/device-link/v0", msg, nil); !errors.Is(err, koloid.ErrInvalidArgument) {
		t.Fatalf("reserved context = %v", err)
	}
}

func TestCertificateRecordRefusals(t *testing.T) {
	s := newStore(t, nil)
	person, dev := generate(t), generate(t)
	b := found(t, s, generate(t), kolobraid.GroupConfig{})
	good := certify(t, person, dev, 1, false)
	enc := good.Encode()
	// A bad signature, a certificate for a person that did not sign it, and every kind of bad body are rejected on apply.
	flip := func(i int) []byte {
		c := bytes.Clone(enc)
		c[i] ^= 1
		forged, err := koloid.DecodeDeviceCert(c)
		if err != nil {
			t.Fatalf("flip %d: %v", i, err)
		}
		return certBody(t, forged)
	}
	for name, body := range map[string][]byte{
		"issuer signature": flip(len(enc) - 130),
		"device signature": flip(len(enc) - 66),
		"issue time":       flip(99),
		"trailing byte":    append(certBody(t, good), 0),
		"empty":            nil,
		"other op":         op(t, 3),
		"cut":              certBody(t, good)[:40],
	} {
		if name == "empty" {
			continue // an empty body is not a roster entry: Apply does not handle it
		}
		if !record(t, b, body) {
			t.Errorf("%s: accepted", name)
		}
	}
	if _, err := s.Cert(view(t, b), person.PublicKey(), dev.PublicKey()); !errors.Is(err, kolobraid.ErrNotFound) {
		t.Fatalf("a rejected certificate is stored: %v", err)
	}
	// The same certificate, written by a member that is not the device, is accepted: a record is its own proof.
	mustRecord(t, b, certBody(t, good))
}

func op(t testing.TB, n uint64) []byte {
	var e kolocodec.Encoder
	e.PutString("kolo-braid/devices/v0")
	e.PutUvarint(n)
	return e.Bytes()
}

func TestCertificateSerials(t *testing.T) {
	s := newStore(t, nil)
	person, dev := generate(t), generate(t)
	b := found(t, s, generate(t), kolobraid.GroupConfig{})
	c1, c2, c3 := certify(t, person, dev, 1, false), certify(t, person, dev, 2, false), certify(t, person, dev, 3, false)
	mustRecord(t, b, certBody(t, c2))
	stored := func() *koloid.DeviceCert {
		got, err := s.Cert(view(t, b), person.PublicKey(), dev.PublicKey())
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	if stored().Serial() != 2 {
		t.Fatal("serial 2 not stored")
	}
	// A lower serial, and the same serial with other bytes, are rejected; the same bytes again change nothing.
	if !record(t, b, certBody(t, c1)) {
		t.Fatal("a lower serial was accepted")
	}
	other := certify(t, person, dev, 2, false)
	if bytes.Equal(other.Encode(), c2.Encode()) {
		other = certify(t, person, dev, 2, true)
	}
	if !record(t, b, certBody(t, other)) {
		t.Fatal("another certificate with the same serial was accepted")
	}
	mustRecord(t, b, certBody(t, c2))
	if got := stored(); !bytes.Equal(got.Encode(), c2.Encode()) {
		t.Fatal("the stored certificate changed")
	}
	mustRecord(t, b, certBody(t, c3))
	if stored().Serial() != 3 {
		t.Fatal("a higher serial did not replace the certificate")
	}
	if devs, _ := s.Devices(view(t, b), person.PublicKey()); len(devs) != 1 {
		t.Fatalf("Devices = %d, want 1", len(devs))
	}
}

func TestDeviceBoundPerPerson(t *testing.T) {
	s := newStore(t, &devices.Options{MaxCertsPerPerson: 3})
	person := generate(t)
	b := found(t, s, generate(t), kolobraid.GroupConfig{})
	devs := []*koloid.Identity{generate(t), generate(t), generate(t), generate(t)}
	for i, d := range devs[:3] {
		if record(t, b, certBody(t, certify(t, person, d, 1, i == 0))) {
			t.Fatalf("device %d rejected", i)
		}
	}
	if !record(t, b, certBody(t, certify(t, person, devs[3], 1, false))) {
		t.Fatal("a fourth device was accepted past the bound")
	}
	// A new serial of a known device and the devices of another person are not counted against it.
	mustRecord(t, b, certBody(t, certify(t, person, devs[1], 2, false)))
	mustRecord(t, b, certBody(t, certify(t, generate(t), devs[3], 1, false)))
}

func TestRevocationRecords(t *testing.T) {
	s := newStore(t, nil)
	person, laptop, phone := generate(t), generate(t), generate(t)
	b := found(t, s, generate(t), kolobraid.GroupConfig{})
	lc := certify(t, person, laptop, 1, true)
	pc := certifyBy(t, laptop, lc, phone, 1)
	mustRecord(t, b, certBody(t, lc))
	mustRecord(t, b, certBody(t, pc))
	if err := check(t, s, b, person, phone, time.Time{}); err != nil {
		t.Fatal(err)
	}
	// The device revokes itself: its certificate is void, and a higher serial recovers it.
	mustRecord(t, b, revBody(t, revokeBySerial(t, phone, person.PublicKey(), phone.PublicKey(), 1), nil))
	if err := check(t, s, b, person, phone, time.Time{}); !errors.Is(err, koloid.ErrCertRevoked) {
		t.Fatalf("after a self revocation = %v", err)
	}
	pc2 := certifyBy(t, laptop, lc, phone, 2)
	mustRecord(t, b, certBody(t, pc2))
	if err := check(t, s, b, person, phone, time.Time{}); err != nil {
		t.Fatalf("with a new serial = %v", err)
	}
	// The person key revokes up to serial 2: both are void again. A lower record later changes nothing.
	mustRecord(t, b, revBody(t, revokeBySerial(t, person, person.PublicKey(), phone.PublicKey(), 2), nil))
	mustRecord(t, b, revBody(t, revokeBySerial(t, person, person.PublicKey(), phone.PublicKey(), 1), nil))
	revs, err := s.Revocations(view(t, b), person.PublicKey(), phone.PublicKey())
	if err != nil || len(revs) != 1 || revs[0].UpToSerial() != 2 {
		t.Fatalf("Revocations = %v, %v", revs, err)
	}
	if err := check(t, s, b, person, phone, time.Time{}); !errors.Is(err, koloid.ErrCertRevoked) {
		t.Fatalf("after the person's revocation = %v", err)
	}
	// Revoking the approver voids the devices it certified.
	other := generate(t)
	oc := certifyBy(t, laptop, lc, other, 1)
	mustRecord(t, b, certBody(t, oc))
	if err := check(t, s, b, person, other, time.Time{}); err != nil {
		t.Fatal(err)
	}
	mustRecord(t, b, revBody(t, revokeBySerial(t, person, person.PublicKey(), laptop.PublicKey(), 1), nil))
	if err := check(t, s, b, person, other, time.Time{}); !errors.Is(err, koloid.ErrCertRevoked) {
		t.Fatalf("device of a revoked approver = %v", err)
	}
	if err := check(t, s, b, person, laptop, time.Time{}); !errors.Is(err, koloid.ErrCertRevoked) {
		t.Fatalf("the revoked approver = %v", err)
	}
}

func TestApproverRevocationNamesOneCertificate(t *testing.T) {
	s := newStore(t, nil)
	person, laptop, phone := generate(t), generate(t), generate(t)
	b := found(t, s, generate(t), kolobraid.GroupConfig{})
	lc := certify(t, person, laptop, 1, true)
	pc1 := certifyBy(t, laptop, lc, phone, 1)
	mustRecord(t, b, certBody(t, lc))
	mustRecord(t, b, certBody(t, pc1))
	rev := revokeByApprover(t, laptop, lc, pc1)
	mustRecord(t, b, revBody(t, rev, pc1))
	if err := check(t, s, b, person, phone, time.Time{}); !errors.Is(err, koloid.ErrCertRevoked) {
		t.Fatalf("after an approver revocation = %v", err)
	}
	// The same record again is a no-op; a new certificate of the same key is another certificate and is not covered.
	mustRecord(t, b, revBody(t, rev, pc1))
	pc2 := certifyBy(t, laptop, lc, phone, 2)
	mustRecord(t, b, certBody(t, pc2))
	if err := check(t, s, b, person, phone, time.Time{}); err != nil {
		t.Fatalf("with a new certificate = %v", err)
	}
	// An approver cannot revoke by range or burn: its record covers the one serial it names.
	revs, _ := s.Revocations(view(t, b), person.PublicKey(), phone.PublicKey())
	if len(revs) != 1 || revs[0].UpToSerial() != 1 || !bytes.Equal(revs[0].Signer(), laptop.PublicKey()) {
		t.Fatalf("Revocations = %v", revs)
	}
}

func TestRevocationRecordRefusals(t *testing.T) {
	s := newStore(t, nil)
	person, laptop, phone, rogue := generate(t), generate(t), generate(t), generate(t)
	b := found(t, s, generate(t), kolobraid.GroupConfig{})
	lc := certify(t, person, laptop, 1, true)
	pc := certifyBy(t, laptop, lc, phone, 1)
	mustRecord(t, b, certBody(t, lc))
	mustRecord(t, b, certBody(t, pc))
	good := revokeByApprover(t, laptop, lc, pc)
	raw := good.Encode()
	// A bad signature, a record naming another person than the one that signed, and a bad body.
	forged := bytes.Clone(raw)
	forged[len(forged)-70] ^= 1
	badSig, err := koloid.DecodeRevocation(forged)
	if err != nil {
		t.Fatal(err)
	}
	// A stranger can only revoke its own key, so a record of its own key is all it can add.
	foreign := revokeBySerial(t, rogue, person.PublicKey(), rogue.PublicKey(), 1)
	// An approver record with no target, and a person record with one, built by hand.
	var noTarget, extraTarget kolocodec.Encoder
	for _, e := range []*kolocodec.Encoder{&noTarget, &extraTarget} {
		e.PutString("kolo-braid/devices/v0")
		e.PutUvarint(2)
	}
	noTarget.PutBytes(raw)
	noTarget.PutBool(false)
	self := revokeBySerial(t, phone, person.PublicKey(), phone.PublicKey(), 1)
	extraTarget.PutBytes(self.Encode())
	extraTarget.PutBool(true)
	extraTarget.PutBytes(pc.Encode())
	// A target that is not a real certificate: its device signature is forged.
	forgedTarget := bytes.Clone(pc.Encode())
	forgedTarget[len(forgedTarget)-289] ^= 1
	var fake kolocodec.Encoder
	fake.PutString("kolo-braid/devices/v0")
	fake.PutUvarint(2)
	fake.PutBytes(raw)
	fake.PutBool(true)
	fake.PutBytes(forgedTarget)
	bodies := map[string][]byte{
		"bad signature":       revBody(t, badSig, pc),
		"approver, no target": noTarget.Bytes(),
		"person, with target": extraTarget.Bytes(),
		"forged target":       fake.Bytes(),
		"other target":        revBody(t, good, certifyBy(t, laptop, lc, generate(t), 1)),
		"trailing byte":       append(revBody(t, good, pc), 0),
	}
	for name, body := range bodies {
		if !record(t, b, body) {
			t.Errorf("%s: accepted", name)
		}
	}
	revs, err := s.Revocations(view(t, b), person.PublicKey(), phone.PublicKey())
	if err != nil || len(revs) != 0 {
		t.Fatalf("a rejected revocation is stored: %v, %v", revs, err)
	}
	// A stranger's revocation of its own key is accepted, and touches nobody else.
	mustRecord(t, b, revBody(t, foreign, nil))
	if revs, _ := s.Revocations(view(t, b), person.PublicKey(), phone.PublicKey()); len(revs) != 0 {
		t.Fatalf("a stranger revoked the phone: %v", revs)
	}
	if err := check(t, s, b, person, phone, time.Time{}); err != nil {
		t.Fatalf("the device was revoked by a rejected record: %v", err)
	}
	// The helpers refuse the same mistakes before anything is written.
	if _, err := devices.RevocationBody(good, nil); !errors.Is(err, kolobraid.ErrInvalidArgument) {
		t.Fatalf("approver record without target = %v", err)
	}
	if _, err := devices.RevocationBody(self, pc); !errors.Is(err, kolobraid.ErrInvalidArgument) {
		t.Fatalf("person record with target = %v", err)
	}
	if _, err := devices.RevocationBody(nil, nil); !errors.Is(err, kolobraid.ErrInvalidArgument) {
		t.Fatalf("nil revocation = %v", err)
	}
	if _, err := devices.CertBody(nil); !errors.Is(err, kolobraid.ErrInvalidArgument) {
		t.Fatalf("nil certificate = %v", err)
	}
}

func TestApproverRevocationBound(t *testing.T) {
	s := newStore(t, &devices.Options{MaxRevocationsPerDevice: 2})
	person, laptop, phone := generate(t), generate(t), generate(t)
	b := found(t, s, generate(t), kolobraid.GroupConfig{})
	lc := certify(t, person, laptop, 1, true)
	mustRecord(t, b, certBody(t, lc))
	for serial := uint64(1); serial <= 3; serial++ {
		pc := certifyBy(t, laptop, lc, phone, serial)
		rejected := record(t, b, revBody(t, revokeByApprover(t, laptop, lc, pc), pc))
		if rejected != (serial == 3) {
			t.Fatalf("serial %d: rejected = %v", serial, rejected)
		}
	}
}

// TestRetiredApproverRecords: the order stands in for the time a record was first seen. A record an approver signed after the
// order holds a revocation of that approver by the person key, or by itself, is not passed to the verifier; one signed before is.
func TestRetiredApproverRecords(t *testing.T) {
	for _, tc := range []struct {
		name   string
		retire func(t *testing.T, person, a *koloid.Identity, ac *koloid.DeviceCert) []byte
	}{
		{"by the person key", func(t *testing.T, person, a *koloid.Identity, ac *koloid.DeviceCert) []byte {
			return revBody(t, revokeBySerial(t, person, person.PublicKey(), a.PublicKey(), 1), nil)
		}},
		{"by the approver itself", func(t *testing.T, person, a *koloid.Identity, ac *koloid.DeviceCert) []byte {
			return revBody(t, revokeBySerial(t, a, person.PublicKey(), a.PublicKey(), 1), nil)
		}},
	} {
		for _, retireFirst := range []bool{true, false} {
			name := tc.name + " after"
			if retireFirst {
				name = tc.name + " before"
			}
			t.Run(name, func(t *testing.T) {
				s := newStore(t, nil)
				person, a, bApp, victim := generate(t), generate(t), generate(t), generate(t)
				b := found(t, s, generate(t), kolobraid.GroupConfig{})
				ac, bc := certify(t, person, a, 1, true), certify(t, person, bApp, 1, true)
				vc := certifyBy(t, bApp, bc, victim, 1) // the victim is certified by another approver, so it outlives a
				for _, c := range []*koloid.DeviceCert{ac, bc, vc} {
					mustRecord(t, b, certBody(t, c))
				}
				hit := revBody(t, revokeByApprover(t, a, ac, vc), vc)
				retire := tc.retire(t, person, a, ac)
				if retireFirst {
					mustRecord(t, b, retire)
					mustRecord(t, b, hit)
				} else {
					mustRecord(t, b, hit)
					mustRecord(t, b, retire)
				}
				err := check(t, s, b, person, victim, time.Time{})
				if retireFirst && err != nil {
					t.Fatalf("a record signed after its signer was retired voided the victim: %v", err)
				}
				if !retireFirst && !errors.Is(err, koloid.ErrCertRevoked) {
					t.Fatalf("a record signed before its signer was retired is not kept: %v", err)
				}
			})
		}
	}
}

func TestSelfRevocationKeepsTheFirstPosition(t *testing.T) {
	s := newStore(t, nil)
	person, a, bApp, victim, victim2 := generate(t), generate(t), generate(t), generate(t), generate(t)
	b := found(t, s, generate(t), kolobraid.GroupConfig{})
	ac, bc := certify(t, person, a, 1, true), certify(t, person, bApp, 1, true)
	vc, v2c := certifyBy(t, bApp, bc, victim, 1), certifyBy(t, bApp, bc, victim2, 1)
	for _, c := range []*koloid.DeviceCert{ac, bc, vc, v2c} {
		mustRecord(t, b, certBody(t, c))
	}
	mustRecord(t, b, revBody(t, revokeBySerial(t, person, person.PublicKey(), a.PublicKey(), 1), nil))
	mustRecord(t, b, revBody(t, revokeByApprover(t, a, ac, vc), vc))
	// A bigger revocation of its own key, signed later, does not move the retirement forward.
	mustRecord(t, b, revBody(t, revokeBySerial(t, a, person.PublicKey(), a.PublicKey(), 5), nil))
	mustRecord(t, b, revBody(t, revokeByApprover(t, a, ac, v2c), v2c))
	for _, d := range []*koloid.Identity{victim, victim2} {
		if err := check(t, s, b, person, d, time.Time{}); err != nil {
			t.Fatalf("a retired approver's later record counted: %v", err)
		}
	}
}

func TestExpiryIsDecidedByTheReader(t *testing.T) {
	s := newStore(t, nil)
	person, dev := generate(t), generate(t)
	b := found(t, s, generate(t), kolobraid.GroupConfig{})
	exp := time.Now().Add(time.Hour).Truncate(time.Second)
	o, err := person.OfferDeviceCert(dev.PublicKey(), &koloid.IssueOptions{Serial: 1, ExpiresAt: exp})
	if err != nil {
		t.Fatal(err)
	}
	c, err := dev.AcceptDeviceCert(o, person.PublicKey())
	if err != nil {
		t.Fatal(err)
	}
	mustRecord(t, b, certBody(t, c))
	if err := check(t, s, b, person, dev, exp.Add(-time.Second)); err != nil {
		t.Fatalf("before the expiry: %v", err)
	}
	if err := check(t, s, b, person, dev, exp); !errors.Is(err, koloid.ErrCertExpired) {
		t.Fatalf("at the expiry: %v", err)
	}
	// A certificate that has expired already is still recorded: apply has no clock.
	o, _ = person.OfferDeviceCert(dev.PublicKey(), &koloid.IssueOptions{Serial: 2, IssuedAt: time.Unix(1000, 0), ExpiresAt: time.Unix(2000, 0)})
	old, _ := dev.AcceptDeviceCert(o, person.PublicKey())
	mustRecord(t, b, certBody(t, old))
	if got, err := s.Cert(view(t, b), person.PublicKey(), dev.PublicKey()); err != nil || got.Serial() != 2 {
		t.Fatalf("Cert = %v, %v", got, err)
	}
}

func TestApplyLeavesOtherEntriesAlone(t *testing.T) {
	s := newStore(t, nil)
	var handled []bool
	opts := options(t, s)
	opts.Apply = func(tx *kolobraid.Tx, e *kolobraid.Entry) error {
		h, err := s.Apply(tx, e)
		if e.Kind == kolobraid.KindData {
			handled = append(handled, h)
		}
		return err
	}
	b, err := kolobraid.Create(ctxT(t), generate(t), kolobraid.GroupConfig{App: app}, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	mustRecord(t, b, []byte("not a roster entry"))
	mustRecord(t, b, certBody(t, certify(t, generate(t), generate(t), 1, false)))
	if len(handled) != 2 || handled[0] || !handled[1] {
		t.Fatalf("handled = %v", handled)
	}
}

func TestApplyIsDeterministic(t *testing.T) {
	s := newStore(t, nil)
	person, a, d1, d2, d3 := generate(t), generate(t), generate(t), generate(t), generate(t)
	ac := certify(t, person, a, 1, true)
	c1, c2, c3 := certifyBy(t, a, ac, d1, 1), certifyBy(t, a, ac, d2, 1), certifyBy(t, a, ac, d3, 1)
	bodies := [][]byte{
		certBody(t, ac), certBody(t, c1), certBody(t, c2), []byte("other"), certBody(t, c3), certBody(t, certifyBy(t, a, ac, d1, 2)),
		revBody(t, revokeByApprover(t, a, ac, c2), c2), revBody(t, revokeBySerial(t, d3, person.PublicKey(), d3.PublicKey(), 1), nil),
		revBody(t, revokeBySerial(t, person, person.PublicKey(), a.PublicKey(), 1), nil), revBody(t, revokeByApprover(t, a, ac, c1), c1),
		append(certBody(t, c1), 0),
	}
	var es []kolobraid.Entry
	writers := [][]byte{generate(t).PublicKey(), generate(t).PublicKey()}
	for i, body := range bodies {
		es = append(es, kolobraid.Entry{Writer: writers[i%2], Kind: kolobraid.KindData, Body: body, Position: uint64(i)})
	}
	kolobraidtest.CheckApply(t, applyFn(s), es)
}

func TestNewChecksItsOptions(t *testing.T) {
	for name, bad := range map[string]*devices.Options{
		"negative ceiling": {MaxDevices: -1}, "certs negative": {MaxCertsPerPerson: -1}, "certs too many": {MaxCertsPerPerson: 1<<20 + 1},
		"reserve below -1": {PersonCertReserve: -2}, "reserve over the bound": {MaxCertsPerPerson: 10, PersonCertReserve: 11},
		"default reserve over a tiny bound": {MaxCertsPerPerson: 1, PersonCertReserve: 2},
		"revocations negative":              {MaxRevocationsPerDevice: -1}, "revocations too many": {MaxRevocationsPerDevice: 1<<16 + 1},
	} {
		if _, err := devices.New(nil, bad); !errors.Is(err, kolobraid.ErrInvalidArgument) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if _, err := devices.New(make([]byte, 1025), nil); !errors.Is(err, kolobraid.ErrInvalidArgument) {
		t.Errorf("long prefix: %v", err)
	}
	if _, err := devices.New(make([]byte, 1024), nil); err != nil {
		t.Errorf("prefix of 1024 bytes: %v", err)
	}
	for name, ok := range map[string]*devices.Options{"nil": nil, "empty": {}, "ceiling of one": {MaxDevices: 1}, "any ceiling": {MaxDevices: 1 << 30},
		"smallest bounds": {MaxCertsPerPerson: 1, MaxRevocationsPerDevice: 1}, "largest bounds": {MaxCertsPerPerson: 1 << 20, MaxRevocationsPerDevice: 1 << 16}} {
		if _, err := devices.New(nil, ok); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestFirstSightOfARecordStands(t *testing.T) {
	// A record written again after its signer was retired keeps the position of its first sight: replaying it cannot undo a
	// revocation by moving it behind the retirement.
	s := newStore(t, nil)
	person, a, other, victim := generate(t), generate(t), generate(t), generate(t)
	b := found(t, s, generate(t), kolobraid.GroupConfig{})
	ac, oc := certify(t, person, a, 1, true), certify(t, person, other, 1, true)
	vc := certifyBy(t, other, oc, victim, 1)
	for _, c := range []*koloid.DeviceCert{ac, oc, vc} {
		mustRecord(t, b, certBody(t, c))
	}
	hit := revBody(t, revokeByApprover(t, a, ac, vc), vc)
	mustRecord(t, b, hit)
	mustRecord(t, b, revBody(t, revokeBySerial(t, person, person.PublicKey(), a.PublicKey(), 1), nil))
	mustRecord(t, b, hit)
	if err := check(t, s, b, person, victim, time.Time{}); !errors.Is(err, koloid.ErrCertRevoked) {
		t.Fatalf("a replayed record moved behind the retirement: %v", err)
	}
}

func TestApproverRevocationBoundIsPerDevice(t *testing.T) {
	s := newStore(t, &devices.Options{MaxRevocationsPerDevice: 1})
	person, laptop, d1, d2 := generate(t), generate(t), generate(t), generate(t)
	b := found(t, s, generate(t), kolobraid.GroupConfig{})
	lc := certify(t, person, laptop, 1, true)
	mustRecord(t, b, certBody(t, lc))
	for _, d := range []*koloid.Identity{d1, d2} {
		c := certifyBy(t, laptop, lc, d, 1)
		mustRecord(t, b, revBody(t, revokeByApprover(t, laptop, lc, c), c))
	}
	// The bound counts the records of one device of one person, not the records of the person or of the roster.
	other := generate(t)
	mustRecord(t, b, revBody(t, revokeBySerial(t, d1, other.PublicKey(), d1.PublicKey(), 1), nil))
}

func TestDevicesReadIsBounded(t *testing.T) {
	s := newStore(t, &devices.Options{MaxCertsPerPerson: 2})
	person := generate(t)
	b := found(t, s, generate(t), kolobraid.GroupConfig{})
	mustRecord(t, b, certBody(t, certify(t, person, generate(t), 1, false)))
	mustRecord(t, b, certBody(t, certify(t, person, generate(t), 1, false)))
	if devs, err := s.Devices(view(t, b), person.PublicKey()); err != nil || len(devs) != 2 {
		t.Fatalf("Devices = %d, %v", len(devs), err)
	}
	// A reader with a lower bound reads no more than it.
	tight := newStore(t, &devices.Options{MaxCertsPerPerson: 1})
	if devs, _ := tight.Devices(view(t, b), person.PublicKey()); len(devs) != 1 {
		t.Fatalf("Devices past the bound = %d", len(devs))
	}
}

func TestRetiredApproverRecordsAllDropped(t *testing.T) {
	// Two certificates of one device, both revoked by an approver that is retired before it signs: neither record is passed.
	s := newStore(t, nil)
	person, a, other, victim := generate(t), generate(t), generate(t), generate(t)
	b := found(t, s, generate(t), kolobraid.GroupConfig{})
	ac, oc := certify(t, person, a, 1, true), certify(t, person, other, 1, true)
	for _, c := range []*koloid.DeviceCert{ac, oc} {
		mustRecord(t, b, certBody(t, c))
	}
	mustRecord(t, b, revBody(t, revokeBySerial(t, person, person.PublicKey(), a.PublicKey(), 1), nil))
	for serial := uint64(1); serial <= 3; serial++ {
		vc := certifyBy(t, other, oc, victim, serial)
		mustRecord(t, b, revBody(t, revokeByApprover(t, a, ac, vc), vc))
	}
	revs, err := s.Revocations(view(t, b), person.PublicKey(), victim.PublicKey())
	if err != nil || len(revs) != 0 {
		t.Fatalf("Revocations = %d, %v; want none", len(revs), err)
	}
}

func TestDefaultBounds(t *testing.T) {
	s := newStore(t, nil)
	certs, reserved, revs := devices.Caps(s)
	if certs != 1024 || reserved != 128 || revs != 64 || devices.DefaultMaxCertsPerPerson != 1024 || devices.DefaultMaxRevocationsPerDevice != 64 {
		t.Fatalf("bounds = %d %d %d", certs, reserved, revs)
	}
	// The reserve is an eighth of the bound unless it is given, and -1 reserves nothing.
	for _, c := range []struct {
		opts devices.Options
		want int
	}{{devices.Options{MaxCertsPerPerson: 80}, 10}, {devices.Options{MaxCertsPerPerson: 4}, 1}, {devices.Options{MaxCertsPerPerson: 2}, 1}, {devices.Options{MaxCertsPerPerson: 1}, 0}, {devices.Options{MaxCertsPerPerson: 80, PersonCertReserve: 3}, 3},
		{devices.Options{MaxCertsPerPerson: 80, PersonCertReserve: -1}, 0},
		{devices.Options{MaxCertsPerPerson: 8, PersonCertReserve: 8}, 8}} {
		o := c.opts
		if _, got, _ := devices.Caps(newStore(t, &o)); got != c.want {
			t.Errorf("%+v: reserve %d, want %d", c.opts, got, c.want)
		}
	}
}

// TestAStolenApproverCannotFillTheBound: certificates an approver issued stop short of the bound, so the person key can still add
// devices, and an approver that was retired issues no more.
func TestAStolenApproverCannotFillTheBound(t *testing.T) {
	s := newStore(t, &devices.Options{MaxCertsPerPerson: 5, PersonCertReserve: 2}) // 3 certificates through approvers, 2 kept
	person, thief := generate(t), generate(t)
	b := found(t, s, generate(t), kolobraid.GroupConfig{})
	tc := certify(t, person, thief, 1, true) // the approver, issued by the person key: 1 of 5
	mustRecord(t, b, certBody(t, tc))
	for i := 0; i < 4; i++ {
		if rejected := record(t, b, certBody(t, certifyBy(t, thief, tc, generate(t), 1))); rejected != (i == 3) {
			t.Fatalf("approver-issued certificate %d: rejected = %v", i, rejected)
		}
	}
	// The person key adds a device whose certificate it issued itself: the reserved slots are free.
	if record(t, b, certBody(t, certify(t, person, generate(t), 1, false))) {
		t.Fatal("the person key could not add a device after an approver filled its share")
	}
	if !record(t, b, certBody(t, certify(t, person, generate(t), 1, false))) {
		t.Fatal("the total bound did not hold")
	}
	// A new serial of a known device is not a new device.
	devs, _ := s.Devices(view(t, b), person.PublicKey())
	if len(devs) != 5 {
		t.Fatalf("devices = %d, want 5", len(devs))
	}
}

func TestRetiredApproverIssuesNoCertificates(t *testing.T) {
	for name, retire := range map[string]func(person, a *koloid.Identity) *koloid.Revocation{
		"by the person key": func(person, a *koloid.Identity) *koloid.Revocation {
			return revokeBySerial(t, person, person.PublicKey(), a.PublicKey(), 1)
		},
		"by the approver itself": func(person, a *koloid.Identity) *koloid.Revocation {
			return revokeBySerial(t, a, person.PublicKey(), a.PublicKey(), 1)
		},
	} {
		t.Run(name, func(t *testing.T) {
			s := newStore(t, nil)
			person, a := generate(t), generate(t)
			b := found(t, s, generate(t), kolobraid.GroupConfig{})
			ac := certify(t, person, a, 1, true)
			mustRecord(t, b, certBody(t, ac))
			before := certifyBy(t, a, ac, generate(t), 1)
			mustRecord(t, b, certBody(t, before))
			mustRecord(t, b, revBody(t, retire(person, a), nil))
			// Issued before or after, a certificate of the retired approver is recorded no more; the person key can still issue.
			if !record(t, b, certBody(t, certifyBy(t, a, ac, generate(t), 1))) {
				t.Fatal("a certificate of a retired approver was accepted")
			}
			mustRecord(t, b, certBody(t, certify(t, person, generate(t), 1, false)))
		})
	}
}

func TestReadersCheckTheirKeys(t *testing.T) {
	s := newStore(t, nil)
	b := found(t, s, generate(t), kolobraid.GroupConfig{})
	v := view(t, b)
	good, short := generate(t).PublicKey(), []byte{1, 2, 3}
	for name, f := range map[string]func() error{
		"Cert person":        func() error { _, err := s.Cert(v, short, good); return err },
		"Cert device":        func() error { _, err := s.Cert(v, good, short); return err },
		"Anchored":           func() error { _, err := s.Anchored(v, good, nil); return err },
		"Devices":            func() error { _, err := s.Devices(v, short); return err },
		"Revocations person": func() error { _, err := s.Revocations(v, nil, good); return err },
		"Revocations device": func() error { _, err := s.Revocations(v, good, short); return err },
		"Verify person":      func() error { return s.Verify(v, short, good, time.Time{}, msgCtx, nil, nil) },
		"Verify device":      func() error { return s.Verify(v, good, short, time.Time{}, msgCtx, nil, nil) },
	} {
		if err := f(); !errors.Is(err, kolobraid.ErrInvalidArgument) {
			t.Errorf("%s: %v, want ErrInvalidArgument", name, err)
		}
	}
	// Keys of the right size are looked up: the answers are not found.
	if _, err := s.Cert(v, good, good); !errors.Is(err, kolobraid.ErrNotFound) {
		t.Errorf("Cert = %v", err)
	}
}

// TestAdmissionReadsApproverRecordsFromTheSettledView: an approver-signed revocation that is only tentative does not count at
// admission, one by the person key or the device itself does.
func TestAdmissionReadsApproverRecordsFromTheSettledView(t *testing.T) {
	s := newStore(t, nil)
	person, a, d := generate(t), generate(t), generate(t)
	b := found(t, s, generate(t), kolobraid.GroupConfig{})
	ac := certify(t, person, a, 1, true)
	dc := certifyBy(t, a, ac, d, 1)
	mustRecord(t, b, certBody(t, ac))
	mustRecord(t, b, certBody(t, dc))
	older := view(t, b) // frozen before the records below
	mustRecord(t, b, revBody(t, revokeByApprover(t, a, ac, dc), dc))
	selfRev := revokeBySerial(t, d, person.PublicKey(), d.PublicKey(), 1)
	mustRecord(t, b, revBody(t, selfRev, nil))
	now := view(t, b)
	revs, err := devices.RevocationsOf(s, now, older, person.PublicKey(), dc)
	if err != nil {
		t.Fatal(err)
	}
	if len(revs) != 1 || revs[0].TargetHash() != nil || revs[0].UpToSerial() != 1 {
		t.Fatalf("revocations = %d: want only the device's own", len(revs))
	}
	revs, err = devices.RevocationsOf(s, now, now, person.PublicKey(), dc)
	if err != nil || len(revs) != 2 {
		t.Fatalf("revocations from one snapshot = %d, %v; want 2", len(revs), err)
	}
}

func TestNewSerialOfAKnownDeviceAtTheBound(t *testing.T) {
	// A device already counted is not a new device: its new certificate fits when the person is at the bound.
	s := newStore(t, &devices.Options{MaxCertsPerPerson: 2})
	person, d1, d2 := generate(t), generate(t), generate(t)
	b := found(t, s, generate(t), kolobraid.GroupConfig{})
	mustRecord(t, b, certBody(t, certify(t, person, d1, 1, false)))
	mustRecord(t, b, certBody(t, certify(t, person, d2, 1, false)))
	mustRecord(t, b, certBody(t, certify(t, person, d1, 2, false)))
	mustRecord(t, b, certBody(t, certify(t, person, d2, 2, true)))
	if !record(t, b, certBody(t, certify(t, person, generate(t), 1, false))) {
		t.Fatal("a third device was accepted at the bound")
	}
}

// TestApproverClassChangeIsCounted: a new serial that turns a person-issued certificate into an approver-issued one counts against
// the approver share.
func TestApproverClassChangeIsCounted(t *testing.T) {
	s := newStore(t, &devices.Options{MaxCertsPerPerson: 3, PersonCertReserve: 2}) // one certificate through approvers
	person, a, d1, d2 := generate(t), generate(t), generate(t), generate(t)
	b := found(t, s, generate(t), kolobraid.GroupConfig{})
	ac := certify(t, person, a, 1, true)
	mustRecord(t, b, certBody(t, ac))
	mustRecord(t, b, certBody(t, certify(t, person, d1, 1, false)))
	mustRecord(t, b, certBody(t, certifyBy(t, a, ac, d2, 1))) // the one approver-issued slot
	// d1 moves from the person key to the approver: the share is full.
	if !record(t, b, certBody(t, certifyBy(t, a, ac, d1, 2))) {
		t.Fatal("a device moved into a full approver share")
	}
	// Moving back is fine.
	mustRecord(t, b, certBody(t, certify(t, person, d2, 2, false)))
}
