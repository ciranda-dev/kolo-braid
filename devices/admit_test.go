package devices_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"testing"
	"time"

	kolocodec "github.com/ciranda-dev/kolo-codec"
	koloid "github.com/ciranda-dev/kolo-id"
	kololog "github.com/ciranda-dev/kolo-log"
	kolosecure "github.com/ciranda-dev/kolo-secure"

	kolobraid "github.com/ciranda-dev/kolo-braid"
	"github.com/ciranda-dev/kolo-braid/devices"
)

const admitCtx = "kolo-braid/device-admit/v0"

// household is a person whose first device, the laptop, founded a group and anchored its own approver certificate.
type household struct {
	t      *testing.T
	s      *devices.Store
	person *koloid.Identity
	laptop *koloid.Identity
	cert   *koloid.DeviceCert
	b      *kolobraid.Braid
}

func newHousehold(t *testing.T, o *devices.Options, cfg kolobraid.GroupConfig) *household {
	t.Helper()
	return newHouseholdOf(t, o, cfg, generate(t))
}

// newHouseholdOf founds a group whose person is the given one.
func newHouseholdOf(t *testing.T, o *devices.Options, cfg kolobraid.GroupConfig, person *koloid.Identity) *household {
	t.Helper()
	h := &household{t: t, s: newStore(t, o), person: person, laptop: generate(t)}
	h.cert = certify(t, h.person, h.laptop, 1, true)
	h.b = found(t, h.s, h.laptop, cfg)
	// The first device of a person is anchored by an admin: here the founder records its own certificate.
	if err := h.s.Record(ctxT(t), h.b, h.cert); err != nil {
		t.Fatal(err)
	}
	waitSettledAnchor(t, h.s, h.b, h.person.PublicKey(), h.laptop.PublicKey())
	return h
}

// waitSettledAnchor waits until the settled view holds the certificate of device anchored, which Admit trusts.
func waitSettledAnchor(t *testing.T, s *devices.Store, b *kolobraid.Braid, person, device []byte) {
	t.Helper()
	until(t, b, func() bool {
		v := b.Settled()
		defer v.Close()
		ok, err := s.Anchored(v, person, device)
		return err == nil && ok
	})
}

// device makes a new device certified by the laptop.
func (h *household) device(serial uint64) (*koloid.Identity, *koloid.DeviceCert) {
	h.t.Helper()
	dev := generate(h.t)
	return dev, certifyBy(h.t, h.laptop, h.cert, dev, serial)
}

func (h *household) newJoin(dev *koloid.Identity) *kolobraid.Joining {
	h.t.Helper()
	j, err := kolobraid.NewJoin(ctxT(h.t), dev, options(h.t, h.s))
	if err != nil {
		h.t.Fatal(err)
	}
	return j
}

func (h *household) request(j *kolobraid.Joining, dev *koloid.Identity, cert *koloid.DeviceCert, app []byte) []byte {
	h.t.Helper()
	req, err := devices.NewJoinRequest(j, dev, cert, h.b.GroupID(), app)
	if err != nil {
		h.t.Fatal(err)
	}
	return req
}

// admitDevice builds the request of a new device and admits it.
func (h *household) admitDevice(dev *koloid.Identity, cert *koloid.DeviceCert) ([]byte, error) {
	h.t.Helper()
	return h.admit(dev, h.request(h.newJoin(dev), dev, cert, nil))
}

func (h *household) admit(joiner *koloid.Identity, req []byte) ([]byte, error) {
	h.t.Helper()
	return h.s.Admit(ctxT(h.t), h.b, joiner.PublicKey(), req)
}

// seats returns the keys of the active members.
func (h *household) seats() map[string]bool { return seatsOf(h.b) }

func seatsOf(b *kolobraid.Braid) map[string]bool {
	v := b.View()
	defer v.Close()
	out := map[string]bool{}
	for _, m := range v.Members() {
		out[string(m.Key)] = true
	}
	return out
}

// strandOf returns the strand of a join.
func strandOf(t *testing.T, j *kolobraid.Joining) kololog.LogID {
	t.Helper()
	first, err := j.Request(nil)
	if err != nil {
		t.Fatal(err)
	}
	jr, err := kolobraid.ParseJoinRequest(first)
	if err != nil {
		t.Fatal(err)
	}
	return jr.Strand
}

// proofData is the data of a join request that carries cert and sig, written by hand from the documented layout.
func proofData(cert *koloid.DeviceCert, sig, app []byte) []byte {
	var e kolocodec.Encoder
	e.PutString("kolo-braid/devices/proof/v0")
	e.PutBytes(cert.Encode())
	e.PutFixed(sig)
	e.PutBytes(app)
	return e.Bytes()
}

// proofBy is a request of j whose proof for cert is signed by signer over the person, group and the request's own strand.
func (h *household) proofBy(j *kolobraid.Joining, cert *koloid.DeviceCert, signer *koloid.Identity) []byte {
	h.t.Helper()
	return proofFor(h.t, j, cert, h.b.GroupID(), signer)
}

func proofFor(t *testing.T, j *kolobraid.Joining, cert *koloid.DeviceCert, group kolobraid.GroupID, signer *koloid.Identity) []byte {
	t.Helper()
	strand := strandOf(t, j)
	msg := append(append(cert.Person(), group[:]...), strand[:]...)
	req, err := j.Request(proofData(cert, signAs(t, signer, admitCtx, msg), nil))
	if err != nil {
		t.Fatal(err)
	}
	return req
}

// rosterEntries counts the data entries of the order that start with the roster tag; it reads again when the order changes under it.
func rosterEntries(t *testing.T, b *kolobraid.Braid) int {
	t.Helper()
	for {
		n, again := 0, false
		for o, err := range b.Entries(0) {
			if errors.Is(err, kolobraid.ErrReorged) {
				again = true
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			if o.Kind == kolobraid.KindData && bytes.Contains(o.Body, []byte("kolo-braid/devices/v0")) {
				n++
			}
		}
		if !again {
			return n
		}
	}
}

// pair is a group of two peers on one store: a founded it, and b is a plain member (not an admin) that a admitted.
type pair struct {
	s        *devices.Store
	a, b     *kolobraid.Braid
	aid, bid *koloid.Identity
	conn     *kolosecure.Conn
}

func newPair(t *testing.T, o *devices.Options) *pair { return newPairWith(t, o, nil) }

// newPairWith is newPair with the options b is admitted with.
func newPairWith(t *testing.T, o *devices.Options, admit *kolobraid.AdmitOptions) *pair {
	t.Helper()
	return newPairCfg(t, o, admit, kolobraid.GroupConfig{})
}

// newPairCfg is newPairWith for a group of the given config.
func newPairCfg(t *testing.T, o *devices.Options, admit *kolobraid.AdmitOptions, cfg kolobraid.GroupConfig) *pair {
	t.Helper()
	ctx := ctxT(t)
	s := newStore(t, o)
	aid, bid := generate(t), generate(t)
	a := found(t, s, aid, cfg)
	bopts := options(t, s)
	j, err := kolobraid.NewJoin(ctx, bid, bopts)
	if err != nil {
		t.Fatal(err)
	}
	req, err := j.Request(nil)
	if err != nil {
		t.Fatal(err)
	}
	w, err := a.Admit(ctx, bid.PublicKey(), req, admit)
	if err != nil {
		t.Fatal(err)
	}
	b, err := j.Complete(ctx, w)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	l, err := kolosecure.Listen(ctx, "127.0.0.1:0", aid, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	go func() {
		if c, err := l.Accept(ctx); err == nil {
			a.ServeConn(ctx, c)
		}
	}()
	c, err := kolosecure.Dial(ctx, l.Addr().String(), bid, aid.PublicKey(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	go b.ServeConn(ctx, c)
	return &pair{s: s, a: a, b: b, aid: aid, bid: bid, conn: c}
}

// waitSettledCert waits until the settled view of b holds a certificate of device under person.
func waitSettledCert(t *testing.T, s *devices.Store, b *kolobraid.Braid, person, device []byte) {
	t.Helper()
	until(t, b, func() bool {
		v := b.Settled()
		defer v.Close()
		_, err := s.Cert(v, person, device)
		return err == nil
	})
}

// waitCert waits until the view of b holds a certificate of device under person.
func waitCert(t *testing.T, s *devices.Store, b *kolobraid.Braid, person, device []byte, serial uint64) {
	t.Helper()
	until(t, b, func() bool {
		v := b.View()
		defer v.Close()
		c, err := s.Cert(v, person, device)
		return err == nil && c.Serial() == serial
	})
}

func anchored(t *testing.T, s *devices.Store, b *kolobraid.Braid, person, device []byte) bool {
	t.Helper()
	v := b.View()
	defer v.Close()
	ok, err := s.Anchored(v, person, device)
	if err != nil {
		t.Fatal(err)
	}
	return ok
}

func TestAdmitACertifiedDevice(t *testing.T) {
	h := newHousehold(t, nil, kolobraid.GroupConfig{})
	dev, cert := h.device(1)
	j := h.newJoin(dev)
	req := h.request(j, dev, cert, []byte("Maria's phone"))
	if app, err := devices.AppData(req); err != nil || string(app) != "Maria's phone" {
		t.Fatalf("AppData = %q, %v", app, err)
	}
	if got, err := devices.Certificate(req); err != nil || !bytes.Equal(got.Encode(), cert.Encode()) {
		t.Fatalf("Certificate = %v, %v", got, err)
	}
	welcome, err := h.admit(dev, req)
	if err != nil {
		t.Fatal(err)
	}
	b, err := j.Complete(ctxT(t), welcome)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	if !h.seats()[string(dev.PublicKey())] {
		t.Fatal("the device is not a member")
	}
	// The roster learned the certificate, anchored by the admitting peer, and the new member is a plain member.
	v := view(t, h.b)
	if got, err := h.s.Cert(v, h.person.PublicKey(), dev.PublicKey()); err != nil || !bytes.Equal(got.Encode(), cert.Encode()) {
		t.Fatalf("Cert = %v, %v", got, err)
	}
	if !anchored(t, h.s, h.b, h.person.PublicKey(), dev.PublicKey()) {
		t.Fatal("the certificate is not anchored")
	}
	for _, m := range v.Members() {
		if bytes.Equal(m.Key, dev.PublicKey()) && (m.Roles != 0 || m.Seat) {
			t.Fatalf("the device is %+v", m)
		}
	}
	if err := check(t, h.s, h.b, h.person, dev, time.Time{}); err != nil {
		t.Fatal(err)
	}
	// The same device again is an active key, as with Braid.Admit; nothing is recorded.
	n := rosterEntries(t, h.b)
	if _, err := h.admit(dev, req); !errors.Is(err, kolobraid.ErrActiveKey) {
		t.Fatalf("second admission = %v, want ErrActiveKey", err)
	}
	if rosterEntries(t, h.b) != n {
		t.Fatal("a refused admission wrote an entry")
	}
}

func TestAdmitRefusals(t *testing.T) {
	h := newHousehold(t, nil, kolobraid.GroupConfig{})
	n, entries := len(h.seats()), rosterEntries(t, h.b)
	refused := func(name string, want error, joiner *koloid.Identity, req []byte) {
		t.Helper()
		_, err := h.admit(joiner, req)
		if !errors.Is(err, want) {
			t.Errorf("%s: %v, want %v", name, err, want)
		}
		if len(h.seats()) != n {
			t.Errorf("%s: a member was added", name)
		}
		if rosterEntries(t, h.b) != entries {
			t.Errorf("%s: a roster entry was written", name)
		}
	}
	victim, vcert := h.device(1)

	// No proof: a plain join request.
	plainDev := generate(t)
	pj := h.newJoin(plainDev)
	plain, _ := pj.Request([]byte("no proof here"))
	refused("no proof", devices.ErrNotProven, plainDev, plain)
	if _, err := devices.AppData(plain); !errors.Is(err, devices.ErrNotProven) {
		t.Errorf("AppData of a plain request = %v", err)
	}
	if _, err := devices.Certificate(plain); !errors.Is(err, devices.ErrNotProven) {
		t.Errorf("Certificate of a plain request = %v", err)
	}

	// Someone else's certificate: the joiner is not the device it names, whatever it signs.
	thief := generate(t)
	tj := h.newJoin(thief)
	if _, err := devices.NewJoinRequest(tj, thief, vcert, h.b.GroupID(), nil); !errors.Is(err, kolobraid.ErrInvalidArgument) {
		t.Errorf("NewJoinRequest with another key's certificate = %v", err)
	}
	refused("certificate of another key", devices.ErrNotProven, thief, h.proofBy(tj, vcert, thief))
	refused("signed by the thief for the victim's key", kolobraid.ErrInvalidArgument, victim, h.proofBy(tj, vcert, thief))
	for name, signer := range map[string]*koloid.Identity{"person key": h.person, "approver": h.laptop} {
		vj := h.newJoin(victim)
		refused("proof signed by the "+name, devices.ErrNotProven, victim, h.proofBy(vj, vcert, signer))
	}

	// A proof replayed on a new strand: the victim's own proof of strand 1 inside its request for strand 2.
	vj1 := h.newJoin(victim)
	first := h.request(vj1, victim, vcert, nil)
	vj2 := h.newJoin(victim)
	if strandOf(t, vj1) == strandOf(t, vj2) {
		t.Fatal("two joins share a strand")
	}
	jr, _ := kolobraid.ParseJoinRequest(first)
	replayed, err := vj2.Request(jr.AppData)
	if err != nil {
		t.Fatal(err)
	}
	refused("proof replayed on another strand", devices.ErrNotProven, victim, replayed)

	// A device that cooperates: the victim signs a proof for the thief's strand, but the thief is not the device named.
	refused("proof signed by the device for another key", devices.ErrNotProven, thief, h.proofBy(tj, vcert, victim))

	// A person nobody anchored.
	strangerPerson, strangerDev := generate(t), generate(t)
	scert := certify(t, strangerPerson, strangerDev, 1, false)
	refused("unknown person", devices.ErrUnknownPerson, strangerDev, h.request(h.newJoin(strangerDev), strangerDev, scert, nil))

	// A certificate whose signature is forged.
	enc := bytes.Clone(vcert.Encode())
	enc[len(enc)-353] ^= 1
	forged, err := koloid.DecodeDeviceCert(enc)
	if err != nil {
		t.Fatal(err)
	}
	refused("forged certificate", devices.ErrNotProven, victim, h.proofBy(h.newJoin(victim), forged, victim))

	// A revoked certificate, by the person key and by an approver.
	other, ocert := h.device(1)
	mustRecord(t, h.b, revBody(t, revokeBySerial(t, h.person, h.person.PublicKey(), other.PublicKey(), 1), nil))
	entries = rosterEntries(t, h.b)
	_, err = h.admit(other, h.request(h.newJoin(other), other, ocert, nil))
	if !errors.Is(err, devices.ErrNotProven) || !errors.Is(err, koloid.ErrCertRevoked) {
		t.Errorf("revoked by the person key: %v", err)
	}
	third, tcert := h.device(1)
	mustRecord(t, h.b, revBody(t, revokeByApprover(t, h.laptop, h.cert, tcert), tcert))
	entries = rosterEntries(t, h.b)
	_, err = h.admit(third, h.request(h.newJoin(third), third, tcert, nil))
	if !errors.Is(err, devices.ErrNotProven) || !errors.Is(err, koloid.ErrCertRevoked) {
		t.Errorf("revoked by an approver: %v", err)
	}
	if rosterEntries(t, h.b) != entries {
		t.Error("a revoked device wrote a roster entry")
	}

	// Arguments.
	if _, err := h.s.Admit(ctxT(t), nil, plainDev.PublicKey(), plain); !errors.Is(err, kolobraid.ErrInvalidArgument) {
		t.Errorf("nil braid = %v", err)
	}
	if _, err := h.s.Admit(ctxT(t), h.b, plainDev.PublicKey()[:5], plain); !errors.Is(err, kolobraid.ErrInvalidArgument) {
		t.Errorf("short key = %v", err)
	}
	if _, err := h.s.Admit(ctxT(t), h.b, plainDev.PublicKey(), []byte("garbage")); !errors.Is(err, kolobraid.ErrMalformed) {
		t.Errorf("garbage request = %v", err)
	}
	group := h.b.GroupID()
	for name, f := range map[string]func() error{
		"nil join":        func() error { _, err := devices.NewJoinRequest(nil, plainDev, vcert, group, nil); return err },
		"nil identity":    func() error { _, err := devices.NewJoinRequest(pj, nil, vcert, group, nil); return err },
		"nil certificate": func() error { _, err := devices.NewJoinRequest(pj, plainDev, nil, group, nil); return err },
		"request too big": func() error {
			_, err := devices.NewJoinRequest(vj1, victim, vcert, group, make([]byte, kolobraid.MaxJoinRequest))
			return err
		},
	} {
		if err := f(); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
}

func TestProofIsBoundToTheGroup(t *testing.T) {
	person := generate(t)
	h1 := newHouseholdOf(t, nil, kolobraid.GroupConfig{}, person)
	h2 := newHouseholdOf(t, nil, kolobraid.GroupConfig{}, person)
	dev := generate(t)
	cert := certifyBy(t, h1.laptop, h1.cert, dev, 1)
	cert2 := certifyBy(t, h2.laptop, h2.cert, dev, 1)
	if h1.b.GroupID() == h2.b.GroupID() {
		t.Fatal("two groups share an ID")
	}
	// The proof the device made for group 1 is useless in group 2: the very same request, strand and all, is refused there.
	req := h1.request(h1.newJoin(dev), dev, cert, nil)
	if _, err := h2.admit(dev, req); !errors.Is(err, devices.ErrNotProven) {
		t.Fatalf("a proof of another group = %v, want ErrNotProven", err)
	}
	if h2.seats()[string(dev.PublicKey())] {
		t.Fatal("the device was admitted")
	}
	j2 := h2.newJoin(dev)
	// Made for group 2, it is accepted there.
	if _, err := h2.admit(dev, h2.request(j2, dev, cert2, nil)); err != nil {
		t.Fatalf("proof for the group = %v", err)
	}
}

func TestRequestStrandDoesNotChangeWithData(t *testing.T) {
	h := newHousehold(t, nil, kolobraid.GroupConfig{})
	dev := generate(t)
	j := h.newJoin(dev)
	a, _ := j.Request(nil)
	b, _ := j.Request([]byte("some data"))
	ja, _ := kolobraid.ParseJoinRequest(a)
	jb, _ := kolobraid.ParseJoinRequest(b)
	if ja.Strand != jb.Strand || !bytes.Equal(ja.Header, jb.Header) {
		t.Fatal("Request(data) names another strand than Request(nil)")
	}
}

func TestAdmitRefusesTheLaptopRevoked(t *testing.T) {
	h := newHousehold(t, nil, kolobraid.GroupConfig{})
	dev, cert := h.device(1)
	req := h.request(h.newJoin(dev), dev, cert, nil)
	// The person revokes the only device that is a member: the person no longer has a device in the group.
	mustRecord(t, h.b, revBody(t, revokeBySerial(t, h.person, h.person.PublicKey(), h.laptop.PublicKey(), 1), nil))
	if _, err := h.admit(dev, req); !errors.Is(err, devices.ErrUnknownPerson) {
		t.Fatalf("admit after the revocation = %v, want ErrUnknownPerson", err)
	}
}

func TestAdmitExpiredCertificate(t *testing.T) {
	h := newHousehold(t, &devices.Options{Now: func() time.Time { return time.Now().Add(48 * time.Hour) }}, kolobraid.GroupConfig{})
	late := generate(t)
	o, err := h.laptop.OfferDeviceCert(late.PublicKey(), &koloid.IssueOptions{Parent: h.cert, Serial: 1, ExpiresAt: time.Now().Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	cert, err := late.AcceptDeviceCert(o, h.person.PublicKey())
	if err != nil {
		t.Fatal(err)
	}
	_, err = h.admitDevice(late, cert)
	if !errors.Is(err, devices.ErrNotProven) || !errors.Is(err, koloid.ErrCertExpired) {
		t.Fatalf("expired certificate = %v", err)
	}
	// With the real clock the same request passes.
	h2 := newHousehold(t, nil, kolobraid.GroupConfig{})
	dev := generate(t)
	o, _ = h2.laptop.OfferDeviceCert(dev.PublicKey(), &koloid.IssueOptions{Parent: h2.cert, Serial: 1, ExpiresAt: time.Now().Add(time.Hour)})
	cert, _ = dev.AcceptDeviceCert(o, h2.person.PublicKey())
	if _, err := h2.admitDevice(dev, cert); err != nil {
		t.Fatalf("unexpired certificate = %v", err)
	}
}

func TestAdmitSupersededCertificate(t *testing.T) {
	h := newHousehold(t, nil, kolobraid.GroupConfig{})
	dev, c2 := h.device(2)
	mustRecord(t, h.b, certBody(t, c2))
	c1 := certifyBy(t, h.laptop, h.cert, dev, 1)
	if _, err := h.admitDevice(dev, c1); !errors.Is(err, devices.ErrNotProven) {
		t.Fatalf("older certificate = %v", err)
	}
	same := certifyBy(t, h.laptop, h.cert, dev, 2)
	if bytes.Equal(same.Encode(), c2.Encode()) {
		same = certify(t, h.person, dev, 2, false)
	}
	if _, err := h.admitDevice(dev, same); !errors.Is(err, devices.ErrNotProven) {
		t.Fatalf("another certificate with the same serial = %v", err)
	}
	// The certificate the roster holds, written by an admin, is admitted and not recorded again; one written by a member that
	// is no admin is anchored by the admission.
	mustRecord(t, h.b, certBody(t, c2)) // the founder is an admin: this anchors it
	before := rosterEntries(t, h.b)
	if _, err := h.admitDevice(dev, c2); err != nil {
		t.Fatalf("recorded certificate = %v", err)
	}
	if after := rosterEntries(t, h.b); after != before {
		t.Fatalf("roster entries went from %d to %d: the certificate was recorded again", before, after)
	}
}

func TestAdmitDeviceCeiling(t *testing.T) {
	h := newHousehold(t, &devices.Options{MaxDevices: 2}, kolobraid.GroupConfig{})
	d2, c2 := h.device(1)
	if _, err := h.admitDevice(d2, c2); err != nil {
		t.Fatal(err)
	}
	// The laptop and d2 are two devices: the third is refused, and nothing of it is written.
	d3, c3 := h.device(1)
	entries := rosterEntries(t, h.b)
	if _, err := h.admitDevice(d3, c3); !errors.Is(err, devices.ErrTooManyDevices) {
		t.Fatalf("third device = %v, want ErrTooManyDevices", err)
	}
	if rosterEntries(t, h.b) != entries || h.seats()[string(d3.PublicKey())] {
		t.Fatal("a refused device left a trace")
	}
	if _, err := h.s.Cert(view(t, h.b), h.person.PublicKey(), d3.PublicKey()); !errors.Is(err, kolobraid.ErrNotFound) {
		t.Fatalf("the refused certificate is in the roster: %v", err)
	}
	// A seat that is freed is counted again: removing d2 makes room.
	if _, err := h.b.RemoveMember(ctxT(t), d2.PublicKey()); err != nil {
		t.Fatal(err)
	}
	until(t, h.b, func() bool { return !h.seats()[string(d2.PublicKey())] })
	if _, err := h.admitDevice(d3, c3); err != nil {
		t.Fatalf("after a seat was freed = %v", err)
	}
}

// TestCeilingCountsSeats: a revoked device keeps its seat until an admin removes it, so it counts against the ceiling, and a device
// that revokes itself does not free a slot.
func TestCeilingCountsSeats(t *testing.T) {
	h := newHousehold(t, &devices.Options{MaxDevices: 2}, kolobraid.GroupConfig{})
	d2, c2 := h.device(1)
	if _, err := h.admitDevice(d2, c2); err != nil {
		t.Fatal(err)
	}
	// d2 revokes itself, and the person key revokes the laptop's serial too: both keep their seats.
	mustRecord(t, h.b, revBody(t, revokeBySerial(t, d2, h.person.PublicKey(), d2.PublicKey(), 1), nil))
	d3, c3 := h.device(1)
	entries := rosterEntries(t, h.b)
	if _, err := h.admitDevice(d3, c3); !errors.Is(err, devices.ErrTooManyDevices) {
		t.Fatalf("with a revoked device holding a seat = %v, want ErrTooManyDevices", err)
	}
	if rosterEntries(t, h.b) != entries {
		t.Fatal("a refused admission wrote an entry")
	}
	// An admin removes the revoked device: its seat is free.
	if _, err := h.b.RemoveMember(ctxT(t), d2.PublicKey()); err != nil {
		t.Fatal(err)
	}
	until(t, h.b, func() bool { return !h.seats()[string(d2.PublicKey())] })
	if _, err := h.admitDevice(d3, c3); err != nil {
		t.Fatalf("after the revoked device was removed = %v", err)
	}
}

func TestAdmitCeilingCountsOnlyThePersonsDevices(t *testing.T) {
	h := newHousehold(t, &devices.Options{MaxDevices: 2}, kolobraid.GroupConfig{})
	// A member of another person, admitted and anchored by a decision, takes a seat but is not a device of this person.
	strangerPerson, stranger := generate(t), generate(t)
	sj := h.newJoin(stranger)
	sreq, _ := sj.Request(nil)
	if _, err := h.b.Admit(ctxT(t), stranger.PublicKey(), sreq, nil); err != nil {
		t.Fatal(err)
	}
	if err := h.s.Record(ctxT(t), h.b, certify(t, strangerPerson, stranger, 1, true)); err != nil {
		t.Fatal(err)
	}
	// A plain member with no certificate does not count either.
	plain := generate(t)
	pj := h.newJoin(plain)
	preq, _ := pj.Request(nil)
	if _, err := h.b.Admit(ctxT(t), plain.PublicKey(), preq, nil); err != nil {
		t.Fatal(err)
	}
	d, c := h.device(1)
	if _, err := h.admitDevice(d, c); err != nil {
		t.Fatalf("second device of the person with other members around = %v", err)
	}
	d3, c3 := h.device(1)
	if _, err := h.admitDevice(d3, c3); !errors.Is(err, devices.ErrTooManyDevices) {
		t.Fatalf("third device = %v", err)
	}
}

// TestNoCeilingByDefault: without Options.MaxDevices the group's seats are the only bound.
func TestNoCeilingByDefault(t *testing.T) {
	h := newHousehold(t, nil, kolobraid.GroupConfig{})
	for i := 0; i < 12; i++ {
		d, c := h.device(1)
		if _, err := h.admitDevice(d, c); err != nil {
			t.Fatalf("device %d of a person with no ceiling: %v", i, err)
		}
	}
}

func TestExistingAdmitIsUnchanged(t *testing.T) {
	// A group that never calls Store.Admit admits a person's device only with Braid.Admit, as before.
	h := newHousehold(t, nil, kolobraid.GroupConfig{})
	dev := generate(t)
	j := h.newJoin(dev)
	req, err := j.Request([]byte("plain request"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.b.Admit(ctxT(t), dev.PublicKey(), req, nil); err != nil {
		t.Fatal(err)
	}
	until(t, h.b, func() bool { return h.seats()[string(dev.PublicKey())] })
	// Such a member has no certificate in the roster.
	if _, err := h.s.Cert(view(t, h.b), h.person.PublicKey(), dev.PublicKey()); !errors.Is(err, kolobraid.ErrNotFound) {
		t.Fatalf("Cert = %v", err)
	}
}

// TestAdmitGivesAnOrdinaryMember: a key that was an admin and was removed is admitted again as an ordinary member.
func TestAdmitGivesAnOrdinaryMember(t *testing.T) {
	h := newHousehold(t, nil, kolobraid.GroupConfig{})
	dev, cert := h.device(1)
	admin := kolobraid.RoleAdmin
	j := h.newJoin(dev)
	req := h.request(j, dev, cert, nil)
	if _, err := h.b.Admit(ctxT(t), dev.PublicKey(), req, &kolobraid.AdmitOptions{Roles: &admin}); err != nil {
		t.Fatal(err)
	}
	if err := h.s.Record(ctxT(t), h.b, cert); err != nil {
		t.Fatal(err)
	}
	waitSettledAnchor(t, h.s, h.b, h.person.PublicKey(), dev.PublicKey())
	role := func() (kolobraid.Role, bool) {
		v := h.b.View()
		defer v.Close()
		for _, m := range v.Members() {
			if bytes.Equal(m.Key, dev.PublicKey()) {
				return m.Roles, true
			}
		}
		return 0, false
	}
	if r, ok := role(); !ok || r&kolobraid.RoleAdmin == 0 {
		t.Fatalf("the device is not an admin: %v, %v", r, ok)
	}
	if _, err := h.b.RemoveMember(ctxT(t), dev.PublicKey()); err != nil {
		t.Fatal(err)
	}
	until(t, h.b, func() bool { _, ok := role(); return !ok })
	// The old request names a strand the order already used; Braid.Admit refuses it, the roster is not touched.
	entries := rosterEntries(t, h.b)
	if _, err := h.admit(dev, req); !errors.Is(err, kolobraid.ErrKnownStrand) {
		t.Fatalf("the old strand = %v, want ErrKnownStrand", err)
	}
	if rosterEntries(t, h.b) != entries {
		t.Fatal("a refused admission wrote an entry")
	}
	if _, err := h.admitDevice(dev, cert); err != nil {
		t.Fatalf("admitting the removed admin again: %v", err)
	}
	if r, ok := role(); !ok || r != 0 {
		t.Fatalf("the re-admitted key has roles %v (member %v), want an ordinary member", r, ok)
	}
}

// TestAPlainMemberCannotInventAPerson: any member may write a roster record, but only an admin's record anchors a person.
func TestAPlainMemberCannotInventAPerson(t *testing.T) {
	p := newPair(t, nil)
	ctx := ctxT(t)
	invented, mid := generate(t), p.bid
	mcert := certify(t, invented, mid, 1, true)
	// The plain member b records a certificate for a person it just made up.
	if _, err := p.b.Append(ctx, certBody(t, mcert)); err != nil {
		t.Fatal(err)
	}
	waitCert(t, p.s, p.a, invented.PublicKey(), mid.PublicKey(), 1)
	waitSettledCert(t, p.s, p.a, invented.PublicKey(), mid.PublicKey())
	if anchored(t, p.s, p.a, invented.PublicKey(), mid.PublicKey()) {
		t.Fatal("a plain member's record is anchored")
	}
	// Writing the same bytes again does not anchor them either.
	if _, err := p.b.Append(ctx, certBody(t, mcert)); err != nil {
		t.Fatal(err)
	}
	if _, err := p.b.Append(ctx, certBody(t, certify(t, generate(t), generate(t), 1, false))); err != nil { // a later entry of b, applied after
		t.Fatal(err)
	}
	until(t, p.a, func() bool { return rosterEntries(t, p.a) >= 3 })
	if anchored(t, p.s, p.a, invented.PublicKey(), mid.PublicKey()) {
		t.Fatal("the same bytes written again by a plain member anchored the certificate")
	}
	// A new device of that person is refused by the admin: nobody anchored the person.
	dev := generate(t)
	dcert := certifyBy(t, mid, mcert, dev, 1)
	j, err := kolobraid.NewJoin(ctx, dev, options(t, p.s))
	if err != nil {
		t.Fatal(err)
	}
	req, err := devices.NewJoinRequest(j, dev, dcert, p.a.GroupID(), nil)
	if err != nil {
		t.Fatal(err)
	}
	entries := rosterEntries(t, p.a)
	if _, err := p.s.Admit(ctx, p.a, dev.PublicKey(), req); !errors.Is(err, devices.ErrUnknownPerson) {
		t.Fatalf("Admit for an invented person = %v, want ErrUnknownPerson", err)
	}
	if rosterEntries(t, p.a) != entries || seatsOf(p.a)[string(dev.PublicKey())] {
		t.Fatal("a refused admission left a trace")
	}
	// A plain member cannot admit either, and writes nothing.
	if _, err := p.s.Admit(ctx, p.b, dev.PublicKey(), req); !errors.Is(err, kolobraid.ErrNotAdmin) {
		t.Fatalf("Admit by a member that is no admin = %v, want ErrNotAdmin", err)
	}
	if err := p.s.Record(ctx, p.b, mcert); !errors.Is(err, kolobraid.ErrNotAdmin) {
		t.Fatalf("Record by a member that is no admin = %v, want ErrNotAdmin", err)
	}
	// The admin decides to anchor the person's first device: the same bytes, recorded by an admin, flip the flag.
	if err := p.s.Record(ctx, p.a, mcert); err != nil {
		t.Fatal(err)
	}
	if !anchored(t, p.s, p.a, invented.PublicKey(), mid.PublicKey()) {
		t.Fatal("the admin's record did not anchor the certificate")
	}
	waitSettledAnchor(t, p.s, p.a, invented.PublicKey(), mid.PublicKey())
	// A higher serial written by the plain member keeps the anchor.
	mcert2 := certify(t, invented, mid, 2, true)
	if _, err := p.b.Append(ctx, certBody(t, mcert2)); err != nil {
		t.Fatal(err)
	}
	waitCert(t, p.s, p.a, invented.PublicKey(), mid.PublicKey(), 2)
	if !anchored(t, p.s, p.a, invented.PublicKey(), mid.PublicKey()) {
		t.Fatal("a replacement by a plain member dropped the anchor")
	}
	// Now the person is known and its new device is admitted.
	dcert2 := certifyBy(t, mid, mcert2, dev, 1)
	j2, err := kolobraid.NewJoin(ctx, dev, options(t, p.s))
	if err != nil {
		t.Fatal(err)
	}
	req2, err := devices.NewJoinRequest(j2, dev, dcert2, p.a.GroupID(), nil)
	if err != nil {
		t.Fatal(err)
	}
	// The device's own certificate was already written by a plain member: that record is not an anchor, and the admission anchors it.
	if _, err := p.b.Append(ctx, certBody(t, dcert2)); err != nil {
		t.Fatal(err)
	}
	waitCert(t, p.s, p.a, invented.PublicKey(), dev.PublicKey(), 1)
	if anchored(t, p.s, p.a, invented.PublicKey(), dev.PublicKey()) {
		t.Fatal("the plain member's record of the device is anchored")
	}
	if _, err := p.s.Admit(ctx, p.a, dev.PublicKey(), req2); err != nil {
		t.Fatalf("Admit after the admin anchored the person = %v", err)
	}
	if !anchored(t, p.s, p.a, invented.PublicKey(), dev.PublicKey()) {
		t.Fatal("the admission did not anchor the certificate a plain member had written")
	}
}

// TestPlainMemberCertificatesDoNotCount: a certificate only a plain member recorded is not an anchor, and does not count against
// a ceiling either.
func TestPlainMemberCertificatesDoNotCount(t *testing.T) {
	p := newPair(t, &devices.Options{MaxDevices: 1})
	ctx := ctxT(t)
	person := generate(t)
	mcert := certify(t, person, p.bid, 1, true)
	if _, err := p.b.Append(ctx, certBody(t, mcert)); err != nil {
		t.Fatal(err)
	}
	waitCert(t, p.s, p.a, person.PublicKey(), p.bid.PublicKey(), 1)
	v := p.a.View()
	defer v.Close()
	if ok, err := p.s.Anchored(v, person.PublicKey(), p.bid.PublicKey()); err != nil || ok {
		t.Fatalf("Anchored = %v, %v", ok, err)
	}
	if _, err := p.s.Anchored(v, person.PublicKey(), generate(t).PublicKey()); !errors.Is(err, kolobraid.ErrNotFound) {
		t.Fatalf("Anchored of nothing = %v", err)
	}
}

func TestRecordedCertificateThatAppliesNothingGrantsNoSeat(t *testing.T) {
	// The roster rejects the certificate (the person is at the bound of the view): no seat is granted.
	h := newHousehold(t, &devices.Options{MaxCertsPerPerson: 1}, kolobraid.GroupConfig{})
	dev, cert := h.device(1)
	seats := len(h.seats())
	if _, err := h.admitDevice(dev, cert); !errors.Is(err, devices.ErrNotRecorded) {
		t.Fatalf("Admit past the bound of the view = %v, want ErrNotRecorded", err)
	}
	if len(h.seats()) != seats || h.seats()[string(dev.PublicKey())] {
		t.Fatal("a seat was granted although the record was rejected")
	}
	// Record reports the same.
	if err := h.s.Record(ctxT(t), h.b, cert); !errors.Is(err, devices.ErrNotRecorded) {
		t.Fatalf("Record past the bound = %v, want ErrNotRecorded", err)
	}
}

func TestRecordRefusals(t *testing.T) {
	h := newHousehold(t, nil, kolobraid.GroupConfig{})
	if err := h.s.Record(ctxT(t), nil, h.cert); !errors.Is(err, kolobraid.ErrInvalidArgument) {
		t.Errorf("nil braid = %v", err)
	}
	if err := h.s.Record(ctxT(t), h.b, nil); !errors.Is(err, kolobraid.ErrInvalidArgument) {
		t.Errorf("nil certificate = %v", err)
	}
	enc := bytes.Clone(h.cert.Encode())
	enc[len(enc)-100] ^= 1
	if bad, err := koloid.DecodeDeviceCert(enc); err == nil {
		if err := h.s.Record(ctxT(t), h.b, bad); !errors.Is(err, devices.ErrNotProven) {
			t.Errorf("forged certificate = %v", err)
		}
	}
	// A certificate no one can apply: a cancelled context returns what it was waiting for.
	ctx, cancel := context.WithCancel(ctxT(t))
	cancel()
	dev, cert := h.device(1)
	if err := h.s.Record(ctx, h.b, cert); err == nil {
		t.Errorf("record with a cancelled context returned nil for %v", dev.PublicKey()[:1])
	}
}

// TestCeilingDoesNotCountCertificatesNoAdminRecorded: a member that wrote its own certificate for the person does not use up the
// person's ceiling.
func TestCeilingDoesNotCountCertificatesNoAdminRecorded(t *testing.T) {
	p := newPair(t, &devices.Options{MaxDevices: 2})
	ctx := ctxT(t)
	person := generate(t)
	// The plain member b records its own certificate of the person; an admin anchors another device of it.
	bcert := certify(t, person, p.bid, 1, true)
	if _, err := p.b.Append(ctx, certBody(t, bcert)); err != nil {
		t.Fatal(err)
	}
	waitCert(t, p.s, p.a, person.PublicKey(), p.bid.PublicKey(), 1)
	anchor := generate(t)
	acj, err := kolobraid.NewJoin(ctx, anchor, options(t, p.s))
	if err != nil {
		t.Fatal(err)
	}
	areq, _ := acj.Request(nil)
	if _, err := p.a.Admit(ctx, anchor.PublicKey(), areq, nil); err != nil {
		t.Fatal(err)
	}
	until(t, p.a, func() bool { return seatsOf(p.a)[string(anchor.PublicKey())] })
	if err := p.s.Record(ctx, p.a, certify(t, person, anchor, 1, true)); err != nil {
		t.Fatal(err)
	}
	waitSettledAnchor(t, p.s, p.a, person.PublicKey(), anchor.PublicKey())
	// One anchored device is a member, and b's own record is not counted: a second device fits under the ceiling of 2.
	dev := generate(t)
	dcert := certifyBy(t, anchor, certify(t, person, anchor, 1, true), dev, 1)
	j, err := kolobraid.NewJoin(ctx, dev, options(t, p.s))
	if err != nil {
		t.Fatal(err)
	}
	req, err := devices.NewJoinRequest(j, dev, dcert, p.a.GroupID(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.s.Admit(ctx, p.a, dev.PublicKey(), req); err != nil {
		t.Fatalf("Admit with a certificate nobody anchored around = %v", err)
	}
}

// TestAdmissionIgnoresATentativeApproverRevocation: an approver-signed revocation counts at admission only once it is settled, so
// admins that see the entries in another order agree. Here settlement is stalled, the record stays tentative, and the device is
// admitted; the person's own revocation, which every admin sees alike, is honoured at once.
func TestAdmissionIgnoresATentativeApproverRevocation(t *testing.T) {
	p := newPair(t, nil)
	ctx := ctxT(t)
	// Two settlers: settlement needs both online.
	if _, err := p.a.SetSettlers(ctx, []ed25519.PublicKey{p.aid.PublicKey(), p.bid.PublicKey()}, kolobraid.Majority); err != nil {
		t.Fatal(err)
	}
	until(t, p.a, func() bool { return p.a.Status().Seats == 2 })
	person := generate(t)
	acert := certify(t, person, p.aid, 1, true)
	if err := p.s.Record(ctx, p.a, acert); err != nil {
		t.Fatal(err)
	}
	waitSettledAnchor(t, p.s, p.a, person.PublicKey(), p.aid.PublicKey())
	// The second settler goes away: no quorum, nothing new settles.
	p.conn.Close()
	p.b.Close()
	dev, other := generate(t), generate(t)
	dcert, ocert := certifyBy(t, p.aid, acert, dev, 1), certifyBy(t, p.aid, acert, other, 1)
	if _, err := p.a.Append(ctx, revBody(t, revokeByApprover(t, p.aid, acert, dcert), dcert)); err != nil {
		t.Fatal(err)
	}
	if _, err := p.a.Append(ctx, revBody(t, revokeBySerial(t, person, person.PublicKey(), other.PublicKey(), 1), nil)); err != nil {
		t.Fatal(err)
	}
	until(t, p.a, func() bool {
		v := p.a.View()
		defer v.Close()
		r1, _ := p.s.Revocations(v, person.PublicKey(), dev.PublicKey())
		r2, _ := p.s.Revocations(v, person.PublicKey(), other.PublicKey())
		return len(r1) == 1 && len(r2) == 1
	})
	if s := p.a.Settled(); true {
		r, _ := p.s.Revocations(s, person.PublicKey(), dev.PublicKey())
		s.Close()
		if len(r) != 0 {
			t.Fatal("the approver revocation settled: the test lost its premise")
		}
	}
	jd, err := kolobraid.NewJoin(ctx, dev, options(t, p.s))
	if err != nil {
		t.Fatal(err)
	}
	reqD, err := devices.NewJoinRequest(jd, dev, dcert, p.a.GroupID(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.s.Admit(ctx, p.a, dev.PublicKey(), reqD); err != nil {
		t.Fatalf("Admit with a tentative approver revocation = %v", err)
	}
	jo, err := kolobraid.NewJoin(ctx, other, options(t, p.s))
	if err != nil {
		t.Fatal(err)
	}
	reqO, err := devices.NewJoinRequest(jo, other, ocert, p.a.GroupID(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.s.Admit(ctx, p.a, other.PublicKey(), reqO); !errors.Is(err, koloid.ErrCertRevoked) {
		t.Fatalf("Admit with a tentative revocation by the person key = %v, want ErrCertRevoked", err)
	}
}

// TestFullGroupIsRefusedBeforeAnyWrite: Braid.Admit would append an AddMember entry that the roster leaves void, so Admit looks first.
func TestFullGroupIsRefusedBeforeAnyWrite(t *testing.T) {
	h := newHousehold(t, nil, kolobraid.GroupConfig{MaxMembers: 2})
	d2, c2 := h.device(1)
	if _, err := h.admitDevice(d2, c2); err != nil {
		t.Fatal(err)
	}
	d3, c3 := h.device(1)
	entries := rosterEntries(t, h.b)
	addMembers := countKind(t, h.b, kolobraid.KindAddMember)
	if _, err := h.admitDevice(d3, c3); !errors.Is(err, kolobraid.ErrGroupFull) {
		t.Fatalf("a full group = %v, want ErrGroupFull", err)
	}
	if rosterEntries(t, h.b) != entries || countKind(t, h.b, kolobraid.KindAddMember) != addMembers {
		t.Fatal("a refused admission wrote an entry")
	}
	// Braid.Admit itself still appends the entry, which the roster leaves void: waiting for that seat ends with ErrNotAdmitted.
	j := h.newJoin(d3)
	req := h.request(j, d3, c3, nil)
	welcome, err := h.b.Admit(ctxT(t), d3.PublicKey(), req, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := devices.WaitSeat(h.s, ctxT(t), h.b, welcome, d3.PublicKey()); !errors.Is(err, devices.ErrNotAdmitted) {
		t.Fatalf("waiting for a seat in a full group = %v, want ErrNotAdmitted", err)
	}
	if h.seats()[string(d3.PublicKey())] {
		t.Fatal("the device has a seat in a full group")
	}
}

func countKind(t *testing.T, b *kolobraid.Braid, k kolobraid.Kind) int {
	t.Helper()
	for {
		n, again := 0, false
		for o, err := range b.Entries(0) {
			if errors.Is(err, kolobraid.ErrReorged) {
				again = true
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			if o.Kind == k {
				n++
			}
		}
		if !again {
			return n
		}
	}
}

// TestReplayedRequestAppendsNothing answers whether a replayed identical request appends an AddMember every time: it does not,
// because the first admission makes the key active (Braid.Admit, invite.go:288-294) and, once it is removed, its strand used
// (invite.go:295-297).
func TestReplayedRequestAppendsNothing(t *testing.T) {
	h := newHousehold(t, nil, kolobraid.GroupConfig{})
	dev, cert := h.device(1)
	j0 := h.newJoin(dev)
	req := h.request(j0, dev, cert, nil)
	if _, err := h.admit(dev, req); err != nil {
		t.Fatal(err)
	}
	adds, roster := countKind(t, h.b, kolobraid.KindAddMember), rosterEntries(t, h.b)
	for i := 0; i < 3; i++ {
		if _, err := h.admit(dev, req); !errors.Is(err, kolobraid.ErrActiveKey) {
			t.Fatalf("replay %d = %v, want ErrActiveKey", i, err)
		}
	}
	if _, err := h.b.RemoveMember(ctxT(t), dev.PublicKey()); err != nil {
		t.Fatal(err)
	}
	until(t, h.b, func() bool { return !h.seats()[string(dev.PublicKey())] })
	if countKind(t, h.b, kolobraid.KindAddMember) != adds {
		t.Fatal("a replay appended an AddMember")
	}
	adds = countKind(t, h.b, kolobraid.KindAddMember)
	for i := 0; i < 3; i++ {
		if _, err := h.admit(dev, req); !errors.Is(err, kolobraid.ErrKnownStrand) {
			t.Fatalf("replay %d after a removal = %v, want ErrKnownStrand", i, err)
		}
	}
	if countKind(t, h.b, kolobraid.KindAddMember) != adds || rosterEntries(t, h.b) != roster {
		t.Fatal("a replayed request appended an entry")
	}
	// The same strand with a newer certificate: the strand is used, so nothing is recorded for it either.
	newer := certifyBy(t, h.laptop, h.cert, dev, 2)
	again := h.request(j0, dev, newer, nil)
	if _, err := h.admit(dev, again); !errors.Is(err, kolobraid.ErrKnownStrand) {
		t.Fatalf("a newer certificate on a used strand = %v, want ErrKnownStrand", err)
	}
	if rosterEntries(t, h.b) != roster {
		t.Fatal("a refused admission recorded the newer certificate")
	}
}

// TestACertificateThatAppliesUnanchoredEndsTheWait: the writer lost its admin role before the entry's position, so the entry applies
// without an anchor and is not rejected; the record reports that instead of waiting for ever.
func TestACertificateThatAppliesUnanchoredEndsTheWait(t *testing.T) {
	p := newPair(t, nil)
	person := generate(t)
	cert := certify(t, person, p.bid, 1, true)
	err := devices.RecordUnchecked(p.s, ctxT(t), p.b, cert) // b is no admin
	if !errors.Is(err, devices.ErrNotRecorded) {
		t.Fatalf("record by a plain member = %v, want ErrNotRecorded", err)
	}
}

// TestRecordAcceptsANewerAnchoredCertificate: another member's newer certificate of the same device orders after the admin's record;
// the stored certificate is anchored and newer, so the record is held.
func TestRecordAcceptsANewerAnchoredCertificate(t *testing.T) {
	p := newPair(t, nil)
	ctx := ctxT(t)
	person := generate(t)
	c1, c2 := certify(t, person, p.bid, 1, true), certify(t, person, p.bid, 2, true)
	if err := p.s.Record(ctx, p.a, c1); err != nil {
		t.Fatal(err)
	}
	if _, err := p.b.Append(ctx, certBody(t, c2)); err != nil {
		t.Fatal(err)
	}
	waitCert(t, p.s, p.a, person.PublicKey(), p.bid.PublicKey(), 2)
	// The older certificate is recorded again by the admin: the roster holds a newer anchored one.
	if err := p.s.Record(ctx, p.a, c1); err != nil {
		t.Fatalf("Record with a newer anchored certificate stored = %v", err)
	}
	// A newer certificate that no admin anchored does not count for an older record.
	op := generate(t)
	if _, err := p.b.Append(ctx, certBody(t, certify(t, op, p.bid, 5, true))); err != nil {
		t.Fatal(err)
	}
	waitCert(t, p.s, p.a, op.PublicKey(), p.bid.PublicKey(), 5)
	if err := p.s.Record(ctx, p.a, certify(t, op, p.bid, 1, true)); !errors.Is(err, devices.ErrNotRecorded) {
		t.Fatalf("Record under a newer unanchored certificate = %v, want ErrNotRecorded", err)
	}
}

// TestRecordFindsItsEntryBehindManyPositions: the record is found however far from the tail it is ordered.
func TestRecordFindsItsEntryBehindManyPositions(t *testing.T) {
	p := newPair(t, nil)
	ctx := ctxT(t)
	// The plain member floods the braid; the admin's record is ordered among 700 entries.
	for i := 0; i < 700; i++ {
		if _, err := p.b.Append(ctx, []byte("noise")); err != nil {
			t.Fatal(err)
		}
	}
	person := generate(t)
	if err := p.s.Record(ctx, p.a, certify(t, person, p.aid, 1, true)); err != nil {
		t.Fatalf("Record among many entries = %v", err)
	}
}

// TestACallerWithAShortContextDoesNotWaitForTheLock: every caller honours its own context while another holds the store.
func TestACallerWithAShortContextDoesNotWaitForTheLock(t *testing.T) {
	h := newHousehold(t, nil, kolobraid.GroupConfig{})
	if err := devices.Acquire(h.s, ctxT(t)); err != nil {
		t.Fatal(err)
	}
	released := false
	defer func() {
		if !released {
			devices.Release(h.s)
		}
	}()
	dev, cert := h.device(1)
	req := h.request(h.newJoin(dev), dev, cert, nil)
	short, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := h.s.Admit(short, h.b, dev.PublicKey(), req); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Admit behind a held lock = %v, want the context's error", err)
	}
	if err := h.s.Record(short, h.b, cert); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Record behind a held lock = %v, want the context's error", err)
	}
	if time.Since(start) > 5*time.Second {
		t.Fatal("the callers waited for the lock past their contexts")
	}
	devices.Release(h.s)
	released = true
	if _, err := h.s.Admit(ctxT(t), h.b, dev.PublicKey(), req); err != nil {
		t.Fatalf("Admit once the lock is free = %v", err)
	}
}

// TestWaitsEndWithTheContext: with settlement stalled nothing is final, so a record that was rejected and a seat that is void are
// waited for until the caller's context ends, and the errors say so.
func TestWaitsEndWithTheContext(t *testing.T) {
	p := newPairCfg(t, &devices.Options{MaxCertsPerPerson: 1}, nil, kolobraid.GroupConfig{MaxMembers: 2})
	ctx := ctxT(t)
	if _, err := p.a.SetSettlers(ctx, []ed25519.PublicKey{p.aid.PublicKey(), p.bid.PublicKey()}, kolobraid.Majority); err != nil {
		t.Fatal(err)
	}
	until(t, p.a, func() bool { return p.a.Status().Seats == 2 })
	person := generate(t)
	if err := p.s.Record(ctx, p.a, certify(t, person, p.aid, 1, true)); err != nil {
		t.Fatal(err)
	}
	waitSettledAnchor(t, p.s, p.a, person.PublicKey(), p.aid.PublicKey())
	p.conn.Close()
	p.b.Close()
	short := func() (context.Context, context.CancelFunc) {
		return context.WithTimeout(context.Background(), 300*time.Millisecond)
	}
	// A second device of the person is over the bound of one: rejected, but only tentatively.
	c1, cancel := short()
	defer cancel()
	err := devices.RecordUnchecked(p.s, c1, p.a, certify(t, person, generate(t), 1, false))
	if !errors.Is(err, devices.ErrNotRecorded) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("record that cannot settle = %v, want ErrNotRecorded wrapping the deadline", err)
	}
	// The group is full (MaxMembers 2): Braid.Admit appends an AddMember the roster leaves void, but it cannot settle either.
	dev := generate(t)
	j, err := kolobraid.NewJoin(ctx, dev, options(t, p.s))
	if err != nil {
		t.Fatal(err)
	}
	req, _ := j.Request(nil)
	welcome, err := p.a.Admit(ctx, dev.PublicKey(), req, nil)
	if err != nil {
		t.Fatal(err)
	}
	c2, cancel2 := short()
	defer cancel2()
	err = devices.WaitSeat(p.s, c2, p.a, welcome, dev.PublicKey())
	if !errors.Is(err, devices.ErrNotAdmitted) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("seat that cannot settle = %v, want ErrNotAdmitted wrapping the deadline", err)
	}
}

// TestRecordOfANewerCertificateTheRosterRejects: the roster holds an older anchored certificate and rejects the newer one (its
// issuer was retired), so the record is not held, whatever the older one says.
func TestRecordOfANewerCertificateTheRosterRejects(t *testing.T) {
	h := newHousehold(t, nil, kolobraid.GroupConfig{})
	approver, dev := generate(t), generate(t)
	acert := certify(t, h.person, approver, 1, true)
	mustRecord(t, h.b, certBody(t, acert))
	older := certify(t, h.person, dev, 1, false)
	if err := h.s.Record(ctxT(t), h.b, older); err != nil {
		t.Fatal(err)
	}
	mustRecord(t, h.b, revBody(t, revokeBySerial(t, h.person, h.person.PublicKey(), approver.PublicKey(), 1), nil))
	newer := certifyBy(t, approver, acert, dev, 2)
	if err := h.s.Record(ctxT(t), h.b, newer); !errors.Is(err, devices.ErrNotRecorded) {
		t.Fatalf("Record of a certificate the roster rejects = %v, want ErrNotRecorded", err)
	}
}
