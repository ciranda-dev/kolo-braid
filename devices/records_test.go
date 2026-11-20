package devices

import (
	"bytes"
	"errors"
	"testing"

	kolocodec "github.com/ciranda-dev/kolo-codec"
	koloid "github.com/ciranda-dev/kolo-id"

	kolobraid "github.com/ciranda-dev/kolo-braid"
)

func seededIdentity(t testing.TB, n byte) *koloid.Identity {
	t.Helper()
	id, err := koloid.FromSeed(bytes.Repeat([]byte{n}, 32))
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// fixtures returns a person, an approver certificate, an ordinary certificate and the three kinds of revocation.
func fixtures(t testing.TB) (ac, oc *koloid.DeviceCert, byPerson, bySelf, byApprover *koloid.Revocation) {
	t.Helper()
	person, a, d := seededIdentity(t, 1), seededIdentity(t, 2), seededIdentity(t, 3)
	o, err := person.OfferDeviceCert(a.PublicKey(), &koloid.IssueOptions{Serial: 1, Approver: true})
	if err != nil {
		t.Fatal(err)
	}
	if ac, err = a.AcceptDeviceCert(o, person.PublicKey()); err != nil {
		t.Fatal(err)
	}
	o, err = a.OfferDeviceCert(d.PublicKey(), &koloid.IssueOptions{Parent: ac, Serial: 1})
	if err != nil {
		t.Fatal(err)
	}
	if oc, err = d.AcceptDeviceCert(o, person.PublicKey()); err != nil {
		t.Fatal(err)
	}
	rev := func(signer *koloid.Identity, o *koloid.RevokeOptions) *koloid.Revocation {
		o.Person = person.PublicKey()
		r, err := signer.RevokeDevice(o)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	byPerson = rev(person, &koloid.RevokeOptions{Device: d.PublicKey(), UpToSerial: 1})
	bySelf = rev(d, &koloid.RevokeOptions{Device: d.PublicKey(), UpToSerial: 2})
	byApprover = rev(a, &koloid.RevokeOptions{Device: d.PublicKey(), SignerCert: ac, Target: oc})
	return
}

func TestBodiesRoundTrip(t *testing.T) {
	ac, oc, byPerson, bySelf, byApprover := fixtures(t)
	body, err := CertBody(oc)
	if err != nil {
		t.Fatal(err)
	}
	r, err := decodeRecord(body)
	if err != nil || r.op != opCert || !bytes.Equal(r.cert.Encode(), oc.Encode()) {
		t.Fatalf("certificate record = %+v, %v", r, err)
	}
	for name, c := range map[string]struct {
		rev    *koloid.Revocation
		target *koloid.DeviceCert
	}{"person": {byPerson, nil}, "self": {bySelf, nil}, "approver": {byApprover, oc}} {
		body, err := RevocationBody(c.rev, c.target)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		r, err := decodeRecord(body)
		if err != nil || r.op != opRevocation || !bytes.Equal(r.rev.Encode(), c.rev.Encode()) || (c.target == nil) != (r.target == nil) {
			t.Fatalf("%s record = %+v, %v", name, r, err)
		}
		if c.target != nil && !bytes.Equal(r.target.Encode(), c.target.Encode()) {
			t.Fatalf("%s: the target changed", name)
		}
	}
	_ = ac
}

func TestDecodeRecordRefusals(t *testing.T) {
	ac, oc, _, bySelf, byApprover := fixtures(t)
	cert, _ := CertBody(oc)
	rev, _ := RevocationBody(byApprover, oc)
	put := func(f func(e *kolocodec.Encoder)) []byte {
		var e kolocodec.Encoder
		e.PutString(tag)
		f(&e)
		return e.Bytes()
	}
	otherOp := bytes.Clone(rev)
	otherOp[len(tag)+1] = 3
	var wrongTag kolocodec.Encoder
	wrongTag.PutString("kolo-braid/kv/v0")
	wrongTag.PutUvarint(1)
	for name, b := range map[string][]byte{
		"empty":               nil,
		"wrong tag":           wrongTag.Bytes(),
		"tag only":            put(func(e *kolocodec.Encoder) {}),
		"op zero":             put(func(e *kolocodec.Encoder) { e.PutUvarint(0) }),
		"op three":            put(func(e *kolocodec.Encoder) { e.PutUvarint(3) }),
		"revocation as op 3":  otherOp,
		"certificate cut":     cert[:len(cert)-1],
		"certificate trailer": append(bytes.Clone(cert), 0),
		"certificate garbage": put(func(e *kolocodec.Encoder) { e.PutUvarint(opCert); e.PutBytes([]byte{1, 2, 3}) }),
		"certificate oversized": put(func(e *kolocodec.Encoder) {
			e.PutUvarint(opCert)
			e.PutBytes(make([]byte, maxCertBytes+1))
		}),
		"offer instead":      put(func(e *kolocodec.Encoder) { e.PutUvarint(opCert); e.PutBytes(bySelf.Encode()) }),
		"revocation cut":     rev[:len(rev)-1],
		"revocation trailer": append(bytes.Clone(rev), 0),
		"revocation garbage": put(func(e *kolocodec.Encoder) { e.PutUvarint(opRevocation); e.PutBytes([]byte{9}) }),
		"revocation oversized": put(func(e *kolocodec.Encoder) {
			e.PutUvarint(opRevocation)
			e.PutBytes(make([]byte, maxRevocationBytes+1))
		}),
		"target flag two": put(func(e *kolocodec.Encoder) {
			e.PutUvarint(opRevocation)
			e.PutBytes(bySelf.Encode())
			e.PutFixed([]byte{2})
		}),
		"target garbage": put(func(e *kolocodec.Encoder) {
			e.PutUvarint(opRevocation)
			e.PutBytes(bySelf.Encode())
			e.PutBool(true)
			e.PutBytes([]byte{1})
		}),
		"target oversized": put(func(e *kolocodec.Encoder) {
			e.PutUvarint(opRevocation)
			e.PutBytes(bySelf.Encode())
			e.PutBool(true)
			e.PutBytes(make([]byte, maxCertBytes+1))
		}),
		"target is a revocation": put(func(e *kolocodec.Encoder) {
			e.PutUvarint(opRevocation)
			e.PutBytes(bySelf.Encode())
			e.PutBool(true)
			e.PutBytes(bySelf.Encode())
		}),
	} {
		if len(b) == 0 || !isRecord(b) {
			if isRecord(b) {
				t.Errorf("%s: isRecord", name)
			}
			continue
		}
		if _, err := decodeRecord(b); !errors.Is(err, kolobraid.ErrMalformed) {
			t.Errorf("%s: %v, want ErrMalformed", name, err)
		}
	}
	if isRecord(wrongTag.Bytes()) || isRecord(nil) {
		t.Error("isRecord accepts another tag")
	}
	_ = ac
}

func TestProofRoundTripAndRefusals(t *testing.T) {
	_, oc, _, _, _ := fixtures(t)
	sig := bytes.Repeat([]byte{7}, 64)
	p := proof{cert: oc, sig: sig, app: []byte("name")}
	got, err := decodeProof(p.encode())
	if err != nil || !bytes.Equal(got.cert.Encode(), oc.Encode()) || !bytes.Equal(got.sig, sig) || string(got.app) != "name" {
		t.Fatalf("proof = %+v, %v", got, err)
	}
	if got, err := decodeProof(proof{cert: oc, sig: sig}.encode()); err != nil || len(got.app) != 0 {
		t.Fatalf("proof without data = %+v, %v", got, err)
	}
	enc := p.encode()
	var otherTag kolocodec.Encoder
	otherTag.PutString("kolo-braid/devices/v0")
	for name, b := range map[string][]byte{
		"empty": nil, "other tag": otherTag.Bytes(), "cut at the signature": enc[:len(enc)-len("name")-1-20],
		"cut at the data": enc[:len(enc)-2], "trailing": append(bytes.Clone(enc), 0),
	} {
		_, err := decodeProof(b)
		if err == nil {
			t.Errorf("%s: decoded", name)
		}
		if name == "empty" || name == "other tag" {
			if !errors.Is(err, errNoProof) {
				t.Errorf("%s: %v, want errNoProof", name, err)
			}
		} else if !errors.Is(err, kolobraid.ErrMalformed) {
			t.Errorf("%s: %v, want ErrMalformed", name, err)
		}
	}
	var badCert kolocodec.Encoder
	badCert.PutString(proofTag)
	badCert.PutBytes([]byte{1, 2})
	if _, err := decodeProof(badCert.Bytes()); !errors.Is(err, kolobraid.ErrMalformed) {
		t.Errorf("bad certificate: %v", err)
	}
	var big kolocodec.Encoder
	big.PutString(proofTag)
	big.PutBytes(make([]byte, maxCertBytes+1))
	if _, err := decodeProof(big.Bytes()); !errors.Is(err, kolobraid.ErrMalformed) {
		t.Errorf("oversized certificate: %v", err)
	}
}

func TestStoredRecordRoundTrip(t *testing.T) {
	s := stored{position: 1 << 40, anchored: true, approver: true, raw: []byte("abc")}
	got, err := decodeStored(s.encode())
	if err != nil || got.position != s.position || !got.anchored || !got.approver || string(got.raw) != "abc" {
		t.Fatalf("stored = %+v, %v", got, err)
	}
	for name, b := range map[string][]byte{"empty": nil, "cut": s.encode()[:3], "trailing": append(s.encode(), 0),
		"flag two": {0, 2, 0, 0}, "second flag two": {0, 0, 2, 0}} {
		if _, err := decodeStored(b); !errors.Is(err, kolobraid.ErrCorrupt) {
			t.Errorf("%s: %v, want ErrCorrupt", name, err)
		}
	}
}

func FuzzDecodeRecord(f *testing.F) {
	_, oc, byPerson, _, byApprover := fixtures(f)
	c, _ := CertBody(oc)
	r1, _ := RevocationBody(byPerson, nil)
	r2, _ := RevocationBody(byApprover, oc)
	f.Add(c)
	f.Add(r1)
	f.Add(r2)
	f.Add([]byte(tag))
	f.Fuzz(func(t *testing.T, b []byte) {
		r, err := decodeRecord(b)
		if err != nil {
			if !errors.Is(err, kolobraid.ErrMalformed) {
				t.Fatalf("error does not wrap ErrMalformed: %v", err)
			}
			return
		}
		var again []byte
		if r.op == opCert {
			again, _ = CertBody(r.cert)
		} else {
			again, err = RevocationBody(r.rev, r.target)
			if err != nil {
				// a decoded record that the writer would refuse: its target flag disagrees with its form; apply rejects it
				return
			}
		}
		if !bytes.Equal(again, b) {
			t.Fatalf("decoded %x, encodes as %x", b, again)
		}
	})
}

func FuzzDecodeProof(f *testing.F) {
	_, oc, _, _, _ := fixtures(f)
	f.Add(proof{cert: oc, sig: bytes.Repeat([]byte{1}, 64), app: []byte("x")}.encode())
	f.Add([]byte(proofTag))
	f.Fuzz(func(t *testing.T, b []byte) {
		p, err := decodeProof(b)
		if err != nil {
			return
		}
		if again := p.encode(); !bytes.Equal(again, b) {
			t.Fatalf("decoded %x, encodes as %x", b, again)
		}
	})
}

func FuzzDecodeStored(f *testing.F) {
	f.Add(stored{position: 3, anchored: true, raw: []byte("raw")}.encode())
	f.Add(stored{position: 3, approver: true, raw: []byte("raw")}.encode())
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, b []byte) {
		s, err := decodeStored(b)
		if err != nil {
			return
		}
		if again := s.encode(); !bytes.Equal(again, b) {
			t.Fatalf("decoded %x, encodes as %x", b, again)
		}
	})
}
