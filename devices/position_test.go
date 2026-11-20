package devices_test

import (
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	koloid "github.com/ciranda-dev/kolo-id"
	kolostore "github.com/ciranda-dev/kolo-store"

	kolobraid "github.com/ciranda-dev/kolo-braid"
	"github.com/ciranda-dev/kolo-braid/devices"
)

// checkApply is the roster's apply plus a data entry "check", which records under "check/<position>" whether the phone's
// certificate in the roster is revoked at that position, and "keys", which records whether the Tx readers refuse short keys.
func checkApply(s *devices.Store, person, phone *koloid.Identity) kolobraid.ApplyFunc {
	return func(tx *kolobraid.Tx, e *kolobraid.Entry) error {
		if handled, err := s.Apply(tx, e); handled || err != nil {
			return err
		}
		if e.Kind != kolobraid.KindData {
			return nil
		}
		key := fmt.Appendf(nil, "check/%020d", e.Position)
		switch string(e.Body) {
		case "check":
			c, err := s.CertTx(tx, person.PublicKey(), phone.PublicKey())
			if err != nil {
				return err
			}
			revs, err := s.RevocationsTx(tx, person.PublicKey(), phone.PublicKey())
			if err != nil {
				return err
			}
			verdict := fmt.Sprintf("serial %d valid", c.Serial())
			if err := c.Verify(&koloid.VerifyOptions{Person: person.PublicKey(), Now: time.Unix(0, 0), Revocations: revs}); err != nil {
				verdict = fmt.Sprintf("serial %d revoked", c.Serial())
			}
			return tx.Put(key, []byte(verdict))
		case "keys":
			short := []byte{1, 2, 3}
			_, e1 := s.CertTx(tx, person.PublicKey(), short)
			_, e2 := s.RevocationsTx(tx, short, phone.PublicKey())
			return tx.Put(key, fmt.Appendf(nil, "%v %v", errors.Is(e1, kolobraid.ErrInvalidArgument),
				errors.Is(e2, kolobraid.ErrInvalidArgument)))
		}
		return nil
	}
}

// TestRevocationAtAnEntrysPosition: whether a device was revoked at or before the position of an entry is read inside apply
// (CertTx, RevocationsTx) and after it (Revocations on Braid.SettledAt) with the same answer, by an approver or by serial,
// which a read of the current roster cannot give: a revocation by serial that a later one raises keeps its first position.
func TestRevocationAtAnEntrysPosition(t *testing.T) {
	s := newStore(t, nil)
	person, phone := generate(t), generate(t)
	opts := options(t, s)
	opts.Apply = checkApply(s, person, phone)
	b, err := kolobraid.Create(ctxT(t), generate(t), kolobraid.GroupConfig{App: app}, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	laptop := generate(t)
	lc := certify(t, person, laptop, 1, true)
	pc1, pc2 := certifyBy(t, laptop, lc, phone, 1), certifyBy(t, laptop, lc, phone, 2)
	steps := [][]byte{
		certBody(t, lc), certBody(t, pc1), []byte("check"),
		revBody(t, revokeBySerial(t, phone, person.PublicKey(), phone.PublicKey(), 1), nil), []byte("check"),
		certBody(t, pc2), []byte("check"),
		revBody(t, revokeByApprover(t, laptop, lc, pc2), pc2), []byte("check"),
		revBody(t, revokeBySerial(t, person, person.PublicKey(), phone.PublicKey(), 2), nil), []byte("check"),
		[]byte("keys"),
	}
	for _, body := range steps {
		mustRecord(t, b, body)
	}
	var checks []uint64
	var keys uint64
	for o, err := range b.Entries(0) {
		if err != nil {
			t.Fatal(err)
		}
		switch string(o.Body) {
		case "check":
			checks = append(checks, o.Position)
		case "keys":
			keys = o.Position
		}
	}
	settled := b.Settled()
	defer settled.Close()
	want := []string{"serial 1 valid", "serial 1 revoked", "serial 2 valid", "serial 2 revoked", "serial 2 revoked"}
	for i, p := range checks {
		v, err := settled.Get(fmt.Appendf(nil, "check/%020d", p))
		if err != nil || string(v) != want[i] {
			t.Fatalf("check at %d inside apply: %q %v, want %q", p, v, err, want[i])
		}
	}
	if v, err := settled.Get(fmt.Appendf(nil, "check/%020d", keys)); err != nil || string(v) != "true true" {
		t.Fatalf("Tx readers with short keys: %q %v", v, err)
	}
	// After it: the snapshot at the third check holds the first revocation only, so serial 2 was valid there.
	for i, p := range checks {
		at, err := b.SettledAt(ctxT(t), p)
		if err != nil {
			t.Fatal(err)
		}
		c, err := s.Cert(at, person.PublicKey(), phone.PublicKey())
		if err != nil {
			t.Fatal(err)
		}
		revs, err := s.Revocations(at, person.PublicKey(), phone.PublicKey())
		if err != nil {
			t.Fatal(err)
		}
		got := fmt.Sprintf("serial %d valid", c.Serial())
		if c.Verify(&koloid.VerifyOptions{Person: person.PublicKey(), Revocations: revs}) != nil {
			got = fmt.Sprintf("serial %d revoked", c.Serial())
		}
		if got != want[i] {
			t.Fatalf("check at %d from SettledAt: %q, want %q", p, got, want[i])
		}
		if err := at.Close(); err != nil {
			t.Fatal(err)
		}
	}
	revs, err := s.Revocations(settled, person.PublicKey(), phone.PublicKey())
	if err != nil {
		t.Fatal(err)
	}
	if pc2.Verify(&koloid.VerifyOptions{Person: person.PublicKey(), Revocations: revs}) == nil {
		t.Fatal("the current roster does not revoke serial 2")
	}
}

var errScan = errors.New("scan failed on purpose")

// scanFault is a backend whose scans fail while fail is set.
type scanFault struct {
	kolostore.Backend
	fail *atomic.Bool
}

func (f scanFault) Scan(start, end []byte, reverse bool, limit int, fn func(k, v []byte) bool) error {
	if f.fail.Load() {
		return errScan
	}
	return f.Backend.Scan(start, end, reverse, limit, fn)
}

// TestReadsReportAStorageError: Devices and Revocations return the error of a scan that a storage error cut short instead of
// a shorter list.
func TestReadsReportAStorageError(t *testing.T) {
	s := newStore(t, nil)
	person, laptop, phone := generate(t), generate(t), generate(t)
	opts := options(t, s)
	f := scanFault{Backend: opts.Backend, fail: new(atomic.Bool)}
	opts.Backend = f
	b, err := kolobraid.Create(ctxT(t), generate(t), kolobraid.GroupConfig{App: app}, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	lc := certify(t, person, laptop, 1, true)
	pc := certifyBy(t, laptop, lc, phone, 1)
	mustRecord(t, b, certBody(t, lc))
	mustRecord(t, b, certBody(t, pc))
	mustRecord(t, b, revBody(t, revokeByApprover(t, laptop, lc, pc), pc))
	for name, read := range map[string]func(*kolobraid.Snapshot) (int, error){
		"Devices": func(v *kolobraid.Snapshot) (int, error) {
			cs, err := s.Devices(v, person.PublicKey())
			return len(cs), err
		},
		"Revocations": func(v *kolobraid.Snapshot) (int, error) {
			rs, err := s.Revocations(v, person.PublicKey(), phone.PublicKey())
			return len(rs), err
		},
	} {
		v := b.Settled()
		if n, err := read(v); err != nil || n == 0 {
			t.Fatalf("%s: %d, %v", name, n, err)
		}
		v.Close()
		v = b.Settled()
		f.fail.Store(true)
		n, err := read(v)
		f.fail.Store(false)
		v.Close()
		if !errors.Is(err, errScan) {
			t.Fatalf("%s after a storage error: %d, %v", name, n, err)
		}
	}
}

// TestAnEarlierScanErrorDoesNotStay: a read after one that a storage error cut short reads the whole range again from the same
// snapshot.
func TestAnEarlierScanErrorDoesNotStay(t *testing.T) {
	s := newStore(t, nil)
	person, laptop := generate(t), generate(t)
	opts := options(t, s)
	f := scanFault{Backend: opts.Backend, fail: new(atomic.Bool)}
	opts.Backend = f
	b, err := kolobraid.Create(ctxT(t), generate(t), kolobraid.GroupConfig{App: app}, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	mustRecord(t, b, certBody(t, certify(t, person, laptop, 1, true)))
	v := b.Settled()
	defer v.Close()
	f.fail.Store(true)
	_, err = s.Devices(v, person.PublicKey())
	f.fail.Store(false)
	if !errors.Is(err, errScan) {
		t.Fatalf("Devices during a storage error: %v", err)
	}
	if cs, err := s.Devices(v, person.PublicKey()); err != nil || len(cs) != 1 {
		t.Fatalf("Devices after it: %d, %v", len(cs), err)
	}
}

// TestRevocationsOfHoldTheIssuers: a device an approver certified is void once the approver is revoked; RevocationsOf and
// RevocationsOfTx hold that revocation, the device's own Revocations do not.
func TestRevocationsOfHoldTheIssuers(t *testing.T) {
	s := newStore(t, nil)
	person, laptop, phone := generate(t), generate(t), generate(t)
	opts := options(t, s)
	opts.Apply = func(tx *kolobraid.Tx, e *kolobraid.Entry) error {
		if handled, err := s.Apply(tx, e); handled || err != nil {
			return err
		}
		if e.Kind != kolobraid.KindData {
			return nil
		}
		key := fmt.Appendf(nil, "check/%020d", e.Position)
		switch string(e.Body) {
		case "chain":
			c, err := s.CertTx(tx, person.PublicKey(), phone.PublicKey())
			if err != nil {
				return err
			}
			own, err := s.RevocationsTx(tx, person.PublicKey(), phone.PublicKey())
			if err != nil {
				return err
			}
			all, err := s.RevocationsOfTx(tx, person.PublicKey(), c)
			if err != nil {
				return err
			}
			return tx.Put(key, fmt.Appendf(nil, "%v %v", verify(person, c, own) == nil, verify(person, c, all) == nil))
		case "args":
			c, err := s.CertTx(tx, person.PublicKey(), phone.PublicKey())
			if err != nil {
				return err
			}
			_, e1 := s.RevocationsOfTx(tx, []byte{1}, c)
			_, e2 := s.RevocationsOfTx(tx, person.PublicKey(), nil)
			return tx.Put(key, fmt.Appendf(nil, "%v %v", errors.Is(e1, kolobraid.ErrInvalidArgument),
				errors.Is(e2, kolobraid.ErrInvalidArgument)))
		}
		return nil
	}
	b, err := kolobraid.Create(ctxT(t), generate(t), kolobraid.GroupConfig{App: app}, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	lc := certify(t, person, laptop, 1, true)
	pc := certifyBy(t, laptop, lc, phone, 1)
	for _, body := range [][]byte{certBody(t, lc), certBody(t, pc), []byte("chain"),
		revBody(t, revokeBySerial(t, person, person.PublicKey(), laptop.PublicKey(), 1), nil), []byte("chain"), []byte("args")} {
		mustRecord(t, b, body)
	}
	var got []string
	for o, err := range b.Entries(0) {
		if err != nil {
			t.Fatal(err)
		}
		if o.Kind == kolobraid.KindData && (string(o.Body) == "chain" || string(o.Body) == "args") {
			v := b.Settled()
			val, err := v.Get(fmt.Appendf(nil, "check/%020d", o.Position))
			v.Close()
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, string(val))
		}
	}
	// own revocations / with the issuer's: valid before the approver is revoked, then only the full set voids the device
	want := []string{"true true", "true false", "true true"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("inside apply: %q, want %q", got, want)
	}
	v := b.Settled()
	defer v.Close()
	own, err := s.Revocations(v, person.PublicKey(), phone.PublicKey())
	if err != nil {
		t.Fatal(err)
	}
	all, err := s.RevocationsOf(v, person.PublicKey(), pc)
	if err != nil {
		t.Fatal(err)
	}
	if verify(person, pc, own) != nil || !errors.Is(verify(person, pc, all), koloid.ErrCertRevoked) {
		t.Fatalf("settled view: own %v, with the issuer's %v", verify(person, pc, own), verify(person, pc, all))
	}
	if _, err := s.RevocationsOf(v, person.PublicKey(), nil); !errors.Is(err, kolobraid.ErrInvalidArgument) {
		t.Fatalf("no certificate: %v", err)
	}
	if err := s.Verify(v, person.PublicKey(), phone.PublicKey(), time.Time{}, msgCtx, []byte("m"), signAs(t, phone, msgCtx, []byte("m"))); !errors.Is(err, koloid.ErrCertRevoked) {
		t.Fatalf("Verify = %v, the set RevocationsOf gives", err)
	}
}

func verify(person *koloid.Identity, c *koloid.DeviceCert, revs []*koloid.Revocation) error {
	return c.Verify(&koloid.VerifyOptions{Person: person.PublicKey(), Now: time.Unix(0, 0), Revocations: revs})
}
