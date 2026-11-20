package devices_test

import (
	"context"
	"testing"
	"time"

	koloid "github.com/ciranda-dev/kolo-id"
	kolostore "github.com/ciranda-dev/kolo-store"

	kolobraid "github.com/ciranda-dev/kolo-braid"
	"github.com/ciranda-dev/kolo-braid/devices"
)

const app = "devices/test"

func ctxT(t testing.TB) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func generate(t testing.TB) *koloid.Identity {
	t.Helper()
	id, err := koloid.Generate()
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func newStore(t testing.TB, o *devices.Options) *devices.Store {
	t.Helper()
	s, err := devices.New([]byte("dev/"), o)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// certify makes dev a device of person: an approver when approver is set, signed by the person key.
func certify(t testing.TB, person, dev *koloid.Identity, serial uint64, approver bool) *koloid.DeviceCert {
	t.Helper()
	o, err := person.OfferDeviceCert(dev.PublicKey(), &koloid.IssueOptions{Serial: serial, Approver: approver})
	if err != nil {
		t.Fatal(err)
	}
	c, err := dev.AcceptDeviceCert(o, person.PublicKey())
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// certifyBy makes dev an ordinary device of the person of parent, signed by the approver a.
func certifyBy(t testing.TB, a *koloid.Identity, parent *koloid.DeviceCert, dev *koloid.Identity, serial uint64) *koloid.DeviceCert {
	t.Helper()
	o, err := a.OfferDeviceCert(dev.PublicKey(), &koloid.IssueOptions{Parent: parent, Serial: serial})
	if err != nil {
		t.Fatal(err)
	}
	c, err := dev.AcceptDeviceCert(o, parent.Person())
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func applyFn(s *devices.Store) kolobraid.ApplyFunc {
	return func(tx *kolobraid.Tx, e *kolobraid.Entry) error {
		_, err := s.Apply(tx, e)
		return err
	}
}

var fastLimits = &kolobraid.Limits{PulseGap: 10 * time.Millisecond, PivotWait: 100 * time.Millisecond,
	AnnounceGap: 10 * time.Millisecond, ConfirmWait: time.Second}

func options(t testing.TB, s *devices.Store) kolobraid.Options {
	t.Helper()
	db := kolostore.NewMemory()
	t.Cleanup(func() { db.Close() })
	return kolobraid.Options{Backend: db, App: app, Apply: applyFn(s), Limits: fastLimits}
}

// found starts a braid of one member, id, whose apply routes roster entries to s.
func found(t testing.TB, s *devices.Store, id *koloid.Identity, cfg kolobraid.GroupConfig) *kolobraid.Braid {
	t.Helper()
	cfg.App = app
	b, err := kolobraid.Create(ctxT(t), id, cfg, options(t, s))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	return b
}

// until waits, on the braid's events, for cond to be true.
func until(t testing.TB, b *kolobraid.Braid, cond func() bool) {
	t.Helper()
	ctx := ctxT(t)
	for !cond() {
		if _, err := b.Next(ctx); err != nil {
			t.Fatalf("waiting: %v", err)
		}
	}
}

// view returns the tentative view and closes it with the test.
func view(t testing.TB, b *kolobraid.Braid) *kolobraid.Snapshot {
	t.Helper()
	v := b.View()
	t.Cleanup(func() { v.Close() })
	return v
}

// record appends body and waits until the entry is settled; it reports whether apply rejected it.
func record(t testing.TB, b *kolobraid.Braid, body []byte) (rejected bool) {
	t.Helper()
	ref, err := b.Append(ctxT(t), body)
	if err != nil {
		t.Fatal(err)
	}
	// the verdict of apply is known once the settled view has passed the entry
	until(t, b, func() bool {
		for o, err := range b.Entries(0) {
			if err != nil {
				t.Fatal(err)
			}
			if o.ID == ref.ID {
				rejected = o.Rejected
				return o.Settled && b.Status().Settled > o.Position
			}
		}
		return false
	})
	return rejected
}

// mustRecord is record for a body that must be accepted.
func mustRecord(t testing.TB, b *kolobraid.Braid, body []byte) {
	t.Helper()
	if record(t, b, body) {
		t.Fatal("apply rejected the entry")
	}
}

func certBody(t testing.TB, c *koloid.DeviceCert) []byte {
	t.Helper()
	body, err := devices.CertBody(c)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func revBody(t testing.TB, r *koloid.Revocation, target *koloid.DeviceCert) []byte {
	t.Helper()
	body, err := devices.RevocationBody(r, target)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func revokeBySerial(t testing.TB, signer *koloid.Identity, person, device []byte, upTo uint64) *koloid.Revocation {
	t.Helper()
	r, err := signer.RevokeDevice(&koloid.RevokeOptions{Person: person, Device: device, UpToSerial: upTo})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// revokeByApprover makes the approver a (certificate ac) revoke the certificate target.
func revokeByApprover(t testing.TB, a *koloid.Identity, ac, target *koloid.DeviceCert) *koloid.Revocation {
	t.Helper()
	r, err := a.RevokeDevice(&koloid.RevokeOptions{Person: ac.Person(), Device: target.Device(), SignerCert: ac, Target: target})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// signAs signs msg under context with id.
func signAs(t testing.TB, id *koloid.Identity, context string, msg []byte) []byte {
	t.Helper()
	sig, err := id.Sign(context, msg)
	if err != nil {
		t.Fatal(err)
	}
	return sig
}
