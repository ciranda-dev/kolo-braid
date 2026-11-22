package devices

import (
	"context"

	koloid "github.com/ciranda-dev/kolo-id"

	kolobraid "github.com/ciranda-dev/kolo-braid"
)

// Caps returns the bounds of apply in a store.
func Caps(s *Store) (certs, reserved, revocations int) { return s.certCap, s.reserve, s.revCap }

// CountKey is the key of the counter of the approver revocations of a device.
func CountKey(s *Store, person, device []byte) []byte { return s.key(kindCount, person, device) }

// RevocationsOf exposes revocationsOf, which reads the approver-signed records from another snapshot than the others.
func RevocationsOf(s *Store, selfView, approverView *kolobraid.Snapshot, person []byte, c *koloid.DeviceCert) ([]*koloid.Revocation, error) {
	return s.revocationsOf(selfView, approverView, person, c)
}

// Acquire and Release take and give back the store's lock, to hold it from a test.
func Acquire(s *Store, ctx context.Context) error { return s.acquire(ctx) }

func Release(s *Store) { s.release() }

// RecordUnchecked appends the certificate from this peer without the admin check of Record, as a member whose role changed would.
func RecordUnchecked(s *Store, ctx context.Context, b *kolobraid.Braid, c *koloid.DeviceCert) error {
	return s.record(ctx, b, c)
}

// WaitSeat is waitSeat for a welcome that Braid.Admit returned.
func WaitSeat(s *Store, ctx context.Context, b *kolobraid.Braid, welcome []byte, joiner []byte) error {
	return s.waitSeat(ctx, b, welcome, joiner, 0)
}
