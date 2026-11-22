package devices_test

import (
	"sync/atomic"
	"testing"
	"time"

	kolobraid "github.com/ciranda-dev/kolo-braid"
	"github.com/ciranda-dev/kolo-braid/devices"
)

// timedApply times the apply calls made while on is set.
type timedApply struct {
	on        atomic.Bool
	calls, ns atomic.Int64
}

func (a *timedApply) wrap(s *devices.Store) kolobraid.ApplyFunc {
	return func(tx *kolobraid.Tx, e *kolobraid.Entry) error {
		if !a.on.Load() {
			return applyFn(s)(tx, e)
		}
		t0 := time.Now()
		err := applyFn(s)(tx, e)
		a.ns.Add(int64(time.Since(t0)))
		a.calls.Add(1)
		return err
	}
}

func (a *timedApply) report(b *testing.B) {
	b.ReportMetric(float64(a.ns.Load())/float64(a.calls.Load()), "ns/apply")
}

func benchBraid(b *testing.B, o *devices.Options) (*kolobraid.Braid, *timedApply) {
	s := newStore(b, o)
	var a timedApply
	opts := options(b, s)
	opts.Apply = a.wrap(s)
	br, err := kolobraid.Create(ctxT(b), generate(b), kolobraid.GroupConfig{App: app}, opts)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { br.Close() })
	return br, &a
}

// BenchmarkApplyCertAtTheBound times the apply of a certificate that is rejected after the scan of 1,024 certificates.
func BenchmarkApplyCertAtTheBound(b *testing.B) {
	br, a := benchBraid(b, nil)
	person := generate(b)
	for range devices.DefaultMaxCertsPerPerson {
		mustRecord(b, br, certBody(b, certify(b, person, generate(b), 1, false)))
	}
	a.on.Store(true)
	b.ResetTimer()
	for range b.N {
		if !record(b, br, certBody(b, certify(b, person, generate(b), 1, false))) {
			b.Fatal("a certificate past the bound applied")
		}
	}
	a.report(b)
}

// BenchmarkApplyRevocationAtTheBound times the apply of an approver revocation that is rejected at 1,024 revocations of a device.
func BenchmarkApplyRevocationAtTheBound(b *testing.B) {
	const bound = 1024
	br, a := benchBraid(b, &devices.Options{MaxRevocationsPerDevice: bound})
	person, laptop, phone := generate(b), generate(b), generate(b)
	lc := certify(b, person, laptop, 1, true)
	mustRecord(b, br, certBody(b, lc))
	revoke := func(serial uint64) []byte {
		pc := certifyBy(b, laptop, lc, phone, serial)
		return revBody(b, revokeByApprover(b, laptop, lc, pc), pc)
	}
	for serial := uint64(1); serial <= bound; serial++ {
		mustRecord(b, br, revoke(serial))
	}
	a.on.Store(true)
	b.ResetTimer()
	for i := range b.N {
		if !record(b, br, revoke(bound+1+uint64(i))) {
			b.Fatal("a revocation past the bound applied")
		}
	}
	a.report(b)
}
