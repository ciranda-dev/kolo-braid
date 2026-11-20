package roster

import (
	"runtime"
	"testing"
)

// bigRoster returns a roster at MaxMembers 1024 with 1023 admins, 64 settlers and used strand log IDs in all.
func bigRoster(tb testing.TB, used int) *Roster {
	tb.Helper()
	g := Genesis{Group: eid("big"), Founder: kid(0), Strand: sid(0), Rule: Majority, MaxMembers: 1024, MaxSettlers: 64}
	r, err := New(g)
	if err != nil {
		tb.Fatal(err)
	}
	p := uint64(0)
	apply := func(e Entry) {
		e.Strand, e.Index = sid(0), p
		if !r.Apply(p, &e).Effective() {
			tb.Fatalf("position %d", p)
		}
		p++
	}
	apply(Entry{Kind: KindGenesis, ID: g.Group})
	for i := 1; i < used; i++ {
		apply(addW(kid(i), sid(i), Admin))
		if i >= 1023 {
			apply(removeW(kid(i)))
		}
	}
	var keys []Key
	for i := range 64 {
		keys = append(keys, kid(i))
	}
	apply(settlersE(Majority, keys...))
	return r
}

// TestMemoryBound measures the heap of a roster: per active member and per used strand log ID, the only part that grows
// with history (one per effective AddMember).
func TestMemoryBound(t *testing.T) {
	if raceEnabled {
		t.Skip("heap sizes differ under the race detector")
	}
	measure := func(used int) uint64 {
		var a, b runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&a)
		r := bigRoster(t, used)
		runtime.GC()
		runtime.ReadMemStats(&b)
		runtime.KeepAlive(r)
		return b.HeapAlloc - a.HeapAlloc
	}
	small, large := measure(1024), measure(1024+100000)
	perMember := small / 1023
	perUsed := (large - small) / 100000
	t.Logf("1023 members: %d bytes (%d per member); %d bytes per further used strand", small, perMember, perUsed)
	if perMember > 600 || perUsed > 160 {
		t.Fatalf("%d bytes per member, %d per used strand", perMember, perUsed)
	}
}

func BenchmarkApplyMembership(b *testing.B) {
	r := bigRoster(b, 1024)
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		k := kid(1 + i%1000)
		e := Entry{Kind: KindAddMember, Strand: sid(0), Index: uint64(i), Add: Add{Key: k, Strand: sid(5000 + i)}}
		if i%2 == 1 {
			e = Entry{Kind: KindRemoveMember, Strand: sid(0), Index: uint64(i), Target: k}
		}
		r.Apply(uint64(i), &e)
	}
}

func BenchmarkApplyData(b *testing.B) {
	r := bigRoster(b, 1024)
	e := Entry{Kind: KindData, Strand: sid(7)}
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		r.Apply(uint64(i), &e)
	}
}

func BenchmarkClone(b *testing.B) {
	r := bigRoster(b, 1024)
	b.ReportAllocs()
	for b.Loop() {
		r.Clone()
	}
}

func BenchmarkLoad(b *testing.B) {
	r := bigRoster(b, 1024)
	recs := r.Records()
	b.ReportAllocs()
	for b.Loop() {
		if _, err := Load(r.Genesis(), pairs(recs)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAdmitDefaults(b *testing.B) {
	r := bigRoster(b, 1000)
	_, seats := r.Settlers()
	b.ReportAllocs()
	for b.Loop() {
		r.AdmitDefaults(kid(2000), seats[:2], 64)
	}
}
