package kolobraid

import (
	"errors"
	"math"
	"slices"
	"testing"

	kololog "github.com/ciranda-dev/kolo-log"
	kolostore "github.com/ciranda-dev/kolo-store"

	"github.com/ciranda-dev/kolo-braid/internal/dag"
)

// TestLinkCapFits: an entry with linkCap links of the longest index and a MaxBody body fits one ENTRY answer, and one link more
// is refused.
func TestLinkCapFits(t *testing.T) {
	for _, body := range []int{1 << 10, 1 << 20, 3 << 20} {
		n := linkCap(body)
		bd := formatBounds{MaxBody: body, MaxSettlers: 1}
		c := entryContent{Group: GroupID(eid(1)), Kind: KindData, Prev: eid(2), Body: make([]byte, body)}
		for i := 0; i <= n; i++ {
			c.Links = append(c.Links, Link{Strand: kololog.LogID{1, byte(i >> 16), byte(i >> 8), byte(i)}, Index: math.MaxUint64})
		}
		over := c
		c.Links = c.Links[:n]
		enc, err := encodeContent(c, testStrand, 3, bd)
		if err != nil {
			t.Fatalf("body %d: %d links: %v", body, n, err)
		}
		l := newSigner(t, kololog.WithValueEncoding(kololog.Bytes)).log
		if _, err := l.Append(enc); err != nil {
			t.Fatal(err)
		}
		e, err := l.Entry(0)
		if err != nil {
			t.Fatal(err)
		}
		if size := len(e.Record) + len(e.Ciphertext) + len(e.Key) + maxEvidenceObject + 4*5; size > entryBudget {
			t.Errorf("body %d: %d links make an answer of %d bytes, over %d", body, n, size, entryBudget)
		}
		if _, err := encodeContent(over, testStrand, 3, bd); !errors.Is(err, ErrMalformed) {
			t.Errorf("body %d: %d links: %v", body, n+1, err)
		}
	}
	if bd := (formatBounds{MaxMembers: 5, MaxSettlers: 1, MaxBody: 1 << 20}); bd.links() != 4 || (formatBounds{MaxBody: 1 << 20}).links() != linkCap(1<<20) {
		t.Error("links follow MaxMembers when it is set and the cap otherwise")
	}
	if c := (GroupConfig{App: "x", MaxSettlers: linkCap(1<<20) + 1, MinSettlers: 1}).WithDefaults(); c.Validate() != nil {
		t.Errorf("MaxSettlers at the cap plus one: %v", c.Validate())
	}
	if c := (GroupConfig{App: "x", MaxSettlers: linkCap(1<<20) + 2, MinSettlers: 1}).WithDefaults(); c.Validate() == nil {
		t.Error("MaxSettlers above the cap plus one")
	}
}

// TestCheckAdmitNoCeiling: past the old default of 64 members a group without a ceiling still has a free seat, and a group that sets
// one is full at it.
func TestCheckAdmitNoCeiling(t *testing.T) {
	check := func(f *peer) error {
		s := newSigner(t)
		req, err := encodeJoinRequest(JoinRequest{Strand: s.log.ID(), Header: s.header()})
		if err != nil {
			t.Fatal(err)
		}
		return f.b.CheckAdmit(s.id.PublicKey(), req)
	}
	add := func(f *peer, n int) {
		for range n {
			s := newSigner(t)
			if _, err := f.b.AddMember(t.Context(), NewMember{Key: s.id.PublicKey(), Strand: s.log.ID(), Header: s.header()}); err != nil {
				t.Fatal(err)
			}
		}
	}
	f := newFounder(t, "f", GroupConfig{}, appApply)
	add(f, 70)
	if err := check(f); err != nil {
		t.Fatalf("70 members, no ceiling: %v", err)
	}
	g := newFounder(t, "g", GroupConfig{MaxMembers: 3}, appApply)
	add(g, 2)
	if err := check(g); !errors.Is(err, ErrGroupFull) {
		t.Fatalf("3 members at MaxMembers 3: %v", err)
	}
}

// TestAnnounceSplitsAtFrame: more heads than one frame holds go out in several messages that each decode, and a group that sets
// MaxMembers keeps announcing at most that many.
func TestAnnounceSplitsAtFrame(t *testing.T) {
	bd := defaultBounds()
	full := testHead(1, 1<<63)
	heads := slices.Repeat([]kololog.Head{full}, 30000)
	var got, msgs int
	for c := range slices.Chunk(heads, bd.announceHeads()) {
		frame, err := encodeMessage(announceMsg{Group: GroupID(eid(1)), Heads: c}, bd)
		if err != nil {
			t.Fatalf("message %d: %v", msgs, err)
		}
		m, err := decodeMessage(frame, bd)
		if err != nil {
			t.Fatalf("message %d: %v", msgs, err)
		}
		got += len(m.(announceMsg).Heads)
		msgs++
	}
	if msgs != 2 || got != len(heads) {
		t.Fatalf("%d messages with %d heads", msgs, got)
	}
	if n := (formatBounds{MaxMembers: 64, MaxSettlers: 7, MaxBody: 1 << 20}).announceHeads(); n != 64 {
		t.Errorf("heads per announcement at MaxMembers 64: %d", n)
	}
}

func TestLimitsRanges(t *testing.T) {
	d := (Limits{}).withDefaults(defaultMaxBody)
	if d.MaxPendingPerStrand != 256<<10 || d.MaxUnknownHeads != headsPerAnnounce || d.MaxEarlyBytes != 4<<20 || d.validate() != nil {
		t.Fatalf("defaults %+v", d)
	}
	for name, f := range map[string]func(l *Limits){
		"pending per strand 0.5 KiB":  func(l *Limits) { l.MaxPendingPerStrand = 512 },
		"pending per strand over all": func(l *Limits) { l.MaxPendingPerStrand = l.MaxPendingBytes + 1 },
		"unknown heads over 2^20":     func(l *Limits) { l.MaxUnknownHeads = 1<<20 + 1 },
		"early bytes 8 KiB":           func(l *Limits) { l.MaxEarlyBytes = 8 << 10 },
		"early bytes over 1 GiB":      func(l *Limits) { l.MaxEarlyBytes = 1<<30 + 1 },
	} {
		l := d
		f(&l)
		if err := l.validate(); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("%s: %v", name, err)
		}
	}
	for name, f := range map[string]func(l *Limits){
		"pending per strand 1 KiB": func(l *Limits) { l.MaxPendingPerStrand = 1 << 10 },
		"unknown heads 1":          func(l *Limits) { l.MaxUnknownHeads = 1 },
		"unknown heads 2^20":       func(l *Limits) { l.MaxUnknownHeads = 1 << 20 },
		"early bytes 16 KiB":       func(l *Limits) { l.MaxEarlyBytes = 16 << 10 },
		"early bytes 1 GiB":        func(l *Limits) { l.MaxEarlyBytes = 1 << 30 },
	} {
		l := d
		f(&l)
		if err := l.validate(); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}

// TestUnknownHeadsLimit: a peer that announces heads of strands not followed keeps at most Limits.MaxUnknownHeads of them, whatever
// the group's MaxMembers is.
func TestUnknownHeadsLimit(t *testing.T) {
	f := newFounder(t, "f", GroupConfig{}, appApply)
	f.b.mu.Lock()
	f.b.lim.MaxUnknownHeads = 5
	f.b.mu.Unlock()
	r := &remote{s: f.b.sync, key: keyOf(f), heads: map[kololog.LogID]uint64{}, unknown: map[kololog.LogID]kololog.Head{}, alive: true}
	for i := range 50 {
		r.keepUnknown(kololog.Head{LogID: kololog.LogID{byte(i + 1)}, Size: 5})
	}
	if len(r.unknown) != 5 {
		t.Fatalf("%d unknown heads kept, bound 5", len(r.unknown))
	}
}

// TestEarlyEvidenceBytes: early evidence against strands not followed is kept up to Limits.MaxEarlyBytes, counted back when a
// strand is followed or dropped.
func TestEarlyEvidenceBytes(t *testing.T) {
	f := newFounder(t, "f", GroupConfig{}, appApply)
	mk := func(i byte) evidenceBody {
		strand := kololog.LogID{0xee, i}
		return evidenceBody{Key: keyOf(f), Strand: strand, Type: evidenceRecords, A: forgeRecord(t, f, strand, 1), B: forgeRecord(t, f, strand, 2)}
	}
	size := earlySize(mk(0))
	f.b.mu.Lock()
	defer f.b.mu.Unlock()
	f.b.lim.MaxEarlyBytes = 3*size + size/2
	for i := range byte(20) {
		f.b.keepEarly(mk(i))
	}
	if len(f.b.early) != 3 || f.b.earlyBytes != 3*size {
		t.Fatalf("%d strands, %d bytes kept; bound %d", len(f.b.early), f.b.earlyBytes, f.b.lim.MaxEarlyBytes)
	}
	f.b.dropEarly(kololog.LogID{0xee, 0})
	if len(f.b.early) != 2 || f.b.earlyBytes != 2*size {
		t.Fatalf("after a drop: %d strands, %d bytes", len(f.b.early), f.b.earlyBytes)
	}
	f.b.keepEarly(mk(30))
	if len(f.b.early) != 3 {
		t.Fatalf("a freed slot was not reused: %d strands", len(f.b.early))
	}
}

// TestPendingPerStrandLimit: one strand's entries that wait for a predecessor stop at Limits.MaxPendingPerStrand, and another strand
// still parks its own.
func TestPendingPerStrandLimit(t *testing.T) {
	opts := testOptions(kolostore.NewMemory(), appApply)
	opts.Limits.MaxPendingPerStrand = 1536
	b, err := Create(t.Context(), newIdentity(t), GroupConfig{App: "test/v1"}, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	b.mu.Lock()
	defer b.mu.Unlock()
	park := func(strand byte, i int) error {
		_, err := b.d.Add(dag.Node{ID: dag.ID{strand, byte(i)}, Strand: dag.StrandID{strand}, Index: 5, Prev: dag.ID{9, strand, byte(i)}, Class: dag.Other})
		return err
	}
	var full int
	for i := range 20 {
		if errors.Is(park(1, i), dag.ErrPendingFull) {
			full++
		}
	}
	if _, bytes := b.d.Pending(); bytes > 1536 || full != 17 {
		t.Fatalf("%d bytes pending, %d refused", bytes, full)
	}
	if err := park(2, 0); err != nil {
		t.Fatalf("another strand: %v", err)
	}
}

// TestLookupCacheBytes: the clocks the entry index lookup caches take at most 32 MiB, however many segments they span.
func TestLookupCacheBytes(t *testing.T) {
	const segs = 4096
	db := kolostore.NewMemory()
	strand := kololog.LogID{1}
	var seg [4]byte
	seg[3] = 7
	l := &lookup{db: db, segs: func() int { return segs }, cache: map[dag.Link]lookupHit{}}
	for i := range 1200 {
		var id EntryID
		id[0], id[1] = byte(i), byte(i>>8)
		rec := dag.Record{Seg: 7, Index: uint64(i), Full: true, Comps: []dag.Comp{{Seg: segs - 1, Value: 1}}}
		if err := db.Put(entryKey(strand, uint64(i), id), seg[:]); err != nil {
			t.Fatal(err)
		}
		if err := db.Put(segKey(7, uint64(i)), encodeSegEntry(id, dag.Other, rec.Encode())); err != nil {
			t.Fatal(err)
		}
		if _, _, ok, err := l.find(dag.Link{Strand: dag.StrandID(strand), Index: uint64(i), ID: dag.ID(id)}); !ok || err != nil {
			t.Fatalf("entry %d: found %v, error %v", i, ok, err)
		}
	}
	var held int
	for _, h := range l.cache {
		held += 8 * len(h.clock)
	}
	if held > maxLookupBytes || held == 0 {
		t.Fatalf("%d bytes of clocks cached, bound %d", held, maxLookupBytes)
	}
}

// TestCapLinks: past the cap the seats stay linked and the others rotate, so every strand is linked again within a few own entries.
func TestCapLinks(t *testing.T) {
	var links []Link
	seats := map[kololog.LogID]bool{}
	for i := range 10 {
		l := Link{Strand: kololog.LogID{1, byte(i)}}
		links = append(links, l)
		if i < 2 {
			seats[l.Strand] = true
		}
	}
	if got := capLinks(slices.Clone(links), seats, 10, 0); len(got) != 10 {
		t.Fatalf("under the cap: %d links", len(got))
	}
	seen := map[kololog.LogID]int{}
	for index := range uint64(4) {
		got := capLinks(slices.Clone(links), seats, 4, index)
		if len(got) != 4 || !slices.IsSortedFunc(got, func(a, c Link) int { return slices.Compare(a.Strand[:], c.Strand[:]) }) {
			t.Fatalf("entry %d: %d links", index, len(got))
		}
		for _, l := range got {
			seen[l.Strand]++
		}
	}
	for _, l := range links {
		if seen[l.Strand] == 0 || (seats[l.Strand] && seen[l.Strand] != 4) {
			t.Errorf("strand %v linked %d times in 4 entries", l.Strand[:2], seen[l.Strand])
		}
	}
}
