package kolobraid

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	kolocodec "github.com/ciranda-dev/kolo-codec"
	kololog "github.com/ciranda-dev/kolo-log"

	"github.com/ciranda-dev/kolo-braid/internal/roster"
)

// forgeRecord signs, with signer's own key, a kolo-log record whose log ID names another member's strand.
func forgeRecord(t testing.TB, signer *peer, strand kololog.LogID, salt byte) []byte {
	t.Helper()
	r := kololog.Record{LogKey: keyOf(signer), LogID: strand, Index: 0, Size: 64}
	r.ContentHash[0] = salt
	var e kolocodec.Encoder
	e.PutString(kololog.RecordContext)
	e.PutFixed(r.LogKey)
	e.PutFixed(r.LogID[:])
	e.PutUvarint(r.Index)
	e.PutUvarint(r.Size)
	e.PutFixed(r.ContentHash[:])
	e.PutFixed(r.KeyCommit[:])
	sig, err := signer.id.Sign(kololog.RecordContext, e.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	r.Signature = sig
	return r.Encode()
}

// forgeHeader signs, with a fresh key, a log header that names strand.
func forgeHeader(t testing.TB, strand kololog.LogID) NewMember {
	t.Helper()
	fake := newIdentity(t)
	var e kolocodec.Encoder
	e.PutString(kololog.HeaderContext)
	e.PutFixed(fake.PublicKey())
	e.PutFixed(strand[:])
	e.PutUvarint(uint64(kololog.Bytes))
	sig, err := fake.Sign(kololog.HeaderContext, e.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	return NewMember{Key: fake.PublicKey(), Strand: strand, Header: append(bytes.Clone(e.Bytes()), sig...)}
}

// TestForgedEvidenceEntry: an Evidence entry that names a seat's strand but is signed by another key, from a plain or a
// removed member, marks nothing: settlement goes on everywhere, also after a reopen.
func TestForgedEvidenceEntry(t *testing.T) {
	for _, removed := range []bool{false, true} {
		nt, ps := group(t, 4, GroupConfig{}, appApply, false)
		setSettlers(t, nt, ps, ps[:3])
		victim, attacker := ps[0], ps[3]
		if removed {
			if _, err := ps[0].b.RemoveMember(ctx10(t), keyOf(attacker)); err != nil {
				t.Fatal(err)
			}
			nt.settle(nil, 20*time.Second, func() bool { return quiet(ps...) && !hasMember(ps[0].b, keyOf(attacker), true) })
		}
		vs := victim.b.meta.OwnStrand
		ev := evidenceBody{Key: keyOf(attacker), Strand: vs, Type: evidenceRecords, A: forgeRecord(t, attacker, vs, 1),
			B: forgeRecord(t, attacker, vs, 2)}
		body, err := encodeBody(ev, attacker.b.groupBounds())
		if err != nil {
			t.Fatal(err)
		}
		attacker.b.mu.Lock()
		_, err = attacker.b.appendLocked(KindEvidence, body)
		attacker.b.mu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
		mustAppend(t, ps[1].b, "after=1")
		nt.settle(nil, 20*time.Second, func() bool { return quiet(ps[:3]...) && settledGet(t, ps[2].b, "after") == "1" })
		ps[1].reopen()
		nt.confirm(ps[1])
		for _, p := range ps {
			if p.b.holdsEvidence(vs) || p.b.Status().SettlementHalted != nil {
				t.Fatalf("removed %v, %s: forged evidence held %v, halted %v", removed, p.name, p.b.holdsEvidence(vs),
					p.b.Status().SettlementHalted)
			}
		}
	}
}

// TestStrandIDSquat: a plain member's AddMember entry that names another joiner's strand under its own key is void, and no
// peer follows that strand under the squatter's key, whatever entry it receives first.
func TestStrandIDSquat(t *testing.T) {
	nt, ps := group(t, 3, GroupConfig{}, appApply, false)
	a, v, r := ps[0], ps[1], ps[2]
	j, nw := newJoiner(t, "j", a, appApply)
	nt.partition([]int{0}, []int{1})
	if _, err := a.b.AddMember(ctx10(t), nw); err != nil {
		t.Fatal(err)
	}
	j.start()
	fake := forgeHeader(t, nw.Strand)
	body, err := encodeBody(fake, r.b.bounds)
	if err != nil {
		t.Fatal(err)
	}
	mustAppend(t, r.b, "r=1")
	prev := entryIDOf(mustRecord(t, r, r.b.own.Len()-1))
	rogueAppend(t, r, rogueContent(t, r, entryContent{Group: a.b.meta.Group, Kind: KindAddMember, Prev: prev, Body: body}))
	pull(t, r, v, 0)
	if s := v.b.strandFor(nw.Strand); s != nil {
		t.Fatalf("v follows j's strand from a void AddMember (squatter's key %v)", bytes.Equal(s.key, fake.Key))
	}
	all := append(ps, j)
	nt2 := newNet(t, all...)
	nt2.settle(nil, 20*time.Second, func() bool { return !j.b.Status().Unconfirmed && hasMember(j.b, keyOf(j), false) })
	mustAppend(t, j.b, "jj=1")
	nt2.settle(nil, 20*time.Second, func() bool { return settledGet(t, v.b, "jj") == "1" })
	if s := v.b.strandFor(nw.Strand); s == nil || !bytes.Equal(s.key, keyOf(j)) {
		t.Fatal("v does not follow j's strand under j's key")
	}
}

// TestVoidAddMemberNotFollowed: AddMember entries that a plain member writes for fresh keys are void, so no peer follows their
// strands or treats their keys as known peers.
func TestVoidAddMemberNotFollowed(t *testing.T) {
	nt, ps := group(t, 3, GroupConfig{}, appApply, false)
	attacker := ps[2]
	var last NewMember
	for range 80 {
		last = newSigner(t).member()
		body, err := encodeBody(last, attacker.b.groupBounds())
		if err != nil {
			t.Fatal(err)
		}
		attacker.b.mu.Lock()
		_, err = attacker.b.appendLocked(KindAddMember, body)
		attacker.b.mu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
	}
	mustAppend(t, ps[0].b, "after=1")
	nt.settle(nil, 20*time.Second, func() bool { return quiet(ps...) && settledGet(t, ps[1].b, "after") == "1" })
	for _, p := range ps {
		if n := len(p.b.followed()); n != 3 || p.b.sync.known(last.Key) {
			t.Fatalf("%s follows %d strands, fake key known %v", p.name, n, p.b.sync.known(last.Key))
		}
	}
}

// TestKnownFollowsTheRoster: a removed member's key is no longer a known peer once the removal is in the roster, though its
// strand stays followed.
func TestKnownFollowsTheRoster(t *testing.T) {
	nt, ps := group(t, 3, GroupConfig{}, appApply, false)
	gone := ps[2]
	if !ps[0].b.sync.known(keyOf(gone)) {
		t.Fatal("a member is not known")
	}
	if _, err := ps[0].b.RemoveMember(ctx10(t), keyOf(gone)); err != nil {
		t.Fatal(err)
	}
	nt.settle(nil, 20*time.Second, func() bool { return quiet(ps[:2]...) && !hasMember(ps[1].b, keyOf(gone), true) })
	for _, p := range ps[:2] {
		if p.b.strandFor(gone.b.meta.OwnStrand) == nil || p.b.sync.known(keyOf(gone)) {
			t.Fatalf("%s: removed member's strand followed %v, key known %v", p.name, p.b.strandFor(gone.b.meta.OwnStrand) != nil,
				p.b.sync.known(keyOf(gone)))
		}
	}
}

// TestStoredForgedEvidenceIgnored: evidence stored by an earlier run whose key is not the key of the strand it names is not
// loaded at Open.
func TestStoredForgedEvidenceIgnored(t *testing.T) {
	nt, ps := group(t, 3, GroupConfig{}, appApply, false)
	v, attacker := ps[1], ps[2]
	vs := ps[0].b.meta.OwnStrand
	ev := evidenceBody{Key: keyOf(attacker), Strand: vs, Type: evidenceRecords, A: forgeRecord(t, attacker, vs, 1),
		B: forgeRecord(t, attacker, vs, 2)}
	enc, err := encodeBody(ev, v.b.groupBounds())
	if err != nil {
		t.Fatal(err)
	}
	v.close()
	db := PrefixBackend(v.db, nil)
	if err := db.Put(append([]byte(keyEvidence), vs[:]...), append([]byte{1}, enc...)); err != nil {
		t.Fatal(err)
	}
	v.reopen()
	nt.round(nil, 0)
	if v.b.holdsEvidence(vs) {
		t.Fatal("stored forged evidence was loaded")
	}
}

// TestFollowRefusesAnotherKey: a strand that is followed is never followed again under another key.
func TestFollowRefusesAnotherKey(t *testing.T) {
	f := newFounder(t, "f", GroupConfig{}, appApply)
	other := newSigner(t)
	f.b.mu.Lock()
	err := f.b.follow(f.b.meta.FounderStrand, other.log.Header().LogKey, other.header(), nil)
	key := f.b.strands[f.b.meta.FounderStrand].key
	f.b.mu.Unlock()
	if !errors.Is(err, ErrKnownStrand) || !bytes.Equal(key, keyOf(f)) {
		t.Fatalf("follow under another key: %v", err)
	}
}

// TestSettledAddMemberFollowed: an AddMember entry that settles in the same step it is placed, as for a peer that reads a
// whole strand at Open, makes the strand it adds followed.
func TestSettledAddMemberFollowed(t *testing.T) {
	nt, ps := group(t, 3, GroupConfig{}, appApply, false)
	mustAppend(t, ps[1].b, "x=1")
	nt.settle(nil, 10*time.Second, func() bool { return quiet(ps...) && settledGet(t, ps[0].b, "x") == "1" })
	j, _ := newJoiner(t, "j", ps[0], appApply)
	j.start()
	j.b.mu.Lock()
	defer j.b.mu.Unlock()
	if j.b.pivots == 0 || j.b.strands[ps[1].b.meta.OwnStrand] == nil {
		t.Fatalf("after %d settled batches the strand that an AddMember settled there is not followed", j.b.pivots)
	}
}

// TestEvidenceBeforeFollow: an Evidence entry that a peer places in the same read as the AddMember entry of the strand it
// accuses, before the order follows that strand, is checked once the strand is followed.
func TestEvidenceBeforeFollow(t *testing.T) {
	_, ps := group(t, 2, GroupConfig{}, appApply, false)
	f, s := ps[0], ps[1]
	ss := s.b.meta.OwnStrand
	ev := evidenceBody{Key: keyOf(s), Strand: ss, Type: evidenceRecords, A: forgeRecord(t, s, ss, 1), B: forgeRecord(t, s, ss, 2)}
	if err := f.b.addEvidence(ev); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 10*time.Second, func() bool {
		f.b.mu.Lock()
		defer f.b.mu.Unlock()
		h := f.b.evidence[ss]
		return h != nil && h.appended
	})
	mustAppend(t, f.b, "after=1")
	j, nw := newJoiner(t, "j", f, appApply)
	if _, err := f.b.AddMember(ctx10(t), nw); err != nil {
		t.Fatal(err)
	}
	j.start()
	nt := newNet(t, f, s, j)
	nt.settle(nil, 20*time.Second, func() bool { return quiet(f, j) && settledGet(t, j.b, "after") == "1" })
	if !j.b.holdsEvidence(ss) {
		t.Fatal("the joiner dropped the Evidence entry it placed before following the accused strand")
	}
}

// TestSettledKeyReplacesTentativeFollow: a strand followed under the key of an AddMember entry that only a tentative order
// made effective is read under the key of the AddMember entry that settles, after the braid stops and opens again.
func TestSettledKeyReplacesTentativeFollow(t *testing.T) {
	f := newFounder(t, "f", GroupConfig{}, appApply)
	owner := newSigner(t)
	id := owner.log.ID()
	fake := forgeHeader(t, id)
	f.b.mu.Lock()
	if err := f.b.follow(id, fake.Key, fake.Header, nil); err != nil {
		f.b.mu.Unlock()
		t.Fatal(err)
	}
	n := &node{dec: NewMember{Key: owner.log.Header().LogKey, Strand: id, Header: owner.header()}}
	err := f.b.followAdded(n, roster.Result{Change: roster.Added}, nil, false)
	if err != nil {
		f.b.mu.Unlock()
		t.Fatalf("a tentative AddMember of another key: %v", err)
	}
	err = f.b.followAdded(n, roster.Result{Change: roster.Added}, nil, true)
	f.b.mu.Unlock()
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("a settled AddMember of another key: %v", err)
	}
	f.reopen()
	if s := f.b.strandFor(id); s == nil || !bytes.Equal(s.key, owner.log.Header().LogKey) {
		t.Fatal("the strand is not read under the settled key after Open")
	}
}

// TestTentativeStrandNotLinked: a strand that only the tentative order admits is not linked, so no own entry can link entries
// read under a key that settlement may not give it; once the AddMember settles, it is linked and its entries settle.
func TestTentativeStrandNotLinked(t *testing.T) {
	nt, ps := group(t, 3, GroupConfig{}, appApply, false)
	setSettlers(t, nt, ps, ps)
	a := ps[0]
	j, nw := newJoiner(t, "j", a, appApply)
	nt.partition([]int{0}, []int{1, 2})
	if _, err := a.b.AddMember(ctx10(t), nw); err != nil {
		t.Fatal(err)
	}
	j.start()
	aj := newNet(t, a, j)
	aj.settle(nil, 20*time.Second, func() bool { return hasMember(j.b, keyOf(j), false) })
	mustAppend(t, j.b, "jj=1")
	aj.settle(nil, 20*time.Second, func() bool {
		a.b.mu.Lock()
		defer a.b.mu.Unlock()
		s := a.b.strands[nw.Strand]
		return s != nil && s.hasTop
	})
	a.b.mu.Lock()
	settled := a.b.settledKey(nw.Strand, nw.Key)
	links := a.b.links()
	a.b.mu.Unlock()
	if settled || slices.ContainsFunc(links, func(l Link) bool { return l.Strand == nw.Strand }) {
		t.Fatalf("a strand admitted only tentatively is linked (settled %v)", settled)
	}
	all := newNet(t, append(ps, j)...)
	all.settle(nil, 30*time.Second, func() bool { return settledGet(t, ps[1].b, "jj") == "1" })
}

// TestEarlyEvidenceByKey: early evidence is kept per key, so a self-consistent Evidence entry for the same strand under
// another key does not push out the evidence against the strand's real key; it is checked once the strand is followed.
func TestEarlyEvidenceByKey(t *testing.T) {
	_, ps := group(t, 2, GroupConfig{}, appApply, false)
	f, attacker := ps[0], ps[1]
	j, nw := newJoiner(t, "j", f, appApply)
	honest := evidenceBody{Key: keyOf(j), Strand: nw.Strand, Type: evidenceRecords, A: forgeRecord(t, j, nw.Strand, 1),
		B: forgeRecord(t, j, nw.Strand, 2)}
	forged := evidenceBody{Key: keyOf(attacker), Strand: nw.Strand, Type: evidenceRecords,
		A: forgeRecord(t, attacker, nw.Strand, 1), B: forgeRecord(t, attacker, nw.Strand, 2)}
	f.b.mu.Lock()
	defer f.b.mu.Unlock()
	f.b.holdEvidence(honest, nil, true)
	f.b.holdEvidence(forged, nil, true)
	if err := f.b.follow(nw.Strand, nw.Key, nw.Header, nil); err != nil {
		t.Fatal(err)
	}
	if f.b.evidence[nw.Strand] == nil {
		t.Fatal("evidence for another key pushed out the early evidence against the strand's key")
	}
}

// TestEarlyEvidenceLeaves: early evidence against a strand that no AddMember entry still to settle names is dropped with the
// next settled batch, so random strand IDs cannot fill the bound for good.
func TestEarlyEvidenceLeaves(t *testing.T) {
	f := newFounder(t, "f", GroupConfig{}, appApply)
	other := newSigner(t).log.ID()
	f.b.mu.Lock()
	f.b.holdEvidence(evidenceBody{Key: keyOf(f), Strand: other, Type: evidenceRecords, A: forgeRecord(t, f, other, 1),
		B: forgeRecord(t, f, other, 2)}, nil, true)
	kept := len(f.b.early)
	f.b.mu.Unlock()
	mustAppend(t, f.b, "a=1")
	waitUntil(t, 10*time.Second, func() bool { return quiet(f) && settledGet(t, f.b, "a") == "1" })
	f.b.mu.Lock()
	left := len(f.b.early)
	f.b.mu.Unlock()
	if kept != 1 || left != 0 {
		t.Fatalf("early evidence: %d kept, %d left after a settled batch", kept, left)
	}
}

// TestPlainMemberWritesEvidence: a member that is no seat appends an Evidence entry against a writer that signs two
// histories, also after more than pulseSpan entries of its own.
func TestPlainMemberWritesEvidence(t *testing.T) {
	for _, writes := range []int{3, pulseSpan + 8} {
		t.Run(fmt.Sprint(writes), func(t *testing.T) {
			nt, ps := group(t, 3, GroupConfig{}, appApply, false)
			m, w := ps[1], ps[2]
			mustAppend(t, w.b, "base=1")
			for i := range writes {
				mustAppend(t, m.b, fmt.Sprintf("m%d=1", i))
			}
			nt.settle(nil, 20*time.Second, func() bool { return quiet(ps...) })
			ws := w.b.meta.OwnStrand
			h1, h2 := fork(t, w, []*peer{m}, nil)
			deliverAll(t, []*peer{m}, h1, h2)
			waitUntil(t, 10*time.Second, func() bool {
				m.b.mu.Lock()
				defer m.b.mu.Unlock()
				h := m.b.evidence[ws]
				return h != nil && h.appended
			})
		})
	}
}
