package kolobraid

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	koloid "github.com/ciranda-dev/kolo-id"
	kololog "github.com/ciranda-dev/kolo-log"
	kolostore "github.com/ciranda-dev/kolo-store"
)

// fastLimits are the smallest pacing values the ranges allow, so tests settle in milliseconds.
func fastLimits() *Limits {
	return &Limits{MaxTail: 64, PulseGap: 10 * time.Millisecond, PivotWait: 100 * time.Millisecond, ConfirmWait: time.Second,
		AnnounceGap: 10 * time.Millisecond}
}

// appApply is a small application: a data body "k=v" puts v under k, "-k" deletes k, "!" is rejected, "panic" panics and
// "fail" returns an error; every effective data entry also bumps the counter "#".
func appApply(tx *Tx, e *Entry) error {
	if e.Kind != KindData {
		return nil
	}
	s := string(e.Body)
	switch {
	case s == "!":
		return Reject("refused")
	case s == "panic":
		panic("apply panicked on purpose")
	case s == "fail":
		return errors.New("apply failed on purpose")
	case strings.HasPrefix(s, "-"):
		if err := tx.Delete([]byte(s[1:])); err != nil {
			return err
		}
	default:
		k, v, ok := strings.Cut(s, "=")
		if !ok {
			k, v = s, ""
		}
		if err := tx.Put([]byte(k), []byte(v)); err != nil {
			return err
		}
	}
	n := 0
	if v, err := tx.Get([]byte("#")); err == nil {
		fmt.Sscan(string(v), &n)
	}
	return tx.Put([]byte("#"), []byte(fmt.Sprint(n+1)))
}

// peer is one in-process participant: its identity, its backend and its open braid.
type peer struct {
	t    testing.TB
	name string
	id   *koloid.Identity
	db   kolostore.Backend
	opts Options
	b    *Braid
}

func newIdentity(t testing.TB) *koloid.Identity {
	t.Helper()
	id, err := koloid.Generate()
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func testOptions(db kolostore.Backend, apply ApplyFunc) Options {
	return Options{Backend: db, App: "test/v1", Apply: apply, Limits: fastLimits()}
}

func newFounder(t testing.TB, name string, cfg GroupConfig, apply ApplyFunc) *peer {
	t.Helper()
	if cfg.App == "" {
		cfg.App = "test/v1"
	}
	p := &peer{t: t, name: name, id: newIdentity(t), db: kolostore.NewMemory()}
	p.opts = testOptions(p.db, apply)
	b, err := Create(context.Background(), p.id, cfg, p.opts)
	if err != nil {
		t.Fatal(err)
	}
	p.b = b
	t.Cleanup(func() { p.close() })
	return p
}

func (p *peer) close() {
	if p.b != nil {
		p.b.Close()
		p.b = nil
	}
}

// reopen closes the braid and opens it again from the same backend.
func (p *peer) reopen() {
	p.t.Helper()
	p.close()
	b, err := Open(context.Background(), p.id, p.opts)
	if err != nil {
		p.t.Fatal(err)
	}
	p.b = b
}

// newJoiner makes a peer that follows the founder's braid: its own strand is a fresh log, the founder strand a replica. It is
// what admission (kolo-invite helpers) will do; here the founder's AddMember is written by the test.
func newJoiner(t testing.TB, name string, founder *peer, apply ApplyFunc) (*peer, NewMember) {
	t.Helper()
	p, nw := joiner(t, name, founder, apply, ^uint64(0))
	t.Cleanup(func() { p.close() })
	return p, nw
}

// joiner is newJoiner without the cleanup, with the first held entries of the founder strand copied in: the caller closes the
// peer, and nothing keeps it reachable afterwards.
func joiner(t testing.TB, name string, founder *peer, apply ApplyFunc, held uint64) (*peer, NewMember) {
	t.Helper()
	p := &peer{t: t, name: name, id: newIdentity(t), db: kolostore.NewMemory()}
	p.opts = testOptions(p.db, apply)
	db := PrefixBackend(p.db, nil)
	st := kolostore.New(PrefixBackend(db, append([]byte{storePrefix}, p.id.PublicKey()...)))
	l, err := st.Create(p.id, kololog.WithValueEncoding(kololog.Bytes))
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	nw := NewMember{Key: p.id.PublicKey(), Strand: l.ID(), Header: l.Header().Encode()}
	fb := founder.b
	m := meta{Group: fb.meta.Group, FounderKey: fb.meta.FounderKey, FounderStrand: fb.meta.FounderStrand,
		FounderHeader: fb.meta.FounderHeader, OwnStrand: l.ID()}
	// the founder strand is copied in before Open, as a joiner pulls it first
	rep, err := kolostore.New(PrefixBackend(db, append([]byte{storePrefix}, m.FounderKey...))).OpenReplica(m.FounderKey,
		m.FounderStrand, m.FounderHeader)
	if err != nil {
		t.Fatal(err)
	}
	defer rep.Close()
	transfer(t, fb.strandFor(m.FounderStrand), rep, held)
	bt := db.NewBatch()
	bt.Put([]byte(keyMeta), m.encode())
	id := l.ID()
	bt.Put(append([]byte(keyStrand), id[:]...), strandRec{Key: nw.Key, Header: nw.Header}.encode())
	if err := bt.Commit(); err != nil {
		t.Fatal(err)
	}
	return p, nw
}

// start opens a joiner made by newJoiner; its strand has no entry, so nothing can be newer elsewhere.
func (p *peer) start() {
	p.t.Helper()
	b, err := open(context.Background(), p.id, p.opts, true)
	if err != nil {
		p.t.Fatal(err)
	}
	p.b = b
}

func (b *Braid) strandFor(id kololog.LogID) *strand {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.strands[id]
}

func (b *Braid) followed() []kololog.LogID {
	b.mu.Lock()
	defer b.mu.Unlock()
	ids := make([]kololog.LogID, 0, len(b.strands))
	for id := range b.strands {
		ids = append(ids, id)
	}
	slices.SortFunc(ids, func(a, c kololog.LogID) int { return bytes.Compare(a[:], c[:]) })
	return ids
}

func headOf(s *strand) (kololog.Head, error) {
	if s.log != nil {
		return s.log.Head()
	}
	return s.rep.Head()
}

func entryOf(s *strand, i uint64) (kololog.Entry, error) {
	if s.log != nil {
		return s.log.Entry(i)
	}
	return s.rep.Entry(i)
}

func proofOf(s *strand, a, c uint64) (kololog.ConsistencyProof, error) {
	if s.log != nil {
		return s.log.ConsistencyProof(a, c)
	}
	return s.rep.ConsistencyProof(a, c)
}

// transfer grows dst with src's entries up to upto (at most src's length), as a pull would.
func transfer(t testing.TB, src *strand, dst *kolostore.Replica, upto uint64) bool {
	t.Helper()
	h, err := headOf(src)
	if err != nil {
		return false
	}
	n, size := dst.Len(), h.Size
	if th, _, ok := dst.Target(); ok {
		// a chunk toward the pending head may stop short of it; one toward a longer head must first reach it
		if size < th.Size {
			return false
		}
		if size > th.Size {
			upto = max(upto, th.Size)
		}
	}
	m := min(size, upto)
	if m <= n {
		return false
	}
	var es []kololog.Entry
	for i := n; i < m; i++ {
		e, err := entryOf(src, i)
		if err != nil {
			t.Fatal(err)
		}
		es = append(es, e)
	}
	var p kololog.ConsistencyProof
	switch {
	case m < size:
		p, err = proofOf(src, m, size)
	case n > 0:
		p, err = proofOf(src, n, size)
	}
	if err != nil {
		t.Fatal(err)
	}
	if err := dst.Update(h, p, es); err != nil {
		if errors.Is(err, kololog.ErrDiverged) || errors.Is(err, kololog.ErrRollback) || errors.Is(err, kololog.ErrBadProof) || errors.Is(err, errCrash) {
			return false
		}
		t.Fatalf("update: %v", err)
	}
	return true
}

// pull copies into to every strand it follows that from holds further, and shows to the heads of its own strand that from
// holds. max limits how many entries of each strand one call moves; 0 means all.
func pull(t testing.TB, from, to *peer, max uint64) bool {
	t.Helper()
	if from.b == nil || to.b == nil {
		return false
	}
	moved := false
	for _, id := range to.b.followed() {
		src := from.b.strandFor(id)
		if src == nil {
			continue
		}
		if id == to.b.meta.OwnStrand {
			if h, err := headOf(src); err == nil && h.Size > 0 {
				to.b.ownHead(from.id.PublicKey(), h)
			}
			continue
		}
		dst := to.b.replicaOf(id)
		if dst == nil {
			continue
		}
		upto := ^uint64(0)
		if max > 0 {
			upto = dst.Len() + max
		}
		if transfer(t, src, dst, upto) {
			moved = true
			if err := to.b.strandGrew(id); err != nil && !errors.Is(err, ErrClosed) && !errors.Is(err, errCrash) {
				t.Fatalf("%s: strand grew: %v", to.name, err)
			}
		}
	}
	return moved
}

// net is a set of peers with links between some of them.
type net struct {
	t     testing.TB
	peers []*peer
	cut   map[[2]int]bool
	mu    sync.Mutex
}

func newNet(t testing.TB, peers ...*peer) *net {
	return &net{t: t, peers: peers, cut: map[[2]int]bool{}}
}

// partition cuts every link between the two groups.
func (n *net) partition(a, c []int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, i := range a {
		for _, j := range c {
			n.cut[[2]int{i, j}], n.cut[[2]int{j, i}] = true, true
		}
	}
}

func (n *net) heal() {
	n.mu.Lock()
	defer n.mu.Unlock()
	clear(n.cut)
}

// round moves entries over every live link once, in an order rng chooses; max as in pull.
func (n *net) round(rng *rand.Rand, max uint64) bool {
	n.mu.Lock()
	var pairs [][2]int
	for i := range n.peers {
		for j := range n.peers {
			if i != j && !n.cut[[2]int{i, j}] {
				pairs = append(pairs, [2]int{i, j})
			}
		}
	}
	n.mu.Unlock()
	if rng != nil {
		rng.Shuffle(len(pairs), func(a, c int) { pairs[a], pairs[c] = pairs[c], pairs[a] })
	}
	moved := false
	for _, p := range pairs {
		if pull(n.t, n.peers[p[0]], n.peers[p[1]], max) {
			moved = true
		}
	}
	return moved
}

// settle syncs until every live peer has settled cond's view and nothing moves; it fails after timeout.
func (n *net) settle(rng *rand.Rand, timeout time.Duration, done func() bool) {
	n.t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		n.round(rng, 0)
		if done() {
			return
		}
		if time.Now().After(deadline) {
			for _, p := range n.peers {
				if p.b != nil {
					st := p.b.Status()
					n.t.Logf("%s: %+v", p.name, st)
				}
			}
			n.t.Fatal("peers did not converge in time")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// quiet reports whether every peer has settled every entry it holds but pulses, and they agree on the settled digest.
func quiet(peers ...*peer) bool {
	var d *Digest
	var next uint64
	for _, p := range peers {
		if p.b == nil {
			continue
		}
		p.b.mu.Lock()
		w, jobs := 0, len(p.b.jobs)
		if p.b.busy || (p.b.plan != nil && p.b.plan.gen != p.b.appliedGen && p.b.viewHalt == nil) {
			jobs++
		}
		if _, _, seat := p.b.mySeat(); seat && p.b.hasWork(p.b.pastOf(p.b.links())) {
			w = 1
		}
		p.b.mu.Unlock()
		pr := p.b.view.Progress()
		if w != 0 || jobs != 0 {
			return false
		}
		if d == nil {
			d, next = &pr.Digest, pr.Next
		} else if *d != pr.Digest || next != pr.Next {
			return false
		}
	}
	return true
}

// dump returns the settled view of a peer as one string.
func dump(t testing.TB, b *Braid) string {
	t.Helper()
	s := b.Settled()
	defer s.Close()
	var sb strings.Builder
	for k, v := range s.Scan(nil, nil) {
		fmt.Fprintf(&sb, "%q=%q;", k, v)
	}
	return sb.String()
}

func get(t testing.TB, s *Snapshot, k string) string {
	t.Helper()
	v, err := s.Get([]byte(k))
	if errors.Is(err, ErrNotFound) {
		return "<none>"
	}
	if err != nil {
		t.Fatal(err)
	}
	return string(v)
}

func settledGet(t testing.TB, b *Braid, k string) string {
	t.Helper()
	s := b.Settled()
	defer s.Close()
	return get(t, s, k)
}

func waitUntil(t testing.TB, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not reached in time")
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func mustAppend(t testing.TB, b *Braid, body string) EntryRef {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ref, err := b.Append(ctx, []byte(body))
	if err != nil {
		t.Fatalf("append %q: %v", body, err)
	}
	return ref
}

func ctx10(t testing.TB) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func keyOf(p *peer) ed25519.PublicKey { return p.id.PublicKey() }

// confirm syncs until p's own strand is confirmed by the heads its peers hold.
func (n *net) confirm(p *peer) {
	n.t.Helper()
	n.settle(nil, 10*time.Second, func() bool { return !p.b.Status().Unconfirmed })
}
