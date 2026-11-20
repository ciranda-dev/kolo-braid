package kolobraid

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"log/slog"
	"net/netip"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	kolocodec "github.com/ciranda-dev/kolo-codec"
	kolodht "github.com/ciranda-dev/kolo-dht"
	koloid "github.com/ciranda-dev/kolo-id"
	invite "github.com/ciranda-dev/kolo-invite"
	kololog "github.com/ciranda-dev/kolo-log"
	kolosecure "github.com/ciranda-dev/kolo-secure"
	kolostore "github.com/ciranda-dev/kolo-store"
	koloswarm "github.com/ciranda-dev/kolo-swarm"

	"sync/atomic"

	"github.com/ciranda-dev/kolo-braid/internal/dag"
)

// invitePeer is a kolo-secure connection as kolo-invite's ServePeer takes it.
type invitePeer struct{ c *kolosecure.Conn }

func (p invitePeer) Conn() koloswarm.Conn         { return p.c }
func (p invitePeer) PublicKey() ed25519.PublicKey { return p.c.RemotePublicKey() }
func (p invitePeer) Topics() []koloswarm.Topic    { return nil }
func (p invitePeer) Addr() netip.AddrPort {
	a, _ := netip.ParseAddrPort(p.c.RemoteAddr().String())
	return a
}

// decision is what the host did with one Requested event.
type decision struct {
	joiner                 ed25519.PublicKey
	welcome                []byte
	admit, approve, remove error
}

// host is an admin's application: a kolo-invite Inviter on its own listener on 127.0.0.1 that answers every Requested event, each
// on its own goroutine, with Admit then Approve, Deny when Admit refuses, and RemoveMember when Approve fails.
type host struct {
	t   testing.TB
	p   *peer
	in  *invite.Inviter
	ln  *kolosecure.Listener
	ctx context.Context

	mu            sync.Mutex
	opts          *AdmitOptions
	gate          chan struct{}
	beforeApprove func()
	done          []decision
}

func (d decision) String() string {
	return fmt.Sprintf("admit %v, approve %v, remove %v", d.admit, d.approve, d.remove)
}

func newHost(t testing.TB, p *peer, opts *invite.Options) *host {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	db := kolostore.NewMemory()
	in, err := invite.Open(db, p.id, opts)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := kolosecure.Listen(ctx, "127.0.0.1:0", p.id, nil)
	if err != nil {
		t.Fatal(err)
	}
	h := &host{t: t, p: p, in: in, ln: ln, ctx: ctx}
	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			c, err := ln.Accept(ctx)
			if err != nil {
				return
			}
			wg.Go(func() {
				in.ServePeer(ctx, invitePeer{c})
				c.Close()
			})
		}
	})
	wg.Go(func() {
		for {
			ev, err := in.Next(ctx)
			if err != nil {
				return
			}
			if ev.Kind != invite.Requested {
				continue
			}
			h.mu.Lock()
			gate := h.gate
			h.mu.Unlock()
			wg.Go(func() {
				if gate != nil {
					select {
					case <-gate:
					case <-ctx.Done():
						return
					}
				}
				h.decide(ev)
			})
		}
	})
	t.Cleanup(func() {
		cancel()
		ln.Close()
		in.Close()
		wg.Wait()
		db.Close()
	})
	return h
}

func (h *host) decide(ev invite.Event) {
	h.mu.Lock()
	opts, before := h.opts, h.beforeApprove
	h.mu.Unlock()
	if opts == nil {
		opts = growOpts
	}
	d := decision{joiner: ev.Joiner}
	d.welcome, d.admit = h.p.b.Admit(h.ctx, ev.Joiner, ev.Data, opts)
	if d.admit != nil {
		h.in.Deny(ev.Request)
	} else {
		if before != nil {
			before()
		}
		if d.approve = h.in.Approve(ev.Request, d.welcome); d.approve != nil {
			_, d.remove = h.p.b.RemoveMember(h.ctx, ev.Joiner)
		}
	}
	h.mu.Lock()
	h.done = append(h.done, d)
	h.mu.Unlock()
}

func (h *host) set(f func(h *host)) {
	h.mu.Lock()
	defer h.mu.Unlock()
	f(h)
}

func (h *host) decisions() []decision {
	h.mu.Lock()
	defer h.mu.Unlock()
	return slices.Clone(h.done)
}

func (h *host) waitDecision(n int) decision {
	h.t.Helper()
	waitUntil(h.t, 20*time.Second, func() bool { return len(h.decisions()) >= n })
	return h.decisions()[n-1]
}

func (h *host) create(o *invite.CreateOptions) (string, invite.ID) {
	h.t.Helper()
	code, info, err := h.in.Create(o)
	if err != nil {
		h.t.Fatal(err)
	}
	return code, info.ID
}

// redeem asks the host, over a new kolo-secure connection on 127.0.0.1, to let id join with data, and returns the payload.
func (h *host) redeem(id *koloid.Identity, code string, data []byte, maxPayload int) ([]byte, error) {
	c, err := invite.Parse(code)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	conn, err := kolosecure.Dial(ctx, h.ln.Addr().String(), id, h.p.id.PublicKey(), nil)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return invite.JoinConn(ctx, conn, id, c, &invite.JoinOptions{Data: data, MaxPayload: maxPayload})
}

// candidate is a device that wants to join: its identity, backend and new strand.
type candidate struct {
	p   *peer
	j   *Joining
	req []byte
}

func newCandidate(t testing.TB, name string, id *koloid.Identity) *candidate {
	t.Helper()
	if id == nil {
		id = newIdentity(t)
	}
	p := &peer{t: t, name: name, id: id, db: kolostore.NewMemory()}
	p.opts = testOptions(p.db, appApply)
	j, err := NewJoin(ctx10(t), p.id, p.opts)
	if err != nil {
		t.Fatal(err)
	}
	req, err := j.Request([]byte(name))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.close)
	return &candidate{p: p, j: j, req: req}
}

func (c *candidate) complete(t testing.TB, welcome []byte) *peer {
	t.Helper()
	b, err := c.j.Complete(ctx10(t), welcome)
	if err != nil {
		t.Fatal(err)
	}
	c.p.b = b
	return c.p
}

// inviteJoin redeems code at h for a new device and completes its join; the new member is not linked to anyone yet.
func inviteJoin(t testing.TB, h *host, code, name string) *peer {
	t.Helper()
	c := newCandidate(t, name, nil)
	w, err := h.redeem(c.p.id, code, c.req, 0)
	if err != nil {
		t.Fatal(err)
	}
	return c.complete(t, w)
}

func ownLen(p *peer) uint64 {
	p.b.mu.Lock()
	defer p.b.mu.Unlock()
	return p.b.own.Len()
}

func rolesOf(t testing.TB, b *Braid, k ed25519.PublicKey) (Role, bool) {
	s := b.Settled()
	defer s.Close()
	for _, m := range s.Members() {
		if bytes.Equal(m.Key, k) {
			return m.Roles, true
		}
	}
	return 0, false
}

// TestInviteThreeMembers: three devices join one braid through one manual kolo-invite invite over kolo-secure on 127.0.0.1. The
// default roles make the first two joiners admins and grow the seats to three and then four; the invite is used up after
// three; a member removed afterwards gets nothing more and what it writes is void, and the others keep converging.
func TestInviteThreeMembers(t *testing.T) {
	m := newMesh(t)
	f := newFounder(t, "f", GroupConfig{}, appApply)
	h := newHost(t, f, nil)
	code, id := h.create(&invite.CreateOptions{Manual: true, MaxUses: 3, TTL: time.Hour})
	ps := []*peer{f}
	for i := 1; i <= 3; i++ {
		p := inviteJoin(t, h, code, fmt.Sprintf("j%d", i))
		ps = append(ps, p)
		m.full(ps...)
		waitNet(t, 30*time.Second, ps, func() bool { return synced(ps...) && hasMember(p.b, keyOf(p), true) })
		if d := h.waitDecision(i); d.admit != nil || d.approve != nil {
			t.Fatalf("decision %d: %+v", i, d)
		}
		if i == 2 {
			seats(t, ps, 3)
			if q := f.b.Status().Quorum; q != 2 {
				t.Fatalf("quorum %d with three seats, want Majority's 2", q)
			}
		}
	}
	for i, d := range h.decisions() {
		w, err := parseWelcome(d.welcome)
		if err != nil {
			t.Fatal(err)
		}
		if i > 0 && w.Anchor == nil {
			t.Fatalf("welcome %d carries no anchor although the inviter had settled", i+1)
		}
		if !bytes.Equal(w.InviterKey, keyOf(f)) || w.Group != f.b.GroupID() {
			t.Fatalf("welcome %d names another inviter or group", i+1)
		}
	}
	joined, err := h.in.Joiners(id)
	if err != nil || len(joined) != 3 {
		t.Fatalf("kolo-invite joiners %d, %v", len(joined), err)
	}
	var sent []string
	for _, jn := range joined {
		r, err := ParseJoinRequest(jn.Data)
		if err != nil {
			t.Fatal(err)
		}
		sent = append(sent, string(r.AppData))
	}
	if slices.Sort(sent); !slices.Equal(sent, []string{"j1", "j2", "j3"}) {
		t.Fatalf("the joiners sent %q", sent)
	}
	for i, p := range ps[1:] {
		r, ok := rolesOf(t, f.b, keyOf(p))
		if !ok || (r&RoleAdmin != 0) != (i < 2) {
			t.Fatalf("%s: roles %d, member %v; the first two joiners are admins", p.name, r, ok)
		}
	}
	fourSeats(t, ps)

	before := ownLen(f)
	late := newCandidate(t, "late", nil)
	if _, err := h.redeem(late.p.id, code, late.req, 0); !errors.Is(err, invite.ErrUsedUp) {
		t.Fatalf("fourth redeem: %v", err)
	}
	if ownLen(f) != before || len(h.decisions()) != 3 {
		t.Fatal("a used-up invite appended an entry")
	}

	for _, p := range ps {
		mustAppend(t, p.b, p.name+"=1")
	}
	waitNet(t, 30*time.Second, ps, func() bool {
		if !synced(ps...) {
			return false
		}
		for _, p := range ps {
			for _, q := range ps {
				if settledGet(t, p.b, q.name) != "1" {
					return false
				}
			}
		}
		return true
	})
	checkSame(t, ps)

	gone, stay := ps[3], ps[:3]
	if _, err := f.b.RemoveMember(ctx10(t), keyOf(gone)); err != nil {
		t.Fatal(err)
	}
	// quiet, not synced: the removed seat's last entries may reach one peer and not the other, as nothing links them
	waitNet(t, 30*time.Second, stay, func() bool {
		return quiet(stay...) && !hasMember(ps[1].b, keyOf(gone), true) && ps[1].b.Status().Seats == 3
	})
	for _, p := range stay {
		if p.b.access(keyOf(gone)) != accessNone {
			t.Fatalf("%s still serves the removed member", p.name)
		}
	}
	if _, err := gone.b.Append(ctx10(t), []byte("after=1")); err != nil && !errors.Is(err, ErrRemoved) {
		t.Fatal(err)
	}
	mustAppend(t, ps[2].b, "later=1")
	waitNet(t, 30*time.Second, stay, func() bool { return quiet(stay...) && settledGet(t, ps[1].b, "later") == "1" })
	for _, p := range stay {
		if settledGet(t, p.b, "after") != "<none>" {
			t.Fatalf("%s applied an entry the removed member wrote", p.name)
		}
	}
	checkSame(t, stay)
}

// seats appends until n seats are active on every peer.
func seats(t testing.TB, ps []*peer, n int) {
	t.Helper()
	for range 20 {
		if !slices.ContainsFunc(ps, func(p *peer) bool { return p.b.Status().Seats != n }) {
			return
		}
		mustAppend(t, ps[0].b, "grow=1")
		waitNet(t, 30*time.Second, ps, func() bool { return synced(ps...) })
	}
	t.Fatalf("%d seats did not activate", n)
}

// TestInviteDenyAppendsNothing: a request that Admit refuses (a key that is already a member, a known strand) is denied, the
// joiner gets ErrDenied, and nothing is appended.
func TestInviteDenyAppendsNothing(t *testing.T) {
	m := newMesh(t)
	f := newFounder(t, "f", GroupConfig{}, appApply)
	h := newHost(t, f, nil)
	code, _ := h.create(&invite.CreateOptions{Manual: true})
	j := inviteJoin(t, h, code, "j")
	m.connect(j, f)
	waitNet(t, 20*time.Second, []*peer{f, j}, func() bool { return synced(f, j) && hasMember(f.b, keyOf(j), true) })
	before := ownLen(f)
	j.b.mu.Lock()
	active, _ := encodeJoinRequest(JoinRequest{Strand: j.b.meta.OwnStrand, Header: j.b.own.Header().Encode()})
	j.b.mu.Unlock()
	for i, tc := range []struct {
		name string
		id   *koloid.Identity
		req  []byte
		opts *AdmitOptions
		want error
	}{
		{"the member's new device", j.id, newCandidate(t, "j2", j.id).req, nil, ErrActiveKey},
		{"the member's active strand with Replace", j.id, active, &AdmitOptions{Replace: true}, ErrKnownStrand},
		{"a request under another key", newIdentity(t), active, nil, ErrInvalidArgument},
	} {
		code, _ := h.create(&invite.CreateOptions{Manual: true})
		h.set(func(h *host) { h.opts = tc.opts })
		if _, err := h.redeem(tc.id, code, tc.req, 0); !errors.Is(err, invite.ErrDenied) {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if d := h.waitDecision(i + 2); !errors.Is(d.admit, tc.want) {
			t.Fatalf("%s: decision %+v", tc.name, d)
		}
	}
	if ownLen(f) != before {
		t.Fatal("a denied request appended an entry")
	}
	if s := h.in.Stats(); s.Denied != 3 || s.Accepted != 1 {
		t.Fatalf("kolo-invite stats %+v", s)
	}
}

// TestInviteReplace: a member's new device, with the same key and a new strand, joins through an invite with
// AdmitOptions.Replace; its strand replaces the old one and keeps the roles.
func TestInviteReplace(t *testing.T) {
	m := newMesh(t)
	f := newFounder(t, "f", GroupConfig{}, appApply)
	h := newHost(t, f, nil)
	code, _ := h.create(&invite.CreateOptions{Manual: true})
	j := inviteJoin(t, h, code, "j")
	m.connect(j, f)
	waitNet(t, 20*time.Second, []*peer{f, j}, func() bool { return synced(f, j) && hasMember(f.b, keyOf(j), true) })
	roles, _ := rolesOf(t, f.b, keyOf(j))
	h.set(func(h *host) { h.opts = &AdmitOptions{Replace: true} })
	c := newCandidate(t, "j-new", j.id)
	code, _ = h.create(&invite.CreateOptions{Manual: true})
	w, err := h.redeem(j.id, code, c.req, 0)
	if err != nil {
		t.Fatal(err)
	}
	nj := c.complete(t, w)
	m.isolate(j)
	m.connect(nj, f)
	waitNet(t, 20*time.Second, []*peer{f, nj}, func() bool { return synced(f, nj) && hasMember(nj.b, keyOf(nj), true) })
	f.b.mu.Lock()
	mw, ok := f.b.tent.Member(rosterKey(keyOf(nj)))
	f.b.mu.Unlock()
	nj.b.mu.Lock()
	own := nj.b.meta.OwnStrand
	nj.b.mu.Unlock()
	if !ok || mw.Strand != [32]byte(own) {
		t.Fatal("the new device's strand is not the member's active strand")
	}
	if r, _ := rolesOf(t, f.b, keyOf(nj)); r != roles {
		t.Fatalf("roles %d after the replacement, %d before", r, roles)
	}
	mustAppend(t, nj.b, "device=new")
	waitNet(t, 20*time.Second, []*peer{f, nj}, func() bool { return synced(f, nj) && settledGet(t, f.b, "device") == "new" })
	checkSame(t, []*peer{f, nj})
}

// TestInviteApproveFails: when Approve fails after Admit appended the AddMember entry (the invite expired, was revoked or was
// used up meanwhile), the host removes the member again, the joiner gets the invite's error and never a welcome, every peer
// settles the removal, and the strand cannot be admitted again.
func TestInviteApproveFails(t *testing.T) {
	for _, tc := range []struct {
		name string
		want error
	}{{"expired", invite.ErrExpired}, {"revoked", invite.ErrRevoked}, {"usedup", invite.ErrUsedUp}} {
		t.Run(tc.name, func(t *testing.T) {
			m := newMesh(t)
			f := newFounder(t, "f", GroupConfig{}, appApply)
			w := m.join(f, "w", nil)
			m.connect(w, f)
			waitNet(t, 20*time.Second, []*peer{f, w}, func() bool { return synced(f, w) && hasMember(w.b, keyOf(w), true) })
			h := newHost(t, f, nil)
			c := newCandidate(t, "j", nil)
			var code string
			var id invite.ID
			switch tc.name {
			case "expired":
				code, _ = h.create(&invite.CreateOptions{Manual: true, TTL: 300 * time.Millisecond})
				h.set(func(h *host) { h.beforeApprove = func() { time.Sleep(500 * time.Millisecond) } })
			case "revoked":
				code, id = h.create(&invite.CreateOptions{Manual: true})
				h.set(func(h *host) { h.beforeApprove = func() { h.in.Revoke(id) } })
			case "usedup":
				code, _ = h.create(&invite.CreateOptions{Manual: true, MaxUses: 1})
				gate := make(chan struct{})
				h.set(func(h *host) { h.gate = gate })
				// two requests wait for a decision; the first approval uses the invite up, so the other Approve fails
				first := newCandidate(t, "first", nil)
				errs := make(chan error, 2)
				for _, x := range []*candidate{first, c} {
					go func() {
						_, err := h.redeem(x.p.id, code, x.req, 0)
						errs <- err
					}()
				}
				waitUntil(t, 10*time.Second, func() bool { return h.in.Stats().Pending == 2 })
				time.Sleep(50 * time.Millisecond)
				gate <- struct{}{}
				h.waitDecision(1)
				gate <- struct{}{}
				h.waitDecision(2)
				var ok, failed decision
				for _, d := range h.decisions() {
					if d.approve == nil {
						ok = d
					} else {
						failed = d
					}
				}
				if ok.admit != nil || failed.admit != nil || !errors.Is(failed.approve, tc.want) || failed.remove != nil {
					t.Fatalf("decisions %+v", h.decisions())
				}
				if e1, e2 := <-errs, <-errs; (e1 == nil) == (e2 == nil) || !(errors.Is(e1, tc.want) || errors.Is(e2, tc.want)) {
					t.Fatalf("joiners: %v, %v", e1, e2)
				}
				if bytes.Equal(failed.joiner, keyOf(first.p)) {
					c = first
				}
				checkRemovedAgain(t, m, f, w, h, c)
				return
			}
			if _, err := h.redeem(c.p.id, code, c.req, 0); !errors.Is(err, tc.want) {
				t.Fatalf("joiner: %v", err)
			}
			if d := h.waitDecision(1); d.admit != nil || !errors.Is(d.approve, tc.want) || d.remove != nil {
				t.Fatalf("decision %+v", d)
			}
			checkRemovedAgain(t, m, f, w, h, c)
		})
	}
}

// checkRemovedAgain checks that the joiner c, admitted and removed again, is not a member on f and w once settled, and that its
// strand is refused by Admit while the key itself can still join with a new strand.
func checkRemovedAgain(t *testing.T, m *mesh, f, w *peer, h *host, c *candidate) {
	t.Helper()
	ps := []*peer{f, w}
	waitNet(t, 30*time.Second, ps, func() bool {
		if !synced(ps...) {
			return false
		}
		for _, p := range ps {
			p.b.mu.Lock()
			_, used := p.b.settled.Used(dag.StrandID(c.j.strand))
			p.b.mu.Unlock()
			if !used || hasMember(p.b, keyOf(c.p), true) || hasMember(p.b, keyOf(c.p), false) {
				return false
			}
		}
		return true
	})
	checkSame(t, ps)
	if _, err := f.b.Admit(ctx10(t), keyOf(c.p), c.req, growOpts); !errors.Is(err, ErrKnownStrand) {
		t.Fatalf("the removed strand again: %v", err)
	}
	again := newCandidate(t, "again", c.p.id)
	code, _ := h.create(&invite.CreateOptions{Manual: true})
	h.set(func(h *host) { h.beforeApprove, h.gate = nil, nil })
	wl, err := h.redeem(again.p.id, code, again.req, 0)
	if err != nil {
		t.Fatal(err)
	}
	p := again.complete(t, wl)
	m.full(f, w, p)
	all := []*peer{f, w, p}
	waitNet(t, 30*time.Second, all, func() bool { return synced(all...) && hasMember(f.b, keyOf(p), true) })
}

// TestInviteReplay: asking again with an accepted invite returns the same welcome and appends nothing; a completed join refuses
// a second Complete; a join request replayed under another key is refused; the request of a removed member is refused.
func TestInviteReplay(t *testing.T) {
	m := newMesh(t)
	f := newFounder(t, "f", GroupConfig{}, appApply)
	h := newHost(t, f, nil)
	code, _ := h.create(&invite.CreateOptions{Manual: true, MaxUses: 5})
	c := newCandidate(t, "j", nil)
	w1, err := h.redeem(c.p.id, code, c.req, 0)
	if err != nil {
		t.Fatal(err)
	}
	j := c.complete(t, w1)
	m.connect(j, f)
	waitNet(t, 20*time.Second, []*peer{f, j}, func() bool { return synced(f, j) && hasMember(f.b, keyOf(j), true) })
	before := ownLen(f)
	w2, err := h.redeem(c.p.id, code, c.req, 0)
	if err != nil || !bytes.Equal(w1, w2) {
		t.Fatalf("asking again: %v, same welcome %v", err, bytes.Equal(w1, w2))
	}
	if ownLen(f) != before || len(h.decisions()) != 1 {
		t.Fatal("asking again appended an entry")
	}
	if _, err := c.j.Complete(ctx10(t), w2); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("a second Complete: %v", err)
	}
	if _, err := h.redeem(newIdentity(t), code, c.req, 0); !errors.Is(err, invite.ErrDenied) {
		t.Fatalf("the request under another key: %v", err)
	}
	if d := h.waitDecision(2); !errors.Is(d.admit, ErrInvalidArgument) {
		t.Fatalf("decision %+v", d)
	}
	if _, err := f.b.RemoveMember(ctx10(t), keyOf(j)); err != nil {
		t.Fatal(err)
	}
	code2, _ := h.create(&invite.CreateOptions{Manual: true})
	if _, err := h.redeem(c.p.id, code2, c.req, 0); !errors.Is(err, invite.ErrDenied) {
		t.Fatalf("the removed member's request again: %v", err)
	}
	if d := h.waitDecision(3); !errors.Is(d.admit, ErrKnownStrand) {
		t.Fatalf("decision %+v", d)
	}
}

// TestInviteByRemovedOrPlainMember: an admin that holds its own removal refuses to admit (ErrRemoved), and a member that is not
// an admin refuses too (ErrNotAdmin); both deny and append nothing.
func TestInviteByRemovedOrPlainMember(t *testing.T) {
	m := newMesh(t)
	ps := grow(t, m, 4)
	f, a, plain := ps[0], ps[1], ps[3]
	if r, _ := rolesOf(t, f.b, keyOf(a)); r&RoleAdmin == 0 {
		t.Fatal("the first joiner is not an admin")
	}
	if r, _ := rolesOf(t, f.b, keyOf(plain)); r&RoleAdmin != 0 {
		t.Fatal("the third joiner is an admin")
	}
	hp := newHost(t, plain, nil)
	code, _ := hp.create(&invite.CreateOptions{Manual: true})
	x := newCandidate(t, "x", nil)
	if _, err := hp.redeem(x.p.id, code, x.req, 0); !errors.Is(err, invite.ErrDenied) {
		t.Fatalf("invite of a plain member: %v", err)
	}
	if d := hp.waitDecision(1); !errors.Is(d.admit, ErrNotAdmin) {
		t.Fatalf("decision %+v", d)
	}

	ha := newHost(t, a, nil)
	code, _ = ha.create(&invite.CreateOptions{Manual: true})
	m.isolate(a)
	if _, err := f.b.RemoveMember(ctx10(t), keyOf(a)); err != nil {
		t.Fatal(err)
	}
	// peers that hold the removal serve the removed admin nothing, so it learns of it from a peer that does not hold it yet
	pull(t, f, a, 0)
	waitUntil(t, 10*time.Second, func() bool { return !hasMember(a.b, keyOf(a), false) })
	before := ownLen(a)
	y := newCandidate(t, "y", nil)
	if _, err := ha.redeem(y.p.id, code, y.req, 0); !errors.Is(err, invite.ErrDenied) {
		t.Fatalf("invite of a removed admin: %v", err)
	}
	if d := ha.waitDecision(1); !errors.Is(d.admit, ErrRemoved) {
		t.Fatalf("decision %+v", d)
	}
	if ownLen(a) != before {
		t.Fatal("a removed admin appended an entry")
	}
}

// TestInviteWrongBraid: a welcome of a braid of another application halts the joiner with ErrWrongApp once it holds that braid's
// genesis entry.
func TestInviteWrongBraid(t *testing.T) {
	m := newMesh(t)
	g := &peer{t: t, name: "g", id: newIdentity(t), db: kolostore.NewMemory()}
	g.opts = testOptions(g.db, appApply)
	g.opts.App = "other/v1"
	b, err := Create(ctx10(t), g.id, GroupConfig{App: "other/v1"}, g.opts)
	if err != nil {
		t.Fatal(err)
	}
	g.b = b
	t.Cleanup(g.close)
	h := newHost(t, g, nil)
	code, _ := h.create(&invite.CreateOptions{Manual: true})
	j := inviteJoin(t, h, code, "j")
	m.connect(j, g)
	waitUntil(t, 20*time.Second, func() bool { return errors.Is(j.b.Status().Halted, ErrWrongApp) })
	if _, err := j.b.Append(ctx10(t), []byte("x=1")); !errors.Is(err, ErrHalted) {
		t.Fatalf("append after the wrong app: %v", err)
	}
}

// TestInviteForgedWelcome: Complete refuses a welcome that does not parse, and a joiner halts on a welcome whose founder is not
// the genesis entry's writer (ErrWrongGroup) or whose anchor is not a committed pivot of its config (settlement halts).
func TestInviteForgedWelcome(t *testing.T) {
	m := newMesh(t)
	ps := grow(t, m, 2)
	f, a := ps[0], ps[1]
	mustAppend(t, f.b, "x=1")
	waitNet(t, 20*time.Second, ps, func() bool { return synced(ps...) && settledGet(t, a.b, "x") == "1" })
	admit := func(name string) (*candidate, welcomeMsg) {
		c := newCandidate(t, name, nil)
		raw, err := a.b.Admit(ctx10(t), keyOf(c.p), c.req, growOpts)
		if err != nil {
			t.Fatal(err)
		}
		w, err := parseWelcome(raw)
		if err != nil {
			t.Fatal(err)
		}
		return c, w
	}
	c, w := admit("parse")
	good, _ := encodeWelcome(w)
	bad := func(edit func([]byte) []byte) []byte { return edit(slices.Clone(good)) }
	for name, raw := range map[string][]byte{
		"truncated": good[:len(good)-1],
		"trailing":  append(slices.Clone(good), 0),
		"tag":       bad(func(b []byte) []byte { b[1] ^= 1; return b }),
		"founder header": bad(func(b []byte) []byte {
			i := bytes.Index(b, w.FounderHeader)
			b[i+len(w.FounderHeader)-1] ^= 1
			return b
		}),
		"inviter header": bad(func(b []byte) []byte {
			i := bytes.Index(b, w.InviterHeader)
			b[i+len(w.InviterHeader)-1] ^= 1
			return b
		}),
		"over the bound":  make([]byte, MaxWelcome+1),
		"another inviter": func() []byte { x := w; x.InviterKey = keyOf(f); return rawWelcome(x) }(),
	} {
		if _, err := c.j.Complete(ctx10(t), raw); !errors.Is(err, ErrMalformed) {
			t.Errorf("%s: %v, want ErrMalformed", name, err)
		}
	}
	if _, err := c.j.Complete(ctx10(t), make([]byte, MaxWelcome+1)); !errors.Is(err, ErrTooLarge) {
		t.Errorf("over the bound: %v, want ErrTooLarge", err)
	}
	// welcomes that parse but name the joiner, or one strand under two keys
	other := newCandidate(t, "same key", c.p.id)
	stolen := newIdentity(t)
	for name, x := range map[string]welcomeMsg{
		"own strand": func() welcomeMsg {
			x := w
			x.InviterStrand, x.InviterHeader, x.InviterKey = c.j.strand, c.j.header, keyOf(c.p)
			return x
		}(),
		"own key": func() welcomeMsg {
			x := w
			x.InviterStrand, x.InviterHeader, x.InviterKey = other.j.strand, other.j.header, keyOf(c.p)
			return x
		}(),
		"own strand under another key": func() welcomeMsg {
			x := w
			x.FounderKey, x.FounderHeader, x.FounderStrand = stolen.PublicKey(), signedHeader(t, stolen, c.j.strand), c.j.strand
			return x
		}(),
		"shared strand": func() welcomeMsg {
			x := w
			x.InviterKey, x.InviterHeader = stolen.PublicKey(), signedHeader(t, stolen, x.FounderStrand)
			x.InviterStrand = x.FounderStrand
			return x
		}(),
	} {
		raw, err := encodeWelcome(x)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if _, err := c.j.Complete(ctx10(t), raw); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("%s: %v, want ErrInvalidArgument", name, err)
		}
	}

	_, fw := admit("first")
	for _, tc := range []struct {
		name string
		edit func(*welcomeMsg)
		halt func(Status) bool
	}{
		{"none", func(*welcomeMsg) {}, func(s Status) bool { return s.Settled > 0 && s.Halted == nil && s.SettlementHalted == nil }},
		{"another member's entry", func(x *welcomeMsg) { x.AddIndex, x.AddID = fw.AddIndex, fw.AddID },
			func(s Status) bool { return errors.Is(s.Halted, ErrNotMember) }},
		{"founder", func(x *welcomeMsg) {
			x.FounderKey, x.FounderStrand, x.FounderHeader = x.InviterKey, x.InviterStrand, x.InviterHeader
		},
			func(s Status) bool { return errors.Is(s.Halted, ErrWrongGroup) }},
		{"anchor not a pivot", func(x *welcomeMsg) { x.Anchor = &anchorRec{Pivot: x.AddID, Config: x.Anchor.Config} },
			func(s Status) bool { return s.SettlementHalted != nil }},
		{"anchor config", func(x *welcomeMsg) { x.Anchor.Config++ },
			func(s Status) bool { return s.SettlementHalted != nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, w := admit(tc.name)
			if w.Anchor == nil {
				t.Fatal("no anchor to forge")
			}
			tc.edit(&w)
			raw, err := encodeWelcome(w)
			if err != nil {
				t.Fatal(err)
			}
			j := c.complete(t, raw)
			// a listener takes at most four connections from one address
			t.Cleanup(func() { m.isolate(j) })
			m.connect(j, a)
			m.connect(j, f)
			waitUntil(t, 20*time.Second, func() bool { return tc.halt(j.b.Status()) })
			if tc.name == "none" {
				waitNet(t, 20*time.Second, ps, func() bool { return hasMember(j.b, keyOf(j), true) })
			}
		})
	}
}

// signedHeader returns a Bytes log header signed by id that names the strand id.
func signedHeader(t testing.TB, id *koloid.Identity, strand kololog.LogID) []byte {
	t.Helper()
	var e kolocodec.Encoder
	e.PutString(kololog.HeaderContext)
	e.PutFixed(id.PublicKey())
	e.PutFixed(strand[:])
	e.PutUvarint(uint64(kololog.Bytes))
	msg := e.Bytes()
	sig, err := id.Sign(kololog.HeaderContext, msg)
	if err != nil {
		t.Fatal(err)
	}
	return append(msg, sig...)
}

// rawWelcome encodes a welcome field by field, without the checks of EncodeWelcome.
func rawWelcome(w welcomeMsg) []byte {
	var e kolocodec.Encoder
	e.PutString(welcomeTag)
	e.PutFixed(w.Group[:])
	e.PutFixed(w.FounderKey)
	e.PutFixed(w.FounderStrand[:])
	e.PutBytes(w.FounderHeader)
	e.PutFixed(w.InviterKey)
	e.PutFixed(w.InviterStrand[:])
	e.PutBytes(w.InviterHeader)
	e.PutUvarint(w.AddIndex)
	e.PutFixed(w.AddID[:])
	e.PutBool(false)
	return e.Bytes()
}

// TestInviteOversized: a join request over the bound is refused by Request and the largest one passes kolo-invite's default
// data bound; an inviter with a smaller data bound refuses before any decision; a joiner whose payload bound is below the
// welcome gets ErrTooLarge and gets the same welcome when it asks again with a larger bound.
func TestInviteOversized(t *testing.T) {
	m := newMesh(t)
	f := newFounder(t, "f", GroupConfig{}, appApply)
	c := newCandidate(t, "big", nil)
	if _, err := c.j.Request(make([]byte, MaxJoinRequest)); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("request over the bound: %v", err)
	}
	empty, _ := c.j.Request(nil)
	n := MaxJoinRequest - len(empty) - 2
	req, err := c.j.Request(make([]byte, n))
	if err != nil || len(req) != MaxJoinRequest {
		t.Fatalf("largest request: %d bytes, %v", len(req), err)
	}
	h := newHost(t, f, nil)
	code, _ := h.create(&invite.CreateOptions{Manual: true, MaxUses: 3})
	if _, err := h.redeem(c.p.id, code, req, 64); !errors.Is(err, invite.ErrTooLarge) {
		t.Fatalf("welcome over the joiner's payload bound: %v", err)
	}
	if d := h.waitDecision(1); d.admit != nil || d.approve != nil {
		t.Fatalf("decision %+v", d)
	}
	w, err := h.redeem(c.p.id, code, req, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(h.decisions()) != 1 {
		t.Fatal("asking again appended a second AddMember entry")
	}
	j := c.complete(t, w)
	m.connect(j, f)
	waitNet(t, 20*time.Second, []*peer{f, j}, func() bool { return synced(f, j) && hasMember(j.b, keyOf(j), true) })

	small := newHost(t, f, &invite.Options{MaxData: 1024})
	code, _ = small.create(&invite.CreateOptions{Manual: true})
	d := newCandidate(t, "d", nil)
	r2, _ := d.j.Request(make([]byte, 2048))
	before := ownLen(f)
	if _, err := small.redeem(d.p.id, code, r2, 0); !errors.Is(err, invite.ErrTooLarge) {
		t.Fatalf("data over the inviter's bound: %v", err)
	}
	if len(small.decisions()) != 0 || ownLen(f) != before {
		t.Fatal("a request over the data bound reached Admit")
	}
}

// TestAdmitOneAtATime: Admit calls run one at a time, so each joiner's default roles count the entries of the one before; a
// call that waits honours its context.
func TestAdmitOneAtATime(t *testing.T) {
	f := newFounder(t, "f", GroupConfig{}, appApply)
	c := newCandidate(t, "c", nil)
	f.b.admitting <- struct{}{}
	done := make(chan error, 1)
	go func() {
		_, err := f.b.Admit(ctx10(t), keyOf(c.p), c.req, growOpts)
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("Admit ran while another held the slot: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	ctx, cancel := shortCtx()
	d := newCandidate(t, "d", nil)
	if _, err := f.b.Admit(ctx, keyOf(d.p), d.req, growOpts); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiting Admit with an ended context: %v", err)
	}
	cancel()
	<-f.b.admitting
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	// the third and fourth members admitted at once: the last Settlers entry names all four
	var wg sync.WaitGroup
	for _, x := range []*candidate{d, newCandidate(t, "e", nil)} {
		wg.Go(func() {
			if _, err := f.b.Admit(ctx10(t), keyOf(x.p), x.req, growOpts); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	var last settlersBody
	for o, err := range f.b.Entries(0) {
		if err != nil {
			t.Fatal(err)
		}
		if o.Kind == KindSettlers {
			body, err := decodeBody(KindSettlers, o.Body, f.b.bounds)
			if err != nil {
				t.Fatal(err)
			}
			last = body.(settlersBody)
		}
	}
	if len(last.Keys) != 4 {
		t.Fatalf("the last Settlers entry names %d keys, want 4", len(last.Keys))
	}
}

// swarmApp is one peer's application on kolo-swarm with its one event loop: every connection that shares the braid's topic is
// served by the braid, every other one goes to other.
func swarmApp(t testing.TB, ctx context.Context, id *koloid.Identity, boot netip.AddrPort, b func() *Braid, other func(*koloswarm.Peer)) *koloswarm.Swarm {
	t.Helper()
	sw, err := koloswarm.New(ctx, "127.0.0.1:0", id, &koloswarm.Config{LookupInterval: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if err := sw.Bootstrap(ctx, []netip.AddrPort{boot}); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	t.Cleanup(func() {
		sw.Close()
		wg.Wait()
	})
	wg.Go(func() {
		for {
			ev, err := sw.Next(ctx)
			if err != nil {
				return
			}
			if ev.Kind != koloswarm.Connected {
				continue
			}
			br := b()
			if br != nil && slices.Contains(ev.Peer.Topics(), koloswarm.Topic(br.Topic())) {
				wg.Go(func() { br.ServeConn(ctx, ev.Peer.Conn()) })
				continue
			}
			other(ev.Peer)
		}
	})
	return sw
}

// TestInviteOverSwarm is the application flow end to end on 127.0.0.1 with an in-process kolo-dht server: the admin announces a
// manual invite on its swarm and answers the request with Admit and Approve; the joiner looks the invite topic up, joins on
// that connection, leaves the topic, completes, joins the braid's topic, and both settle each other's entries.
func TestInviteOverSwarm(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	server, err := kolodht.New(ctx, "127.0.0.1:0", newIdentity(t), &kolodht.Config{Server: true})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	boot, err := netip.ParseAddrPort(server.Addr().String())
	if err != nil {
		t.Fatal(err)
	}

	f := newFounder(t, "f", GroupConfig{}, appApply)
	db := kolostore.NewMemory()
	defer db.Close()
	in, err := invite.Open(db, f.id, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	go func() {
		for {
			ev, err := in.Next(ctx)
			if err != nil {
				return
			}
			if ev.Kind != invite.Requested {
				continue
			}
			w, err := f.b.Admit(ctx, ev.Joiner, ev.Data, growOpts)
			if err != nil {
				in.Deny(ev.Request)
				continue
			}
			if err := in.Approve(ev.Request, w); err != nil {
				f.b.RemoveMember(ctx, ev.Joiner)
			}
		}
	}()
	var served sync.WaitGroup
	defer func() {
		cancel()
		served.Wait()
	}()
	fsw := swarmApp(t, ctx, f.id, boot, func() *Braid { return f.b }, func(p *koloswarm.Peer) {
		served.Go(func() { in.ServePeer(ctx, p) })
	})
	served.Go(func() { in.Announce(ctx, fsw) })
	if err := fsw.Join(ctx, koloswarm.Topic(f.b.Topic()), &koloswarm.JoinOptions{Announce: true, Lookup: true}); err != nil {
		t.Fatal(err)
	}
	s, _, err := in.Create(&invite.CreateOptions{Manual: true, TTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	code, err := invite.Parse(s)
	if err != nil {
		t.Fatal(err)
	}

	c := newCandidate(t, "j", nil)
	var mu sync.Mutex
	var jb *Braid
	inviter := make(chan koloswarm.Conn, 1)
	jsw := swarmApp(t, ctx, c.p.id, boot, func() *Braid {
		mu.Lock()
		defer mu.Unlock()
		return jb
	}, func(p *koloswarm.Peer) {
		if code.Matches(p.PublicKey()) {
			select {
			case inviter <- p.Conn():
			default:
			}
		}
	})
	if err := jsw.Join(ctx, code.Topic(), &koloswarm.JoinOptions{Lookup: true}); err != nil {
		t.Fatal(err)
	}
	var conn koloswarm.Conn
	select {
	case conn = <-inviter:
	case <-ctx.Done():
		t.Fatal("the invite topic found no inviter")
	}
	w, err := invite.JoinConn(ctx, conn, c.p.id, code, &invite.JoinOptions{Data: c.req})
	if err != nil {
		t.Fatal(err)
	}
	jsw.Leave(code.Topic())
	select {
	case <-conn.Context().Done():
	case <-time.After(5 * time.Second):
		t.Fatal("leaving the invite topic kept the invite connection")
	}
	j := c.complete(t, w)
	mu.Lock()
	jb = j.b
	mu.Unlock()
	if err := jsw.Join(ctx, koloswarm.Topic(j.b.Topic()), &koloswarm.JoinOptions{Announce: true, Lookup: true}); err != nil {
		t.Fatal(err)
	}
	mustAppend(t, f.b, "from-f=1")
	mustAppend(t, j.b, "from-j=1")
	ps := []*peer{f, j}
	waitNet(t, 60*time.Second, ps, func() bool {
		return synced(ps...) && settledGet(t, f.b, "from-j") == "1" && settledGet(t, j.b, "from-f") == "1"
	})
	checkSame(t, ps)
}

// TestAdmitRefusesAPendingStrand: a strand whose head a connected peer announced, before its AddMember reached this peer, is
// known: Admit refuses it, so two admins never add one strand twice.
func TestAdmitRefusesAPendingStrand(t *testing.T) {
	m := newMesh(t)
	ps := grow(t, m, 2)
	f, a := ps[0], ps[1]
	m.cut([]*peer{f}, []*peer{a})
	c := newCandidate(t, "x", nil)
	w, err := a.b.Admit(ctx10(t), keyOf(c.p), c.req, growOpts)
	if err != nil {
		t.Fatal(err)
	}
	x := c.complete(t, w)
	m.connect(x, a)
	waitNet(t, 20*time.Second, []*peer{a, x}, func() bool { return !waiting(x) && hasMember(x.b, keyOf(x), false) })
	mustAppend(t, x.b, "x=1")
	m.connect(x, f)
	waitUntil(t, 20*time.Second, func() bool { return f.b.sync.pendingHead(c.j.strand, keyOf(x)) })
	if _, err := f.b.Admit(ctx10(t), keyOf(x), c.req, growOpts); !errors.Is(err, ErrKnownStrand) {
		t.Fatalf("Admit of an announced strand: %v", err)
	}
}

// TestAdmitExplicitRoles: roles given to Admit are the joiner's, and no Settlers entry is appended for them.
func TestAdmitExplicitRoles(t *testing.T) {
	f := newFounder(t, "f", GroupConfig{}, appApply)
	none, admin := Role(0), RoleAdmin
	for _, c := range []struct {
		name  string
		roles *Role
	}{{"plain", &none}, {"admin", &admin}, {"third", &none}} {
		x := newCandidate(t, c.name, nil)
		if _, err := f.b.Admit(ctx10(t), keyOf(x.p), x.req, &AdmitOptions{Roles: c.roles}); err != nil {
			t.Fatal(err)
		}
		f.b.mu.Lock()
		m, ok := f.b.tent.Member(rosterKey(keyOf(x.p)))
		f.b.mu.Unlock()
		if !ok || Role(m.Roles) != *c.roles {
			t.Fatalf("%s: roles %d, member %v", c.name, m.Roles, ok)
		}
	}
	for o, err := range f.b.Entries(0) {
		if err != nil {
			t.Fatal(err)
		}
		if o.Kind == KindSettlers {
			t.Fatal("a Settlers entry was appended for explicit roles")
		}
	}
}

// TestWelcomeWaitsForTheKey: a joiner follows its inviter's strand from Complete on, before the founder strand names it, so it
// can hold the welcome's AddMember entry first; held without its key, that entry neither passes nor halts the joiner, and once
// its key arrives the welcome is checked. Entries move in process, with no timing.
func TestWelcomeWaitsForTheKey(t *testing.T) {
	f := newFounder(t, "f", GroupConfig{}, appApply)
	a, nw := newJoiner(t, "a", f, appApply)
	nw.Roles = RoleAdmin
	if _, err := f.b.AddMember(ctx10(t), nw); err != nil {
		t.Fatal(err)
	}
	a.start()
	pull(t, f, a, 0)
	c := newCandidate(t, "x", nil)
	raw, err := a.b.Admit(ctx10(t), keyOf(c.p), c.req, growOpts)
	if err != nil {
		t.Fatal(err)
	}
	w, _ := parseWelcome(raw)
	x := c.complete(t, raw)
	rep := x.b.replicaOf(w.InviterStrand)
	if rep == nil {
		t.Fatal("the joiner does not follow its inviter's strand before it holds the founder strand")
	}
	src := a.b.strandFor(w.InviterStrand)
	h, err := headOf(src)
	if err != nil {
		t.Fatal(err)
	}
	var es []kololog.Entry
	for i := range h.Size {
		e, err := entryOf(src, i)
		if err != nil {
			t.Fatal(err)
		}
		e.Key = nil
		es = append(es, e)
	}
	if err := rep.Update(h, kololog.ConsistencyProof{}, es); err != nil {
		t.Fatal(err)
	}
	if err := x.b.strandGrew(w.InviterStrand); err != nil {
		t.Fatal(err)
	}
	pull(t, f, x, 0)
	st := x.b.Status()
	x.b.mu.Lock()
	pending, wait := x.b.welcome != nil, x.b.waiting
	x.b.mu.Unlock()
	if wait || st.Halted != nil || !pending {
		t.Fatalf("keyless AddMember entry: waiting %v, halted %v, welcome pending %v", wait, st.Halted, pending)
	}
	key, err := entryOf(src, w.AddIndex)
	if err != nil {
		t.Fatal(err)
	}
	if err := rep.AddKey(w.AddIndex, key.Key); err != nil {
		t.Fatal(err)
	}
	if err := x.b.strandGrew(w.InviterStrand); err != nil {
		t.Fatal(err)
	}
	x.b.mu.Lock()
	pending = x.b.welcome != nil
	x.b.mu.Unlock()
	if pending || x.b.Status().Halted != nil {
		t.Fatalf("the welcome was not checked once the key arrived: pending %v, halted %v", pending, x.b.Status().Halted)
	}
}

// TestAppendWaitsForTheJoin: after Complete, writing calls wait for the joiner's own AddMember entry instead of refusing, and
// write once it is ordered.
func TestAppendWaitsForTheJoin(t *testing.T) {
	f := newFounder(t, "f", GroupConfig{}, appApply)
	mustAppend(t, f.b, "a=1")
	p := &peer{t: t, name: "j", id: newIdentity(t), db: kolostore.NewMemory()}
	p.opts = testOptions(p.db, appApply)
	j, err := NewJoin(ctx10(t), p.id, p.opts)
	if err != nil {
		t.Fatal(err)
	}
	req, err := j.Request(nil)
	if err != nil {
		t.Fatal(err)
	}
	w, err := f.b.Admit(ctx10(t), keyOf(p), req, growOpts)
	if err != nil {
		t.Fatal(err)
	}
	if p.b, err = j.Complete(ctx10(t), w); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.close)
	pull(t, f, p, 1)
	if waiting(p) {
		t.Fatal("the joiner still waits for the genesis entry")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := p.b.Append(ctx, []byte("j=1")); !errors.Is(err, ErrUnconfirmed) {
		t.Fatalf("Append before the own AddMember entry: %v", err)
	}
	nt := newNet(t, f, p)
	nt.round(nil, 0)
	mustAppend(t, p.b, "j=1")
	nt.settle(nil, 10*time.Second, func() bool { return settledGet(t, f.b, "j") == "1" })
}

// TestJoinSurvivesRestart: a joiner that stops between its request and the welcome calls NewJoin again on the same prefix and
// gets the same strand and request back, and the welcome issued for the first request completes the join.
func TestJoinSurvivesRestart(t *testing.T) {
	f := newFounder(t, "f", GroupConfig{}, appApply)
	p := &peer{t: t, name: "j", id: newIdentity(t), db: kolostore.NewMemory()}
	p.opts = testOptions(p.db, appApply)
	j, err := NewJoin(ctx10(t), p.id, p.opts)
	if err != nil {
		t.Fatal(err)
	}
	req, err := j.Request([]byte("app"))
	if err != nil {
		t.Fatal(err)
	}
	w, err := f.b.Admit(ctx10(t), keyOf(p), req, growOpts)
	if err != nil {
		t.Fatal(err)
	}
	again, err := NewJoin(ctx10(t), p.id, p.opts)
	if err != nil {
		t.Fatal(err)
	}
	if req2, err := again.Request([]byte("app")); err != nil || !bytes.Equal(req2, req) {
		t.Fatalf("the join after a restart has another request: %v", err)
	}
	if _, err := NewJoin(ctx10(t), newIdentity(t), p.opts); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("NewJoin of another key over a pending join: %v", err)
	}
	if p.b, err = again.Complete(ctx10(t), w); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.close)
	nt := newNet(t, f, p)
	nt.settle(nil, 10*time.Second, func() bool { return !waiting(p) && hasMember(p.b, keyOf(p), false) })
	mustAppend(t, p.b, "j=1")
	nt.settle(nil, 10*time.Second, func() bool { return settledGet(t, f.b, "j") == "1" })
	if v, err := getOpt(PrefixBackend(p.db, nil), []byte(keyJoin)); err != nil || v != nil {
		t.Fatalf("the pending join record stays after Complete: %v", err)
	}
}

// errAfter is a context whose Err reports cancellation from its n-th call on, to stop a call between two of its writes.
type errAfter struct {
	context.Context
	n *atomic.Int32
}

func (c errAfter) Err() error {
	if c.n.Add(-1) < 0 {
		return context.Canceled
	}
	return nil
}

// TestAdmitFailsBetweenItsWrites: when the Settlers entry of the default roles cannot be written after the AddMember entry,
// Admit says so, and RemoveMember, as the guide does after Deny, leaves the joiner out.
func TestAdmitFailsBetweenItsWrites(t *testing.T) {
	m := newMesh(t)
	ps := grow(t, m, 2)
	f := ps[0]
	c := newCandidate(t, "x", nil)
	n := new(atomic.Int32)
	n.Store(1)
	_, err := f.b.Admit(errAfter{ctx10(t), n}, keyOf(c.p), c.req, growOpts)
	if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "AddMember entry is written") {
		t.Fatalf("Admit stopped between its writes: %v", err)
	}
	waitUntil(t, 10*time.Second, func() bool { return hasMember(f.b, keyOf(c.p), false) })
	if _, err := f.b.RemoveMember(ctx10(t), keyOf(c.p)); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 10*time.Second, func() bool { return !hasMember(f.b, keyOf(c.p), false) })
}

// TestAppendHonoursContext: a done context stops Append before it writes, also with a confirmed strand.
func TestAppendHonoursContext(t *testing.T) {
	f := newFounder(t, "f", GroupConfig{}, appApply)
	// the genesis is work: the writer loop settles it with a pulse of its own, which must not count as the Append
	waitUntil(t, 10*time.Second, func() bool { return quiet(f) })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	n := ownLen(f)
	if _, err := f.b.Append(ctx, []byte("a=1")); !errors.Is(err, context.Canceled) || ownLen(f) != n {
		t.Fatalf("Append with a done context: %v, %d entries of %d", err, ownLen(f), n)
	}
}

// TestOpenAfterFailedCreateOrComplete: Create or Complete that fail once the genesis or the welcome is stored leave a braid that
// Open continues, while a second Create or Complete is refused.
func TestOpenAfterFailedCreateOrComplete(t *testing.T) {
	other := newFounder(t, "o", GroupConfig{}, appApply)
	p := &peer{t: t, name: "f", id: newIdentity(t), db: kolostore.NewMemory()}
	p.opts = testOptions(p.db, appApply)
	bad := p.opts
	bad.Predecessor = other.b
	if _, err := Create(ctx10(t), p.id, GroupConfig{App: "test/v1"}, bad); !errors.Is(err, ErrWrongGroup) {
		t.Fatalf("Create that fails after the genesis: %v", err)
	}
	if _, err := Create(ctx10(t), p.id, GroupConfig{App: "test/v1"}, p.opts); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("a second Create: %v", err)
	}
	p.reopen()
	t.Cleanup(p.close)
	mustAppend(t, p.b, "a=1")

	c := newCandidate(t, "x", nil)
	w, err := p.b.Admit(ctx10(t), keyOf(c.p), c.req, growOpts)
	if err != nil {
		t.Fatal(err)
	}
	n := new(atomic.Int32)
	n.Store(1)
	if _, err := c.j.Complete(errAfter{ctx10(t), n}, w); !errors.Is(err, context.Canceled) {
		t.Fatalf("Complete that fails after the welcome: %v", err)
	}
	if _, err := c.j.Complete(ctx10(t), w); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("a second Complete: %v", err)
	}
	c.p.b, err = Open(ctx10(t), c.p.id, c.p.opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.p.close)
	nt := newNet(t, p, c.p)
	nt.settle(nil, 10*time.Second, func() bool { return !waiting(c.p) && hasMember(c.p.b, keyOf(c.p), false) })
}

// TestVoidJoinIsNotMember: when the welcome's AddMember entry is void at its position, here written by a member that is not
// an admin, the joiner's writing calls end with ErrNotMember once that entry is ordered, instead of waiting. (The in-process
// transport serves the joiner although nobody admitted it, as no peer would over the network.)
func TestVoidJoinIsNotMember(t *testing.T) {
	_, ps := group(t, 3, GroupConfig{}, appApply, false)
	f, p := ps[0], ps[2]
	c := newCandidate(t, "x", nil)
	body, err := encodeBody(NewMember{Key: keyOf(c.p), Strand: c.j.strand, Header: c.j.header}, p.b.groupBounds())
	if err != nil {
		t.Fatal(err)
	}
	p.b.mu.Lock()
	ref, err := p.b.appendLocked(KindAddMember, body)
	p.b.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	w := welcomeMsg{Group: f.b.meta.Group, FounderKey: f.b.meta.FounderKey, FounderStrand: f.b.meta.FounderStrand,
		FounderHeader: f.b.meta.FounderHeader, InviterKey: keyOf(p), InviterStrand: p.b.meta.OwnStrand,
		InviterHeader: p.b.own.Header().Encode(), AddIndex: ref.Index, AddID: ref.ID}
	enc, err := encodeWelcome(w)
	if err != nil {
		t.Fatal(err)
	}
	x := c.complete(t, enc)
	t.Cleanup(x.close)
	nt2 := newNet(t, append(ps, x)...)
	nt2.settle(nil, 10*time.Second, func() bool {
		x.b.mu.Lock()
		defer x.b.mu.Unlock()
		return !x.b.waiting && x.b.welcome == nil
	})
	if _, err := x.b.Append(ctx10(t), []byte("x=1")); !errors.Is(err, ErrNotMember) {
		t.Fatalf("Append after a void join: %v", err)
	}
}

// TestJoinThroughNonFounderInviter: founder F admits X, X admits I, I admits J, and J reaches only I: J keeps I known after
// its welcome check, until its own AddMember entry is ordered, so it learns X's strand and joins.
func TestJoinThroughNonFounderInviter(t *testing.T) {
	m := newMesh(t)
	f := newFounder(t, "p0", GroupConfig{}, appApply)
	x := m.join(f, "px", nil)
	m.full(f, x)
	waitNet(t, 30*time.Second, []*peer{f, x}, func() bool { return synced(f, x) && hasMember(x.b, keyOf(x), false) })
	i := m.join(x, "pi", nil)
	m.full(f, x, i)
	waitNet(t, 30*time.Second, []*peer{f, x, i}, func() bool { return synced(f, x, i) && hasMember(i.b, keyOf(i), false) })
	mustAppend(t, i.b, "i=1")
	j := m.join(i, "pj", nil)
	m.connect(j, i)
	waitNet(t, 60*time.Second, []*peer{i, j}, func() bool {
		return !waiting(j) && hasMember(j.b, keyOf(i), false) && hasMember(j.b, keyOf(j), false)
	})
}

// TestJoinWaitSurvivesRestart: once the welcome is checked but before the joiner's AddMember entry is ordered, writing calls
// wait, also after the joiner restarts.
func TestJoinWaitSurvivesRestart(t *testing.T) {
	_, ps := group(t, 2, GroupConfig{}, appApply, false)
	f, a := ps[0], ps[1]
	mustAppend(t, a.b, "a=1")
	p := &peer{t: t, name: "j", id: newIdentity(t), db: kolostore.NewMemory()}
	p.opts = testOptions(p.db, appApply)
	j, err := NewJoin(ctx10(t), p.id, p.opts)
	if err != nil {
		t.Fatal(err)
	}
	req, err := j.Request(nil)
	if err != nil {
		t.Fatal(err)
	}
	w, err := a.b.Admit(ctx10(t), keyOf(p), req, growOpts)
	if err != nil {
		t.Fatal(err)
	}
	if p.b, err = j.Complete(ctx10(t), w); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.close)
	// the founder strand holds only its genesis, so the inviter's AddMember entries cannot be ordered yet
	fs, as := f.b.meta.FounderStrand, a.b.meta.OwnStrand
	transfer(t, f.b.strandFor(fs), p.b.replicaOf(fs), 1)
	if err := p.b.strandGrew(fs); err != nil {
		t.Fatal(err)
	}
	transfer(t, a.b.strandFor(as), p.b.replicaOf(as), ^uint64(0))
	if err := p.b.strandGrew(as); err != nil {
		t.Fatal(err)
	}
	p.b.mu.Lock()
	checked := p.b.welcome == nil
	p.b.mu.Unlock()
	if !checked {
		t.Fatal("the welcome is not checked")
	}
	try := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		_, err := p.b.Append(ctx, []byte("j=1"))
		return err
	}
	if err := try(); !errors.Is(err, ErrUnconfirmed) {
		t.Fatalf("before the restart: %v", err)
	}
	p.reopen()
	// over the network a member confirms the empty strand; the in-process transport gives no such answer
	p.b.mu.Lock()
	p.b.confirm()
	p.b.mu.Unlock()
	if err := try(); !errors.Is(err, ErrUnconfirmed) {
		t.Fatalf("after the restart: %v", err)
	}
	nt := newNet(t, f, a, p)
	nt.round(nil, 0)
	mustAppend(t, p.b, "j=1")
}

// keepWelcome is a backend whose direct Delete of the welcome record fails, as a crash between a settled batch and that delete.
type keepWelcome struct{ kolostore.Backend }

func (k keepWelcome) Delete(key []byte) error {
	if bytes.HasSuffix(key, []byte(keyWelcome)) {
		return errCrash
	}
	return k.Backend.Delete(key)
}

// TestVoidJoinAfterRestart: a joiner whose AddMember entry settles void returns ErrNotMember after a restart too: the settled
// batch deletes the welcome record, so the restart does not wait for the join again.
func TestVoidJoinAfterRestart(t *testing.T) {
	_, ps := group(t, 3, GroupConfig{}, appApply, false)
	f, p := ps[0], ps[2]
	x := &peer{t: t, name: "x", id: newIdentity(t)}
	x.db = keepWelcome{kolostore.NewMemory()}
	x.opts = testOptions(x.db, appApply)
	j, err := NewJoin(ctx10(t), x.id, x.opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(x.close)
	body, err := encodeBody(NewMember{Key: keyOf(x), Strand: j.strand, Header: j.header}, p.b.groupBounds())
	if err != nil {
		t.Fatal(err)
	}
	p.b.mu.Lock()
	ref, err := p.b.appendLocked(KindAddMember, body)
	p.b.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	w := welcomeMsg{Group: f.b.meta.Group, FounderKey: f.b.meta.FounderKey, FounderStrand: f.b.meta.FounderStrand,
		FounderHeader: f.b.meta.FounderHeader, InviterKey: keyOf(p), InviterStrand: p.b.meta.OwnStrand,
		InviterHeader: p.b.own.Header().Encode(), AddIndex: ref.Index, AddID: ref.ID}
	enc, err := encodeWelcome(w)
	if err != nil {
		t.Fatal(err)
	}
	if x.b, err = j.Complete(ctx10(t), enc); err != nil {
		t.Fatal(err)
	}
	nt2 := newNet(t, append(ps, x)...)
	// the joiner settles its own void AddMember entry
	nt2.settle(nil, 20*time.Second, func() bool {
		x.b.mu.Lock()
		_, held := x.b.nodes[ref.ID]
		seen := x.b.joinSeen
		x.b.mu.Unlock()
		if seen && held {
			mustAppend(t, f.b, "after=1")
		}
		return seen && !held
	})
	x.reopen()
	done := make(chan error, 1)
	go func() {
		_, err := x.b.Append(ctx10(t), []byte("x=1"))
		done <- err
	}()
	var err2 error
	nt2.settle(nil, 15*time.Second, func() bool {
		select {
		case err2 = <-done:
			return true
		default:
			return false
		}
	})
	if !errors.Is(err2, ErrNotMember) {
		t.Fatalf("Append after a void join and a restart: %v", err2)
	}
}

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// TestWelcomeRecordLogsItsError: when the welcome record cannot be deleted the log line carries the error as an attribute.
func TestWelcomeRecordLogsItsError(t *testing.T) {
	_, ps := group(t, 3, GroupConfig{}, appApply, false)
	f, p := ps[0], ps[2]
	var logs lockedBuffer
	x := &peer{t: t, name: "x", id: newIdentity(t)}
	x.db = keepWelcome{kolostore.NewMemory()}
	x.opts = testOptions(x.db, appApply)
	x.opts.Logger = slog.New(slog.NewTextHandler(&logs, nil))
	j, err := NewJoin(ctx10(t), x.id, x.opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(x.close)
	body, err := encodeBody(NewMember{Key: keyOf(x), Strand: j.strand, Header: j.header}, p.b.groupBounds())
	if err != nil {
		t.Fatal(err)
	}
	p.b.mu.Lock()
	ref, err := p.b.appendLocked(KindAddMember, body)
	p.b.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	enc, err := encodeWelcome(welcomeMsg{Group: f.b.meta.Group, FounderKey: f.b.meta.FounderKey, FounderStrand: f.b.meta.FounderStrand,
		FounderHeader: f.b.meta.FounderHeader, InviterKey: keyOf(p), InviterStrand: p.b.meta.OwnStrand,
		InviterHeader: p.b.own.Header().Encode(), AddIndex: ref.Index, AddID: ref.ID})
	if err != nil {
		t.Fatal(err)
	}
	if x.b, err = j.Complete(ctx10(t), enc); err != nil {
		t.Fatal(err)
	}
	nt := newNet(t, append(ps, x)...)
	nt.settle(nil, 20*time.Second, func() bool { return strings.Contains(logs.String(), "the welcome record stays") })
	if line := logs.String(); !strings.Contains(line, "error=") || strings.Contains(line, "%v") {
		t.Fatalf("log line: %s", line)
	}
}
