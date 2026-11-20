package kolobraid

import (
	"errors"
	"testing"
	"time"

	kolostore "github.com/ciranda-dev/kolo-store"
)

// admitOne makes a joiner request on a fresh backend and has f admit it with opts.
func admitOne(t *testing.T, f *peer, opts *AdmitOptions) (*peer, error) {
	t.Helper()
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
	if _, err := f.b.Admit(ctx10(t), keyOf(p), req, opts); err != nil {
		return nil, err
	}
	return p, nil
}

func memberOf(t *testing.T, b *Braid, p *peer) Member {
	t.Helper()
	var out Member
	waitUntil(t, 5*time.Second, func() bool {
		s := b.View()
		defer s.Close()
		for _, m := range s.Members() {
			if string(m.Key) == string(keyOf(p)) {
				out = m
				return true
			}
		}
		return false
	})
	return out
}

// TestAdmitDefaultIsAPlainMember: without options a joiner is neither admin nor settler and no Settlers entry is written, however
// few members the braid has; GrowSettlers gives the roles that grow the braid toward MinSettlers.
func TestAdmitDefaultIsAPlainMember(t *testing.T) {
	f := newFounder(t, "f", GroupConfig{}, appApply)
	for range 4 {
		p, err := admitOne(t, f, nil)
		if err != nil {
			t.Fatal(err)
		}
		if m := memberOf(t, f.b, p); m.Roles != 0 || m.Seat {
			t.Fatalf("a default joiner has roles %d, seat %v", m.Roles, m.Seat)
		}
	}
	if s := f.b.Status(); s.Seats != 1 || s.PendingConfig != nil {
		t.Fatalf("%d seats, pending config %v: the default changed the settlers", s.Seats, s.PendingConfig)
	}
	g := newFounder(t, "g", GroupConfig{}, appApply)
	p, err := admitOne(t, g, &AdmitOptions{GrowSettlers: true})
	if err != nil {
		t.Fatal(err)
	}
	if m := memberOf(t, g.b, p); m.Roles != RoleAdmin {
		t.Fatalf("a GrowSettlers joiner has roles %d, want admin", m.Roles)
	}
}

func TestAdmitRolesAndGrowSettlersConflict(t *testing.T) {
	f := newFounder(t, "f", GroupConfig{}, appApply)
	none := Role(0)
	if _, err := admitOne(t, f, &AdmitOptions{Roles: &none, GrowSettlers: true}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Roles with GrowSettlers: %v", err)
	}
}

// TestAdmitReplaceKeepsRoles: a plain admit of an active key with Replace keeps the member's roles.
func TestAdmitReplaceKeepsRoles(t *testing.T) {
	f := newFounder(t, "f", GroupConfig{}, appApply)
	admin := RoleAdmin
	p, err := admitOne(t, f, &AdmitOptions{Roles: &admin})
	if err != nil {
		t.Fatal(err)
	}
	j2, err := NewJoin(ctx10(t), p.id, testOptions(kolostore.NewMemory(), appApply))
	if err != nil {
		t.Fatal(err)
	}
	req, err := j2.Request(nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.b.Admit(ctx10(t), keyOf(p), req, &AdmitOptions{Replace: true}); err != nil {
		t.Fatal(err)
	}
	if m := memberOf(t, f.b, p); m.Roles != RoleAdmin {
		t.Fatalf("a replaced device has roles %d, want admin", m.Roles)
	}
}
