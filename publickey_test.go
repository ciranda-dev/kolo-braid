package kolobraid

import (
	"bytes"
	"context"
	"errors"
	"testing"

	kolostore "github.com/ciranda-dev/kolo-store"
)

func TestPublicKeyIsTheIdentitysAndACopy(t *testing.T) {
	id := newIdentity(t)
	db := kolostore.NewMemory()
	defer db.Close()
	b, err := Create(context.Background(), id, GroupConfig{App: "test/v1"}, testOptions(db, appApply))
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	got := b.PublicKey()
	if !bytes.Equal(got, id.PublicKey()) {
		t.Fatal("PublicKey is not the identity's key")
	}
	got[0] ^= 0xff
	if !bytes.Equal(b.PublicKey(), id.PublicKey()) {
		t.Fatal("PublicKey returns the braid's own slice")
	}
}

func TestCheckAdmitAndWelcomeEntryRefuseBadInput(t *testing.T) {
	id := newIdentity(t)
	db := kolostore.NewMemory()
	defer db.Close()
	b, err := Create(context.Background(), id, GroupConfig{App: "test/v1"}, testOptions(db, appApply))
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if err := b.CheckAdmit(make([]byte, 5), nil); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("short key = %v", err)
	}
	if err := b.CheckAdmit(make([]byte, 32), []byte("garbage")); !errors.Is(err, ErrMalformed) {
		t.Fatalf("garbage request = %v", err)
	}
	if _, err := WelcomeEntry([]byte("garbage")); !errors.Is(err, ErrMalformed) {
		t.Fatalf("garbage welcome = %v", err)
	}
	if _, err := WelcomeEntry(nil); !errors.Is(err, ErrMalformed) {
		t.Fatalf("empty welcome = %v", err)
	}
}
