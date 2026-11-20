package kv_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	koloid "github.com/ciranda-dev/kolo-id"
	kolosecure "github.com/ciranda-dev/kolo-secure"
	kolostore "github.com/ciranda-dev/kolo-store"

	kolobraid "github.com/ciranda-dev/kolo-braid"
	"github.com/ciranda-dev/kolo-braid/kv"
)

// Two members edit one settings map. Edits of different keys merge; an edit made after seeing another one wins over it.
func Example() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	settings := kv.New([]byte("settings/"))
	apply := func(tx *kolobraid.Tx, e *kolobraid.Entry) error {
		_, err := settings.Apply(tx, e)
		return err
	}
	options := func() kolobraid.Options {
		return kolobraid.Options{Backend: kolostore.NewMemory(), App: "settings/v1", Apply: apply}
	}
	idA, err := koloid.Generate()
	check(err)
	idB, err := koloid.Generate()
	check(err)
	optsA, optsB := options(), options()
	defer optsA.Backend.Close()
	defer optsB.Backend.Close()

	a, err := kolobraid.Create(ctx, idA, kolobraid.GroupConfig{App: "settings/v1"}, optsA)
	check(err)
	defer a.Close()
	join, err := kolobraid.NewJoin(ctx, idB, optsB)
	check(err)
	request, err := join.Request(nil)
	check(err)
	welcome, err := a.Admit(ctx, idB.PublicKey(), request, nil)
	check(err)
	b, err := join.Complete(ctx, welcome)
	check(err)
	defer b.Close()

	l, err := kolosecure.Listen(ctx, "127.0.0.1:0", idA, nil)
	check(err)
	defer l.Close()
	go func() {
		if c, err := l.Accept(ctx); err == nil {
			a.ServeConn(ctx, c)
		}
	}()
	conn, err := kolosecure.Dial(ctx, l.Addr().String(), idB, idA.PublicKey(), nil)
	check(err)
	defer conn.Close()
	go b.ServeConn(ctx, conn)

	check(settings.Put(ctx, a, []byte("theme"), []byte("light")))
	check(settings.Put(ctx, a, []byte("lang"), []byte("en")))
	wait(ctx, b, settings, "lang", "en")
	check(settings.Put(ctx, b, []byte("theme"), []byte("dark")))
	check(settings.Put(ctx, b, []byte("zone"), []byte("utc")))
	check(settings.Delete(ctx, b, []byte("lang")))
	for _, m := range []*kolobraid.Braid{a, b} {
		wait(ctx, m, settings, "lang", "")
		fmt.Println(strings.Join(pairs(m, settings), " "))
	}
	// Output:
	// theme=dark zone=utc
	// theme=dark zone=utc
}

// wait returns once key holds value in the settled view of b; an empty value waits for the key to be gone.
func wait(ctx context.Context, b *kolobraid.Braid, s *kv.Store, key, value string) {
	for {
		snap := b.Settled()
		v, err := s.Get(snap, []byte(key))
		snap.Close()
		if (err == nil && string(v) == value) || (value == "" && errors.Is(err, kolobraid.ErrNotFound)) {
			return
		}
		if _, err := b.Next(ctx); err != nil {
			log.Fatal(err)
		}
	}
}

// pairs returns the settled map of b as key=value strings in key order.
func pairs(b *kolobraid.Braid, s *kv.Store) []string {
	snap := b.Settled()
	defer snap.Close()
	var out []string
	for k, v := range s.Range(snap, nil, nil) {
		out = append(out, string(k)+"="+string(v))
	}
	return out
}

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
