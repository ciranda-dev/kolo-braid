// Command twopeers starts a braid and admits a second member, each on a directory of kolo-store's on-disk backend under the
// first argument, syncs them over kolo-secure on 127.0.0.1 and prints what each one settled.
package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"

	kolobraid "github.com/ciranda-dev/kolo-braid"
	koloid "github.com/ciranda-dev/kolo-id"
	kolosecure "github.com/ciranda-dev/kolo-secure"
	kolostore "github.com/ciranda-dev/kolo-store"
)

// apply keeps every message under its position and counts them.
func apply(tx *kolobraid.Tx, e *kolobraid.Entry) error {
	if e.Kind != kolobraid.KindData {
		return nil
	}
	if len(e.Body) == 0 {
		return kolobraid.Reject("empty message")
	}
	n := 0
	if v, err := tx.Get([]byte("count")); err == nil {
		n, _ = strconv.Atoi(string(v))
	}
	if err := tx.Put(binary.BigEndian.AppendUint64([]byte("msg/"), e.Position), e.Body); err != nil {
		return err
	}
	return tx.Put([]byte("count"), []byte(strconv.Itoa(n+1)))
}

func main() {
	if len(os.Args) != 2 {
		log.Fatal("usage: twopeers <temp dir>")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	options := func(name string) kolobraid.Options {
		db, err := kolostore.OpenPebble(filepath.Join(os.Args[1], name))
		check(err)
		return kolobraid.Options{Backend: db, App: "chat/v1", Apply: apply}
	}
	idA, err := koloid.Generate()
	check(err)
	idB, err := koloid.Generate()
	check(err)
	optsA, optsB := options("a"), options("b")
	defer optsA.Backend.Close()
	defer optsB.Backend.Close()

	// A creates the braid; B asks to join and A admits it with an AddMember entry.
	a, err := kolobraid.Create(ctx, idA, kolobraid.GroupConfig{App: "chat/v1"}, optsA)
	check(err)
	defer a.Close()
	join, err := kolobraid.NewJoin(ctx, idB, optsB)
	check(err)
	request, err := join.Request(nil)
	check(err)
	// nil options admit B as a plain member: not an admin and not a settler.
	welcome, err := a.Admit(ctx, idB.PublicKey(), request, nil)
	check(err)
	b, err := join.Complete(ctx, welcome)
	check(err)
	defer b.Close()

	// One kolo-secure connection on 127.0.0.1, served by both braids.
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

	_, err = a.Append(ctx, []byte("hello from A"))
	check(err)
	_, err = b.Append(ctx, []byte("hello from B"))
	check(err)
	va, vb := settled(ctx, a, 2), settled(ctx, b, 2)
	fmt.Printf("A settled %d messages\n", len(va))
	fmt.Printf("B settled %d messages\n", len(vb))
	fmt.Println("same order:", fmt.Sprint(va) == fmt.Sprint(vb))
}

// settled waits until the braid's settled view holds n messages and returns them in the braid's order.
func settled(ctx context.Context, b *kolobraid.Braid, n int) []string {
	for {
		s := b.Settled()
		var msgs []string
		for _, v := range s.Scan([]byte("msg/"), []byte("msg0")) {
			msgs = append(msgs, string(v))
		}
		s.Close()
		if len(msgs) >= n {
			return msgs
		}
		if _, err := b.Next(ctx); err != nil {
			log.Fatal(err)
		}
	}
}

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
