// Command removewriter removes a writer from a braid on 127.0.0.1: the removed writer keeps what it received, its strand is
// no longer served to it, and it can join again only with a new strand. Every directory lives under the first argument.
package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"slices"
	"time"

	kolobraid "github.com/ciranda-dev/kolo-braid"
	koloid "github.com/ciranda-dev/kolo-id"
	kololog "github.com/ciranda-dev/kolo-log"
	kolosecure "github.com/ciranda-dev/kolo-secure"
	kolostore "github.com/ciranda-dev/kolo-store"
	kolosync "github.com/ciranda-dev/kolo-sync"
)

// apply keeps every data entry under its position.
func apply(tx *kolobraid.Tx, e *kolobraid.Entry) error {
	if e.Kind != kolobraid.KindData {
		return nil
	}
	return tx.Put(binary.BigEndian.AppendUint64([]byte("msg/"), e.Position), e.Body)
}

func main() {
	if len(os.Args) != 2 {
		log.Fatal("usage: removewriter <temp dir>")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
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

	// A creates the braid and admits B; A serves every connection that arrives on one kolo-secure listener.
	a, err := kolobraid.Create(ctx, idA, kolobraid.GroupConfig{App: "chat/v1"}, optsA)
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
	l, err := kolosecure.Listen(ctx, "127.0.0.1:0", idA, nil)
	check(err)
	defer l.Close()
	go func() {
		for {
			c, err := l.Accept(ctx)
			if err != nil {
				return
			}
			go a.ServeConn(ctx, c)
		}
	}()
	conn, err := kolosecure.Dial(ctx, l.Addr().String(), idB, idA.PublicKey(), nil)
	check(err)
	go b.ServeConn(ctx, conn)

	_, err = b.Append(ctx, []byte("from B"))
	check(err)
	settled(ctx, a, 1)
	settled(ctx, b, 1)
	fmt.Println("B wrote a message and both settled it")

	// B reads A's strand over its connection until A settles B's removal.
	var strand kololog.LogID
	for o, err := range b.Entries(0) {
		check(err)
		if o.Kind == kolobraid.KindGenesis {
			strand = o.Strand
			break
		}
	}
	_, err = kolosync.FetchHead(ctx, conn, idA.PublicKey(), strand)
	check(err)
	fmt.Println("B reads A's strand: ok")

	_, err = a.RemoveMember(ctx, idB.PublicKey())
	check(err)
	for isMember(a, idB) {
		_, err := a.Next(ctx)
		check(err)
	}
	fmt.Println("A removed B")
	_, err = kolosync.FetchHead(ctx, conn, idA.PublicKey(), strand)
	fmt.Println("B asks A for A's strand again, refused with ErrNotFound:", errors.Is(err, kolosync.ErrNotFound))
	fmt.Println("A keeps B's earlier message:", len(settled(ctx, a, 1)))
	fmt.Println("B keeps what it received:", len(settled(ctx, b, 1)))

	// B cannot join again with the strand it used: it asks again with a new one.
	_, err = a.Admit(ctx, idB.PublicKey(), request, nil)
	fmt.Println("A admits B's old strand again, refused with ErrKnownStrand:", errors.Is(err, kolobraid.ErrKnownStrand))
	b.Close()
	conn.Close()
	again, err := kolobraid.NewJoin(ctx, idB, options("b-again"))
	check(err)
	request, err = again.Request(nil)
	check(err)
	welcome, err = a.Admit(ctx, idB.PublicKey(), request, nil)
	check(err)
	b2, err := again.Complete(ctx, welcome)
	check(err)
	defer b2.Close()
	conn2, err := kolosecure.Dial(ctx, l.Addr().String(), idB, idA.PublicKey(), nil)
	check(err)
	defer conn2.Close()
	go b2.ServeConn(ctx, conn2)
	_, err = b2.Append(ctx, []byte("B is back"))
	check(err)
	fmt.Println("A settled", len(settled(ctx, a, 2)), "messages after B joined with a new strand")
}

// isMember reports whether the settled roster of b holds id's key.
func isMember(b *kolobraid.Braid, id *koloid.Identity) bool {
	s := b.Settled()
	defer s.Close()
	return slices.ContainsFunc(s.Members(), func(m kolobraid.Member) bool { return bytes.Equal(m.Key, id.PublicKey()) })
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
