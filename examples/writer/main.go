// Command writer sets up a writer: an identity saved in a file, a kolo-store directory, a braid created once and opened again
// after a restart. Everything lives under the first argument.
package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	kolobraid "github.com/ciranda-dev/kolo-braid"
	koloid "github.com/ciranda-dev/kolo-id"
	kolostore "github.com/ciranda-dev/kolo-store"
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
		log.Fatal("usage: writer <temp dir>")
	}
	ctx := context.Background()
	check(os.Mkdir(filepath.Join(os.Args[1], "id"), 0o700)) // koloid.Save wants a private directory
	idPath := filepath.Join(os.Args[1], "id", "identity")
	dbPath := filepath.Join(os.Args[1], "braid")

	// First run: a new identity, a new braid, one message.
	id, err := koloid.Generate()
	check(err)
	check(id.Save(idPath))
	db, err := kolostore.OpenPebble(dbPath)
	check(err)
	b, err := kolobraid.Create(ctx, id, kolobraid.GroupConfig{App: "notes/v1"},
		kolobraid.Options{Backend: db, App: "notes/v1", Apply: apply})
	check(err)
	_, err = b.Append(ctx, []byte("first"))
	check(err)
	waitMessages(ctx, b, 1)
	fmt.Println("created the braid and wrote 1 message")
	check(b.Close())
	check(db.Close())

	// Second run: the same identity and directory, opened with Open.
	id, err = koloid.Load(idPath)
	check(err)
	db, err = kolostore.OpenPebble(dbPath)
	check(err)
	defer db.Close()
	b, err = kolobraid.Open(ctx, id, kolobraid.Options{Backend: db, App: "notes/v1", Apply: apply})
	check(err)
	defer b.Close()
	fmt.Println("reopened the braid, own strand unconfirmed:", b.Status().Unconfirmed)
	_, err = b.Append(ctx, []byte("second"))
	check(err)
	fmt.Println("messages in the settled view:", strings.Join(waitMessages(ctx, b, 2), ", "))
}

// waitMessages waits until the settled view holds n messages and returns them in the braid's order.
func waitMessages(ctx context.Context, b *kolobraid.Braid, n int) []string {
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
