// Command apply writes to a one-member braid on a kolo-store directory under the first argument and reads the settled view that its
// apply function built.
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
	kolostore "github.com/ciranda-dev/kolo-store"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatal("usage: apply <temp dir>")
	}
	ctx := context.Background()
	id, err := koloid.Generate()
	check(err)
	db, err := kolostore.OpenPebble(filepath.Join(os.Args[1], "store"))
	check(err)
	defer db.Close()
	apply := func(tx *kolobraid.Tx, e *kolobraid.Entry) error {
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
	b, err := kolobraid.Create(ctx, id, kolobraid.GroupConfig{App: "chat/v1"},
		kolobraid.Options{Backend: db, App: "chat/v1", Apply: apply})
	check(err)
	defer b.Close()
	_, err = b.Append(ctx, []byte("hello"))
	check(err)
	last, err := b.Append(ctx, nil)
	check(err)

	// Wait until the last entry is settled and applied, then read what apply built.
	for {
		if settledPast(b, last.ID) {
			break
		}
		if _, err := b.Next(ctx); err != nil {
			log.Fatal(err)
		}
	}
	s := b.Settled()
	defer s.Close()
	count, err := s.Get([]byte("count"))
	check(err)
	fmt.Printf("%s settled message(s)\n", count)
	rejected := 0
	for o, err := range b.Entries(0) {
		check(err)
		if o.Rejected {
			rejected++
		}
	}
	fmt.Printf("%d rejected by apply\n", rejected)
}

// settledPast reports whether the entry with the given ID is in the settled view.
func settledPast(b *kolobraid.Braid, id kolobraid.EntryID) bool {
	for o, err := range b.Entries(0) {
		check(err)
		if o.ID == id {
			return b.Status().Settled > o.Position
		}
	}
	return false
}

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
