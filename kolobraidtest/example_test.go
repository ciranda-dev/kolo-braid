package kolobraidtest_test

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"

	kolobraid "github.com/ciranda-dev/kolo-braid"
	"github.com/ciranda-dev/kolo-braid/kolobraidtest"
)

func ExampleFindDivergence() {
	entries := make([]kolobraid.Entry, 12)
	for i := range entries {
		entries[i] = kolobraid.Entry{Writer: bytes.Repeat([]byte{byte(1 + i%3)}, 32), Kind: kolobraid.KindData,
			Body: []byte(fmt.Sprintf("k%d=%d", i%4, i))}
	}
	// good is pure: its effect depends only on the view, the entry and the roster.
	good := func(tx *kolobraid.Tx, e *kolobraid.Entry) error {
		k, v, _ := strings.Cut(string(e.Body), "=")
		return tx.Put([]byte(k), []byte(v))
	}
	// bad is not: it counts its calls in a variable outside the view, and replicas call it a different number of times.
	calls := 0
	bad := func(tx *kolobraid.Tx, e *kolobraid.Entry) error {
		calls++
		return tx.Put([]byte("calls"), []byte(strconv.Itoa(calls)))
	}
	for _, c := range []struct {
		name  string
		apply kolobraid.ApplyFunc
	}{{"good", good}, {"bad", bad}} {
		d, err := kolobraidtest.FindDivergence(c.apply, entries, 4, 1)
		switch {
		case err != nil:
			fmt.Println(c.name, "halted:", err)
		case d == nil:
			fmt.Println(c.name, "agrees on every replica")
		default:
			fmt.Println(c.name, "diverges at position", d.Position, "on replica", d.Replica)
		}
	}
	// Output:
	// good agrees on every replica
	// bad diverges at position 0 on replica 1
}
