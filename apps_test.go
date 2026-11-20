package kolobraid_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
	"testing"

	kolobraid "github.com/ciranda-dev/kolo-braid"
	"github.com/ciranda-dev/kolo-braid/kolobraidtest"
)

// These applications are built on apply alone, with no change to the library: the merge rule is theirs.

// kvApp: "set k v" and "del k"; the last effective operation in the order wins.
func kvApp(tx *kolobraid.Tx, e *kolobraid.Entry) error {
	if e.Kind != kolobraid.KindData {
		return nil
	}
	op, rest, _ := strings.Cut(string(e.Body), " ")
	switch op {
	case "set":
		k, v, ok := strings.Cut(rest, " ")
		if !ok {
			return kolobraid.Reject("set needs a key and a value")
		}
		return tx.Put([]byte("kv/"+k), []byte(v))
	case "del":
		return tx.Delete([]byte("kv/" + rest))
	}
	return kolobraid.Reject("unknown operation")
}

// chatApp keeps messages by position and a per-author count; only active members' messages are shown.
func chatApp(tx *kolobraid.Tx, e *kolobraid.Entry) error {
	if e.Kind != kolobraid.KindData {
		return nil
	}
	if len(e.Body) == 0 {
		return kolobraid.Reject("empty message")
	}
	if err := tx.Put(binary.BigEndian.AppendUint64([]byte("msg/"), e.Position), e.Body); err != nil {
		return err
	}
	k := []byte(fmt.Sprintf("by/%x", e.Writer[:4]))
	n := 0
	if v, err := tx.Get(k); err == nil {
		n, _ = strconv.Atoi(string(v))
	}
	return tx.Put(k, []byte(strconv.Itoa(n+1)))
}

// counterApp adds signed integers and refuses to go below zero.
func counterApp(tx *kolobraid.Tx, e *kolobraid.Entry) error {
	if e.Kind != kolobraid.KindData {
		return nil
	}
	d, err := strconv.Atoi(string(e.Body))
	if err != nil {
		return kolobraid.Reject("not a number")
	}
	n := 0
	if v, err := tx.Get([]byte("n")); err == nil {
		n, _ = strconv.Atoi(string(v))
	}
	if n+d < 0 {
		return kolobraid.Reject("below zero")
	}
	return tx.Put([]byte("n"), []byte(strconv.Itoa(n+d)))
}

func TestAppsOnApply(t *testing.T) {
	apps := []struct {
		name   string
		apply  kolobraid.ApplyFunc
		bodies []string
		check  func(t *testing.T, s *kolobraid.Snapshot)
	}{
		{"kv", kvApp, []string{"set a 1", "set b 2", "del a", "set c 3", "bogus", "set a 4"}, func(t *testing.T, s *kolobraid.Snapshot) {
			if v, _ := s.Get([]byte("kv/a")); string(v) != "4" {
				t.Fatalf("kv/a = %q", v)
			}
		}},
		{"chat", chatApp, []string{"hello", "", "how are you", "fine"}, func(t *testing.T, s *kolobraid.Snapshot) {
			n := 0
			for range s.Scan([]byte("msg/"), []byte("msg0")) {
				n++
			}
			if n != 3 {
				t.Fatalf("%d messages", n)
			}
		}},
		{"counter", counterApp, []string{"5", "-3", "-4", "x", "2"}, func(t *testing.T, s *kolobraid.Snapshot) {
			if v, _ := s.Get([]byte("n")); string(v) != "4" {
				t.Fatalf("n = %q", v)
			}
		}},
	}
	for _, app := range apps {
		t.Run(app.name, func(t *testing.T) {
			b, done := kolobraid.NewTestBraid(t, app.apply)
			for _, body := range app.bodies {
				if _, err := b.Append(context.Background(), []byte(body)); err != nil {
					t.Fatal(err)
				}
			}
			done()
			s := b.Settled()
			defer s.Close()
			app.check(t, s)
			var es []kolobraid.Entry
			for o, err := range b.Entries(0) {
				if err != nil {
					t.Fatal(err)
				}
				es = append(es, o.Entry)
			}
			kolobraidtest.CheckApply(t, app.apply, es)
		})
	}
}
