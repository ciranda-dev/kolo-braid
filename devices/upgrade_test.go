package devices_test

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"os"
	"testing"

	kolostore "github.com/ciranda-dev/kolo-store"

	kolobraid "github.com/ciranda-dev/kolo-braid"
	"github.com/ciranda-dev/kolo-braid/devices"
	ireplay "github.com/ciranda-dev/kolo-braid/internal/replay"
)

var newReplayer = ireplay.Get[kolobraid.ApplyFunc, []kolobraid.Member, kolobraid.Entry, kolobraid.Digest, *kolobraid.Snapshot]()

// v010History is a history of devices records and the running digest that kolo-braid v0.1.0 had after each position.
type v010History struct {
	Writer  []byte
	Bodies  [][]byte
	Digests [][]byte
}

// runningDigests settles one entry per body, with a bound of 2 approver revocations per device, and returns the running digests.
func runningDigests(t testing.TB, writer []byte, bodies [][]byte) [][]byte {
	t.Helper()
	db := kolostore.NewMemory()
	defer db.Close()
	members := []kolobraid.Member{{Key: writer, Roles: kolobraid.RoleAdmin, Seat: true}}
	r, err := newReplayer(db, applyFn(newStore(t, &devices.Options{MaxRevocationsPerDevice: 2})), members, ireplay.Options{MaxTail: 1 << 20, MaxTailBytes: 4 << 30})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	var out [][]byte
	for i, body := range bodies {
		e := kolobraid.Entry{ID: sha256.Sum256(binary.BigEndian.AppendUint64(nil, uint64(i))), Writer: writer, Kind: kolobraid.KindData, Index: uint64(i), Body: body}
		if err := r.Settle([]kolobraid.Entry{e}); err != nil {
			t.Fatalf("position %d: %v", i, err)
		}
		d := r.Digest()
		out = append(out, d[:])
	}
	return out
}

// TestV010HistoryReplaysUnchanged: approver revocations write the keys of v0.1.0 only, so the running digests of a v0.1.0 history, which checkpoints claim, do not change.
func TestV010HistoryReplaysUnchanged(t *testing.T) {
	raw, err := os.ReadFile("testdata/v010-history.json")
	if err != nil {
		t.Fatal(err)
	}
	var h v010History
	if err := json.Unmarshal(raw, &h); err != nil {
		t.Fatal(err)
	}
	got := runningDigests(t, h.Writer, h.Bodies)
	for p := range h.Digests {
		if string(got[p]) != string(h.Digests[p]) {
			t.Fatalf("running digest at position %d is %x, v0.1.0 had %x", p, got[p], h.Digests[p])
		}
	}
}
