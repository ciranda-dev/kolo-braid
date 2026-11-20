// Package kolobraidtest checks that an apply function keeps the contract of kolobraid: its effect depends only on the view before
// the entry, the entry and the roster. It replays a recorded history through many settlement paths and delivery orders and
// compares running digests.
package kolobraidtest

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"

	kolobraid "github.com/ciranda-dev/kolo-braid"
	kolostore "github.com/ciranda-dev/kolo-store"

	ireplay "github.com/ciranda-dev/kolo-braid/internal/replay"
)

// TB is the part of testing.TB that the checks use.
type TB interface {
	Helper()
	Fatalf(format string, args ...any)
}

// rounds is how many random settlement paths CheckApply tries.
const rounds = 24

// opts lets the whole history sit in the tail, whatever its length.
func opts(chunk int) ireplay.Options {
	return ireplay.Options{Chunk: chunk, MaxTail: 1 << 20, MaxTailBytes: 4 << 30}
}

var newReplayer = ireplay.Get[kolobraid.ApplyFunc, []kolobraid.Member, kolobraid.Entry, kolobraid.Digest, *kolobraid.Snapshot]()

// CheckApply replays entries, which are in their final order, through many paths and fails t when two of them end with a
// different running digest or view: single and random batch replays, tentative application with a reorganisation and
// promotion, and restarts between batches. Entries without an ID get one; the keys that wrote them form the roster, each an
// admin and a seat, and membership entries never change it, so an apply that reads roles is checked for that roster only.
func CheckApply(t TB, apply kolobraid.ApplyFunc, entries []kolobraid.Entry) {
	t.Helper()
	if len(entries) == 0 {
		t.Fatalf("kolobraidtest: no entries")
	}
	es := prepare(entries)
	members := rosterOf(es)
	ref, refContent := replay(t, apply, members, es, nil, 0)
	for i := 0; i < 3; i++ {
		again, content := replay(t, apply, members, es, nil, 0)
		for p := range ref {
			if again[p] != ref[p] {
				t.Fatalf("kolobraidtest: apply is not deterministic: two fresh replays differ at position %d", p)
			}
		}
		if content != refContent {
			t.Fatalf("kolobraidtest: apply is not deterministic: two fresh replays end with different views")
		}
	}
	rng := rand.New(rand.NewPCG(1, uint64(len(es))))
	for r := 0; r < rounds; r++ {
		cuts := cutPoints(rng, len(es))
		chunk := 1 + rng.IntN(20)
		digests, content := replay(t, apply, members, es, cuts, chunk)
		if digests[len(digests)-1] != ref[len(ref)-1] || content != refContent {
			t.Fatalf("kolobraidtest: settling in batches ending at %v with chunks of %d entries ends with another digest or view than one entry at a time", cuts, chunk)
		}
		reorg(t, apply, members, es, ref, refContent, rng, cuts, chunk)
	}
}

// CheckUpgrade replays entries with the old and the new apply function and fails t unless the running digests are equal at every
// position before marker, the position of the entry that starts the new behaviour. The roster is CheckApply's.
func CheckUpgrade(t TB, old, upgraded kolobraid.ApplyFunc, entries []kolobraid.Entry, marker int) {
	t.Helper()
	es := prepare(entries)
	if marker < 0 || marker > len(es) {
		t.Fatalf("kolobraidtest: marker %d outside the %d entries", marker, len(es))
	}
	members := rosterOf(es)
	a, _ := replay(t, old, members, es, nil, 0)
	b, _ := replay(t, upgraded, members, es, nil, 0)
	for p := 0; p < marker; p++ {
		if a[p] != b[p] {
			t.Fatalf("kolobraidtest: the new apply function changes the effect of position %d, before the marker at %d", p, marker)
		}
	}
}

// Divergence is the first position at which two replicas of one history have different running digests.
type Divergence struct {
	Position uint64
	// Replica differs from replica 0 at Position; Want is replica 0's running digest there and Got the other's.
	Replica   int
	Want, Got kolobraid.Digest
}

// String describes the divergence for a test failure.
func (d *Divergence) String() string {
	return fmt.Sprintf("position %d: replica %d has running digest %x, replica 0 has %x", d.Position, d.Replica, d.Got[:8], d.Want[:8])
}

// FindDivergence runs apply on replicas copies of the history entries (in final order), each fed in its own shuffled delivery
// order seeded by seed, and returns the lowest position where a running digest differs from replica 0's, nil when every replica
// agrees, and an error if apply halts. The roster is CheckApply's.
func FindDivergence(apply kolobraid.ApplyFunc, entries []kolobraid.Entry, replicas int, seed uint64) (*Divergence, error) {
	if len(entries) == 0 || replicas < 2 {
		return nil, errors.New("kolobraidtest: FindDivergence needs entries and at least two replicas")
	}
	es := prepare(entries)
	members := rosterOf(es)
	runs := make([][]kolobraid.Digest, replicas)
	for k := range runs {
		ds, err := shuffled(apply, members, es, rand.New(rand.NewPCG(seed, uint64(k))))
		if err != nil {
			return nil, fmt.Errorf("kolobraidtest: replica %d: %w", k, err)
		}
		runs[k] = ds
	}
	return earliest(runs), nil
}

// earliest returns the lowest position where a run's digest differs from run 0's, the lowest replica on a tie.
func earliest(runs [][]kolobraid.Digest) *Divergence {
	var first *Divergence
	for k := 1; k < len(runs); k++ {
		for p := range runs[k] {
			if runs[k][p] != runs[0][p] {
				if first == nil || uint64(p) < first.Position {
					first = &Divergence{Position: uint64(p), Replica: k, Want: runs[0][p], Got: runs[k][p]}
				}
				break
			}
		}
	}
	return first
}

// shuffled is one replica of FindDivergence; it returns the running digest after every settled position.
func shuffled(apply kolobraid.ApplyFunc, members []kolobraid.Member, es []kolobraid.Entry, rng *rand.Rand) ([]kolobraid.Digest, error) {
	b := kolostore.NewMemory()
	defer b.Close()
	r, err := newReplayer(b, apply, members, opts(0))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	held := make([]bool, len(es))
	digests := make([]kolobraid.Digest, 0, len(es))
	settle := func(to int) error {
		for p := len(digests); p < to; p++ {
			if err := r.Settle(es[p : p+1]); err != nil {
				return fmt.Errorf("apply halted at position %d: %w", p, err)
			}
			digests = append(digests, r.Digest())
		}
		return nil
	}
	for _, i := range rng.Perm(len(es)) {
		held[i] = true
		var tail []kolobraid.Entry
		for j := len(digests); j < len(es); j++ {
			if held[j] {
				tail = append(tail, es[j])
			}
		}
		if _, err := r.Tentative(tail); err != nil {
			return nil, fmt.Errorf("tentative apply: %w", err)
		}
		ready := len(digests)
		for ready < len(es) && held[ready] {
			ready++
		}
		if ready > len(digests) && rng.IntN(3) == 0 {
			if err := settle(len(digests) + 1 + rng.IntN(ready-len(digests))); err != nil {
				return nil, err
			}
		}
	}
	if err := settle(len(es)); err != nil {
		return nil, err
	}
	return digests, nil
}

func prepare(entries []kolobraid.Entry) []kolobraid.Entry {
	es := make([]kolobraid.Entry, len(entries))
	seen := map[kolobraid.EntryID]bool{}
	for i, e := range entries {
		if e.ID == (kolobraid.EntryID{}) || seen[e.ID] {
			var b [8]byte
			binary.BigEndian.PutUint64(b[:], uint64(i))
			e.ID = sha256.Sum256(append([]byte("kolobraidtest/entry/"), b[:]...))
		}
		seen[e.ID] = true
		es[i] = e
	}
	return es
}

func rosterOf(es []kolobraid.Entry) []kolobraid.Member {
	var ws []kolobraid.Member
	seen := map[string]bool{}
	for _, e := range es {
		if k := string(e.Writer); !seen[k] {
			seen[k] = true
			ws = append(ws, kolobraid.Member{Key: ed25519.PublicKey(e.Writer), Roles: kolobraid.RoleAdmin, Seat: true})
		}
	}
	return ws
}

func cutPoints(rng *rand.Rand, n int) []int {
	var cuts []int
	for at := 0; at < n; {
		at = min(n, at+1+rng.IntN(max(1, n/4)))
		cuts = append(cuts, at)
	}
	return cuts
}

// replay settles es in batches that end at cuts (one entry each when nil), reopening the view between batches when chunk is set, and
// returns the running digest after every position (only after the last one of a batch when cuts is set) and the content digest.
func replay(t TB, apply kolobraid.ApplyFunc, members []kolobraid.Member, es []kolobraid.Entry, cuts []int, chunk int) ([]kolobraid.Digest, kolobraid.Digest) {
	t.Helper()
	b := kolostore.NewMemory()
	defer b.Close()
	r, err := newReplayer(b, apply, members, opts(chunk))
	if err != nil {
		t.Fatalf("kolobraidtest: %v", err)
	}
	defer r.Close()
	digests := make([]kolobraid.Digest, len(es))
	if cuts == nil {
		for i := range es {
			if err := r.Settle(es[i : i+1]); err != nil {
				t.Fatalf("kolobraidtest: apply halted at position %d: %v", i, err)
			}
			digests[i] = r.Digest()
		}
	} else {
		from := 0
		for _, to := range cuts {
			if err := r.Settle(es[from:to]); err != nil {
				t.Fatalf("kolobraidtest: apply halted in the batch ending at %d: %v", to, err)
			}
			digests[to-1] = r.Digest()
			from = to
			if err := r.Reopen(); err != nil {
				t.Fatalf("kolobraidtest: %v", err)
			}
		}
	}
	content, err := r.Content()
	if err != nil {
		t.Fatalf("kolobraidtest: %v", err)
	}
	return digests, content
}

// reorg applies a shuffled tail, reorganises it to the true order and settles through promotion; the tail digest and the final digest
// and view must equal the reference.
func reorg(t TB, apply kolobraid.ApplyFunc, members []kolobraid.Member, es []kolobraid.Entry, ref []kolobraid.Digest, refContent kolobraid.Digest, rng *rand.Rand, cuts []int, chunk int) {
	t.Helper()
	b := kolostore.NewMemory()
	defer b.Close()
	r, err := newReplayer(b, apply, members, opts(chunk))
	if err != nil {
		t.Fatalf("kolobraidtest: %v", err)
	}
	defer r.Close()
	wrong := append([]kolobraid.Entry(nil), es...)
	keep := rng.IntN(len(es) + 1)
	rng.Shuffle(len(wrong)-keep, func(i, j int) { wrong[keep+i], wrong[keep+j] = wrong[keep+j], wrong[keep+i] })
	if _, err := r.Tentative(wrong); err != nil {
		t.Fatalf("kolobraidtest: tentative apply: %v", err)
	}
	if n, err := r.Tentative(es); err != nil || n != len(es) {
		t.Fatalf("kolobraidtest: after a reorganisation %d of %d entries applied: %v", n, len(es), err)
	}
	if r.TailDigest() != ref[len(ref)-1] {
		t.Fatalf("kolobraidtest: a reorganisation at position %d and a replay end with another digest than a fresh replay", keep)
	}
	from := 0
	for _, to := range cuts {
		if err := r.Settle(es[from:to]); err != nil {
			t.Fatalf("kolobraidtest: settling over the tail: %v", err)
		}
		from = to
	}
	content, err := r.Content()
	if err != nil {
		t.Fatalf("kolobraidtest: %v", err)
	}
	if r.Digest() != ref[len(ref)-1] || content != refContent {
		t.Fatalf("kolobraidtest: promotion of tentative entries ends with another digest or view than a fresh replay")
	}
}
