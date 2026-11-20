// Package replay hands kolobraidtest and the module's tests the replayer of the root package, which applies given entries to a
// view without a braid, without making it part of the root package's API.
package replay

import (
	"fmt"

	kolostore "github.com/ciranda-dev/kolo-store"
)

// Options are the limits of a replayer; a zero field takes the default of a braid.
type Options struct {
	MaxBody         int
	CheckpointEvery int
	MaxTail         int
	MaxTailBytes    int64
	// Chunk is how many settled entries one durable commit holds; 0 means 256.
	Chunk int
	// Initial is the running digest before the first entry; Next is the first position.
	Initial [32]byte
	Next    uint64
}

// Replayer is the root package's replayer over its entry, digest and snapshot types.
type Replayer[E, D, S any] interface {
	Reopen() error
	Tentative(entries []E) (int, error)
	Settle(entries []E) error
	Digest() D
	TailDigest() D
	Position() uint64
	Content() (D, error)
	Settled() S
	View() S
	Close() error
}

// New is set by the root package, when it is imported, to its func(kolostore.Backend, ApplyFunc, []Member, Options)
// (Replayer[Entry, Digest, *Snapshot], error).
var New any

// Get returns New with its types; a caller that names other types panics at once, at its own package init.
func Get[A, M, E, D, S any]() func(kolostore.Backend, A, M, Options) (Replayer[E, D, S], error) {
	f, ok := New.(func(kolostore.Backend, A, M, Options) (Replayer[E, D, S], error))
	if !ok {
		panic(fmt.Sprintf("kolobraid: the replay hook is a %T", New))
	}
	return f
}
