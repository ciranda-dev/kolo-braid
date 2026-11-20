package kolobraid

import (
	"crypto/ed25519"
	"iter"

	kolostore "github.com/ciranda-dev/kolo-store"

	"github.com/ciranda-dev/kolo-braid/internal/replay"
)

func init() {
	replay.New = func(b kolostore.Backend, apply ApplyFunc, members []Member, o replay.Options) (replay.Replayer[Entry, Digest, *Snapshot], error) {
		r, err := newReplayer(b, apply, members, o)
		if err != nil {
			return nil, err
		}
		return r, nil
	}
}

var _ replay.Replayer[Entry, Digest, *Snapshot] = (*replayer)(nil)

// replayer applies given entries to a view on a backend without a braid: the tail and the settled order are supplied by the
// caller. kolobraidtest and tests of apply functions reach it through internal/replay; a braid does not use it.
type replayer struct {
	b       kolostore.Backend
	apply   ApplyFunc
	cfg     viewConfig
	roster  fixedRoster
	v       *view
	batches uint64
	tail    []EntryID
}

type fixedRoster []Member

func (r fixedRoster) Member(key ed25519.PublicKey) (Member, bool) {
	for _, w := range r {
		if string(w.Key) == string(key) {
			return w, true
		}
	}
	return Member{}, false
}

func (r fixedRoster) Members() []Member { return r }

// newReplayer opens (or creates) the view stored in b, with members as the roster of every entry.
func newReplayer(b kolostore.Backend, apply ApplyFunc, members []Member, opts replay.Options) (*replayer, error) {
	cfg := viewConfig{
		MaxBody: opts.MaxBody, CheckpointEvery: opts.CheckpointEvery, MaxTail: opts.MaxTail, MaxTailBytes: opts.MaxTailBytes,
		Next: opts.Next, Initial: Digest(opts.Initial), chunk: opts.Chunk,
	}
	if cfg.MaxBody == 0 {
		cfg.MaxBody = defaultMaxBody
	}
	if cfg.CheckpointEvery == 0 {
		cfg.CheckpointEvery = defaultCheckpoint
	}
	if cfg.MaxTail == 0 {
		cfg.MaxTail = 2048
	}
	if cfg.MaxTailBytes == 0 {
		cfg.MaxTailBytes = 4 * budgetFactor * int64(cfg.MaxBody)
	}
	r := &replayer{b: b, apply: apply, cfg: cfg, roster: fixedRoster(members)}
	if err := r.Reopen(); err != nil {
		return nil, err
	}
	return r, nil
}

// Reopen closes the view and opens it again from the backend, as a restart does: the tail is lost and the settled state continues
// from the stored progress.
func (r *replayer) Reopen() error {
	if r.v != nil {
		r.v.Close()
	}
	v, err := openView(r.b, r.cfg, r.apply)
	if err != nil {
		return err
	}
	v.setRoster(r.roster)
	r.v, r.tail = v, nil
	r.batches = v.Progress().Batches
	return nil
}

// Tentative makes the tail equal to entries: it keeps the longest prefix that is already applied, and applies the rest. It stops at
// MaxTail and returns how many entries are applied.
func (r *replayer) Tentative(entries []Entry) (int, error) {
	keep := 0
	for keep < len(entries) && keep < len(r.tail) && r.tail[keep] == entries[keep].ID {
		keep++
	}
	if err := r.v.DropTail(keep); err != nil {
		return 0, err
	}
	r.tail = r.tail[:keep]
	for _, e := range entries[keep:] {
		res, err := r.v.ApplyTail(applyStep{Entry: e, Roster: r.roster})
		if err != nil {
			return len(r.tail), err
		}
		if res.Paused {
			break
		}
		r.tail = append(r.tail, e.ID)
	}
	return len(r.tail), nil
}

// Settle settles entries as the next batch. The tail is kept when it started with the same entries.
func (r *replayer) Settle(entries []Entry) error {
	steps := func(yield func(applyStep, error) bool) {
		for _, e := range entries {
			if !yield(applyStep{Entry: e, Roster: r.roster}, nil) {
				return
			}
		}
	}
	_, err := r.v.Settle(r.batches, iter.Seq2[applyStep, error](steps))
	if err != nil {
		return err
	}
	r.batches++
	n := r.v.Tail()
	if n < len(r.tail) {
		r.tail = r.tail[len(r.tail)-n:]
	}
	return nil
}

func (r *replayer) Digest() Digest { return r.v.Progress().Digest }

// TailDigest returns the running digest after the last tail entry, or the settled one when the tail is empty.
func (r *replayer) TailDigest() Digest {
	if n := r.v.Tail(); n > 0 {
		t, _ := r.v.TailEntry(n - 1)
		return t.Digest
	}
	return r.Digest()
}

func (r *replayer) Position() uint64 { return r.v.Progress().Next }

func (r *replayer) Content() (Digest, error) { return r.v.ContentDigest() }

func (r *replayer) Settled() *Snapshot { return r.v.Settled() }

func (r *replayer) View() *Snapshot { return r.v.View() }

func (r *replayer) Close() error {
	if r.v == nil {
		return nil
	}
	err := r.v.Close()
	r.v = nil
	return err
}
