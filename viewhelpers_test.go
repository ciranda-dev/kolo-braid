package kolobraid

import (
	"bytes"
	"crypto/ed25519"
	"encoding/binary"
	"errors"
	"fmt"
	"iter"
	"math/rand/v2"
	"slices"
	"sort"
	"strings"
	"sync/atomic"
	"testing"

	kolostore "github.com/ciranda-dev/kolo-store"
)

// vtOp is a scripted operation of the test apply function; the body of a data entry is its encoding.
type vtOp struct {
	kind byte // p put, d delete, r range delete, a append to the old value, c count, x reject, e error, n panic, g get then put
	a, b string
}

func vtBody(ops ...vtOp) []byte {
	var out []byte
	for _, o := range ops {
		out = append(out, o.kind)
		out = binary.AppendUvarint(out, uint64(len(o.a)))
		out = append(out, o.a...)
		out = binary.AppendUvarint(out, uint64(len(o.b)))
		out = append(out, o.b...)
	}
	return out
}

func vtParse(body []byte) []vtOp {
	var ops []vtOp
	for len(body) > 0 {
		o := vtOp{kind: body[0]}
		body = body[1:]
		for i := 0; i < 2; i++ {
			n, k := binary.Uvarint(body)
			body = body[k:]
			s := string(body[:n])
			body = body[n:]
			if i == 0 {
				o.a = s
			} else {
				o.b = s
			}
		}
		ops = append(ops, o)
	}
	return ops
}

// vtApply is a deterministic apply function: it runs the scripted operations of a data entry.
func vtApply(tx *Tx, e *Entry) error {
	if e.Kind != KindData {
		return nil
	}
	for _, o := range vtParse(e.Body) {
		var err error
		switch o.kind {
		case 'p':
			err = tx.Put([]byte(o.a), []byte(o.b))
		case 'd':
			err = tx.Delete([]byte(o.a))
		case 'r':
			err = tx.DeleteRange([]byte(o.a), []byte(o.b))
		case 'a':
			old, gerr := tx.Get([]byte(o.a))
			if gerr != nil && !errors.Is(gerr, ErrNotFound) {
				return gerr
			}
			err = tx.Put([]byte(o.a), append(old, o.b...))
		case 'c':
			n := 0
			if v, gerr := tx.Get([]byte("count")); gerr == nil {
				n = int(binary.BigEndian.Uint32(v))
			}
			err = tx.Put([]byte("count"), binary.BigEndian.AppendUint32(nil, uint32(n+1)))
			if err == nil {
				err = tx.Put(binary.BigEndian.AppendUint64([]byte("pos/"), e.Position), e.ID[:4])
			}
		case 's': // sum of the keys in [a, b) into key "sum"
			n := 0
			for k := range tx.Scan([]byte(o.a), []byte(o.b)) {
				n += len(k)
			}
			err = tx.Put([]byte("sum"), []byte(fmt.Sprint(n)))
		case 'x':
			return Reject(o.a)
		case 'e':
			return errors.New("app failure")
		case 'n':
			panic("app panic")
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func vtID(n int) EntryID {
	var id EntryID
	binary.BigEndian.PutUint64(id[:], uint64(n)+1)
	copy(id[8:], "vt-entry")
	return id
}

func vtEntry(n int, ops ...vtOp) Entry {
	return Entry{ID: vtID(n), Writer: bytes.Repeat([]byte{1}, 32), Index: uint64(n), Kind: KindData, Body: vtBody(ops...)}
}

func vtMembers() []Member {
	return []Member{{Key: bytes.Repeat([]byte{1}, 32), Roles: RoleAdmin, Seat: true}}
}

func vtConfig() viewConfig {
	return viewConfig{MaxBody: 1 << 20, CheckpointEvery: 64, MaxTail: 2048, MaxTailBytes: 64 << 20, Initial: Digest(bytes.Repeat([]byte{9}, 32))}
}

func vtOpen(t testing.TB, b kolostore.Backend, cfg viewConfig, apply ApplyFunc) *view {
	t.Helper()
	v, err := openView(b, cfg, apply)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

type vtRoster []Member

func (r vtRoster) Member(key ed25519.PublicKey) (Member, bool) { return fixedRoster(r).Member(key) }
func (r vtRoster) Members() []Member                           { return r }

func vtStep(e Entry) applyStep { return applyStep{Entry: e, Roster: vtRoster(vtMembers())} }

func vtSteps(es []Entry) iter.Seq2[applyStep, error] {
	return func(yield func(applyStep, error) bool) {
		for _, e := range es {
			if !yield(vtStep(e), nil) {
				return
			}
		}
	}
}

// vtModel is a naive reference of the view: a map, written from the definition of the operations.
type vtModel struct{ m map[string]string }

func newVtModel() *vtModel { return &vtModel{m: map[string]string{}} }

func (m *vtModel) clone() *vtModel {
	c := newVtModel()
	for k, v := range m.m {
		c.m[k] = v
	}
	return c
}

// apply runs the entry like vtApply on the model: it returns the writes apply makes, and false for a rejected entry (nothing
// changed).
func (m *vtModel) apply(e Entry, pos uint64) (bool, []viewWrite) {
	if e.Kind != KindData {
		return true, nil
	}
	w := m.clone()
	var ws []viewWrite
	in := func(k, a, b string) bool { return k >= a && (b == "" || k < b) }
	for _, o := range vtParse(e.Body) {
		switch o.kind {
		case 'p':
			ws = append(ws, viewWrite{opPut, []byte(o.a), []byte(o.b)})
			w.m[o.a] = o.b
		case 'd':
			ws = append(ws, viewWrite{Op: opDelete, A: []byte(o.a)})
			delete(w.m, o.a)
		case 'r':
			ws = append(ws, viewWrite{opDeleteRange, []byte(o.a), []byte(o.b)})
			for k := range w.m {
				if in(k, o.a, o.b) {
					delete(w.m, k)
				}
			}
		case 'a':
			w.m[o.a] += o.b
			ws = append(ws, viewWrite{opPut, []byte(o.a), []byte(w.m[o.a])})
		case 'c':
			n := uint32(0)
			if v, ok := w.m["count"]; ok {
				n = binary.BigEndian.Uint32([]byte(v))
			}
			c := binary.BigEndian.AppendUint32(nil, n+1)
			pk := binary.BigEndian.AppendUint64([]byte("pos/"), pos)
			ws = append(ws, viewWrite{opPut, []byte("count"), c}, viewWrite{opPut, pk, slices.Clone(e.ID[:4])})
			w.m["count"] = string(c)
			w.m[string(pk)] = string(e.ID[:4])
		case 's':
			n := 0
			for k := range w.m {
				if in(k, o.a, o.b) {
					n += len(k)
				}
			}
			ws = append(ws, viewWrite{opPut, []byte("sum"), []byte(fmt.Sprint(n))})
			w.m["sum"] = fmt.Sprint(n)
		case 'x':
			return false, nil
		}
	}
	m.m = w.m
	return true, ws
}

func (m *vtModel) dump() []string {
	var out []string
	for k, v := range m.m {
		out = append(out, k+"\x00"+v)
	}
	sort.Strings(out)
	return out
}

func vtDump(seq iter.Seq2[[]byte, []byte]) []string {
	var out []string
	for k, v := range seq {
		out = append(out, string(k)+"\x00"+string(v))
	}
	return out
}

// vtScript returns n scripted entries over a small key space, deterministic in seed. Every kind of operation occurs, rejects and
// range deletes included; no entry fails or panics.
func vtScript(seed uint64, n int) []Entry {
	r := rand.New(rand.NewPCG(seed, 77))
	key := func() string { return fmt.Sprintf("k%02d", r.IntN(24)) }
	out := make([]Entry, n)
	for i := range out {
		var ops []vtOp
		for j := r.IntN(4); j >= 0; j-- {
			switch r.IntN(10) {
			case 0, 1, 2:
				ops = append(ops, vtOp{kind: 'p', a: key(), b: strings.Repeat("v", r.IntN(20))})
			case 3:
				ops = append(ops, vtOp{kind: 'd', a: key()})
			case 4:
				a, b := key(), key()
				if a > b {
					a, b = b, a
				}
				if r.IntN(4) == 0 {
					b = ""
				}
				ops = append(ops, vtOp{kind: 'r', a: a, b: b})
			case 5:
				ops = append(ops, vtOp{kind: 'a', a: key(), b: "+"})
			case 6:
				ops = append(ops, vtOp{kind: 'c'})
			case 7:
				ops = append(ops, vtOp{kind: 's', a: "k0", b: key()})
			case 8:
				if r.IntN(3) == 0 {
					ops = append(ops, vtOp{kind: 'x', a: "no"})
				}
			}
		}
		e := vtEntry(i, ops...)
		switch r.IntN(12) {
		case 0:
			e.Kind = KindPulse
		case 1:
			e.Kind = KindAddMember
		}
		out[i] = e
	}
	return out
}

// vtRun computes the expected digests and view of entries applied in order: the model plus the running digests.
func vtRun(prev Digest, start uint64, es []Entry) ([]Digest, *vtModel) {
	return vtRunLib(prev, start, es, false)
}

// vtRunLib is vtRun for steps that carry the library write of libStep.
func vtRunLib(prev Digest, start uint64, es []Entry, lib bool) ([]Digest, *vtModel) {
	m := newVtModel()
	var ds []Digest
	for i, e := range es {
		pos := start + uint64(i)
		ok, ws := m.apply(e, pos)
		status := statusApplied
		if !ok {
			status = statusRejected
		}
		if lib {
			ws = append([]viewWrite{{Op: opPut, A: []byte("r/last"), B: binary.BigEndian.AppendUint64(nil, pos)}}, ws...)
		}
		prev = runningDigest(prev, pos, e.ID, status, ws)
		ds = append(ds, prev)
	}
	return ds, m
}

var errInjected = errors.New("injected failure")

// vtFault is a backend whose batches fail: at the at-th commit (1 based) the batch is dropped, or applied first when after is set,
// and every later commit fails too, as after a crash. The wrapped backend is the disk that survives.
type vtFault struct {
	kolostore.Backend
	at     int
	after  bool
	n      *atomic.Int64
	getErr *atomic.Bool
}

func newVtFault(b kolostore.Backend, at int, after bool) vtFault {
	return vtFault{Backend: b, at: at, after: after, n: new(atomic.Int64), getErr: new(atomic.Bool)}
}

func (f vtFault) Get(k []byte) ([]byte, error) {
	if f.getErr.Load() {
		return nil, errInjected
	}
	return f.Backend.Get(k)
}

func (f vtFault) Scan(start, end []byte, reverse bool, limit int, fn func(k, v []byte) bool) error {
	if f.getErr.Load() {
		return errInjected
	}
	return f.Backend.Scan(start, end, reverse, limit, fn)
}

func (f vtFault) NewBatch() kolostore.Batch { return vtFaultBatch{f.Backend.NewBatch(), f} }

type vtFaultBatch struct {
	kolostore.Batch
	f vtFault
}

func (b vtFaultBatch) Commit() error {
	n := b.f.n.Add(1)
	switch {
	case b.f.at == 0 || int(n) < b.f.at:
		return b.Batch.Commit()
	case int(n) == b.f.at && b.f.after:
		if err := b.Batch.Commit(); err != nil {
			return err
		}
	}
	return errInjected
}

func vtEq(t testing.TB, what string, got, want []string) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Fatalf("%s:\n got %q\nwant %q", what, got, want)
	}
}
