package kolobraid

import (
	"bytes"
	"errors"
	"slices"
	"sort"

	kolostore "github.com/ciranda-dev/kolo-store"
)

type state uint8

const (
	absent  state = iota // the layer says nothing about the key
	present              // the layer holds a value
	deleted              // the layer deleted the key
)

// ver is one write of a key: a nil val is a deletion, so a put always holds a non-nil value.
type ver struct {
	n   int
	val []byte
}

type rng struct {
	n          int
	start, end string
	open       bool
}

func (r rng) covers(k string) bool { return k >= r.start && (r.open || k < r.end) }

// layer is a set of ordered writes over a lower state: per key the versions in write order, and the range deletes. Values are
// never modified after they are stored.
type layer struct {
	vers   map[string][]ver
	keys   []string // sorted; every key with at least one version
	ranges []rng
	n      int
}

func newLayer() *layer { return &layer{vers: make(map[string][]ver)} }

func (l *layer) addKey(k string) {
	if _, ok := l.vers[k]; ok {
		return
	}
	i := sort.SearchStrings(l.keys, k)
	l.keys = slices.Insert(l.keys, i, k)
}

func (l *layer) put(k string, v []byte) {
	l.addKey(k)
	l.n++
	if v == nil {
		v = []byte{}
	}
	l.vers[k] = append(l.vers[k], ver{n: l.n, val: v})
}

func (l *layer) del(k string) {
	l.addKey(k)
	l.n++
	l.vers[k] = append(l.vers[k], ver{n: l.n})
}

func (l *layer) delRange(start, end string, open bool) {
	l.n++
	l.ranges = append(l.ranges, rng{n: l.n, start: start, end: end, open: open})
}

func (l *layer) replay(ws []viewWrite) {
	for _, w := range ws {
		switch w.Op {
		case opPut:
			l.put(string(w.A), w.B)
		case opDelete:
			l.del(string(w.A))
		case opDeleteRange:
			l.delRange(string(w.A), string(w.B), len(w.B) == 0)
		}
	}
}

// unwind undoes the writes of one entry, which must be the most recent ones.
func (l *layer) unwind(ws []viewWrite) {
	for i := len(ws) - 1; i >= 0; i-- {
		w := ws[i]
		l.n--
		if w.Op == opDeleteRange {
			l.ranges = l.ranges[:len(l.ranges)-1]
			continue
		}
		k := string(w.A)
		vs := l.vers[k][:len(l.vers[k])-1]
		if len(vs) > 0 {
			l.vers[k] = vs
			continue
		}
		delete(l.vers, k)
		j := sort.SearchStrings(l.keys, k)
		l.keys = slices.Delete(l.keys, j, j+1)
	}
}

// lookup returns what the layer says about k: a value, a deletion, or nothing.
func (l *layer) lookup(k string) ([]byte, state) {
	vs := l.vers[k]
	newest := 0
	if len(vs) > 0 {
		newest = vs[len(vs)-1].n
	}
	for i := len(l.ranges) - 1; i >= 0 && l.ranges[i].n > newest; i-- {
		if l.ranges[i].covers(k) {
			return nil, deleted
		}
	}
	switch {
	case len(vs) == 0:
		return nil, absent
	case vs[len(vs)-1].val == nil:
		return nil, deleted
	}
	return vs[len(vs)-1].val, present
}

// inRange appends the keys with a version in [start, end) to dst.
func (l *layer) inRange(dst []string, start, end string, open bool) []string {
	i := sort.SearchStrings(l.keys, start)
	j := len(l.keys)
	if !open {
		j = sort.SearchStrings(l.keys, end)
	}
	if i < j {
		dst = append(dst, l.keys[i:j]...)
	}
	return dst
}

// freeze returns an independent copy of what the layer says about [start, end): the newest version of each key in it and every
// range delete.
func (l *layer) freeze(start, end string, open bool) *layer {
	f := &layer{vers: make(map[string][]ver), ranges: slices.Clone(l.ranges), n: l.n}
	for _, k := range l.inRange(nil, start, end, open) {
		vs := l.vers[k]
		f.vers[k] = []ver{vs[len(vs)-1]}
		f.keys = append(f.keys, k)
	}
	return f
}

func (l *layer) clone() *layer {
	c := &layer{vers: make(map[string][]ver, len(l.vers)), keys: slices.Clone(l.keys), ranges: slices.Clone(l.ranges), n: l.n}
	for k, vs := range l.vers {
		c.vers[k] = slices.Clone(vs)
	}
	return c
}

// layerReader reads the view through layers, newest first, and then the durable pairs, which db holds without their prefix.
type layerReader struct {
	layers []*layer
	db     kolostore.Backend
}

// get returns the value of key; it is not a copy.
func (r *layerReader) get(key []byte) ([]byte, bool, error) {
	k := string(key)
	for _, l := range r.layers {
		switch v, st := l.lookup(k); st {
		case present:
			return v, true, nil
		case deleted:
			return nil, false, nil
		}
	}
	v, err := r.db.Get(key)
	if errors.Is(err, kolostore.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return v, true, nil
}

const (
	pageCount = 128
	pageBytes = 1 << 20
)

type pair struct {
	k string
	v []byte
}

// scanner merges layers and durable pairs in ascending key order, one durable page at a time. layers is asked again for every page
// and everything runs under lock, so a reader of a live view stays consistent.
type scanner struct {
	layers       func() []*layer // newest first
	db           kolostore.Backend
	start, end   []byte
	open         bool // no upper bound
	lock, unlock func()
	alive        func() bool
	onErr        func(error)
}

func (s *scanner) run(yield func([]byte, []byte) bool) {
	if !s.open && bytes.Compare(s.start, s.end) >= 0 {
		return
	}
	from := slices.Clone(s.start)
	for s.alive() {
		page, last, more, err := s.window(from)
		if err != nil {
			s.onErr(err)
			return
		}
		for _, p := range page {
			if !s.alive() || !yield([]byte(p.k), slices.Clone(p.v)) {
				return
			}
		}
		if !more {
			return
		}
		from = append(last, 0)
	}
}

// window resolves the keys from from up to the end of one durable page; more is false when the range is exhausted.
func (s *scanner) window(from []byte) (out []pair, last []byte, more bool, err error) {
	if s.lock != nil {
		s.lock()
		defer s.unlock()
	}
	// checked again under the lock: a Close after the check of run may have deleted what the scan reads
	if !s.alive() {
		return nil, nil, false, nil
	}
	var to []byte
	if !s.open {
		to = s.end
	}
	var dk, dv [][]byte
	size := 0
	err = s.db.Scan(from, to, false, pageCount, func(k, v []byte) bool {
		dk, dv = append(dk, k), append(dv, v)
		size += len(k) + len(v)
		return size < pageBytes
	})
	if err != nil {
		return nil, nil, false, err
	}
	more = len(dk) == pageCount || (len(dk) > 0 && size >= pageBytes)
	hi, open := string(s.end), s.open
	if more {
		last = dk[len(dk)-1]
		hi, open = string(last)+"\x00", false
	}
	layers := s.layers()
	var cands []string
	for _, l := range layers {
		cands = l.inRange(cands, string(from), hi, open)
	}
	slices.Sort(cands)
	cands = slices.Compact(cands)
	i, j := 0, 0
	for i < len(cands) || j < len(dk) {
		var k string
		var dval []byte
		hasDurable := false
		switch {
		case j >= len(dk) || (i < len(cands) && cands[i] < string(dk[j])):
			k = cands[i]
			i++
		case i >= len(cands) || cands[i] > string(dk[j]):
			k, dval, hasDurable = string(dk[j]), dv[j], true
			j++
		default:
			k, dval, hasDurable = cands[i], dv[j], true
			i++
			j++
		}
		if v, ok := resolve(layers, k, dval, hasDurable); ok {
			out = append(out, pair{k, v})
		}
	}
	return out, last, more, nil
}

// resolve returns the value of k through layers, falling back to the durable value.
func resolve(layers []*layer, k string, durable []byte, has bool) ([]byte, bool) {
	for _, l := range layers {
		switch v, st := l.lookup(k); st {
		case present:
			return v, true
		case deleted:
			return nil, false
		}
	}
	return durable, has
}
