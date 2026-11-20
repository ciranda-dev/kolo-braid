package dag

import (
	"fmt"
	"math"

	kolocodec "github.com/ciranda-dev/kolo-codec"
)

// maxSeg keeps segment numbers inside an int on 32-bit platforms.
const maxSeg = math.MaxInt32

// Record is the clock part of a placed entry's index record: its segment and index, and its clock either in full (Full) or
// as the components it raises over its strand predecessor's clock. Comps are strictly ascending by segment, never the own
// segment, and nonzero; the own component is always Index + 1 and is not stored.
type Record struct {
	Seg   int
	Index uint64
	Full  bool
	Comps []Comp
}

// Encode returns uvarint seg, uvarint index, bool full, uvarint n, then n times uvarint segment and uvarint value.
func (r Record) Encode() []byte {
	var e kolocodec.Encoder
	e.PutUvarint(uint64(r.Seg))
	e.PutUvarint(r.Index)
	e.PutBool(r.Full)
	e.PutUvarint(uint64(len(r.Comps)))
	for _, c := range r.Comps {
		e.PutUvarint(uint64(c.Seg))
		e.PutUvarint(c.Value)
	}
	return e.Bytes()
}

// DecodeRecord parses an encoded Record. It returns an error wrapping ErrMalformed for a non-canonical encoding, trailing
// bytes, components out of order, on the own segment or zero, and segment numbers or an index out of range.
func DecodeRecord(b []byte) (Record, error) {
	d := kolocodec.NewDecoder(b)
	fail := func(err error) (Record, error) { return Record{}, fmt.Errorf("%w: %w", ErrMalformed, err) }
	seg, err := d.UvarintMax(maxSeg)
	if err != nil {
		return fail(err)
	}
	r := Record{Seg: int(seg)}
	if r.Index, err = d.UvarintMax(math.MaxUint64 - 1); err != nil {
		return fail(err)
	}
	if r.Full, err = d.Bool(); err != nil {
		return fail(err)
	}
	// each component takes at least two bytes
	n, err := d.UvarintMax(uint64(d.Len() / 2))
	if err != nil {
		return fail(err)
	}
	if n > 0 {
		r.Comps = make([]Comp, 0, n)
	}
	for range n {
		s, err := d.UvarintMax(maxSeg)
		if err != nil {
			return fail(err)
		}
		v, err := d.Uvarint()
		if err != nil {
			return fail(err)
		}
		switch {
		case int(s) == r.Seg:
			return fail(fmt.Errorf("component on the own segment"))
		case v == 0:
			return fail(fmt.Errorf("zero component"))
		case len(r.Comps) > 0 && r.Comps[len(r.Comps)-1].Seg >= int(s):
			return fail(fmt.Errorf("components not strictly ascending"))
		}
		r.Comps = append(r.Comps, Comp{Seg: int(s), Value: v})
	}
	if err := d.Done(); err != nil {
		return fail(err)
	}
	return r, nil
}

// Clock returns the entry's clock: from its components alone when Full, else raised over prev, the clock of its strand
// predecessor in the same segment. It returns an error wrapping ErrMalformed when a segment is not below segments, the size
// of the segment table, or prev is not the clock of the entry before it.
func (r Record) Clock(prev Clock, segments int) (Clock, error) {
	if r.Seg < 0 || r.Seg >= segments || len(prev) > segments || r.Index == math.MaxUint64 {
		return nil, fmt.Errorf("%w: record outside the segment table: %w", ErrMalformed, ErrOutsideTable)
	}
	if !r.Full && prev.At(r.Seg) != r.Index {
		return nil, fmt.Errorf("%w: prev is not the clock of the entry before", ErrMalformed)
	}
	var c Clock
	if !r.Full {
		c = append(c, prev...)
	}
	for _, p := range r.Comps {
		if p.Seg < 0 || p.Seg >= segments {
			return nil, fmt.Errorf("%w: record outside the segment table: %w", ErrMalformed, ErrOutsideTable)
		}
		c = raise(c, p.Seg, p.Value)
	}
	return raise(c, r.Seg, r.Index+1), nil
}
