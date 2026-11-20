package kolobraid

import (
	"bytes"
	"testing"

	"github.com/ciranda-dev/kolo-braid/internal/dag"
	kololog "github.com/ciranda-dev/kolo-log"
)

// FuzzDecodeMeta feeds every stored record decoder of the braid core: the meta record, strand, pivot, config, settlement state,
// position, entry index and second-history records. Nothing may panic, and a record that decodes encodes back to the same bytes.
func FuzzDecodeMeta(f *testing.F) {
	m := meta{Group: GroupID{1}, FounderKey: make([]byte, 32), FounderStrand: kololog.LogID{2}, FounderHeader: []byte("h"),
		OwnStrand: kololog.LogID{3}, Anchor: &anchorRec{Pivot: EntryID{4}, Config: 2}}
	f.Add(byte(0), m.encode())
	f.Add(byte(1), strandRec{Key: make([]byte, 32), Header: []byte("hdr")}.encode())
	f.Add(byte(2), pivotRec{ID: EntryID{5}, Config: 1, Round: 6, Activates: true, Clock: dag.Clock{3, 0, 7}, Next: 40}.encode())
	f.Add(byte(3), configRec{Rule: Majority, Seats: []seatRec{{Key: make([]byte, 32), Strand: kololog.LogID{9}}}}.encode())
	f.Add(byte(4), released{Pivots: 2, Next: 9, Frontier: dag.Clock{4}, Segments: []dag.Segment{{Strand: dag.StrandID{1},
		Parent: -1, Released: 4, Base: dag.Clock{4}, First: dag.ID{8}}}, Floors: map[kololog.LogID]uint64{{1}: 4}, Settle: []byte("s")}.encode())
	f.Add(byte(5), posRec{Strand: kololog.LogID{1}, Index: 3, ID: EntryID{2}, Void: true}.encode())
	f.Add(byte(6), encodeSegEntry(EntryID{1}, dag.Other, dag.Record{Seg: 1, Index: 4, Comps: []dag.Comp{{Seg: 0, Value: 2}}}.Encode()))
	f.Add(byte(7), append(make([]byte, 72), 1, 2, 3))
	f.Fuzz(func(t *testing.T, kind byte, b []byte) {
		var out []byte
		var err error
		switch kind % 8 {
		case 0:
			var v meta
			if v, err = decodeMeta(b); err == nil {
				out = v.encode()
			}
		case 1:
			var v strandRec
			if v, err = decodeStrandRec(b); err == nil {
				out = v.encode()
			}
		case 2:
			var v pivotRec
			if v, err = decodePivotRec(b); err == nil {
				out = v.encode()
			}
		case 3:
			var v configRec
			if v, err = decodeConfigRec(b); err == nil {
				out = v.encode()
			}
		case 4:
			var v released
			if v, err = decodeReleased(b); err == nil {
				out = v.encode()
			}
		case 5:
			var v posRec
			if v, err = decodePosRec(b); err == nil {
				out = v.encode()
			}
		case 6:
			var v segEntry
			if v, err = decodeSegEntry(b); err == nil {
				out = encodeSegEntry(v.ID, v.Class, v.Record.Encode())
			}
		case 7:
			if len(b) >= len(keyHistory)+72 {
				k := append([]byte(keyHistory), b[:72]...)
				_, _ = decodeHeld(k, b[72:])
			}
			return
		}
		if err == nil && !bytes.Equal(out, b) {
			t.Fatalf("kind %d: decoded record encodes to other bytes", kind%8)
		}
	})
}
