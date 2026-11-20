package kolobraid

import (
	"bytes"
	"encoding/binary"
	"slices"
	"testing"

	"github.com/ciranda-dev/kolo-braid/internal/dag"
)

// TestOrderKeyIsTheFormat checks that the DAG's Merge breaks ties by the order key of the format.
func TestOrderKeyIsTheFormat(t *testing.T) {
	seed := [32]byte{0xc3, 0x01}
	var runs []dag.Run
	var ids []dag.ID
	for i := range 300 {
		var id dag.ID
		binary.BigEndian.PutUint64(id[:], uint64(i)*0x9e3779b97f4a7c15)
		ids = append(ids, id)
		it := dag.Item{ID: id, Class: dag.Other, Seg: i}
		done := false
		runs = append(runs, dag.Run{Lo: 0, Hi: 1, Next: func() (dag.Item, error) {
			if done {
				t.Fatal("read twice")
			}
			done = true
			return it, nil
		}})
	}
	var got []dag.ID
	if err := dag.Merge(runs, seed, func(it dag.Item) error { got = append(got, it.ID); return nil }); err != nil {
		t.Fatal(err)
	}
	slices.SortFunc(ids, func(a, b dag.ID) int {
		ka, kb := orderKey(seed, EntryID(a)), orderKey(seed, EntryID(b))
		return bytes.Compare(ka[:], kb[:])
	})
	if !slices.Equal(got, ids) {
		t.Fatal("Merge does not order by the order key")
	}
}
