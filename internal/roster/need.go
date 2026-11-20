package roster

import (
	"math"

	"github.com/ciranda-dev/kolo-braid/internal/dag"
)

// Needs tracks, per strand, one more than the highest index that an entry passed to Note holds or links: how far that
// strand must be fetched for those entries to be placed.
type Needs struct {
	m map[StrandID]uint64
}

// Note records the entry at index of strand and the entries it links.
func (n *Needs) Note(strand StrandID, index uint64, links []dag.Link) {
	if n.m == nil {
		n.m = map[StrandID]uint64{}
	}
	n.raise(strand, index)
	for _, l := range links {
		n.raise(l.Strand, l.Index)
	}
}

func (n *Needs) raise(s StrandID, index uint64) {
	if index < math.MaxUint64 && n.m[s] < index+1 {
		n.m[s] = index + 1
	}
}

// Linked returns one more than the highest index of s that was noted, 0 when none was.
func (n *Needs) Linked(s StrandID) uint64 { return n.m[s] }

// Length returns how many entries of s to fetch: what was noted, and for an active strand at least its settled length
// plus ahead (MaxPullAhead). A removed strand gets nothing that no entry needs.
func (n *Needs) Length(s StrandID, active bool, settled, ahead uint64) uint64 {
	need := n.m[s]
	if active {
		lead := settled + ahead
		if lead < settled {
			lead = math.MaxUint64
		}
		need = max(need, lead)
	}
	return need
}
