package roster

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"slices"
	"testing"

	"github.com/ciranda-dev/kolo-braid/internal/dag"
	kolocodec "github.com/ciranda-dev/kolo-codec"
	"golang.org/x/crypto/blake2b"
)

func kid(i int) Key {
	var k Key
	binary.BigEndian.PutUint32(k[:], uint32(i)+1)
	k[31] = 0x4b
	return k
}

func sid(i int) StrandID {
	var s StrandID
	binary.BigEndian.PutUint32(s[:], uint32(i)+1)
	s[31] = 0x5
	return s
}

func eid(label string) ID { return blake2b.Sum256([]byte("entry " + label)) }

// item is one test entry: what the roster sees and what the DAG sees.
type item struct {
	name string
	e    Entry
	node dag.Node
}

func link(it *item) dag.Link { return dag.Link{Strand: it.e.Strand, Index: it.e.Index, ID: it.e.ID} }

// braid builds histories: one chain per strand, links named by item.
type braid struct {
	g     Genesis
	items []*item
	byID  map[ID]*item
	head  map[StrandID]*item
}

func newBraid(founder Key, strand StrandID, seeds uint64) *braid {
	b := &braid{byID: map[ID]*item{}, head: map[StrandID]*item{}}
	b.g = Genesis{Group: eid("genesis"), Founder: founder, Strand: strand, Rule: Majority, MaxMembers: 64, MaxSettlers: 7, Seeds: seeds}
	b.addID("g", b.g.Group, strand, Entry{Kind: KindGenesis})
	return b
}

func (b *braid) roster(t testing.TB) *Roster {
	t.Helper()
	r, err := New(b.g)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// add appends an entry named name to strand, linking the given items (at most one per strand, the newest wins).
func (b *braid) add(name string, strand StrandID, e Entry, links ...*item) *item {
	return b.addID(name, eid(name), strand, e, links...)
}

func (b *braid) addID(name string, id ID, strand StrandID, e Entry, links ...*item) *item {
	if _, dup := b.byID[id]; dup {
		panic("duplicate item " + name)
	}
	e.ID, e.Strand = id, strand
	n := dag.Node{ID: id, Strand: strand, Class: Class(e.Kind, e.Malformed)}
	if p := b.head[strand]; p != nil {
		e.Index = p.e.Index + 1
		n.Prev = p.e.ID
	}
	n.Index = e.Index
	if !e.Malformed {
		byStrand := map[StrandID]dag.Link{}
		for _, l := range links {
			if l.e.Strand == strand {
				panic("self link in " + name)
			}
			if old, ok := byStrand[l.e.Strand]; !ok || old.Index < l.e.Index {
				byStrand[l.e.Strand] = link(l)
			}
		}
		for _, l := range byStrand {
			n.Links = append(n.Links, l)
		}
		slices.SortFunc(n.Links, func(a, b dag.Link) int { return bytes.Compare(a.Strand[:], b.Strand[:]) })
	}
	it := &item{name: name, e: e, node: n}
	b.items = append(b.items, it)
	b.byID[id] = it
	b.head[strand] = it
	return it
}

func addW(k Key, s StrandID, roles Role) Entry {
	return Entry{Kind: KindAddMember, Add: Add{Key: k, Strand: s, Roles: roles}}
}
func removeW(k Key) Entry { return Entry{Kind: KindRemoveMember, Target: k} }
func settlersE(rule Rule, keys ...Key) Entry {
	keys = slices.Clone(keys)
	slices.SortFunc(keys, cmpKey)
	return Entry{Kind: KindSettlers, Settlers: Settlers{Rule: rule, Keys: keys}}
}
func evidenceE(k Key, s StrandID) Entry {
	return Entry{Kind: KindEvidence, Accused: Seat{Key: k, Strand: s}}
}
func dataE() Entry { return Entry{Kind: KindData} }

func newTestDAG(t testing.TB) *dag.DAG {
	t.Helper()
	d, err := dag.New(dag.Config{MaxLinks: dag.MaxLinksLimit, MaxPendingBytes: 1 << 40})
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// orderOf inserts the nodes into a new DAG in the order perm gives and returns the order of all of them under seed.
func orderOf(t testing.TB, nodes []dag.Node, perm []int, seed [32]byte) []ID {
	t.Helper()
	d := newTestDAG(t)
	for _, i := range perm {
		if _, err := d.Add(nodes[i]); err != nil {
			t.Fatal(err)
		}
	}
	if d.Len() != len(nodes) {
		t.Fatalf("%d of %d entries placed", d.Len(), len(nodes))
	}
	out := make([]ID, 0, len(nodes))
	if err := d.Order(nil, nil, seed, func(id ID) error { out = append(out, id); return nil }); err != nil {
		t.Fatal(err)
	}
	return out
}

func identity(n int) []int {
	p := make([]int, n)
	for i := range p {
		p[i] = i
	}
	return p
}

func (b *braid) nodes() []dag.Node {
	out := make([]dag.Node, len(b.items))
	for i, it := range b.items {
		out[i] = it.node
	}
	return out
}

// step is the roster after one position.
type step struct {
	id        ID
	reason    Reason
	change    Change
	effective bool
	members   []Member
	rule      Rule
	settlers  []Key
}

func (s step) String() string {
	return fmt.Sprintf("%x %v %v members=%d settlers=%d", s.id[:4], s.reason, s.change, len(s.members), len(s.settlers))
}

// fold applies the entries in order to r, checking at each position that the writes reproduce the records.
func fold(t testing.TB, r *Roster, order []ID, byID map[ID]*item) []step {
	t.Helper()
	store := map[string][]byte{}
	for _, w := range r.Records() {
		store[string(w.Key)] = w.Value
	}
	out := make([]step, 0, len(order))
	for p, id := range order {
		it := byID[id]
		res := r.Apply(uint64(p), &it.e)
		for _, w := range res.Writes {
			if w.Delete {
				delete(store, string(w.Key))
			} else {
				store[string(w.Key)] = w.Value
			}
		}
		recs := r.Records()
		if len(recs) != len(store) {
			t.Fatalf("position %d: %d records, writes leave %d", p, len(recs), len(store))
		}
		for _, w := range recs {
			if !bytes.Equal(store[string(w.Key)], w.Value) {
				t.Fatalf("position %d: record %x differs from what the writes left", p, w.Key)
			}
		}
		rule, st := r.Settlers()
		out = append(out, step{id: id, reason: res.Reason, change: res.Change, effective: res.Effective(),
			members: r.Members(), rule: rule, settlers: st})
	}
	return out
}

func sameSteps(a, b []step) error {
	if len(a) != len(b) {
		return fmt.Errorf("%d positions against %d", len(a), len(b))
	}
	for i := range a {
		x, y := a[i], b[i]
		if x.id != y.id || x.reason != y.reason || x.change != y.change || x.rule != y.rule ||
			!slices.Equal(x.members, y.members) || !slices.Equal(x.settlers, y.settlers) {
			return fmt.Errorf("position %d: %v against %v", i, x, y)
		}
	}
	return nil
}

func (b *braid) run(t testing.TB, seed [32]byte) []step {
	t.Helper()
	return fold(t, b.roster(t), orderOf(t, b.nodes(), identity(len(b.items)), seed), b.byID)
}

// at returns the step of the named item.
func at(t testing.TB, steps []step, it *item) (int, step) {
	t.Helper()
	for i, s := range steps {
		if s.id == it.e.ID {
			return i, s
		}
	}
	t.Fatalf("%s not ordered", it.name)
	return 0, step{}
}

func orderKey(seed [32]byte, id ID) [32]byte {
	var e kolocodec.Encoder
	e.PutString("kolo-braid/order/v0")
	e.PutFixed(seed[:])
	e.PutFixed(id[:])
	return blake2b.Sum256(e.Bytes())
}

// naiveOrder is Kahn's sort written from the definition: repeatedly emit the smallest ready entry by (class, order key, ID).
func naiveOrder(items []*item, seed [32]byte) []ID {
	done := map[ID]bool{}
	var out []ID
	for len(out) < len(items) {
		var best *item
		var bestKey [32]byte
		for _, it := range items {
			if done[it.e.ID] || !readyIn(it, done) {
				continue
			}
			k := orderKey(seed, it.e.ID)
			if best == nil || it.node.Class < best.node.Class || (it.node.Class == best.node.Class &&
				(bytes.Compare(k[:], bestKey[:]) < 0 || (k == bestKey && bytes.Compare(it.e.ID[:], best.e.ID[:]) < 0))) {
				best, bestKey = it, k
			}
		}
		if best == nil {
			return out
		}
		done[best.e.ID] = true
		out = append(out, best.e.ID)
	}
	return out
}

func readyIn(it *item, done map[ID]bool) bool {
	if it.node.Index > 0 && !done[it.node.Prev] {
		return false
	}
	for _, l := range it.node.Links {
		if !done[l.ID] {
			return false
		}
	}
	return true
}
