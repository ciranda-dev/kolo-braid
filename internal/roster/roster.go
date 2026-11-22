// Package roster evaluates membership at each entry's own position in the total order: the active strands with their keys
// and roles, the settler keys and quorum rule, and the strand log IDs ever used. Validity never depends on arrival time,
// only on the order, so every peer that orders the same entries computes the same roster at every position.
package roster

import (
	"bytes"
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/ciranda-dev/kolo-braid/internal/dag"
)

// Errors of this package.
var (
	ErrInvalid = errors.New("kolobraid: invalid roster input")
	ErrCorrupt = errors.New("kolobraid: corrupt roster record")
)

const minMaxMembers = 2

// ID is an entry ID.
type ID = dag.ID

// StrandID is the kolo-log log ID of a strand.
type StrandID = dag.StrandID

// Key is an Ed25519 public key.
type Key [32]byte

// Kind is an entry kind, with the numbers of the entry format.
type Kind uint8

// Entry kinds.
const (
	KindGenesis Kind = iota
	KindData
	KindAddMember
	KindRemoveMember
	KindSettlers
	KindEvidence
	KindPulse
	KindSeed
)

// Role is a bit set of rights.
type Role uint8

// Admin may change the membership.
const Admin Role = 1

// Rule is a quorum rule, with the numbers of the entry format.
type Rule uint8

// Quorum rules.
const (
	Majority  Rule = 1
	TwoThirds Rule = 2
)

// Class returns the order class of an entry: Membership for a well-formed AddMember, RemoveMember, Settlers or Evidence
// entry, Other for the rest. It depends on the decoded kind only, never on validity at a position.
func Class(k Kind, malformed bool) dag.Class {
	if !malformed && k >= KindAddMember && k <= KindEvidence {
		return dag.Membership
	}
	return dag.Other
}

// Genesis holds what the genesis entry fixes for the roster.
type Genesis struct {
	// Group is the group ID: the entry ID of the genesis entry.
	Group   ID
	Founder Key
	Strand  StrandID
	Rule    Rule
	// MaxMembers is 0 for no ceiling, or 2 and more; MaxSettlers is 1 and more, and at most MaxMembers when that is set.
	MaxMembers, MaxSettlers int
	// Seeds is the number of seed entries after the genesis entry of a successor braid.
	Seeds uint64
}

func (g Genesis) check() error {
	if g.Group == (ID{}) || !knownRule(g.Rule) || g.MaxMembers < 0 || g.MaxMembers == 1 ||
		g.MaxSettlers < 1 || (g.MaxMembers > 0 && g.MaxSettlers > g.MaxMembers) {
		return fmt.Errorf("%w: genesis", ErrInvalid)
	}
	return nil
}

func knownRule(r Rule) bool { return r == Majority || r == TwoThirds }

// Seat is a key with a strand: the target of an Evidence entry, or a settler key with its active strand.
type Seat struct {
	Key    Key
	Strand StrandID
}

// Add is the body of an AddMember entry; the header was verified when it was decoded.
type Add struct {
	Key    Key
	Strand StrandID
	Roles  Role
}

// Settlers is the body of a Settlers entry: keys strictly ascending.
type Settlers struct {
	Rule Rule
	Keys []Key
}

// Entry is what the roster needs of one entry at its position.
type Entry struct {
	ID     ID
	Strand StrandID
	Index  uint64
	Kind   Kind
	// Malformed entries are void whatever their kind says.
	Malformed bool
	// Add is the body of an AddMember entry.
	Add Add
	// Target is the key of a RemoveMember entry.
	Target Key
	// Settlers is the body of a Settlers entry.
	Settlers Settlers
	// Accused is the key and strand that a verified Evidence body names.
	Accused Seat
}

// Member is an active strand.
type Member struct {
	Key    Key
	Strand StrandID
	Roles  Role
	// Added is the position at which the key became active; a strand replacement keeps it.
	Added uint64
}

// Reason says why an entry is void at its position; OK when it is effective.
type Reason uint8

// Reasons.
const (
	OK Reason = iota
	Malformed
	NotGenesis
	Inactive
	NotSeed
	NotAdmin
	Full
	UsedStrand
	LastAdmin
	LastSettler
	NotActive
	NotActiveStrand
	TooManySettlers
)

// String returns the name of the reason.
func (r Reason) String() string {
	names := [...]string{"ok", "malformed", "not the genesis entry", "author not active", "not a seed position",
		"author not admin", "member set full", "strand log ID already used", "no admin would remain",
		"no settler would remain", "key not active", "not the key's active strand", "too many settlers"}
	if int(r) < len(names) {
		return names[r]
	}
	return fmt.Sprintf("reason %d", r)
}

// Change is what an effective entry changed.
type Change uint8

// Changes.
const (
	// None: data, pulse and seed entries.
	None Change = iota
	Founded
	Added
	RolesSet
	Replaced
	Removed
	Fenced
	SettlersSet
)

// Result is the outcome of one entry at its position.
type Result struct {
	Reason Reason
	Change Change
	// Member is the member the change is about: after it for Founded, Added, RolesSet and Replaced, as it was for Removed
	// and Fenced.
	Member Member
	// Old is the strand that a replacement made inactive.
	Old StrandID
	// SettlersChanged is set when the settler keys or the rule changed.
	SettlersChanged bool
	// Writes are the roster records the change puts or deletes, in order; see Records.
	Writes []Write
}

// Effective reports whether the entry took effect.
func (r Result) Effective() bool { return r.Reason == OK }

// Roster is the membership state just after some position. It is not safe for concurrent use.
type Roster struct {
	g        Genesis
	founded  bool
	members  map[Key]Member
	byStrand map[StrandID]Key
	used     map[StrandID]Key
	rule     Rule
	settlers []Key
	admins   int
}

// New returns the roster before the genesis entry: empty, so every entry ordered before the genesis entry is void.
func New(g Genesis) (*Roster, error) {
	if err := g.check(); err != nil {
		return nil, err
	}
	return &Roster{g: g, members: map[Key]Member{}, byStrand: map[StrandID]Key{}, used: map[StrandID]Key{}}, nil
}

// Genesis returns what the genesis entry fixed.
func (r *Roster) Genesis() Genesis { return r.g }

// Founded reports whether the genesis entry was applied.
func (r *Roster) Founded() bool { return r.founded }

// Clone returns a copy that changes independently.
func (r *Roster) Clone() *Roster {
	return &Roster{g: r.g, founded: r.founded, rule: r.rule, settlers: slices.Clone(r.settlers), admins: r.admins,
		members: maps.Clone(r.members), byStrand: maps.Clone(r.byStrand), used: maps.Clone(r.used)}
}

// Check returns why e would be void if it were ordered right after the current position, or OK.
func (r *Roster) Check(e *Entry) Reason {
	reason, _ := r.decide(e)
	return reason
}

func (r *Roster) decide(e *Entry) (Reason, Change) {
	if e.Malformed || e.Kind > KindSeed {
		return Malformed, None
	}
	if e.Kind == KindGenesis {
		if r.founded || e.ID != r.g.Group || e.Strand != r.g.Strand || e.Index != 0 {
			return NotGenesis, None
		}
		return OK, Founded
	}
	ak, ok := r.byStrand[e.Strand]
	if !ok {
		return Inactive, None
	}
	admin := r.members[ak].Roles&Admin != 0
	switch e.Kind {
	case KindData, KindPulse:
		return OK, None
	case KindSeed:
		if e.Strand != r.g.Strand || e.Index < 1 || e.Index > r.g.Seeds {
			return NotSeed, None
		}
		return OK, None
	case KindAddMember:
		return r.decideAdd(&e.Add, admin)
	case KindRemoveMember:
		t, ok := r.members[e.Target]
		switch {
		case !admin:
			return NotAdmin, None
		case !ok:
			return NotActive, None
		case t.Roles&Admin != 0 && r.admins <= 1:
			return LastAdmin, None
		case len(r.settlers) == 0 || r.isSettler(t.Key) && len(r.settlers) == 1:
			return LastSettler, None
		}
		return OK, Removed
	case KindSettlers:
		s := &e.Settlers
		if !knownRule(s.Rule) || len(s.Keys) == 0 || !ascending(s.Keys) {
			return Malformed, None
		}
		if !admin {
			return NotAdmin, None
		}
		if len(s.Keys) > r.g.MaxSettlers {
			return TooManySettlers, None
		}
		for _, k := range s.Keys {
			if _, ok := r.members[k]; !ok {
				return NotActive, None
			}
		}
		return OK, SettlersSet
	}
	t, ok := r.members[e.Accused.Key]
	if !ok {
		return NotActive, None
	}
	if t.Strand != e.Accused.Strand {
		return NotActiveStrand, None
	}
	return OK, Fenced
}

func (r *Roster) decideAdd(a *Add, admin bool) (Reason, Change) {
	if a.Roles&^Admin != 0 {
		return Malformed, None
	}
	if !admin {
		return NotAdmin, None
	}
	cur, active := r.members[a.Key]
	_, used := r.used[a.Strand]
	switch {
	case !active && r.g.MaxMembers > 0 && len(r.members) >= r.g.MaxMembers:
		return Full, None
	case !active && used:
		return UsedStrand, None
	case !active:
		return OK, Added
	case cur.Strand != a.Strand && used:
		return UsedStrand, None
	case cur.Roles&Admin != 0 && a.Roles&Admin == 0 && r.admins <= 1:
		return LastAdmin, None
	case cur.Strand == a.Strand:
		return OK, RolesSet
	}
	return OK, Replaced
}

// Apply evaluates e at position p, which follows the current position, and applies its effect.
func (r *Roster) Apply(p uint64, e *Entry) Result {
	reason, change := r.decide(e)
	res := Result{Reason: reason, Change: change}
	if reason != OK {
		return res
	}
	switch change {
	case Founded:
		r.founded = true
		w := Member{Key: r.g.Founder, Strand: r.g.Strand, Roles: Admin, Added: p}
		r.put(w)
		r.rule, r.settlers = r.g.Rule, []Key{w.Key}
		res.Member, res.SettlersChanged = w, true
		res.Writes = []Write{r.paramsWrite(), r.memberWrite(w), r.usedWrite(w.Strand), r.settlersWrite()}
	case Added:
		w := Member{Key: e.Add.Key, Strand: e.Add.Strand, Roles: e.Add.Roles, Added: p}
		r.put(w)
		res.Member = w
		res.Writes = []Write{r.memberWrite(w), r.usedWrite(w.Strand)}
	case RolesSet:
		w := r.members[e.Add.Key]
		r.drop(w.Key)
		w.Roles = e.Add.Roles
		r.put(w)
		res.Member = w
		res.Writes = []Write{r.memberWrite(w)}
	case Replaced:
		w := r.members[e.Add.Key]
		res.Old = w.Strand
		r.drop(w.Key)
		w.Strand, w.Roles = e.Add.Strand, e.Add.Roles
		r.put(w)
		res.Member = w
		res.Writes = []Write{r.memberWrite(w), r.usedWrite(w.Strand)}
	case Removed, Fenced:
		k := e.Target
		if change == Fenced {
			k = e.Accused.Key
		}
		res.Member = r.members[k]
		res.Writes = []Write{deleteWrite(memberKey(k))}
		r.drop(k)
		if i, ok := slices.BinarySearchFunc(r.settlers, k, cmpKey); ok {
			r.settlers = slices.Delete(r.settlers, i, i+1)
			res.SettlersChanged = true
			res.Writes = append(res.Writes, r.settlersWrite())
		}
	case SettlersSet:
		r.rule, r.settlers = e.Settlers.Rule, slices.Clone(e.Settlers.Keys)
		res.SettlersChanged = true
		res.Writes = []Write{r.settlersWrite()}
	}
	return res
}

func (r *Roster) put(w Member) {
	r.members[w.Key] = w
	r.byStrand[w.Strand] = w.Key
	r.used[w.Strand] = w.Key
	if w.Roles&Admin != 0 {
		r.admins++
	}
}

func (r *Roster) drop(k Key) {
	w := r.members[k]
	if w.Roles&Admin != 0 {
		r.admins--
	}
	delete(r.byStrand, w.Strand)
	delete(r.members, k)
}

func (r *Roster) isSettler(k Key) bool {
	_, ok := slices.BinarySearchFunc(r.settlers, k, cmpKey)
	return ok
}

func cmpKey(a, b Key) int { return bytes.Compare(a[:], b[:]) }

func ascending(keys []Key) bool {
	for i := 1; i < len(keys); i++ {
		if cmpKey(keys[i-1], keys[i]) >= 0 {
			return false
		}
	}
	return true
}

// Member returns the active member with the given key.
func (r *Roster) Member(k Key) (Member, bool) {
	w, ok := r.members[k]
	if !ok {
		return Member{}, false
	}
	return w, true
}

// ByStrand returns the member whose active strand is s.
func (r *Roster) ByStrand(s StrandID) (Member, bool) {
	k, ok := r.byStrand[s]
	if !ok {
		return Member{}, false
	}
	return r.members[k], true
}

// Members returns the active members sorted by key.
func (r *Roster) Members() []Member {
	out := make([]Member, 0, len(r.members))
	for _, w := range r.members {
		out = append(out, w)
	}
	slices.SortFunc(out, func(a, b Member) int { return cmpKey(a.Key, b.Key) })
	return out
}

// Len returns the number of active members.
func (r *Roster) Len() int { return len(r.members) }

// Admins returns the number of active admins.
func (r *Roster) Admins() int { return r.admins }

// Settlers returns the rule and the settler keys of the last effective Settlers entry, minus keys that became inactive.
func (r *Roster) Settlers() (Rule, []Key) { return r.rule, slices.Clone(r.settlers) }

// Seats returns the settler keys with their active strands, sorted by key: the seats a pending config is made of.
func (r *Roster) Seats() []Seat {
	out := make([]Seat, len(r.settlers))
	for i, k := range r.settlers {
		out[i] = Seat{Key: k, Strand: r.members[k].Strand}
	}
	return out
}

// Used reports whether a strand log ID was ever used, and by which key.
func (r *Roster) Used(s StrandID) (Key, bool) {
	k, ok := r.used[s]
	return k, ok
}

// UsedCount returns the number of strand log IDs ever used; 1 means no other strand than the founder's was ever active.
func (r *Roster) UsedCount() int { return len(r.used) }

// Access reports whether a peer key gets entry keys from this peer: only active members do. Removed and never-added keys
// get nothing, from the first request after this roster holds the removal.
func (r *Roster) Access(k Key) bool {
	_, ok := r.members[k]
	return ok
}
