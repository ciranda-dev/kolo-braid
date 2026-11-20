package roster

import (
	"bytes"
	"slices"
)

// ref is a second roster written directly from the membership table, with slices and full scans, to compare the
// production code against.
type ref struct {
	g        Genesis
	founded  bool
	ws       []Member
	used     []StrandID
	rule     Rule
	settlers []Key
}

func (r *ref) find(k Key) int {
	for i, w := range r.ws {
		if w.Key == k {
			return i
		}
	}
	return -1
}

func (r *ref) author(s StrandID) int {
	for i, w := range r.ws {
		if w.Strand == s {
			return i
		}
	}
	return -1
}

func (r *ref) wasUsed(s StrandID) bool { return slices.Contains(r.used, s) }

func (r *ref) adminCount() int {
	n := 0
	for _, w := range r.ws {
		if w.Roles == Admin {
			n++
		}
	}
	return n
}

func (r *ref) inactivate(k Key) {
	r.ws = slices.Delete(r.ws, r.find(k), r.find(k)+1)
	if i := slices.Index(r.settlers, k); i >= 0 {
		r.settlers = slices.Delete(r.settlers, i, i+1)
	}
}

// step returns whether e is effective at position p and applies it.
func (r *ref) step(p uint64, e *Entry) bool {
	if e.Malformed {
		return false
	}
	if e.Kind == KindGenesis {
		if r.founded || e.ID != r.g.Group || e.Strand != r.g.Strand || e.Index != 0 {
			return false
		}
		r.founded = true
		r.ws = []Member{{Key: r.g.Founder, Strand: r.g.Strand, Roles: Admin, Added: p}}
		r.used = []StrandID{r.g.Strand}
		r.rule, r.settlers = r.g.Rule, []Key{r.g.Founder}
		return true
	}
	a := r.author(e.Strand)
	if a < 0 {
		return false
	}
	isAdmin := r.ws[a].Roles == Admin
	switch e.Kind {
	case KindData, KindPulse:
		return true
	case KindSeed:
		return e.Strand == r.g.Strand && e.Index >= 1 && e.Index <= r.g.Seeds
	case KindAddMember:
		ad := e.Add
		if !isAdmin || ad.Roles > Admin {
			return false
		}
		t := r.find(ad.Key)
		if t < 0 {
			if len(r.ws) >= r.g.MaxMembers || r.wasUsed(ad.Strand) {
				return false
			}
			r.ws = append(r.ws, Member{Key: ad.Key, Strand: ad.Strand, Roles: ad.Roles, Added: p})
			r.used = append(r.used, ad.Strand)
			return true
		}
		if r.ws[t].Strand != ad.Strand && r.wasUsed(ad.Strand) {
			return false
		}
		if r.ws[t].Roles == Admin && ad.Roles != Admin && r.adminCount() == 1 {
			return false
		}
		if r.ws[t].Strand != ad.Strand {
			r.used = append(r.used, ad.Strand)
		}
		r.ws[t].Strand, r.ws[t].Roles = ad.Strand, ad.Roles
		return true
	case KindRemoveMember:
		t := r.find(e.Target)
		if !isAdmin || t < 0 {
			return false
		}
		left := slices.DeleteFunc(slices.Clone(r.ws), func(w Member) bool { return w.Key == e.Target })
		admins := 0
		for _, w := range left {
			if w.Roles == Admin {
				admins++
			}
		}
		settlersLeft := slices.DeleteFunc(slices.Clone(r.settlers), func(k Key) bool { return k == e.Target })
		if admins == 0 || len(settlersLeft) == 0 {
			return false
		}
		r.inactivate(e.Target)
		return true
	case KindSettlers:
		s := e.Settlers
		if !isAdmin || (s.Rule != Majority && s.Rule != TwoThirds) || len(s.Keys) == 0 || len(s.Keys) > r.g.MaxSettlers {
			return false
		}
		for i, k := range s.Keys {
			if r.find(k) < 0 || (i > 0 && bytes.Compare(s.Keys[i-1][:], k[:]) >= 0) {
				return false
			}
		}
		r.rule, r.settlers = s.Rule, slices.Clone(s.Keys)
		return true
	case KindEvidence:
		t := r.find(e.Accused.Key)
		if t < 0 || r.ws[t].Strand != e.Accused.Strand {
			return false
		}
		r.inactivate(e.Accused.Key)
		return true
	}
	return false
}

func (r *ref) members() []Member {
	out := slices.Clone(r.ws)
	slices.SortFunc(out, func(a, b Member) int { return bytes.Compare(a.Key[:], b.Key[:]) })
	return out
}

// refFold runs the reference over an order and returns, per position, whether the entry is effective, the members and the
// settlers after it.
func refFold(g Genesis, order []ID, byID map[ID]*item) []step {
	r := &ref{g: g}
	out := make([]step, 0, len(order))
	for p, id := range order {
		ok := r.step(uint64(p), &byID[id].e)
		out = append(out, step{id: id, effective: ok, members: r.members(), rule: r.rule, settlers: slices.Clone(r.settlers)})
	}
	return out
}

func sameAsRef(prod, want []step) (int, bool) {
	if len(prod) != len(want) {
		return 0, false
	}
	for i := range prod {
		p, w := prod[i], want[i]
		if p.id != w.id || p.effective != w.effective || p.rule != w.rule || !slices.Equal(p.members, w.members) ||
			!slices.Equal(p.settlers, w.settlers) {
			return i, false
		}
	}
	return 0, true
}
