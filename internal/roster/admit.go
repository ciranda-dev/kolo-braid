package roster

import "slices"

// AdmitDefaults returns the roles and the sorted Settlers keys (nil when none is due) that Admit appends for joiner when the
// caller gives no roles. seats are the keys of the pending config, else of the current one. The joiner is admin while there
// are fewer than minSettlers-1 admins; an active joiner keeps its roles. A Settlers entry is due when the members, the joiner
// included, number at least minSettlers with fewer seats, or at least three with fewer than min(3, minSettlers) seats; it
// names the active seats, the joiner and the earliest-added other members up to that size.
func (r *Roster) AdmitDefaults(joiner Key, seats []Key, minSettlers int) (Role, []Key) {
	minSettlers = max(1, min(minSettlers, r.g.MaxSettlers))
	var roles Role
	cur, active := r.members[joiner]
	if active {
		roles = cur.Roles
	}
	if r.admins < minSettlers-1 {
		roles |= Admin
	}
	members := len(r.members)
	if !active {
		members++
	}
	var list []Key
	for _, k := range seats {
		if _, ok := r.members[k]; ok && !slices.Contains(list, k) {
			list = append(list, k)
		}
	}
	target := 0
	switch interim := min(3, minSettlers); {
	case members >= minSettlers && len(list) < minSettlers:
		target = minSettlers
	case members >= 3 && len(list) < interim:
		target = interim
	default:
		return roles, nil
	}
	if !slices.Contains(list, joiner) {
		list = append(list, joiner)
	}
	others := r.Members()
	slices.SortFunc(others, func(a, b Member) int {
		switch {
		case a.Added < b.Added:
			return -1
		case a.Added > b.Added:
			return 1
		}
		return cmpKey(a.Key, b.Key)
	})
	for _, w := range others {
		if len(list) >= target {
			break
		}
		if !slices.Contains(list, w.Key) {
			list = append(list, w.Key)
		}
	}
	slices.SortFunc(list, cmpKey)
	return roles, list
}
