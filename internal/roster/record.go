package roster

import (
	"bytes"
	"fmt"
	"iter"
	"math"
	"slices"

	kolocodec "github.com/ciranda-dev/kolo-codec"
)

// Record key prefixes, inside the roster prefix of the store.
const (
	recParams   = 'p'
	recMember   = 'w'
	recUsed     = 'u'
	recSettlers = 's'
)

// Write is one roster record put or deleted by a change. Keys are relative to the roster prefix: "p" the genesis
// parameters, "w" key the active member, "u" strand a used strand log ID, "s" the settler keys and rule.
type Write struct {
	Delete     bool
	Key, Value []byte
}

func deleteWrite(k []byte) Write { return Write{Delete: true, Key: k} }

func memberKey(k Key) []byte { return append([]byte{recMember}, k[:]...) }

func (r *Roster) paramsWrite() Write {
	var e kolocodec.Encoder
	e.PutFixed(r.g.Group[:])
	e.PutFixed(r.g.Founder[:])
	e.PutFixed(r.g.Strand[:])
	e.PutUvarint(uint64(r.g.Rule))
	e.PutUvarint(uint64(r.g.MaxMembers))
	e.PutUvarint(uint64(r.g.MaxSettlers))
	e.PutUvarint(r.g.Seeds)
	return Write{Key: []byte{recParams}, Value: e.Bytes()}
}

func (r *Roster) memberWrite(w Member) Write {
	var e kolocodec.Encoder
	e.PutFixed(w.Strand[:])
	e.PutUvarint(uint64(w.Roles))
	e.PutUvarint(w.Added)
	return Write{Key: memberKey(w.Key), Value: e.Bytes()}
}

func (r *Roster) usedWrite(s StrandID) Write {
	k := r.used[s]
	return Write{Key: append([]byte{recUsed}, s[:]...), Value: slices.Clone(k[:])}
}

func (r *Roster) settlersWrite() Write {
	var e kolocodec.Encoder
	e.PutUvarint(uint64(r.rule))
	e.PutUvarint(uint64(len(r.settlers)))
	for _, k := range r.settlers {
		e.PutFixed(k[:])
	}
	return Write{Key: []byte{recSettlers}, Value: e.Bytes()}
}

// Records returns every record of the roster, ascending by key: what the Writes of all changes so far leave in a store.
func (r *Roster) Records() []Write {
	if !r.founded {
		return nil
	}
	out := []Write{r.paramsWrite(), r.settlersWrite()}
	for s := range r.used {
		out = append(out, r.usedWrite(s))
	}
	for _, w := range r.members {
		out = append(out, r.memberWrite(w))
	}
	slices.SortFunc(out, func(a, b Write) int { return bytes.Compare(a.Key, b.Key) })
	return out
}

// Load rebuilds a roster of the braid that g describes from the records a store holds, as Records returns them. No
// record means the genesis entry was not applied yet.
func Load(g Genesis, records iter.Seq2[[]byte, []byte]) (*Roster, error) {
	r, err := New(g)
	if err != nil {
		return nil, err
	}
	var params, settlers []byte
	n := 0
	for k, v := range records {
		n++
		if err := r.load(k, v, &params, &settlers); err != nil {
			return nil, err
		}
	}
	if n == 0 {
		return r, nil
	}
	if params == nil || settlers == nil {
		return nil, corrupt("missing parameters or settlers")
	}
	if !bytes.Equal(params, r.paramsWrite().Value) {
		return nil, corrupt("parameters of another genesis")
	}
	d := kolocodec.NewDecoder(settlers)
	rule, err := d.UvarintMax(uint64(TwoThirds))
	if err != nil || !knownRule(Rule(rule)) {
		return nil, corrupt("settlers rule")
	}
	ns, err := d.UvarintMax(uint64(g.MaxSettlers))
	if err != nil {
		return nil, corrupt("settler count")
	}
	r.rule = Rule(rule)
	for range ns {
		b, err := d.Fixed(len(Key{}))
		if err != nil {
			return nil, corrupt("settler key")
		}
		k := Key(b)
		if _, ok := r.members[k]; !ok || (len(r.settlers) > 0 && cmpKey(r.settlers[len(r.settlers)-1], k) >= 0) {
			return nil, corrupt("settler keys not active or not ascending")
		}
		r.settlers = append(r.settlers, k)
	}
	if d.Done() != nil {
		return nil, corrupt("settlers trailing bytes")
	}
	for s, k := range r.byStrand {
		if u, ok := r.used[s]; !ok || u != k {
			return nil, corrupt("active strand not recorded as used by its key")
		}
	}
	if g.MaxMembers > 0 && len(r.members) > g.MaxMembers {
		return nil, corrupt("more members than MaxMembers")
	}
	r.founded = true
	return r, nil
}

func (r *Roster) load(k, v []byte, params, settlers *[]byte) error {
	if len(k) == 0 {
		return corrupt("empty key")
	}
	switch {
	case len(k) == 1 && k[0] == recParams && *params == nil:
		*params = bytes.Clone(v)
		if *params == nil {
			*params = []byte{}
		}
	case len(k) == 1 && k[0] == recSettlers && *settlers == nil:
		*settlers = bytes.Clone(v)
		if *settlers == nil {
			*settlers = []byte{}
		}
	case len(k) == 33 && k[0] == recUsed:
		s := StrandID(k[1:])
		if _, dup := r.used[s]; dup || len(v) != len(Key{}) {
			return corrupt("used strand record")
		}
		r.used[s] = Key(v)
	case len(k) == 33 && k[0] == recMember:
		key := Key(k[1:])
		d := kolocodec.NewDecoder(v)
		sb, err := d.Fixed(len(StrandID{}))
		if err != nil {
			return corrupt("member strand")
		}
		roles, err := d.UvarintMax(uint64(Admin))
		if err != nil {
			return corrupt("member roles")
		}
		added, err := d.UvarintMax(math.MaxUint64)
		if err != nil || d.Done() != nil {
			return corrupt("member position")
		}
		s := StrandID(sb)
		if _, dup := r.members[key]; dup {
			return corrupt("member record twice")
		}
		if _, dup := r.byStrand[s]; dup {
			return corrupt("strand active for two keys")
		}
		w := Member{Key: key, Strand: s, Roles: Role(roles), Added: added}
		r.members[key] = w
		r.byStrand[s] = key
		if w.Roles&Admin != 0 {
			r.admins++
		}
	default:
		return corrupt("unknown or repeated record")
	}
	return nil
}

func corrupt(why string) error { return fmt.Errorf("%w: %s", ErrCorrupt, why) }
