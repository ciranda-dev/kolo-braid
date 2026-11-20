package kolobraid

import (
	"bytes"
	"errors"
	"fmt"

	kolostore "github.com/ciranda-dev/kolo-store"

	"github.com/ciranda-dev/kolo-braid/internal/prefixdb"
)

// PrefixBackend returns a view of b in which every key starts with prefix, so several braids and other Kolo users can share one
// backend; no prefix may be a prefix of another. Closing the view leaves b open. An empty prefix returns b itself.
func PrefixBackend(b kolostore.Backend, prefix []byte) kolostore.Backend {
	return prefixdb.New(b, prefix)
}

// prefixRegistry is the key, at the root of a backend, under which every braid prefix used in it is recorded.
const prefixRegistry = "kolo-braid/prefixes/v0/"

// prefixUnused reports whether the braid prefix p holds neither a braid nor a pending join.
func prefixUnused(b kolostore.Backend, p []byte) (bool, error) {
	db := PrefixBackend(b, p)
	for _, k := range []string{keyMeta, keyJoin} {
		if v, err := getOpt(db, []byte(k)); err != nil || v != nil {
			return false, err
		}
	}
	return true, nil
}

// claimPrefix records the prefix of a braid in the registry of b. It refuses a prefix that nests with a recorded one, or
// with the registry: Open clears parts of its own prefix, which would erase the other braid's data.
func claimPrefix(b kolostore.Backend, prefix []byte) error {
	reg := []byte(prefixRegistry)
	if len(prefix) > 0 && (bytes.HasPrefix(reg, prefix) || bytes.HasPrefix(prefix, reg)) {
		return fmt.Errorf("%w: the prefix nests with the prefix registry", ErrInvalidArgument)
	}
	known := false
	err := scanPrefix(b, reg, func(k, _ []byte) error {
		p := k[len(reg):]
		switch {
		case bytes.Equal(p, prefix):
			known = true
		case bytes.HasPrefix(p, prefix) || bytes.HasPrefix(prefix, p):
			// a prefix whose braid and pending join are gone is free again
			if gone, err := prefixUnused(b, p); err != nil || !gone {
				return errors.Join(err, fmt.Errorf("%w: the prefix nests with the prefix of another braid", ErrInvalidArgument))
			}
			return b.Delete(k)
		}
		return nil
	})
	if err != nil || known {
		return err
	}
	return b.Put(append(reg, prefix...), []byte{1})
}
