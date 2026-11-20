package kolobraid

import (
	"testing"

	koloid "github.com/ciranda-dev/kolo-id"
	kololog "github.com/ciranda-dev/kolo-log"
)

// testStrand is the log ID of the strand that entries are decoded on.
var testStrand = kololog.LogID(arr(pat(32, 0xc0)))

// defaultBounds are the bounds of a group with the default config.
func defaultBounds() formatBounds { return GroupConfig{App: "t"}.WithDefaults().bounds() }

// signer is a real kolo-log with a fresh identity, to make signed headers, records and heads.
type signer struct {
	id    *koloid.Identity
	log   *kololog.Log
	empty []byte // the signed head of the empty log, from which fork restores
}

func newSigner(t testing.TB, opts ...kololog.Option) *signer {
	t.Helper()
	id, err := koloid.Generate()
	if err != nil {
		t.Fatal(err)
	}
	l, err := kololog.New(id, kololog.NewMemory(), opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	h, err := l.Head()
	if err != nil {
		t.Fatal(err)
	}
	return &signer{id: id, log: l, empty: h.Encode()}
}

func (s *signer) append(t testing.TB, data string) {
	t.Helper()
	if _, err := s.log.Append([]byte(data)); err != nil {
		t.Fatal(err)
	}
}

func (s *signer) header() []byte { return s.log.Header().Encode() }

func (s *signer) record(t testing.TB, i uint64) kololog.Record {
	t.Helper()
	r, err := s.log.Record(i)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func (s *signer) head(t testing.TB) kololog.Head {
	t.Helper()
	h, err := s.log.Head()
	if err != nil {
		t.Fatal(err)
	}
	return h
}

// fork returns a second history of the same key and log ID: a restored log with no entries.
func (s *signer) fork(t testing.TB) *signer {
	t.Helper()
	header := s.header()
	s.log.Close()
	l, err := kololog.Restore(s.id, header, s.empty, kololog.NewMemory())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	return &signer{id: s.id, log: l, empty: s.empty}
}

func (s *signer) member() NewMember {
	return NewMember{Key: s.id.PublicKey(), Strand: s.log.ID(), Header: s.header()}
}
