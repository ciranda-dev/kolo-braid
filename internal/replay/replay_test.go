package replay_test

import (
	"testing"

	kolobraid "github.com/ciranda-dev/kolo-braid"
	"github.com/ciranda-dev/kolo-braid/internal/replay"
)

// TestGetChecksTheTypes: the hook the root package sets has the types its users name, and other types fail at once.
func TestGetChecksTheTypes(t *testing.T) {
	replay.Get[kolobraid.ApplyFunc, []kolobraid.Member, kolobraid.Entry, kolobraid.Digest, *kolobraid.Snapshot]()
	defer func() {
		if recover() == nil {
			t.Fatal("a hook of other types was returned")
		}
	}()
	replay.Get[int, []kolobraid.Member, kolobraid.Entry, kolobraid.Digest, *kolobraid.Snapshot]()
}
