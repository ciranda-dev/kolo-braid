package kolobraid

import (
	"testing"
	"time"
)

// NewTestBraid creates a one-member braid for the external tests and returns a function that waits until it settled every entry.
func NewTestBraid(t *testing.T, apply ApplyFunc) (*Braid, func()) {
	p := newFounder(t, "x", GroupConfig{}, apply)
	return p.b, func() { waitUntil(t, 10*time.Second, func() bool { return quiet(p) && p.b.view.Tail() <= 1 }) }
}
