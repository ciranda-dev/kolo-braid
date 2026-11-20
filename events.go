package kolobraid

import (
	"context"
	"crypto/ed25519"
	"slices"
	"sync"
)

// EventKind says what an Event reports.
type EventKind int

// Event kinds.
const (
	EventApplied          EventKind = iota + 1 // applied through Position (tentative or settled)
	EventReorg                                 // the tentative order changed from Position on
	EventSettled                               // settled through Position
	EventCertified                             // a claim at Position matched q seats
	EventMemberAdded                           // Member became active at Position
	EventMemberRemoved                         // Member became inactive at Position
	EventRemoved                               // this peer's own strand became inactive at Position
	EventEquivocation                          // evidence against Member was found or ordered
	EventSettlementHalted                      // Member, a seat of the current config, signed two histories; that config settles no more
	EventStrandLost                            // a peer holds a history of the own strand that conflicts with ours; writes are refused
	EventStrandCaughtUp                        // the own strand was behind and was rebuilt from a longer consistent copy
	EventHalted                                // apply failed at Position; Err says why
	EventDiverged                              // this peer's running digest differs from a certified claim
)

// Event is one thing that happened to the braid, as Next returns it.
type Event struct {
	Kind     EventKind
	Position uint64
	Member   ed25519.PublicKey
	Err      error
}

// kept reports whether an event kind is never dropped.
func (k EventKind) kept() bool {
	switch k {
	case EventRemoved, EventSettlementHalted, EventStrandLost, EventHalted, EventDiverged:
		return true
	}
	return false
}

// events is the queue Next reads: past max events the oldest droppable one goes first and is counted.
type events struct {
	mu      sync.Mutex
	q       []Event
	max     int
	dropped uint64
	wake    chan struct{}
	closed  bool
}

func newEvents(max int) *events { return &events{max: max, wake: make(chan struct{})} }

func (e *events) push(ev Event) {
	ev.Member = slices.Clone(ev.Member)
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return
	}
	if len(e.q) >= e.max {
		i := slices.IndexFunc(e.q, func(x Event) bool { return !x.Kind.kept() })
		switch {
		case i >= 0:
			e.q = slices.Delete(e.q, i, i+1)
			e.dropped++
		case !ev.Kind.kept():
			e.dropped++
			return
		}
	}
	e.q = append(e.q, ev)
	close(e.wake)
	e.wake = make(chan struct{})
}

func (e *events) next(ctx context.Context) (Event, error) {
	for {
		e.mu.Lock()
		if len(e.q) > 0 {
			ev := e.q[0]
			e.q[0] = Event{}
			e.q = e.q[1:]
			e.mu.Unlock()
			return ev, nil
		}
		if e.closed {
			e.mu.Unlock()
			return Event{}, ErrClosed
		}
		w := e.wake
		e.mu.Unlock()
		select {
		case <-w:
		case <-ctx.Done():
			return Event{}, ctx.Err()
		}
	}
}

func (e *events) close() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.closed {
		e.closed = true
		close(e.wake)
	}
}

func (e *events) droppedCount() uint64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.dropped
}
