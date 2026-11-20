package kolobraid

import (
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"sync"
	"testing"
	"time"

	kolostore "github.com/ciranda-dev/kolo-store"
)

func TestOptionsAndOpenRefusals(t *testing.T) {
	id := newIdentity(t)
	db := kolostore.NewMemory()
	ctx := context.Background()
	good := testOptions(db, appApply)
	cases := []struct {
		name string
		mod  func(o *Options)
	}{
		{"no backend", func(o *Options) { o.Backend = nil }},
		{"no apply", func(o *Options) { o.Apply = nil }},
		{"no app", func(o *Options) { o.App = "" }},
		{"short pin", func(o *Options) { o.Pins = append(o.Pins, make([]byte, 5)) }},
		{"tail", func(o *Options) { o.Limits = &Limits{MaxTail: 63} }},
		{"tail bytes", func(o *Options) { o.Limits = &Limits{MaxTailBytes: 1<<20 - 1} }},
		{"pending", func(o *Options) { o.Limits = &Limits{MaxPendingBytes: 1<<30 + 1} }},
		{"pull ahead", func(o *Options) { o.Limits = &Limits{MaxPullAhead: 1<<20 + 1} }},
		{"pulse gap", func(o *Options) { o.Limits = &Limits{PulseGap: time.Millisecond} }},
		{"pivot wait", func(o *Options) { o.Limits = &Limits{PivotWait: 6 * time.Minute} }},
		{"announce gap", func(o *Options) { o.Limits = &Limits{AnnounceGap: 11 * time.Second} }},
		{"confirm wait", func(o *Options) { o.Limits = &Limits{ConfirmWait: time.Millisecond} }},
		{"events", func(o *Options) { o.Limits = &Limits{MaxEvents: 15} }},
	}
	for _, c := range cases {
		o := good
		c.mod(&o)
		if _, err := Create(ctx, id, GroupConfig{App: "test/v1"}, o); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("%s: %v", c.name, err)
		}
	}
	if _, err := Create(ctx, nil, GroupConfig{App: "test/v1"}, good); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("nil identity: %v", err)
	}
	if _, err := Create(ctx, id, GroupConfig{App: "other"}, good); !errors.Is(err, ErrWrongApp) {
		t.Errorf("app mismatch: %v", err)
	}
	if _, err := Create(ctx, id, GroupConfig{App: "test/v1", MaxMembers: 1}, good); !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("bad config: %v", err)
	}
	if _, err := Open(ctx, id, good); !errors.Is(err, ErrNotFound) {
		t.Errorf("open of an empty backend: %v", err)
	}
	b, err := Create(ctx, id, GroupConfig{App: "test/v1"}, good)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Create(ctx, id, GroupConfig{App: "test/v1"}, good); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("second create: %v", err)
	}
	if b.Topic() != topicOf(b.GroupID()) {
		t.Error("topic")
	}
	b.Close()
	wrong := good
	wrong.App = "other"
	if _, err := Open(ctx, id, wrong); !errors.Is(err, ErrWrongApp) {
		t.Errorf("open with another app: %v", err)
	}
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := Open(cctx, id, good); !errors.Is(err, context.Canceled) {
		t.Errorf("canceled open: %v", err)
	}
}

func TestBraidsShareABackendUnderPrefixes(t *testing.T) {
	db := kolostore.NewMemory()
	var bs []*Braid
	for i, pre := range []string{"one/", "two/"} {
		o := testOptions(PrefixBackend(db, []byte(pre)), appApply)
		b, err := Create(context.Background(), newIdentity(t), GroupConfig{App: "test/v1"}, o)
		if err != nil {
			t.Fatal(err)
		}
		defer b.Close()
		mustAppend(t, b, fmt.Sprintf("who=%d", i))
		bs = append(bs, b)
	}
	for i, b := range bs {
		waitUntil(t, 5*time.Second, func() bool { return settledGet(t, b, "who") == fmt.Sprint(i) })
	}
	if PrefixBackend(db, nil) != db {
		t.Fatal("an empty prefix wraps the backend")
	}
}

func TestClosedBraidRefusesCalls(t *testing.T) {
	p := newFounder(t, "a", GroupConfig{}, appApply)
	b := p.b
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
	p.b = nil
	if _, err := b.Append(context.Background(), []byte("x")); !errors.Is(err, ErrClosed) {
		t.Fatalf("append: %v", err)
	}
	if _, err := b.Next(context.Background()); !errors.Is(err, ErrClosed) {
		for {
			if _, err = b.Next(context.Background()); errors.Is(err, ErrClosed) {
				break
			}
		}
	}
	for _, err := range b.Entries(0) {
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("entries: %v", err)
		}
	}
}

func TestEventsAreBoundedButKeptKindsStay(t *testing.T) {
	db := kolostore.NewMemory()
	o := testOptions(db, appApply)
	o.Limits.MaxEvents = 16
	b, err := Create(context.Background(), newIdentity(t), GroupConfig{App: "test/v1"}, o)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	b.ev.push(Event{Kind: EventHalted, Err: ErrHalted})
	for i := 0; i < 40; i++ {
		b.ev.push(Event{Kind: EventApplied, Position: uint64(i)})
	}
	if d := b.Status().EventsDropped; d < 40-15 {
		t.Fatalf("%d events dropped", d)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ev, err := b.Next(ctx)
	if err != nil || ev.Kind != EventHalted {
		t.Fatalf("the kept event was dropped: %+v %v", ev, err)
	}
	ctx2, cancel2 := context.WithCancel(context.Background())
	cancel2()
	for {
		if _, err := b.Next(ctx2); err != nil {
			if !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			break
		}
	}
}

func TestTailStopsAtMaxTail(t *testing.T) {
	nt, ps := group(t, 2, GroupConfig{}, appApply, true)
	ps[1].close()
	a := ps[0]
	for i := 0; i < 100; i++ {
		mustAppend(t, a.b, fmt.Sprint("t", i))
	}
	waitUntil(t, 5*time.Second, func() bool { return a.b.Status().Tail == 64 })
	if s := a.b.Status(); s.Tentative != s.Settled+64 {
		t.Fatalf("status %+v", s)
	}
	n := 0
	for o, err := range a.b.Entries(a.b.Status().Settled) {
		if err != nil {
			t.Fatal(err)
		}
		if o.Settled {
			t.Fatal("tail entry marked settled")
		}
		n++
	}
	if n != 64 {
		t.Fatalf("entries yields %d tail entries", n)
	}
	_ = nt
}

func TestEntriesEndsWithErrReorged(t *testing.T) {
	nt, ps := group(t, 2, GroupConfig{}, appApply, true)
	a := ps[0]
	nt.partition([]int{0}, []int{1})
	for i := 0; i < 10; i++ {
		mustAppend(t, a.b, fmt.Sprint("a", i))
	}
	waitUntil(t, 10*time.Second, func() bool { return a.b.Status().Tail >= 10 })
	start := a.b.Status().Settled
	var last error
	n := 0
	for _, err := range a.b.Entries(start) {
		if err != nil {
			last = err
			break
		}
		if n++; n == 3 {
			// a reorg below the yielded positions, as when a concurrent entry sorts before them
			a.b.mu.Lock()
			a.b.reorgGen++
			a.b.reorgs = append(a.b.reorgs, reorgMark{gen: a.b.reorgGen, pos: start + 1})
			a.b.mu.Unlock()
		}
	}
	if !errors.Is(last, ErrReorged) || n != 3 {
		t.Fatalf("after %d entries: %v", n, last)
	}
	// a reorg above every yielded position does not end it
	n, last = 0, nil
	for _, err := range a.b.Entries(start) {
		if err != nil {
			last = err
			break
		}
		if n++; n == 1 {
			a.b.mu.Lock()
			a.b.reorgGen++
			a.b.reorgs = append(a.b.reorgs, reorgMark{gen: a.b.reorgGen, pos: start + 5})
			a.b.mu.Unlock()
		}
		if n == 3 {
			break
		}
	}
	if last != nil {
		t.Fatal(last)
	}
}

// TestConcurrentCalls runs Append, View, Settled, Status, Entries and Next from several goroutines while peers sync, then
// closes; meant for -race.
func TestConcurrentCalls(t *testing.T) {
	nt, ps := group(t, 3, GroupConfig{}, appApply, true)
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for i, p := range ps {
		wg.Add(3)
		go func() {
			defer wg.Done()
			for j := 0; j < 30; j++ {
				if _, err := p.b.Append(ctx, []byte(fmt.Sprintf("g%d=%d", i, j))); err != nil {
					return
				}
			}
		}()
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				v := p.b.View()
				for range v.Scan(nil, nil) {
				}
				v.Close()
				s := p.b.Settled()
				s.Members()
				s.Close()
				p.b.Status()
				for range p.b.Entries(0) {
				}
			}
		}()
		go func() {
			defer wg.Done()
			for {
				if _, err := p.b.Next(ctx); err != nil {
					return
				}
			}
		}()
	}
	for i := 0; i < 50; i++ {
		nt.round(nil, 0)
		time.Sleep(2 * time.Millisecond)
	}
	cancel()
	wg.Wait()
	nt.settle(nil, 20*time.Second, func() bool { return quiet(ps...) })
	checkSame(t, ps)
	for _, p := range ps {
		p.close()
	}
}

// TestEventQueuePolicy: each never-dropped kind survives a full queue, and droppable events go oldest first, counted exactly.
func TestEventQueuePolicy(t *testing.T) {
	for _, k := range []EventKind{EventRemoved, EventSettlementHalted, EventStrandLost, EventHalted, EventDiverged} {
		e := newEvents(16)
		e.push(Event{Kind: k})
		for i := range 40 {
			e.push(Event{Kind: EventApplied, Position: uint64(i)})
		}
		if d := e.droppedCount(); d != 25 {
			t.Fatalf("kind %d: %d events dropped, want 25", k, d)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		first, err := e.next(ctx)
		if err != nil || first.Kind != k {
			cancel()
			t.Fatalf("kind %d was dropped: %+v %v", k, first, err)
		}
		next, err := e.next(ctx)
		cancel()
		if err != nil || next.Kind != EventApplied || next.Position != 25 {
			t.Fatalf("kind %d: the oldest kept droppable event is %+v %v, want position 25", k, next, err)
		}
	}
}

// TestEntriesWhileJoining: Entries on a joiner that waits for the genesis entry, while the genesis arrives and replaces its
// view, races nothing (run with -race).
func TestEntriesWhileJoining(t *testing.T) {
	f := newFounder(t, "f", GroupConfig{}, appApply)
	mustAppend(t, f.b, "a=1")
	j, nw := joiner(t, "j", f, appApply, 0)
	t.Cleanup(j.close)
	if _, err := f.b.AddMember(ctx10(t), nw); err != nil {
		t.Fatal(err)
	}
	j.start()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 200 {
			for _, err := range j.b.Entries(0) {
				if err != nil && !errors.Is(err, ErrReorged) {
					t.Error(err)
					return
				}
			}
		}
	}()
	nt := newNet(t, f, j)
	nt.settle(nil, 10*time.Second, func() bool { return !waiting(j) && settledGet(t, j.b, "a") == "1" })
	<-done
}

// TestEntriesReadsTheViewOnce: Entries takes the braid's view and limits once, under b.mu, which a joiner replaces when the
// genesis arrives; every other read uses that copy (a race the race detector sees only by chance).
func TestEntriesReadsTheViewOnce(t *testing.T) {
	f, err := parser.ParseFile(token.NewFileSet(), "entries.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	reads := func(x ast.Node) map[string]int {
		n := map[string]int{}
		ast.Inspect(x, func(x ast.Node) bool {
			if sel, ok := x.(*ast.SelectorExpr); ok {
				if id, ok := sel.X.(*ast.Ident); ok && id.Name == "b" && (sel.Sel.Name == "view" || sel.Sel.Name == "lim") {
					n[sel.Sel.Name]++
				}
			}
			return true
		})
		return n
	}
	if n := reads(f); n["view"] != 1 || n["lim"] != 1 {
		t.Fatalf("entries.go reads b.view %d and b.lim %d times, want once each", n["view"], n["lim"])
	}
	// the one read sits between the closure's b.mu.Lock() and its first b.mu.Unlock() at the same level
	call := func(st ast.Stmt) string {
		if es, ok := st.(*ast.ExprStmt); ok {
			if c, ok := es.X.(*ast.CallExpr); ok {
				if sel, ok := c.Fun.(*ast.SelectorExpr); ok {
					if mu, ok := sel.X.(*ast.SelectorExpr); ok && mu.Sel.Name == "mu" {
						return sel.Sel.Name
					}
				}
			}
		}
		return ""
	}
	locked := false
	ast.Inspect(f, func(x ast.Node) bool {
		lit, ok := x.(*ast.FuncLit)
		if !ok {
			return true
		}
		held := false
		for _, st := range lit.Body.List {
			switch call(st) {
			case "Lock":
				held = true
			case "Unlock":
				held = false
			}
			if n := reads(st); n["view"] > 0 {
				locked = held
			}
		}
		return true
	})
	if !locked {
		t.Fatal("entries.go reads b.view outside b.mu")
	}
}
