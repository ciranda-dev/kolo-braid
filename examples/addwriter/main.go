// Command addwriter adds a writer to a braid with kolo-invite on 127.0.0.1: an in-process kolo-dht server, the admin who
// holds the braid and a manual invite, and the joiner who asks to join with the invite, receives the welcome from Admit and
// Approve and completes it. Every directory lives under the first argument.
package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	kolobraid "github.com/ciranda-dev/kolo-braid"
	kolodht "github.com/ciranda-dev/kolo-dht"
	koloid "github.com/ciranda-dev/kolo-id"
	invite "github.com/ciranda-dev/kolo-invite"
	kolostore "github.com/ciranda-dev/kolo-store"
	koloswarm "github.com/ciranda-dev/kolo-swarm"
)

// apply keeps every data entry under its position.
func apply(tx *kolobraid.Tx, e *kolobraid.Entry) error {
	if e.Kind != kolobraid.KindData {
		return nil
	}
	return tx.Put(binary.BigEndian.AppendUint64([]byte("msg/"), e.Position), e.Body)
}

func main() {
	if len(os.Args) != 2 {
		log.Fatal("usage: addwriter <temp dir>")
	}
	dir := os.Args[1]
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	var wg sync.WaitGroup
	open := func(name string) kolostore.Backend {
		db, err := kolostore.OpenPebble(filepath.Join(dir, name))
		check(err)
		return db
	}
	identity := func() *koloid.Identity {
		id, err := koloid.Generate()
		check(err)
		return id
	}
	adminDB, inviteDB, joinerDB := open("admin"), open("invites"), open("joiner")
	defer adminDB.Close()
	defer inviteDB.Close()
	defer joinerDB.Close()
	defer wg.Wait()
	defer cancel()

	// The DHT server that peers find each other through.
	server, err := kolodht.New(ctx, "127.0.0.1:0", identity(), &kolodht.Config{Server: true})
	check(err)
	defer server.Close()
	boot, err := netip.ParseAddrPort(server.Addr().String())
	check(err)

	// The admin: a braid, a kolo-invite instance and a swarm.
	idA := identity()
	a, err := kolobraid.Create(ctx, idA, kolobraid.GroupConfig{App: "chat/v1"},
		kolobraid.Options{Backend: adminDB, App: "chat/v1", Apply: apply})
	check(err)
	defer a.Close()
	in, err := invite.Open(inviteDB, idA, nil)
	check(err)
	defer in.Close()
	admitted := make(chan error, 1)
	wg.Go(func() { // a joiner's request is answered with Admit, then Approve; Deny and RemoveMember undo a failure
		for {
			ev, err := in.Next(ctx)
			if err != nil {
				return
			}
			if ev.Kind != invite.Requested {
				continue
			}
			// nil options admit the joiner as a plain member: not an admin and not a settler
			welcome, err := a.Admit(ctx, ev.Joiner, ev.Data, nil)
			if err != nil {
				in.Deny(ev.Request)
				admitted <- err
				continue
			}
			if err := in.Approve(ev.Request, welcome); err != nil {
				a.RemoveMember(ctx, ev.Joiner)
				admitted <- err
				continue
			}
			admitted <- nil
		}
	})
	var adminBraid atomic.Pointer[kolobraid.Braid]
	adminBraid.Store(a)
	adminSwarm := startSwarm(ctx, &wg, idA, boot, &adminBraid, func(p *koloswarm.Peer) {
		wg.Go(func() { in.ServePeer(ctx, p) })
	})
	defer adminSwarm.Close()
	wg.Go(func() { in.Announce(ctx, adminSwarm) })
	check(adminSwarm.Join(ctx, koloswarm.Topic(a.Topic()), &koloswarm.JoinOptions{Announce: true, Lookup: true}))
	invitation, _, err := in.Create(&invite.CreateOptions{Manual: true, TTL: time.Minute})
	check(err)
	code, err := invite.Parse(invitation)
	check(err)
	fmt.Println("admin: created the braid and a manual invite")

	// The joiner: a new strand, found the inviter through the invite's topic, sent the request, received the welcome.
	idB := identity()
	join, err := kolobraid.NewJoin(ctx, idB, kolobraid.Options{Backend: joinerDB, App: "chat/v1", Apply: apply})
	check(err)
	request, err := join.Request(nil)
	check(err)
	var joinerBraid atomic.Pointer[kolobraid.Braid]
	inviter := make(chan koloswarm.Conn, 1)
	joinerSwarm := startSwarm(ctx, &wg, idB, boot, &joinerBraid, func(p *koloswarm.Peer) {
		if code.Matches(p.PublicKey()) {
			select {
			case inviter <- p.Conn():
			default:
			}
		}
	})
	defer joinerSwarm.Close()
	check(joinerSwarm.Join(ctx, code.Topic(), &koloswarm.JoinOptions{Lookup: true}))
	var conn koloswarm.Conn
	select {
	case conn = <-inviter:
	case <-ctx.Done():
		log.Fatal("the invite topic found no inviter")
	}
	welcome, err := invite.JoinConn(ctx, conn, idB, code, &invite.JoinOptions{Data: request})
	check(err)
	fmt.Println("joiner: sent its join request and received a welcome")
	select {
	case err := <-admitted:
		check(err)
	case <-ctx.Done():
		log.Fatal("the admin did not answer")
	}
	fmt.Println("admin: admitted the joiner with an AddMember entry")

	// The invite connection is done: leave its topic, complete the join and sync on the braid's own topic.
	joinerSwarm.Leave(code.Topic())
	select {
	case <-conn.Context().Done():
	case <-ctx.Done():
		log.Fatal("leaving the invite topic kept the invite connection")
	}
	b, err := join.Complete(ctx, welcome)
	check(err)
	defer b.Close()
	joinerBraid.Store(b)
	check(joinerSwarm.Join(ctx, koloswarm.Topic(b.Topic()), &koloswarm.JoinOptions{Announce: true, Lookup: true}))
	fmt.Println("joiner: completed the join")

	_, err = a.Append(ctx, []byte("hello from the admin"))
	check(err)
	_, err = b.Append(ctx, []byte("hello from the joiner"))
	check(err)
	va, vb := settled(ctx, a, 2), settled(ctx, b, 2)
	fmt.Printf("both settled %d messages in the same order: %v\n", len(va), slices.Equal(va, vb))
}

// startSwarm joins the DHT through boot and runs the one event loop of a peer: a connection that shares the topic of the
// peer's braid is served by the braid, every other connection goes to other.
func startSwarm(ctx context.Context, wg *sync.WaitGroup, id *koloid.Identity, boot netip.AddrPort,
	braid *atomic.Pointer[kolobraid.Braid], other func(*koloswarm.Peer)) *koloswarm.Swarm {
	sw, err := koloswarm.New(ctx, "127.0.0.1:0", id, &koloswarm.Config{LookupInterval: time.Second})
	check(err)
	check(sw.Bootstrap(ctx, []netip.AddrPort{boot}))
	wg.Go(func() {
		for {
			ev, err := sw.Next(ctx)
			if err != nil {
				return
			}
			if ev.Kind != koloswarm.Connected {
				continue
			}
			if b := braid.Load(); b != nil && slices.Contains(ev.Peer.Topics(), koloswarm.Topic(b.Topic())) {
				wg.Go(func() { b.ServeConn(ctx, ev.Peer.Conn()) })
				continue
			}
			other(ev.Peer)
		}
	})
	return sw
}

// settled waits until the braid's settled view holds n messages and returns them in the braid's order.
func settled(ctx context.Context, b *kolobraid.Braid, n int) []string {
	for {
		s := b.Settled()
		var msgs []string
		for _, v := range s.Scan([]byte("msg/"), []byte("msg0")) {
			msgs = append(msgs, string(v))
		}
		s.Close()
		if len(msgs) >= n {
			return msgs
		}
		if _, err := b.Next(ctx); err != nil {
			log.Fatal(err)
		}
	}
}

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
