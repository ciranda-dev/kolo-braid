# kolo-braid

The signed strands of several members merged into one deterministic order.

[![status: experimental](https://img.shields.io/badge/status-experimental-orange)](#status)
[![license: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue)](#license)
[![go: 1.26+](https://img.shields.io/badge/go-1.26%2B-00ADD8)](go.mod)

## What it is

kolo-braid merges the signed logs of several writers into one order that every replica computes the same way, by Lamport's [happened-before relation](https://doi.org/10.1145/359545.359563) and the round-based DAG commit rule of [DAG-Rider](https://doi.org/10.1145/3465084.3467905) and [Bullshark](https://doi.org/10.1145/3548606.3559361), applied to a deterministic [state machine](https://doi.org/10.1145/98163.98167). A braid is a set of strands: one signed [kolo-log](https://github.com/ciranda-dev/kolo-log) per member, stored with [kolo-store](https://github.com/ciranda-dev/kolo-store). Every peer that holds the same entries computes the same order, byte for byte, and feeds it to an `apply` function you write, which builds a view on kolo-store. Peers sync with `ServeConn`, which works over direct and relayed connections ([kolo-secure](https://github.com/ciranda-dev/kolo-secure), [kolo-relay](https://github.com/ciranda-dev/kolo-relay) or any [kolo-swarm](https://github.com/ciranda-dev/kolo-swarm) peer), and a new member joins with a request and a welcome that [kolo-invite](https://github.com/ciranda-dev/kolo-invite) can carry.

- The settled prefix of the order never changes. The tentative tail after it is applied in memory and can still reorder, so reads choose `Braid.Settled` or `Braid.View`.
- `apply` must be deterministic: no clock, no randomness, no map iteration order, no I/O, and no floating point that reaches the view unless it is rounded after each operation. `Reject(reason)` discards an entry's writes on every peer. Any other error, and a panic, halts the view on this peer only (`EventHalted`), and the next `Open` retries. `kolobraidtest.CheckApply` and `FindDivergence` check an apply function on histories you give them.
- To change `apply` between versions, never change the effect of a position an older version applied: start new behaviour at a position marked by a settled data entry, and let old code halt on a marker it does not know (`kolobraidtest.CheckUpgrade`).
- Membership is ordered like data. A member or settler change takes effect at its position, and an entry of a removed strand ordered before the removal keeps its effect, those after it are void. A removed member keeps everything it received, and peers that hold the removal serve it nothing more.
- Settlement needs a quorum of settlers online. With Majority, 4 settlers give q = 3: one can be offline and one can sign two histories without splitting settled prefixes; 3 or 5 settlers give no such margin. Settlement stops while fewer than q settlers are online, and a braid with one settler depends on that device. A settler that signs two histories stops settlement for good on every peer that sees both. When the quorum is lost for good, an admin starts a successor braid with `Succeed`, seeded with the settled view; tentative entries of the old braid are not carried. A member that joins the successor and still holds the old braid passes it as `Options.Predecessor`, and its braid halts unless the successor's founder was an admin of the old one and the seeded view is the old settled view.
- The order among concurrent entries is not fair and a settler can try to choose it: a "first wins" rule must not rely on it.
- After every `Open`, a device writes only once a member, a pin or, for a braid that never had another writer, its own storage confirms its strand (a head as long as the strand confirms it at once, a shorter one after `ConfirmWait`); a settler waits for the heads of the other settlers that answer within `ConfirmWait`, and of at least q - 1 of them. Never copy a writer's data directory or run a copy of it.
- `Admit` with no options admits a plain member: it writes, but is no admin and no settler. `AdmitOptions.GrowSettlers` makes the first joiners admins and settlers until the braid has `MinSettlers` seats: an admin can add and remove anyone, so use it only for joiners you trust that far, and with 3 or 5 settlers a badly restored or cloned settler device can split the braid.
- `Braid.ServeConn` trusts `Conn.RemotePublicKey` completely: pass only connections whose key was proved in a handshake, such as a `*kolosecure.Conn`.
- A braid entry can never be erased, so data that may need erasure stays outside the braid. History is never pruned, and each entry costs about 600 bytes of disk on every peer.

## Install

```sh
go get github.com/ciranda-dev/kolo-braid
```

kolo-braid is pure Go. Build with `CGO_ENABLED=0` for a binary with no C code; kolo-store's on-disk backend links a C zstd library when cgo is enabled.

## Quick start

Two members on one machine, each with a directory of kolo-store's on-disk backend under the first argument, synced over kolo-secure on 127.0.0.1: A creates the braid and admits B with an AddMember entry, both append a message, and each settles both in the same order. The program takes a new directory in a temporary location. It is [examples/twopeers](examples/twopeers/main.go); run it with `go run ./examples/twopeers "$(mktemp -d)"` in a clone of this repository.

```go
// Command twopeers starts a braid and admits a second member, each on a directory of kolo-store's on-disk backend under the
// first argument, syncs them over kolo-secure on 127.0.0.1 and prints what each one settled.
package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"

	kolobraid "github.com/ciranda-dev/kolo-braid"
	koloid "github.com/ciranda-dev/kolo-id"
	kolosecure "github.com/ciranda-dev/kolo-secure"
	kolostore "github.com/ciranda-dev/kolo-store"
)

// apply keeps every message under its position and counts them.
func apply(tx *kolobraid.Tx, e *kolobraid.Entry) error {
	if e.Kind != kolobraid.KindData {
		return nil
	}
	if len(e.Body) == 0 {
		return kolobraid.Reject("empty message")
	}
	n := 0
	if v, err := tx.Get([]byte("count")); err == nil {
		n, _ = strconv.Atoi(string(v))
	}
	if err := tx.Put(binary.BigEndian.AppendUint64([]byte("msg/"), e.Position), e.Body); err != nil {
		return err
	}
	return tx.Put([]byte("count"), []byte(strconv.Itoa(n+1)))
}

func main() {
	if len(os.Args) != 2 {
		log.Fatal("usage: twopeers <temp dir>")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	options := func(name string) kolobraid.Options {
		db, err := kolostore.OpenPebble(filepath.Join(os.Args[1], name))
		check(err)
		return kolobraid.Options{Backend: db, App: "chat/v1", Apply: apply}
	}
	idA, err := koloid.Generate()
	check(err)
	idB, err := koloid.Generate()
	check(err)
	optsA, optsB := options("a"), options("b")
	defer optsA.Backend.Close()
	defer optsB.Backend.Close()

	// A creates the braid; B asks to join and A admits it with an AddMember entry.
	a, err := kolobraid.Create(ctx, idA, kolobraid.GroupConfig{App: "chat/v1"}, optsA)
	check(err)
	defer a.Close()
	join, err := kolobraid.NewJoin(ctx, idB, optsB)
	check(err)
	request, err := join.Request(nil)
	check(err)
	// nil options admit B as a plain member: not an admin and not a settler.
	welcome, err := a.Admit(ctx, idB.PublicKey(), request, nil)
	check(err)
	b, err := join.Complete(ctx, welcome)
	check(err)
	defer b.Close()

	// One kolo-secure connection on 127.0.0.1, served by both braids.
	l, err := kolosecure.Listen(ctx, "127.0.0.1:0", idA, nil)
	check(err)
	defer l.Close()
	go func() {
		if c, err := l.Accept(ctx); err == nil {
			a.ServeConn(ctx, c)
		}
	}()
	conn, err := kolosecure.Dial(ctx, l.Addr().String(), idB, idA.PublicKey(), nil)
	check(err)
	defer conn.Close()
	go b.ServeConn(ctx, conn)

	_, err = a.Append(ctx, []byte("hello from A"))
	check(err)
	_, err = b.Append(ctx, []byte("hello from B"))
	check(err)
	va, vb := settled(ctx, a, 2), settled(ctx, b, 2)
	fmt.Printf("A settled %d messages\n", len(va))
	fmt.Printf("B settled %d messages\n", len(vb))
	fmt.Println("same order:", fmt.Sprint(va) == fmt.Sprint(vb))
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
```

```text
$ go run ./examples/twopeers "$(mktemp -d)"
A settled 2 messages
B settled 2 messages
same order: true
```

More programs, each taking a new temporary directory as its only argument and binding 127.0.0.1 only:

- [examples/writer](examples/writer/main.go): an identity saved in a file, a braid created once and opened again after a restart.
- [examples/apply](examples/apply/main.go): one member, an apply function with `Reject`, the settled view and the entries rejected.
- [examples/addwriter](examples/addwriter/main.go): kolo-invite on kolo-swarm with an in-process kolo-dht server, `Admit`, `Approve` and `Complete`.
- [examples/devices](examples/devices/main.go): a device roster, an anchored first device, a certified phone admitted without a new decision, and its revocation.
- [examples/removewriter](examples/removewriter/main.go): `RemoveMember`, what the removed member keeps and is refused, and joining again with a new strand.

## API

| Symbol | What it does |
|---|---|
| `Create`, `Open`, `Close` | Start a braid with its genesis entry, open a stored one, stop it |
| `Succeed` | Start a successor braid seeded with an old braid's settled view, when its settler quorum is lost |
| `Braid.Append` | Write a data entry; after every `Open` it waits until the own strand is confirmed |
| `Braid.AddMember`, `RemoveMember`, `SetSettlers` | Membership entries, effective at their position in the order |
| `NewJoin`, `Joining.Request`, `Braid.Admit`, `Joining.Complete` | A new member's strand, its join request, the admin's AddMember and welcome, and the joiner's braid, whose writing calls wait until its own AddMember entry is ordered |
| `Braid.ServeConn`, `Conn` | Sync with one peer over a direct or relayed connection: strands through kolo-sync, heads, second histories and evidence |
| `ApplyFunc`, `Tx`, `Reject` | The apply contract: one transaction per entry, `Reject` as the only deterministic failure |
| `Braid.View`, `Braid.Settled`, `Snapshot` | Frozen reads of the view with the roster of the same position: settled plus tentative, or settled only; `Snapshot.Settlers` gives the seats and the quorum of one config, `ScanErr` a scan with its own error and `Err` the first error of any scan |
| `Braid.SettledAt` | The settled view and roster as they were at a past position, replayed into scratch storage from the last batch end a call reached |
| `Braid.Entries`, `Ordered` | The order from a position on; ends with `ErrReorged` when a yielded position changed |
| `Braid.Next`, `Event` | Applied, settled, reorg, membership, equivocation, halt and divergence events |
| `Braid.Status` | Positions, settler config, pending config, confirmation and halt state |
| `PrefixBackend` | Several braids and other Kolo users on one backend |
| `kolobraidtest.CheckApply`, `CheckUpgrade`, `FindDivergence` | Check that an apply function is deterministic, that a new version keeps old positions, and where replicas first differ |
| `kv.New`, `Store.Apply`, `Put`, `Delete`, `Get`, `Range` | A key-value map in the view: the last effective write in the braid's order wins |
| `devices.New`, `Store.Apply`, `CertBody`, `RevocationBody`, `Cert`, `Anchored`, `Devices`, `Revocations`, `Verify` | A roster of device certificates and revocations in the view, and `Verify` of a device's message for a person; `RevocationsOf` the revocations that void a certificate, its issuer's included; `CertTx`, `RevocationsTx` and `RevocationsOfTx` read it inside apply, at an entry's position |
| `devices.NewJoinRequest`, `Store.Record`, `Store.Admit` | A certified device joins with a proof; an admin anchors a person's first device with `Record` and admits its later devices with `Admit`, without a new decision; an opt-in `Options.MaxDevices` ceiling gives `ErrTooManyDevices` |
| `Braid.PublicKey`, `Braid.CheckAdmit`, `WelcomeEntry` | Read-only: this peer's key, what `Admit` would refuse now (and a full group), and the AddMember entry a welcome names |

Errors work with `errors.Is`; the library's own error messages start with `kolobraid:`.

## Defaults

Fixed by the genesis entry (`GroupConfig`): `Quorum` Majority (crash faults; TwoThirds when a settler device may be compromised), `MaxMembers` 64 (one node's default peer budget), `MaxSettlers` 7 (Majority then tolerates 3 offline), `MinSettlers` 4 (the margin above), `MaxBody` 1 MiB (the view's value bound) and `CheckpointEvery` 1024 settled positions (a divergence is caught within that many). Local bounds (`Limits`) have their own defaults, ranges and reasons in [docs/limits.md](docs/limits.md).

## Formats

Every hash is BLAKE2b-256 over a kolo-codec encoding that starts with its own tag: `kolo-braid/entry-id/v0`, `kolo-braid/order/v0`, `kolo-braid/config/v0`, `kolo-braid/digest/v0`, `kolo-braid/content/v0` and `kolo-braid/topic/v0`. Entries start with `kolo-braid/entry/v0`, the join request with `kolo-braid/join/v0`, the welcome with `kolo-braid/welcome/v0` and the kv helper's body with `kolo-braid/kv/v0`. kolo-braid adds no signature of its own: kolo-log signs every entry. The entry kinds and the stream messages are in the [guide](docs/guide.md).

## More

- [docs/guide.md](docs/guide.md): strands and the order, the apply function, writing, membership, joining, syncing, successor braids, the kv helper and the devices of a person.
- [docs/limits.md](docs/limits.md): every default, range and fixed bound with its reason.
- [SECURITY.md](SECURITY.md): security policy.

## Status

Beta and experimental, like every Kolo library at v0.x: the API may change before v1.0.0.

Tested with unit, property and fuzz tests, bounded model checks of settlement and membership (`scripts/modelcheck.sh`),
which cover only the sizes they enumerate and are not a proof for any size, and scenarios with several processes on one
Mac, on 127.0.0.1 only. It has not been tested over a real network or through NATs. The Linux and Windows builds were vetted,
not run. The full test run and the race detector run (`go test -race`) on the root package, `internal/dag`, `internal/settle`
and `kv` passed on 2026-11-18; CI runs only when started by hand.

Settlement keeps one round mark per settler per round of the current settler config, until the config changes: 16 bytes
in memory and one key of about 40 bytes on disk, written once. A braid that settles all day with 4 settlers at the default
`PulseGap` can add up to about 1.4 million a day, about 22 MB of memory and 55 MB of disk; with 64 settlers 16 times that
(about 350 MB of memory and 880 MB of disk a day), and more with a shorter `PulseGap` (up to 50 times at 10 ms). These are
upper bounds; slice growth adds up to a quarter to the memory figure. They are what lets a pulse that arrives late get its exact round, and a settler config change removes them.

## License

Licensed under either of the [Apache License, Version 2.0](LICENSE) or the [MIT license](LICENSE-MIT), at your option.
