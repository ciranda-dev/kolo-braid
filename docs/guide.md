# Guide

## Strands, the order and the view

A braid is a set of strands. A strand is one member's kolo-log, created with
the `Bytes` value encoding; each of its entries is one braid entry: the group
ID, a kind, the ID of the strand's previous entry, at most one link per other
strand and a body. kolo-log signs and encrypts every entry; kolo-braid adds no
signature of its own.

Links make the entries a DAG. Every peer orders the entries it holds the same
way:

- The settled prefix is made of batches. Settlers write pulse entries; a pulse
  of an even round on the round's pivot seat is a pivot, and q seats that hold
  it in their next pulses commit it. Each committed pivot settles the causal
  past it adds, in a topological order whose ties are broken by a hash seeded
  with the pivot's ID. Settled positions never change.
- The tentative tail is everything else, ordered as the batches it is
  predicted to settle in. It is applied in memory on top of the settled view
  and can reorder when entries arrive; `EventReorg` gives the first position
  that changed. When a batch settles in the predicted order, its tentative
  write-sets are kept as they are, without calling apply again.

Positions of tentative entries change on a reorg: data kept outside the view
refers to entries by `Entry.ID` or by settled position. `Entry.Index` is not
unique, because a writer that signs two histories has two entries at one
index.

## Create and open

```go
b, err := kolobraid.Create(ctx, id, kolobraid.GroupConfig{App: "chat/v1"},
	kolobraid.Options{Backend: db, App: "chat/v1", Apply: apply})
check(err)
defer b.Close()
```

`GroupConfig` is fixed by the genesis entry and agreed by every peer.
`Options` are local: the backend, a key prefix, the apply function, pins and
`Limits`. Several braids share one backend through `PrefixBackend(db, prefix)`
or `Options.Prefix`, with prefixes of which none is a prefix of another:
`Create`, `Open` and `NewJoin` record each prefix at the root of the backend
and refuse one that nests with a recorded one (`ErrInvalidArgument`).
`Create` that fails after it stored the genesis leaves a braid that `Open`
continues; a second `Create` on that prefix is refused. Keep one `Braid`
value per prefix at a time: `Open` clears the prefix's temporary and scratch
keys, so a second value opened on a prefix in use breaks the first one; close
a value before you open the prefix again.

`Open` opens a stored braid. It continues from the stored settled state: the
committed pivot list, the settlement state and the view's progress, all
written in atomic batches. Batches that were settled but not yet applied when
the process stopped are applied again from the entry index; a crash at any
point leaves a state that continues to the same view. `Options.Rebuild`
replays the whole view from genesis along the stored pivots, with the apply
function given now.

## The apply function

```go
type ApplyFunc func(tx *Tx, e *Entry) error
```

- apply must be a pure deterministic function of the view before `e`, `e`
  and the roster at `e`'s position (`Tx.Member`, `Tx.Members`; `Member.Seat`
  is set for seats of the settler config of that position). It sees no clock,
  no randomness, no map iteration order and no I/O.
- Floating point that reaches the view must be rounded explicitly after each
  operation, for example `float64(x * y)`, and functions of `math` other than
  `Abs`, `Copysign`, `Floor`, `Ceil`, `Trunc` and `Sqrt` must not reach it.
  Integers or decimal strings are the simple choice.
- apply is called for effective entries of kinds genesis, data, AddMember,
  RemoveMember, Settlers and Evidence; never for pulses, seeds and void
  entries. For membership kinds the library has already applied the effect:
  rejecting one discards only apply's own writes.
- nil commits the writes. `Reject(reason)` discards them and records the entry
  as rejected on every peer. Any other error, or a panic, halts the view on
  this peer only: `EventHalted` and `Status.Halted` carry the position, and the
  next `Open` retries.
- Per entry: keys at most 32 KiB, values at most 1 MiB, and a write budget of
  16 x MaxBody where each operation costs its key and value plus 256 bytes.
  Past a bound the call returns `ErrViewLimit` and the entry is rejected on
  every peer, whatever apply returns.
- A `Tx` used after apply returned fails with `ErrTxDone`.
- apply runs on its own goroutine: a slow apply delays only application. It
  can also run on other goroutines at the same time (`Braid.SettledAt` calls
  it on the caller's) and more than once for one position (tentatively, at
  settlement, after `Open`, in a replay), which a pure apply allows. It must
  never call a method of the `Braid`: a `SettledAt` from apply waits for
  itself until its context ends.

Changing apply between versions: never change the effect of an entry at a
position an older version already applied. Start new behaviour at a position
marked by a settled data entry the application defines (or a version field in
the bodies), keep the old code path before it, and let old code return a
non-`Reject` error on a marker it does not know, so it halts until it is
upgraded. `kolobraidtest.CheckUpgrade(t, old, new, entries, marker)` checks it.

### Checking that apply is pure

The library makes the order deterministic; only you can make apply pure, and
an impure apply breaks convergence even though every peer holds the same
order. Two helpers in `kolobraidtest` run your apply on a history whose final
order you give:

- `CheckApply(t, apply, entries)` fails the test when settling the history one
  entry at a time, in random batches and chunk sizes, across restarts, and
  after a tentative reorganisation and promotion do not all end with the same
  running digest and view.
- `FindDivergence(apply, entries, replicas, seed)` runs replicas that each
  receive the entries in their own shuffled delivery order, apply what they
  hold tentatively, reorganise when an earlier entry arrives and settle
  piece by piece. It returns the first position where a replica's running
  digest differs from replica 0's (`*Divergence`, nil when all agree), or an
  error when apply halted.

This example is `kolobraidtest/example_test.go`; `go test` compiles it and
checks its output. The second apply counts its calls in a variable outside
the view, so replicas that apply tentative entries a different number of
times disagree at the first position:

```go
entries := make([]kolobraid.Entry, 12)
for i := range entries {
	entries[i] = kolobraid.Entry{Writer: bytes.Repeat([]byte{byte(1 + i%3)}, 32), Kind: kolobraid.KindData,
		Body: []byte(fmt.Sprintf("k%d=%d", i%4, i))}
}
// good is pure: its effect depends only on the view, the entry and the roster.
good := func(tx *kolobraid.Tx, e *kolobraid.Entry) error {
	k, v, _ := strings.Cut(string(e.Body), "=")
	return tx.Put([]byte(k), []byte(v))
}
// bad is not: it counts its calls in a variable outside the view, and replicas call it a different number of times.
calls := 0
bad := func(tx *kolobraid.Tx, e *kolobraid.Entry) error {
	calls++
	return tx.Put([]byte("calls"), []byte(strconv.Itoa(calls)))
}
for _, c := range []struct {
	name  string
	apply kolobraid.ApplyFunc
}{{"good", good}, {"bad", bad}} {
	d, err := kolobraidtest.FindDivergence(c.apply, entries, 4, 1)
	switch {
	case err != nil:
		fmt.Println(c.name, "halted:", err)
	case d == nil:
		fmt.Println(c.name, "agrees on every replica")
	default:
		fmt.Println(c.name, "diverges at position", d.Position, "on replica", d.Replica)
	}
}
// Output:
// good agrees on every replica
// bad diverges at position 0 on replica 1
```

A clean result covers only the paths and the entries you gave: build
histories that use every kind of body your application writes.

Every peer keeps a running digest of the settled write-sets. At each
checkpoint (the end of the first settled batch that reaches each multiple of
`CheckpointEvery`) the seats claim their digest in their next pulse; a peer
whose digest differs from one that q seats claimed halts with `ErrDiverged`
and an `EventDiverged`.

## Writing

`Append`, `AddMember`, `RemoveMember` and `SetSettlers` check their context,
the tentative roster (`ErrNotMember`, `ErrNotAdmin`, `ErrRemoved`), the own
strand (`ErrUnconfirmed`, `ErrStrandLost`) and sizes (`ErrTooLarge`), then
write one entry. Whether it takes effect is decided at its position. A braid entry can
never be erased: every peer keeps every entry and its key, so data that may
need erasure stays outside the braid. History is never pruned; each entry
costs about 600 bytes of disk on every peer.

After every `Open` the own strand is unconfirmed, and writing calls wait for
it as long as their context allows:

- a braid that never had another member and has no pin is confirmed by its
  own storage at once;
- a member that is not a settler is confirmed by one consistent head of its
  strand from a member or a pin that is as long as the strand, and by a shorter
  one only once `ConfirmWait` has passed since `Open`, so a member that is as
  far behind as a restored backup cannot confirm it before a member that holds
  a longer head has shown it;
- a settler needs such heads from every other settler that answers within
  `ConfirmWait`, and from at least q - 1 of them;
- an own strand that is still empty counts a peer that holds no head of it
  as such a head of size 0, so a member that never wrote is not stuck;
- a longer consistent head means the device is behind: writing calls wait
  again until it caught up from a complete copy (kolo-store `Store.Adopt`)
  instead of writing a second history; a head that conflicts loses the strand:
  writes fail with `ErrStrandLost` and `EventStrandLost` is raised. Every later
  head is checked the same way, so a second running copy stops at its first
  conflicting head; once the strand wrote after its confirmation, a longer head
  can only come from such a copy, and it loses the strand too.

Never copy a member's data directory or run a copy of it. Two running copies
both confirm and both write before either sees the other.
`Options.AppendUnconfirmed` writes at once, with that risk.

## Reading the view

- `Braid.Settled` and `Braid.View` return frozen snapshots: settled only, or
  settled plus tentative. The view publishes the roster with every durable
  commit and every tentative entry, so a snapshot's `Members` is the roster
  after its last entry, also inside a batch of several commits and while the
  tail is applied. `Member.Seat` marks the seats of one settler config, which
  `Snapshot.Settlers` returns with its rule, its quorum and every seat in the
  same read: the config of the batch that settled the snapshot's last entry
  (the old config through the batch whose pivot activates the next one), or
  for `View` the config its tentative entries are applied under. A removed
  member keeps its seat, with no roles, until a config without it activates.
  `Status.Quorum` belongs to the current config, read at another moment: do
  not pair it with a snapshot's seats.
- `Snapshot.Scan` ends early on a storage error or a `Close`. `ScanErr` returns
  the iteration and a function that reports the error of that scan alone;
  `Snapshot.Err` keeps the first error of any scan of the snapshot. A `Get`
  or a `Scan` that a `Close` overtakes returns `ErrClosed`.
- `Braid.SettledAt(ctx, p)` returns a snapshot of the settled view and roster
  as they were right after the entry at position p, the same as `Settled`
  gives at that position. It replays the settled order into scratch storage
  under the braid's prefix, calling apply on the caller's goroutine, one call
  at a time. The replay is kept at the last batch end a call reached: a later
  position costs a copy of the view there and one apply call per position
  after it, an earlier one a replay from the braid's first position. `Close`
  deletes the snapshot's storage; so does the collection of a snapshot never
  closed, `Close` of the braid (later reads return `ErrClosed`), and `Open`
  after a crash. A position that is not settled yet is `ErrNotFound`:
  tentative positions can still change. One before the braid's first
  position (in a successor, the old braid's position plus one) is
  `ErrInvalidArgument`. A context that ends stops the replay with its error.
- To decide something at an entry's position, decide it in apply: `Tx` reads
  the view and the roster at that position, and what apply records there is
  part of the settled view, the same on every peer, with no replay later.

## Membership

Membership entries are checked at their own position in the order, never
against the roster at arrival time, so every peer decides them the same way.

- AddMember by an admin adds a key with its strand, sets its roles, or
  replaces its strand by a new one. RemoveMember by an admin removes it; at
  least one admin and one settler key always remain.
- Entries of a removed strand ordered before the removal keep their effect;
  those ordered after it are void. Removals go first among entries that are
  ready together, so a concurrent entry is kept only when it was ordered
  before the removal became ready.
- A removed member keeps everything it received. Peers that hold the removal
  stop linking its strand and stop serving it from their next request on, and
  its later entries are void everywhere. A removed settler is the exception
  until a settler config without it activates: its pulses still count, so it
  goes on pulsing, peers go on linking it, serving it entry keys and keeping
  its links. It gets `EventRemoved` once it holds
  the removal settled, which it can only get from a peer that did not hold the
  removal yet; otherwise it simply receives nothing more.
- Settlers entries name the settler keys and the quorum rule. A change that
  adds seats activates at a committed pivot whose past holds a readiness pulse
  from every added seat that the last pivot committed before the change
  settled does not hold (one written after the change settled always counts);
  a change that only removes activates at once. Once every added seat is ready
  (`Status.AwaitingReadiness` empty), the online current settlers pulse until
  that pivot exists, with no other entry needed. Until a change activates, the
  old config settles, so a change never needs a seat that was not needed
  before. A settler that a change removes while it adds another keeps its
  seat, and entry keys, until the change activates (`Status.Config` changes);
  hand settlement over by waiting for that before you retire the old device.
  To cut a removed settler off at once, remove it in a change of its own first
  (which activates when it settles, so check that the seats left reach their
  quorum online and keep the margin you want), then add the new settler.

| Seats | Majority q | settlers that may sign two histories without split prefixes | TwoThirds q |
|---|---|---|---|
| 1 | 1 | 0 | 1 |
| 2 | 2 | 1 | 2 |
| 3 | 2 | 0 | 3 |
| 4 | 3 | 1 | 3 |
| 5 | 3 | 0 | 4 |
| 7 | 4 | 0 | 5 |

Settlement goes on while q settlers of the current config are online. A
braid with one settler depends on that device. A settler of the current config
that signs two histories stops settlement for good on every peer that sees
both (`EventSettlementHalted` with its key); tentative application and new
entries go on. With 3 or 5 settlers under Majority such a settler can split
settled prefixes during a partition; with 4 it cannot.

A writer that is not a settler and signs two histories is fenced: once both
histories reach a peer, it holds two signed records of one index, appends an
Evidence entry, and the strand is inactive from that entry's position on.
Settlement goes on.

The order among concurrent entries is decided by a hash seeded with the
pivot, which the pivot's settler can try to choose: it is not fair, and "first
wins" rules must not rely on it.

## Joining

A new member makes its strand and asks an admin to add it; the admin's answer
is the welcome. kolo-invite carries both: the join request as the join data,
the welcome as the payload sealed to the joiner by the inviter's pinned key.
kolo-braid only exchanges bytes and does not import kolo-invite.

```go
j, err := kolobraid.NewJoin(ctx, id, opts) // the joiner's new strand
check(err)
req, err := j.Request(appData) // to the admin
check(err)
welcome, err := b.Admit(ctx, joinerKey, req, nil) // the admin: appends AddMember
check(err)
nb, err := j.Complete(ctx, welcome) // the joiner's braid
check(err)
defer nb.Close()
```

A join survives a restart of the joiner: `NewJoin` on the same prefix with the
same identity returns the pending join, with the same strand and request, and
the welcome issued for it completes it. `Complete` that fails after it stored
the welcome leaves a join that `Open` continues.

### Admission with kolo-invite

The admin creates invites with `Manual: true` (an invite that accepts on its
own sends one fixed payload, and every joiner needs its own welcome) and
answers each `Requested` event:

```go
code, _, err := in.Create(&invite.CreateOptions{Manual: true, TTL: time.Hour})
check(err)
// for every invite.Requested event ev:
welcome, err := b.Admit(ctx, ev.Joiner, ev.Data, nil)
if err != nil {
	in.Deny(ev.Request)
	refused := errors.Is(err, kolobraid.ErrActiveKey) || errors.Is(err, kolobraid.ErrKnownStrand) ||
		errors.Is(err, kolobraid.ErrInvalidArgument) || errors.Is(err, kolobraid.ErrNotAdmin) ||
		errors.Is(err, kolobraid.ErrRemoved)
	if !refused {
		b.RemoveMember(ctx, ev.Joiner) // a write failed after the AddMember entry
	}
	continue
}
if err := in.Approve(ev.Request, welcome); err != nil {
	b.RemoveMember(ctx, ev.Joiner) // expired, revoked or used up meanwhile
}
```

The joiner sends its request with `invite.JoinConn(ctx, conn, id, code,
&invite.JoinOptions{Data: req})` and completes with the payload. On kolo-swarm
it joins `code.Topic()` with `Lookup`, joins on the inviter's connection,
leaves the invite topic (which closes that connection), calls `Complete`, then
joins `koloswarm.Topic(nb.Topic())` and serves every peer of that topic with
`ServeConn`. The admin's application serves a connection with `ServeConn` when
the peer shares the braid's topic and with the inviter's `ServePeer`
otherwise: each must be the only acceptor of its connection.

What each side gets back:

| Case | Admin | Joiner |
|---|---|---|
| Admitted | `Admit` appends AddMember (and a Settlers entry when one is due); `Approve` sends the welcome | the welcome; `Complete` |
| A write fails after the AddMember entry (the Settlers entry of `GrowSettlers`) | that error; deny and `RemoveMember` | `invite.ErrDenied` |
| Already an active member | `ErrActiveKey`; deny | `invite.ErrDenied` |
| Strand used before or known | `ErrKnownStrand`; deny | `invite.ErrDenied` |
| Request strand signed by another key | `ErrInvalidArgument`; deny | `invite.ErrDenied` |
| The admin is not an admin, or holds its own removal | `ErrNotAdmin`, `ErrRemoved`; deny | `invite.ErrDenied` |
| The invite expired, was revoked or used up before `Approve` | `Approve` returns `invite.ErrExpired`, `ErrRevoked` or `ErrUsedUp`; `RemoveMember` | the same error, no welcome |
| Asking again after an accept | no event, nothing appended | the same welcome |
| Join data over the inviter's `MaxData` | no event | `invite.ErrTooLarge` |
| Welcome over the joiner's `MaxPayload` | `Approve` succeeds | `invite.ErrTooLarge`; asking again with a larger bound gets the same welcome |

`Admit` checks that the request's strand header is signed by the joiner's key,
refuses a key that is already active (`ErrActiveKey`, unless
`AdmitOptions.Replace`, which lets a member's new device take over with a new
strand and keep its roles) and a strand log ID that is used or known
(`ErrKnownStrand`), and appends the AddMember entry. Without options the joiner
is a plain member: it writes, but is no admin and no settler.
`AdmitOptions.Roles` sets the roles. `AdmitOptions.GrowSettlers` grows the
braid toward `MinSettlers` seats and `MinSettlers - 1` admins: the first
joiners become admins (they can add and remove anyone) and settlers (one of
them that signs two histories halts settlement for good), and a Settlers entry
is appended when one is due; use it only for joiners you trust that far. `Roles`
and `GrowSettlers` together are `ErrInvalidArgument`. `Admit` calls run one at
a time, so joiners admitted at once with `GrowSettlers` each count the entries
of the one before. A request at most `MaxJoinRequest` (64 KiB) long fits
kolo-invite's default `MaxData`.

A member removed and asked back joins with a new strand (`NewJoin` on a fresh
prefix); its old strand stays used. Its own old strand, written on another
device, is followed as a replica like any other.

The braid `Complete` returns starts once a member serves it the founder
strand's genesis entry through `ServeConn`; until then the view is empty.
Its writing calls wait, as long as their context allows, until it holds that
entry and its own AddMember entry is in the order, so the first `Append` needs
no wait of its own. `ErrNotMember` after that means the order made the
AddMember entry void, also after a restart: the batch that settles the entry
ends the wait for it. A strand that the order holds no member of gets
`ErrNotMember` or `ErrRemoved` at once, without waiting for confirmation. `Complete` refuses a welcome that does not parse
(`ErrMalformed`: each header must be signed by its key) and one that names the
joiner's own key or strand as another member's, or one strand under two keys
(`ErrInvalidArgument`). Afterwards the joiner halts when the welcome turns out
false: a founder strand whose first entry is not the group's genesis
(`ErrWrongGroup`), a genesis of another application (`ErrWrongApp`), or an
AddMember entry with another ID, or one that adds another key or strand
(`ErrNotMember`). It settles through the inviter's last committed pivot named
in the welcome, and trusts it as it trusts the AddMember: an anchor that is not
a committed pivot of its config halts settlement (`Status.SettlementHalted`).
A welcome of another braid makes the joiner close its connections to that
braid's members (their messages name another group).

An admin removed concurrently with its own Admit (it did not hold the removal
yet) writes an AddMember that is decided at its position like any other: when
the removal is ordered first, the joiner is not a member once that is settled,
and peers that hold the removal serve it nothing. The application gives up on
such a join after a timeout.

## Syncing

Join the braid's kolo-swarm topic (`Braid.Topic`) and serve every connected
peer with `ServeConn`:

```go
sw.Join(ctx, koloswarm.Topic(b.Topic()), nil)
// for every Connected event:
go b.ServeConn(ctx, ev.Peer.Conn())
```

`ServeConn` takes both `AcceptStream` (kolo-sync) and `AcceptUniStream` (the
braid's own messages) of the connection, so a connection that kolo-invite's
`ServePeer` uses cannot also serve the braid. `ServeConn` works over direct and
relayed connections: it takes a `Conn`, which a `*kolosecure.Conn`, a
`*kolorelay.Conn` and a `koloswarm.Conn` satisfy. A relayed connection is
end-to-end kolo-secure and the relay sees only ciphertext; the peer's key is
the one proved end to end, so the member and pin checks are the same as on a
direct connection. `ServeConn` returns nil when its context or the connection ends,
`ErrClosed` when the braid closes, and an error wrapping `ErrProtocol` after it
closed the connection of a peer that broke the protocol (code 1).

What moves on a connection:

- Strands, through kolo-sync, one pull per strand at a time and at most 16 at
  once, with no delete markers: a peer that serves a deleted entry
  (`ErrMarked`) or is behind (`ErrBehind`) leaves the rest of that strand to
  another peer. While it is active a strand is fetched to its settled length
  plus `MaxPullAhead`; a settler's strand also to `MaxPullAhead` past its
  newest pulse this peer holds; and every strand to whatever an entry this
  peer holds links, since every held entry must become placeable. The bound is
  checked every `AnnounceGap`, so a pull passes it by at most what arrives in
  that time, one sync message or more. A seat of the current or the pending
  config writes a pulse at least every 32 of its own entries (half the least
  `MaxPullAhead`), also while settlement is halted. That span pulse is a
  readiness pulse, which counts for nothing and which no rule holds back, so
  no writing call waits for it; votes keep their own rules. So its next pulse
  is always within reach and a burst never hides the pulses that settle it. A member that keeps
  writing while settlement waits costs every other peer about `MaxPullAhead`
  entries of its strand, until an entry that someone places links further; a
  settler is fetched as far as its pulses go. Past `MaxPendingBytes` only
  what pending entries link and the settler windows are fetched. A strand is
  linked only under the key the settled roster gives it, so an own entry never
  links entries read under the key of an AddMember entry that settles void
  (a new member's strand is linked once its AddMember entry settles). Placed
  entries that wait for settlement keep no data body in memory: it is read back from
  the strand when it is applied. Each replica keeps a cursor of the first
  entry still without its key, so a pull asks keys only from there, and keys
  missing below the bound are fetched without the entries. Settlement keeps one
  round mark per settler per round of the current settler config until the
  config changes: 16 bytes in memory and one store key of about 40 bytes,
  written once with its settled batch. With 4 settlers settling without pause at
  the default `PulseGap` that is up to about 1.4 million a day (22 MB of memory,
  55 MB of disk); with 64 settlers 16 times that (about 350 MB of memory and
  880 MB of disk a day), and up to 50 times more at the shortest `PulseGap`
  (10 ms). Slice growth adds up to a quarter to the memory figure. They are what
  lets a pulse that arrives late get its exact round, and a settler config
  change removes them.

  Why this never starves settlement: (1) a held entry becomes placeable,
  since its predecessor is held and every strand is fetched to whatever it
  links, so its whole past is fetched; (2) every pulse of an honest seat of
  the current or pending config is reached, since a peer holding its pulse at
  index i fetches it to at least i + 65 and the next pulse is at most 33
  entries later; (3) a joiner's anchor is in the past of its
  AddMember entry, which the welcome fetches; (4) a config's activating pivot
  and readiness pulses are in the past of its pulses. So nothing settlement
  needs stays out of reach.
- Heads (ANNOUNCE): every head this peer holds on the first message, then only
  the heads that changed, at most one message per `AnnounceGap`, in messages of
  at most `MaxMembers` heads. A receiver verifies only a head larger than what
  it holds; heads of strands it does not follow yet are kept, the newest per
  strand and at most 2 x `MaxMembers` per connection.
- Second histories (ENTRY_REQ, ENTRY): an entry linked by ID that the replica
  holds another entry, or only a delete marker, in place of is fetched with the
  writer's signed head, verified entry by entry against that head, linked to
  what is held by the head's own tree, and stored with the head and proofs so
  this peer serves it on. Per strand, of the entries that the entries of one
  strand wait for, only the lowest is asked, each connected peer once, one
  request per connection at a time; once every peer answered without it, it is
  asked again only after a new head of its strand or a new peer.
- A head of the size this peer holds with other bytes is verified once per
  connection; two such heads with different roots are evidence against the
  strand.
- Evidence (RECORD_REQ, RECORD, EVIDENCE): after a pull finds a second history
  the peer is asked for its record at the last held index; two records of one
  index signed by the writer are evidence, which every peer sends once per
  connection. Evidence counts only when the key that signed both objects is the
  key of the strand they name, from a message or from an Evidence entry; an
  Evidence entry placed before its strand is followed is checked once it is.
- After `Open`, the head of the own strand on each peer, which confirms the own
  strand or lets it catch up (see "Writing").

Access follows the tentative roster: active members and the settlers of the
current and the pending config get entry keys, pins get ciphertext only,
anyone else gets nothing from kolo-sync, and the braid
messages of such a key are ignored: only an ANNOUNCE is read past its prefix.
A member of the settled roster whose removal is only tentative stays known,
and a joiner talks to its inviter and the founder until its own AddMember
entry is ordered, also across a restart. A connection is closed once its key has been unknown for 60 s, which
leaves a joiner time to be added and closes a removed member's too. A strand
is followed under the key of the AddMember entry that the order makes
effective, never under the key of a void one (when a settled AddMember names
another key than a tentative one did, the braid records the settled key and
stops with `ErrCorrupt`, and `Open` reads the strand under it), and a head of a strand nobody
follows blocks `Admit` only when the joiner's own key signed it. Every head, record,
proof and entry from a peer is verified; a message that is malformed, over its
bound, of another group, version or type, with trailing bytes, or that does not
verify closes the connection, and so does a request not answered within 60 s.

## The kv helper

Package `github.com/ciranda-dev/kolo-braid/kv` keeps a key-value map in the
view. Puts and deletes are data entries; the last effective operation on a key
in the braid's order wins, on every peer alike, with no clock. Concurrent
writes are decided by the order, which is not fair (a settler can try to choose
it), so "the first write wins" rules must not rely on it. The map has no access
control unless `Options.Allow` is set: otherwise any active member can put or
delete any key.

```go
settings := kv.New([]byte("settings/"))
apply := func(tx *kolobraid.Tx, e *kolobraid.Entry) error {
	if ok, err := settings.Apply(tx, e); ok || err != nil {
		return err
	}
	return nil // the application's own entries
}
err := settings.Put(ctx, b, []byte("theme"), []byte("dark"))
check(err)
snap := b.Settled() // or b.View() for settled plus tentative
v, err := settings.Get(snap, []byte("theme"))
check(err)
fmt.Printf("theme=%s\n", v)
for k, v := range settings.Range(snap, nil, nil) {
	fmt.Printf("%s=%s\n", k, v) // key order
}
snap.Close()
```

- `New(prefix)`: the pairs live in the view under prefix ‖ key; no other key of
  the application may start with the prefix.
- `NewWith(prefix, Options{Allow: f})`: `f(tx, e, key, del)` reports whether
  entry `e` may write `key` (`del` for a delete; `key` is without the prefix).
  It runs inside apply at the entry's position, so `tx.Member(e.Writer)` gives
  the roster and roles of that position. It must depend only on its arguments,
  write nothing through `tx` and not panic: a panic halts the view on that
  peer. A refused entry is still appended and ordered, so `Put` returns nil
  for it; every peer rejects it (`Reject`) and it enters the running digest.
  For example, `bytes.HasPrefix(key, e.Writer)` lets a member write only the
  keys under its own public key.
- `Apply` handles data entries whose body starts with the kv tag and reports
  false for every other entry, so an application routes its own entries next to
  it. Each kv entry is one transaction. A kv body that does not decode, or whose
  key does not fit with the prefix, or that `Allow` refuses, is rejected
  (`Reject`), the same on every peer.
- `Put` and `Delete` append one entry each; it takes effect at its position.
  Keys are at most 32 KiB with the prefix, values at most 1 MiB
  (`kolobraid.ErrTooLarge`), and the whole body at most the braid's `MaxBody`,
  so the largest values need a larger `MaxBody`.
- `Get` and `Range` read a snapshot: `Braid.Settled` for what never changes
  again, `Braid.View` for settled plus tentative, which a reorg can change.
  `Range(start, end)` yields keys in `[start, end)`; an empty end has no upper
  bound, as in `Snapshot.Scan`, and `start >= end` yields nothing.

The body is, in kolo-codec, `string "kolo-braid/kv/v0" ‖ uvarint op (1 put, 2
delete) ‖ bytes(key) ‖ bytes(value)`, the value empty for a delete. The
package's tests run it through `kolobraidtest.CheckApply` and
`FindDivergence`.

## Devices of a person

Package `github.com/ciranda-dev/kolo-braid/devices` keeps the device certificates and revocations of the persons in a group in the view, and admits a certified device into a group its person already belongs to without a new decision of a group admin. It follows [kolo-id](https://github.com/ciranda-dev/kolo-id): a person key (from the 24 words) certifies an approver device, an approver certifies ordinary devices, and a device key signs the messages. Each device key is an ordinary member of the braid, with its own strand; nothing here changes the order, the settlement or the membership rules (the only addition to the braid is `Braid.PublicKey`), and a group that does not import the package behaves as before.

```go
roster, err := devices.New([]byte("devices/"), nil)
apply := func(tx *kolobraid.Tx, e *kolobraid.Entry) error {
	if ok, err := roster.Apply(tx, e); ok || err != nil {
		return err
	}
	return nil // the application's own entries
}

// An admin anchors the first device of a person after deciding to admit it (Braid.Admit).
err = roster.Record(ctx, b, firstCert)
// Later devices of that person are admitted by an admin without a new decision.
welcome, err := roster.Admit(ctx, b, deviceKey, request)

// Record a revocation as a data entry, and check a message of a device of a person you pinned.
body, err := devices.RevocationBody(revocation, target) // target: the certificate an approver revokes, else nil
_, err = b.Append(ctx, body)
snap := b.View()
err = roster.Verify(snap, personKey, deviceKey, time.Time{}, "myapp/message/v0", msg, sig)
```

### Records and who anchors a person

- Records are data entries whose body starts with the tag `kolo-braid/devices/v0`. Any member may write one: a certificate or a revocation is its own proof. `Apply` rejects, the same on every peer, a body that does not decode, a certificate or revocation that does not verify, a different certificate whose serial is not higher than the stored one, a certificate issued by an approver that was already retired, and a record past a bound. It checks structure and signatures with no clock: expiry, revocation and the time of the check belong to the reader.
- Because any member can invent a person key and a certificate for it, a person is trusted for admission only when an admin recorded a certificate of it. Apply stores, next to the position, whether the writer was an admin at that position. `belongs` (the person is already in the group) and the ceiling count only these anchored certificates. The same bytes written again by an admin anchor a certificate a plain member recorded first; a higher serial written by anyone keeps the anchor. `Store.Record` is the admin's call for the first device of a person; `Store.Admit` anchors the certificates it admits. Anchoring is the human decision: the admin decides, with `Braid.Admit`, to let a person's first device in, then records its certificate.
- The view keeps one certificate per (person, device), the one with the highest serial; for revocations, the highest by serial (from the person key or the device itself) and one per target for an approver. An approver's revocation carries its target certificate in the entry, so it can be checked alone and in any order.
- `Cert`, `Anchored`, `Devices` and `Revocations` read a snapshot; they return `kolobraid.ErrInvalidArgument` unless every key is exactly 32 bytes. `Verify(snap, person, device, now, context, msg, sig)` runs `koloid.VerifyDevice` on the stored certificate with the stored revocations, for the person you pass: pin the person in your own state, never read it from a certificate. `ErrUnknownDevice` means the roster has no certificate for the device. `Devices` and `Revocations` return the storage error that cut a scan of the snapshot short instead of a shorter list.
- `Revocations` returns the revocations of one device key. A certificate an approver issued is also void when the approver is revoked: `RevocationsOf(snap, person, cert)` returns the revocations of the certificate's device and of its issuer, the set `Verify` uses, for `cert.Verify`.
- At a position. `CertTx`, `RevocationsTx` and `RevocationsOfTx` read the same records inside apply, at the position of the entry that the `Tx` applies, with the entry's own writes so far: apply can ask whether a certificate was revoked at or before that position and record the answer with the entry. After the fact, `RevocationsOf` on a snapshot from `Braid.SettledAt(ctx, p)` gives the same answer for an entry that is not itself a devices record; for one that is, the `Tx` sees the view before it plus its own writes so far, and `SettledAt(ctx, p-1)` the view before it. The current view cannot: the revocation by serial of a device keeps its first position while a later one raises its serial.
- Retirement of approvers. The policy of kolo-id for approver revocations needs a time of first sight. apply has no clock, so the roster uses the position in the braid's order, which is the same on every peer: a revocation signed by approver A is not passed to the verifier when its position is after the first revocation of A by the person key or by A itself, and a certificate issued by A is rejected once such a revocation exists. A record replayed later keeps its first position. A retired approver stays retired for good, even if its key is certified again at a higher serial: use a new key. The position of a tentative entry can change in a reorg, so the rule is exact for settled records and, for the tail, until the tail reorders. A successor braid copies the settled view, records and their positions included, and its own positions continue after the old ones (`succeed.go:21`), so the comparison of positions stays within one sequence.
- Bounds. A view holds at most `Options.MaxCertsPerPerson` certificates of one person (1024 by default), of which approver-issued ones stop `PersonCertReserve` short (896 by default), and at most `Options.MaxRevocationsPerDevice` approver revocations of one device key (64 by default). The next record is rejected on every peer; see below.

### Admitting a device

The new device builds its join request with `devices.NewJoinRequest(join, id, cert, group, appData)`: it adds a proof that it holds the certified key, its signature over the person, the group and the request's own strand under `kolo-braid/device-admit/v0`. The application tells the joiner the group ID, for example in its invite. `devices.Certificate(request)` and `devices.AppData(request)` read what the device sent.

An admin calls `roster.Admit(ctx, b, joinerKey, request)` instead of `Braid.Admit`. Admit holds one lock per store while it runs, but a caller waits for it only as long as its own context allows, and every other wait ends with the context too (there is no internal time bound). It checks, in this order, before it writes anything:

1. the request carries a proof and its certificate is for `joinerKey` (`ErrNotProven`);
2. what `Braid.Admit` would refuse, and a full group, through `Braid.CheckAdmit`: this peer is an active admin (`kolobraid.ErrNotAdmin`, `ErrRemoved`, `ErrNotMember`), the key is not already a member (`ErrActiveKey`), the strand is not used, followed or pending (`ErrKnownStrand`), the group has a free seat (`kolobraid.ErrGroupFull`; `Braid.Admit` itself would append an AddMember entry that the roster leaves void);
3. the person the certificate names has a settled active member in the group whose certificate an admin anchored and verifies now (`ErrUnknownPerson`). Settled, so a person an admin anchored a moment ago is known after settlement;
4. the certificate is not older than the one in the roster, and `koloid.VerifyDevice` accepts the proof for that person at `Options.Now`, with the roster's revocations (`ErrNotProven`, which also matches `koloid.ErrCertExpired`, `koloid.ErrCertRevoked` and the other kolo-id errors). Approver-signed revocations are read from the settled view and the others from the whole view, so every admin agrees on the approver ones whatever order the entries arrive in;
5. when `Options.MaxDevices` is set, the person has fewer than that many seats: members that hold a certificate of it an admin anchored, revoked or not (`ErrTooManyDevices`). It is not set by default.

Then it appends the certificate (anchored, because the admitting peer is an admin) and waits until the roster holds a certificate of the device that is anchored and at least as new, or until the entry settles without one (`ErrNotRecorded`: rejected, void, applied unanchored because the writer lost its admin role, or the context ended and the entry may still apply later). Only then it calls `Braid.Admit` with an explicit ordinary role: the new member is no admin and no settler. It then waits until the view shows the new member, so the next admission counts it, or until the AddMember entry settles without a member (`ErrNotAdmitted`), or the context ends; in that case no welcome is returned, the entry may still apply later, and the joiner is removed with `RemoveMember` if it should not stay. Settlement is what makes a failure final: a tentative position can move in a reorg and the view applies entries later than the plan shows. Nothing is granted when any step before `Braid.Admit` failed. If `Braid.Admit` fails after the certificate was recorded, the certificate stays in the roster, anchored, and holds no seat. The group's admin policy stays the group's choice: an application that wants a person to approve every device never calls `roster.Admit`.

A replayed identical request appends nothing: the first admission makes the key active (`ErrActiveKey`), and once it is removed its strand is used (`ErrKnownStrand`). There is no rate limit on admissions: an anchored person can mint certificates for as many devices as the per-person bound allows (`MaxCertsPerPerson`), and an admin admits each one that proves a valid certificate. The group's seats and that bound are the only limits.

Every peer of a group must run the same kolo-id version, because apply verifies signatures with it: a new certificate or revocation format that one version accepts and another rejects would split the view. Put the kolo-id version in the `App` string of the group (for example `chat/v1+id0.1.0`) and change it with the version.

### No ceiling, and the bounds of apply

There is no limit on the devices of a person: each device takes one seat of the group (`MaxMembers`, default 64), and those seats are the only bound. What keeps an invented person from taking seats is the anchor: only an admin's record makes a person known, and only devices of a known person are admitted without a decision. `Options.MaxDevices` is an opt-in ceiling for a group that wants one: a positive value is the most seats of one person that `roster.Admit` lets in, where a seat is a member that holds an anchored certificate of the person, revoked or not (for example 8 devices would be 12.5% of the 64 default seats). A revoked device keeps its braid seat until an admin removes it with `RemoveMember`, and a device that revokes itself does not free one, so an admin removes revoked devices to free their slots. The ceiling is a setting of the admitting peer: admins that admit at the same moment each count from their own view, so two of them can pass it by one each. A person who loses every device and restores the 24 words has the person key again but no seat: an admin admits the new device with `Braid.Admit` and records its certificate.

The bounds of apply are safety bounds against memory and work, set in `Options`: `MaxCertsPerPerson` (default 1024, range 1 to 2^20) bounds the certificates of one person in a view, and `PersonCertReserve` (default an eighth of it, at least 1 when the bound is above 1: 128 at the default; -1 reserves nothing) keeps slots for certificates the person key issued so approver-issued ones cannot fill the roster; `MaxRevocationsPerDevice` (default 64, range 1 to 2^16, arbitrary: one per certificate a key ever had) bounds the approver revocations of one device key. They decide which entries apply rejects, so every peer of a group must use the same values, as it must run the same apply function. A revocation by the person key or by the device itself is one record per device, so it is never past a bound.

The bodies are, in kolo-codec: `string "kolo-braid/devices/v0" ‖ uvarint op (1 certificate, 2 revocation) ‖ bytes(object) ‖ [bool hasTarget ‖ bytes(target certificate)]`; the target part belongs to a revocation only. The proof in a join request's data is `string "kolo-braid/devices/proof/v0" ‖ bytes(certificate) ‖ fixed64 signature ‖ bytes(application data)`. [examples/devices](../examples/devices/main.go) runs the whole flow.

## Successor braids

When the settler quorum is lost for good, an admin of the old settled roster
calls `Succeed`. The new braid's genesis names the old group, its settled
position p and the content digest of the settled view at p; seed entries copy
that view, every peer writes their pairs without calling apply and checks the
digest (`ErrBadSeed` on a mismatch), and positions continue at p + 1.
`Status.SeededFrom` names the old braid. Tentative entries of the old braid
are not carried. A storage error while the old view is read stops `Succeed`.

A joiner of the successor that never was a member trusts its founder for the
seeded view. One that still holds the old braid passes it as
`Options.Predecessor`: once the genesis is held, the old braid is replayed to
p along its stored pivots, and the joiner halts with `ErrNotAdmin`
unless the founder was an admin there, and with `ErrBadSeed` unless the
content digest is the one the genesis names and p is the end of a settled
batch. An old braid that has not settled p yet stops the joiner with
`ErrNotFound`; `Open` checks again. The replay runs in a scratch prefix of the
joiner's backend, cleared before and after it, and takes the old braid's lock
only for each read, so the old braid goes on running.

## Formats

Every hash is BLAKE2b-256 over a kolo-codec encoding that starts with its own
tag: `kolo-braid/entry-id/v0` (entry IDs; the group ID is the genesis entry's
ID), `kolo-braid/order/v0` (tie-break keys), `kolo-braid/config/v0` (config
IDs), `kolo-braid/digest/v0` (running digest), `kolo-braid/content/v0`
(content digest) and `kolo-braid/topic/v0` (the kolo-swarm topic,
`Braid.Topic`). Entries start with `kolo-braid/entry/v0`; the join request and
the welcome with `kolo-braid/join/v0` and `kolo-braid/welcome/v0`.

| Kind | Body |
|---|---|
| 0 genesis | app tag, quorum rule, MaxMembers, MaxSettlers, MinSettlers, MaxBody, CheckpointEvery, and for a successor the old group, position, content digest and seed count |
| 1 data | application bytes, at most MaxBody |
| 2 add member | key, strand log ID, roles, signed log header |
| 3 remove member | key |
| 4 settlers | quorum rule and keys, strictly ascending |
| 5 evidence | key, strand log ID, two signed kolo-log records or heads |
| 6 pulse | config, activating pivot, optional digest claim, optional readiness |
| 7 seed | view pairs of a successor braid |

## References

- Keidar, Kokoris-Kogias, Naor and Spiegelman, [All you need is DAG](https://doi.org/10.1145/3465084.3467905), PODC 2021.
  Takes: ordering the causal history of committed leaders in a round-based DAG.
  Not implemented: a reliable broadcast layer. A seat that equivocates stops settlement instead.
- Spiegelman, Giridharan, Sonnino and Kokoris-Kogias, [Bullshark: DAG BFT protocols made practical](https://doi.org/10.1145/3548606.3559361), CCS 2022.
  Takes: leaders on alternate rounds.
- Lamport, [Time, clocks, and the ordering of events in a distributed system](https://doi.org/10.1145/359545.359563), Communications of the ACM, 1978.
  Takes: the happened-before relation.
  Not implemented: physical clocks.
- Fidge, [Logical time in distributed computing systems](https://doi.org/10.1109/2.84874), Computer, 1991, and Schwarz and Mattern, [Detecting causal relationships in distributed computations: in search of the holy grail](https://doi.org/10.1007/BF02277859), Distributed Computing, 1994.
  Takes: vector clocks derived from links, never sent.
- Kahn, [Topological sorting of large networks](https://doi.org/10.1145/368996.369025), Communications of the ACM, 1962.
  Takes: topological sort with a deterministic tie-break.
- Merkle, [A digital signature based on a conventional encryption function](https://doi.org/10.1007/3-540-48184-2_32), CRYPTO '87.
  Takes: hash-linked chains and the Merkle DAG.
- Kleppmann and Howard, [Byzantine eventual consistency and the fundamental limits of peer-to-peer databases](https://arxiv.org/abs/2012.00472), arXiv:2012.00472, 2020.
  Takes: equivocation as evidence.
- Défago, Schiper and Urbán, [Total order broadcast and multicast algorithms: taxonomy and survey](https://doi.org/10.1145/1041680.1041682), ACM Computing Surveys, 2004, and Gifford, [Weighted voting for replicated data](https://doi.org/10.1145/800215.806583), SOSP 1979.
  Takes: the total-order broadcast class, and quorum intersection.
- Lamport, Malkhi and Zhou, [Reconfiguring a state machine](https://doi.org/10.1145/1753171.1753191), ACM SIGACT News, 2010, and Ongaro and Ousterhout, [In search of an understandable consensus algorithm](https://www.usenix.org/conference/atc14/technical-sessions/presentation/ongaro), USENIX ATC 2014, section 6.
  Takes: reconfiguration as log commands, and new members catching up before they count.
- Saito and Shapiro, [Optimistic replication](https://doi.org/10.1145/1057977.1057980), ACM Computing Surveys, 2005, and Terry et al., [Managing update conflicts in Bayou, a weakly connected replicated storage system](https://doi.org/10.1145/224056.224070), SOSP 1995.
  Takes: tentative execution, rollback and commit.
- Schneider, [Implementing fault-tolerant services using the state machine approach: a tutorial](https://doi.org/10.1145/98163.98167), ACM Computing Surveys, 1990.
  Takes: the deterministic state machine fed by an agreed order.
- Dennis and Van Horn, [Programming semantics for multiprogrammed computations](https://doi.org/10.1145/365230.365252), Communications of the ACM, 1966, and [RFC 2693, SPKI Certificate Theory](https://www.rfc-editor.org/info/rfc2693) (Experimental, September 1999).
  Takes: the capability list, and one-hop authorization.
  Not implemented: delegation chains.
- [RFC 9162, Certificate Transparency Version 2.0](https://www.rfc-editor.org/info/rfc9162) (Experimental, December 2021) and [RFC 8032, Edwards-Curve Digital Signature Algorithm (EdDSA)](https://www.rfc-editor.org/info/rfc8032) (Informational, January 2017).
  Takes: the signed, hash-linked strands and Ed25519ctx, through kolo-log and kolo-id.
- [RFC 7693, The BLAKE2 Cryptographic Hash and MAC](https://www.rfc-editor.org/info/rfc7693) (Informational, November 2015).
  Takes: BLAKE2b-256 for every hash.
- ITU-T Recommendation G.114, One-way transmission time (05/2003, in force).
  Takes: 150 ms and 400 ms one-way delay, for `PivotWait`.
- Kulkarni, Demirbas, Madappa, Avva and Leone, [Logical physical clocks](https://doi.org/10.1007/978-3-319-14472-6_2), OPODIS 2014.
  Not implemented: considered and not used, since an entry has no time field.
