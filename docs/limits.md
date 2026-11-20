# Limits

Every bound has a default and a range. A zero field takes the default; a
value outside the range returns `ErrInvalidConfig` (group) or
`ErrInvalidArgument` (local).

## GroupConfig (agreed in the genesis entry)

| Field | Default | Range | Reason |
|---|---|---|---|
| `Quorum` | Majority | Majority, TwoThirds | crash faults by default; TwoThirds when a settler device may be compromised |
| `MaxMembers` | 64 | 2 to 1024 | kolo-swarm's default peer budget, so every member can keep a direct connection to every other; bounds links to 63 |
| `MaxSettlers` | 7, or MaxMembers if smaller | 1 to 64, at most MaxMembers | Majority then tolerates 3 offline settlers; vote work is linear in the seats |
| `MinSettlers` | 4, or MaxSettlers if smaller | 1 to MaxSettlers | Majority with 4 seats has q 3: one settler can be offline and one that signs two histories cannot split settled prefixes; 3 or 5 give f = 0 |
| `MaxBody` | 1 MiB | 1 KiB to 3 MiB | the view's value bound; a padded entry stays within one 4 MiB sync message |
| `CheckpointEvery` | 1024 settled positions | 64 to 2^20 | a claim adds 41 bytes to one pulse per seat; a divergence is caught within that many positions |

## Limits (local to a peer)

| Field | Default | Range | Reason |
|---|---|---|---|
| `MaxTail` | 2048 | 64 to 2^20 | a reorg replays at most this many entries: about 2.3 us each, within a 50 ms input budget with a margin of ten |
| `MaxTailBytes` | 4 x 16 x MaxBody (64 MiB), at least 1 MiB | 1 MiB to 4 GiB | four entries at the per-entry write budget |
| `MaxPendingBytes` | 16 MiB | 1 MiB to 1 GiB | entries that wait for a predecessor, counted with their index cost (512 bytes plus 256 per link); past it a strand is read no further until an entry it waits for is placed. One strand holds at most MaxPendingBytes / MaxMembers of it (one entry always fits), so a member that links entries that never exist cannot take the room of the others |
| `MaxForks` | 4 | 1 to 64 | a restored backup that writes on is one second history of a strand; each one a hostile pair signs costs every peer a chain segment and a slot in every later clock, so a peer keeps this many and refuses and stops fetching the entries of the next |
| `MaxPullAhead` | MaxTail | 64 to 2^20 | a peer fetches a member's strand this far past its settled length, and a settler's this far past its newest pulse held (a settler pulses at least every 32 of its entries, half the least value), so it never fetches further ahead of settlement than it would apply; what a held entry links is fetched past it; the check runs every AnnounceGap, so a pull passes the bound by what arrives in that time |
| `PulseGap` | 500 ms | 10 ms to 1 min | settlement every one to two seconds under constant activity, at most two pulses per second per seat; the pivot pulse is exempt, and so is the pulse a settler writes after 32 of its own entries without one |
| `PivotWait` | 3 s | 100 ms to 5 min | liveness only: covers one-way delays up to 150 ms (ITU-T G.114) with four pull exchanges; 8 s for links near 400 ms one way |
| `AnnounceGap` | 100 ms | 10 ms to 10 s | at most ten head announcements per second per connection, so at most ten head checks per second per member |
| `ConfirmWait` | 10 s | 1 s to 5 min | kolo-secure's default handshake timeout: a settler that has not connected by then cannot answer now |
| `MaxEvents` | 1024 | 16 to 2^16 | events waiting for `Next`; past it the oldest droppable one goes first and is counted |

## Fixed bounds

| Bound | Value | Reason |
|---|---|---|
| View key | 32 KiB | kolo-kv's key bound |
| View value | 1 MiB | kolo-kv's value bound |
| Per-entry write budget | 16 x MaxBody, each operation costing key + value + 256 bytes | one entry fans out at most 16 times its size; 256 bytes per operation covers the measured overlay cost |
| App tag | 1 to 64 bytes of UTF-8 | |
| Log header in an AddMember | 512 bytes | |
| Evidence object | 4 KiB each | kolo-log's input bound for records and heads |
| Durable commit | 256 settled entries, or one per-entry budget of writes | a crash keeps or loses a whole chunk |
| Full clock | every 256 entries of a strand | any clock is rebuilt from at most 255 deltas |
| Uncertified claims per seat | 16 positions | a seat that claims positions nobody reaches costs a fixed amount |
| Clocks cached from the entry index | 4096 | about 8 bytes per segment each |
| Reorg positions kept for `Entries` | 256 | an iteration older than all of them ends with `ErrReorged` |
| Stream message | 4 MiB | kolo-sync's message bound |
| Join request | 64 KiB | kolo-invite's join data bound, so the largest request passes its default `MaxData` |
| Welcome | 1 MiB | kolo-invite's default payload bound; a welcome is about 600 bytes |
| Admit calls at once | 1 | each joiner admitted with `GrowSettlers` counts the entries of the one before |
| `SettledAt` calls at once | 1 | they share one replay kept at the last batch end a call reached; the others wait as long as their context allows |
| `SettledAt` copy page | 128 pairs or 1 MiB, plus the pair that passes it | the copy of the kept replay's view reads and commits one page of the view's durable scan at a time, so it holds no more of the view in memory than a scan does |
| `Braid` values per prefix at a time | 1 | `Open` clears the prefix's temporary and scratch keys |

## kv helper

| Bound | Value | Reason |
|---|---|---|
| Key, prefix included | 32 KiB | the view key bound, kolo-kv's key bound |
| Value | 1 MiB | the view value bound, kolo-kv's value bound |
| Entry body | at most the braid's `MaxBody` | a put is one data entry: tag, op, key and value |

## Devices helper

`devices.Options`; the first three are safety bounds of apply, so every peer of a group must use the same values.

| Bound | Default | Range | Reason |
|---|---|---|---|
| `MaxCertsPerPerson` | 1024 | 1 to 2^20 | the certificates of one person in a view: one scan of apply; the next is rejected |
| `PersonCertReserve` | an eighth of the bound (128), at least 1 when the bound is above 1 | 0 to `MaxCertsPerPerson`; -1 for none | slots kept for certificates the person key issued, so a stolen approver cannot fill the roster |
| `MaxRevocationsPerDevice` | 64 | 1 to 2^16 | approver revocations of one device key, one per certificate it had; an arbitrary safety bound |
| `MaxDevices` | none (0) | any positive value | an opt-in ceiling on the seats of one person (anchored certificate, revoked or not) that `Admit` lets in; the group's `MaxMembers` seats are the only bound by default; local to the admitting peer |

| Fixed | Value | Reason |
|---|---|---|
| Certificate, offer, revocation | 514, 450, 472 bytes | kolo-id's encodings |
| Join request with a proof | 64 KiB | the join request bound; a proof is under 700 bytes |
| Waits of `Admit` and `Record` (the store lock, the record, the seat) | the caller's context | polled every 2 ms; a record or an AddMember that settled without effect ends the wait at once |

## Sync bounds

| Bound | Value | Reason |
|---|---|---|
| Pulls at once per braid | 16, one per strand | kolo-sync's streams per connection |
| Stream message timeout | 60 s | kolo-sync's message timeout |
| Answer to a request | 60 s, then the connection is closed | a request never blocks a connection for good |
| Unknown key | closed after 60 s | leaves a joiner time to be added |
| Heads per announcement | MaxMembers | one message per member's head |
| Unknown heads kept | 2 x MaxMembers per connection, the newest per strand | a joiner's head that arrives before its AddMember is not lost |
| Requests outstanding | one ENTRY_REQ and one RECORD_REQ per connection | |
| ENTRY_REQ per entry | each connected peer once, then again only after a new head of its strand or a new peer | links to entries that never exist cost a fixed number of requests |
| ENTRY answer | 4 MiB, may stop early | kolo-sync's message bound |
| Evidence | once per strand per connection | an honest peer sends each piece once |

## Settlement memory

Settlement keeps one round mark per settler per round of the current settler config, until the config changes: 16 bytes in
memory and one key of about 40 bytes on disk, written once. A braid that settles all day with 4 settlers at the default
`PulseGap` can add up to about 1.4 million a day, about 22 MB of memory and 55 MB of disk; with 64 settlers 16 times that, and
more with a shorter `PulseGap` (up to 50 times at 10 ms). These are upper bounds; slice growth adds up to a quarter to the
memory figure. They are what lets a pulse that arrives late get its exact round, and a settler config change removes
them.

## What was tested

- Unit, property and fuzz tests.
- Bounded model checks of settlement and membership (`scripts/modelcheck.sh`). They cover only the sizes they enumerate and
  are not a proof for any size.
- Scenarios with several processes on one Mac, on 127.0.0.1 only. Nothing was tested over a real network or through NATs.
- The Linux and Windows builds were vetted, not run.
