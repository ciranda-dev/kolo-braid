# Limits

Every bound has a default and a range. A zero field takes the default; a
value outside the range returns `ErrInvalidConfig` (group) or
`ErrInvalidArgument` (local). A group field is chosen at `Create` and cannot change; a local bound is raised or lowered by
setting its field in `Options.Limits`, and the peers of a group need not agree on it.

Safety does not depend on how many members or settlers a group has: the quorum argument uses only the seat count of a config
and its quorum. The bounds below protect a peer's memory, work and message size from a hostile peer, and each default has its
measured or derived basis in the last section.

## GroupConfig (agreed in the genesis entry)

| Field | Default | Range | Reason |
|---|---|---|---|
| `Quorum` | Majority | Majority, TwoThirds | crash faults by default; TwoThirds when a settler device may be compromised |
| `MaxMembers` | none (0) | 0, or 2 and more | an application's own ceiling on active members; a set value also bounds the links of an entry to MaxMembers - 1. The bounds that protect a peer are below |
| `MaxSettlers` | 7, or MaxMembers if smaller | 1 to MaxMembers when set, at most one more than the links of an entry (39,066 at the default MaxBody) | Majority then tolerates 3 offline settlers; each seat adds a pulse per round that links every other seat, see Cost of many members and settlers |
| `MinSettlers` | 4, or MaxSettlers if smaller | 1 to MaxSettlers | Majority with 4 seats has q 3: one settler can be offline and one that signs two histories cannot split settled prefixes; 3 or 5 give f = 0 |
| `MaxBody` | 1 MiB | 1 KiB to 3 MiB | the view's value bound; a padded entry stays within one 4 MiB sync message |
| `CheckpointEvery` | 1024 settled positions | 64 to 2^20 | a claim adds 41 bytes to one pulse per seat; a divergence is caught within that many positions |

## Limits (local to a peer)

| Field | Default | Range | Reason |
|---|---|---|---|
| `MaxTail` | 2048 | 64 to 2^20 | a reorg replays at most this many entries: about 2.3 us each, within a 50 ms input budget with a margin of ten |
| `MaxTailBytes` | 4 x 16 x MaxBody (64 MiB), at least 1 MiB | 1 MiB to 4 GiB | four entries at the per-entry write budget |
| `MaxPendingBytes` | 16 MiB | 1 MiB to 1 GiB | entries that wait for a predecessor, counted with their index cost (512 bytes plus 256 per link); past it a strand is read no further until an entry it waits for is placed. One strand holds at most `MaxPendingPerStrand` of it |
| `MaxPendingPerStrand` | 256 KiB | 1 KiB to MaxPendingBytes | one strand cannot fill MaxPendingBytes, so a member that links entries that never exist cannot take the room of the others; a strand with nothing pending may always park one entry. The default is a 64th of the default MaxPendingBytes |
| `MaxUnknownHeads` | 21,619 | 1 to 2^20 | heads kept per connection for strands not followed yet (a joiner's head that arrives before its AddMember); the newest per strand. The default is the heads of one announcement, at most 4 MiB |
| `MaxEarlyBytes` | 4 MiB | 16 KiB to 1 GiB | bytes of the two objects of Evidence entries kept against strands not followed yet, at most 4 keys per strand. The default is what 128 strands of 4 keys hold at the longest Evidence body (8,300 bytes), the bound the default group of 64 members had before |
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
| Clocks cached from the entry index | 32 MiB | 8 bytes per segment each: what 4,096 clocks of 1,024 segments take |
| Links per entry | MaxMembers - 1 when set, at most the cap of MaxBody: 53,222 at 1 KiB, 39,066 at 1 MiB, 10,726 at 3 MiB | one entry must fit one 4 MiB ENTRY answer, or no peer can serve it: its record and inclusion proof at 4 KiB each, its key, its ciphertext padded by up to a sixteenth, and 74 bytes per link at the longest index. The measured largest, with real padding, is 44,001 links at 1 MiB (14,042 at 3 MiB) |
| Settlers entry | 11 + 32 x MaxSettlers bytes | the rule, the count and the keys |
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
| `MaxDevices` | none (0) | any positive value | an opt-in ceiling on the seats of one person (anchored certificate, revoked or not) that `Admit` lets in; no ceiling by default; local to the admitting peer |

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
| Heads per announcement | 21,619, or MaxMembers when the group sets it | one frame: 4 MiB less 64 bytes, over the longest head (192 bytes) and its 2-byte length; a longer list goes out in several messages |
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

## Cost of many members and settlers

Measured in process on one 10-core Mac, 127.0.0.1 only, with the ceilings raised; they are not network figures.

- A member that never writes costs about 8.4 KB on every peer and nothing per entry (1,024 and 4,096 members, 8 of them live).
  Admitting members one by one is not linear: 1,024 settled in 2.3 s, 4,096 in 29.8 s.
- A writer costs 66 to 74 bytes in every entry that links it (a 100-byte entry is 69,887 bytes stored at 1,024 writers), and
  placing an entry that links every writer takes 4.0 ms at 1,024 writers and 68 ms at 4,096; DAG memory grows with the square
  of the writers (55.8 MB at 1,024, 880 MB at 4,096).
- Settlement logic is cheap (2 ms per round at 256 seats); the cost is the pulses: each seat writes one per round that links
  every other seat. At 64 members, 4 settlers store 4.3 KB per data entry and settle a round in 0.8 to 1.9 s, 16 settlers 7.7 KB
  and 1.5 to 2.1 s, 64 settlers 23.0 KB and 5.2 to 6.2 s.
- Under Majority, more settlers add crash tolerance and never more than one tolerated equivocating seat; 4 to 7 settlers is the
  measured sweet spot. Between commits at most 2 (n - q + 1) (G + W + delta) pass: 19.8 s with 4 seats and one offline,
  317 s with 64 seats and 31 offline.
- A braid with more writers than an entry may link links the settlers first and the other writers in a window that moves with
  each own entry, so every writer is linked again within a few entries.

To raise a bound that a peer reaches: the group fields only by creating a new group (a successor braid continues the old one);
the local bounds by setting them in `Options.Limits`.

## What was tested

- Unit, property and fuzz tests.
- Bounded model checks of settlement and membership (`scripts/modelcheck.sh`). They cover only the sizes they enumerate (up
  to 7 seats and 3 members) and are not a proof for any size; no group above 7 seats was model checked.
- Scenarios with several processes on one Mac, on 127.0.0.1 only. Nothing was tested over a real network or through NATs.
- The Linux and Windows builds were vetted, not run.
