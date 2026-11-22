# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Breaking

- A group created with the new values is refused by kolo-braid v0.1.0 peers (`GroupConfig.Validate` and the genesis decoder
  there accept only `MaxMembers` 2 to 1024 and `MaxSettlers` up to 64). A group created with the old values keeps working,
  and opens and settles the same in both versions. `GroupConfig{}` now creates a group without a member ceiling, so
  `CheckAdmit` no longer returns `ErrGroupFull` by default. This ships in v0.1.1.

### Changed

- `GroupConfig.MaxMembers` has no ceiling of 1,024: 0, the new default, means no member limit, and a positive value of 2 or
  more is the application's own ceiling. `MaxSettlers` has no ceiling of 64: it is at most `MaxMembers` when that is set and
  one more than the links an entry may hold (39,066 at the default `MaxBody`). `MinSettlers` is unchanged. The settlement
  package no longer limits a config to 64 seats.
- A Settlers entry may name more than 259 settlers: its body bound is 11 + 32 x `MaxSettlers` bytes.
- An entry holds at most the links that fit one ENTRY answer for its `MaxBody` (53,222 at 1 KiB, 39,066 at 1 MiB, 10,726 at
  3 MiB), or `MaxMembers` - 1 when that is smaller. A writer that has more other writers than that links its settlers first
  and the rest in a window that moves with each own entry.
- A head announcement carries at most 21,619 heads, what one frame holds, and a longer list goes out in several messages (it
  failed above about 21,600 heads). A group that sets `MaxMembers` still announces at most that many heads per message.
- Early Evidence and the pending bytes of one strand are bounded in bytes (`Limits.MaxEarlyBytes`,
  `Limits.MaxPendingPerStrand`) instead of by `MaxMembers`. The entry index cache is bounded at 32 MiB of clocks instead of
  4,096 clocks.
- The settled order of every DAG, the entry and message encodings and the golden vectors are unchanged.

### Added

- `Limits.MaxUnknownHeads` (default 21,619, 1 to 2^20): heads kept per connection for strands not followed yet.
- `Limits.MaxPendingPerStrand` (default 256 KiB, 1 KiB to `MaxPendingBytes`) and `Limits.MaxEarlyBytes` (default 4 MiB,
  16 KiB to 1 GiB).
- docs/limits.md lists what each remaining bound protects and its measured basis.

## [0.1.0] - 2026-11-20

### Added

- First release of kolo-braid.
