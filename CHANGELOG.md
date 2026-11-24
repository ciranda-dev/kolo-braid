# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `kv.Open`, `kv.DB` and `kv.Read` (`Tentative`, `Settled`): a `kolokv.Store` over a braid, so code written against kolo-kv's
  interface runs on both writer models. kolo-braid now requires kolo-kv v0.1.0.
- `kv.Options` with an `Allow` rule and `kv.NewWith`: a deterministic per-key check run in apply; a refused entry is a
  `Reject` on every peer.
- Network tests of the kv helper on 127.0.0.1 (a 2/2 partition, a member away for 500 operations, a restart on Pebble,
  redelivery, retried puts, a removed member).

### Documentation

- The kv helper's consistency model and conflict semantics in docs/guide.md, with the test behind each claim, and its
  bounds in docs/limits.md and README.md: settled reads show a prefix of one order, tentative reads show a write once applied,
  the last effective operation wins, a delete keeps key names and old values on every peer.

## [0.1.0] - 2026-11-20

### Added

- First release of kolo-braid.
