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

## [0.1.0] - 2026-11-20

### Added

- First release of kolo-braid.
