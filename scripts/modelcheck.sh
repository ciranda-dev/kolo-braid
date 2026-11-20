#!/bin/sh
# Runs the model checks: the quick mode by default (also part of go test), every scope at full size with -full. Scopes a to d,
# g and h are in internal/settle, scope e (membership) in internal/roster, scope f (replay through the braid) in the root
# package. Each scope prints its counts and its time and fails when it exceeds its limit. Extra arguments go to the settle
# run, for example -modelbudget 30m to stop each scope earlier (it then fails as incomplete). Scope b' also builds a seat of
# the current config that forges a closure certificate, which only the votes of the closed config keep from being used.
#
# Long runs can be split and resumed. Scopes a and a': go test ./internal/settle -run '^TestModelScopeA(Prime)?$' -full
# -modelonly "<size as printed>" -modelshard k/m [-modelfrom N], one process per k; scope f: go test . -run '^TestScopeF$'
# -full -scopeshard k/m [-scopefrom N] -scopebudget 12h. Each run logs "modelshard" lines with the next index to resume
# from; scripts/modelmerge.sh <logs> checks that the shards and their resumed runs cover every job exactly once.
set -e
cd "$(dirname "$0")/.."
if [ "$1" = "-full" ]; then
	shift
	go test ./internal/settle -count=1 -run '^TestModel' -v -timeout 24h "$@" -full
	go test ./internal/roster -count=1 -run '^TestScopeE$' -v -timeout 2h -full
	exec go test . -count=1 -run '^TestScopeF$' -v -timeout 2h -full -scopebudget 30m
fi
go test ./internal/settle -count=1 -run '^TestModel' -v "$@"
go test ./internal/roster -count=1 -run '^TestScopeE$' -v
exec go test . -count=1 -run '^TestScopeF$' -v
