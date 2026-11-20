#!/bin/sh
# Merges the "modelshard" lines of sharded model-check runs (go test ... -modelshard k/m, see modelcheck.sh) from the log
# files given as arguments. A shard may have been resumed several times with -modelfrom: its runs must follow each other
# from index 0 to the scope's total, and each run must have checked exactly its shard's jobs. Prints one line per scope
# and exits non-zero when any scope is incomplete or does not add up.
exec awk '
function cnt(x, k, m) { return x > k ? int((x - 1 - k) / m) + 1 : 0 }
/modelshard scope="/ {
	s = $0; sub(/.*modelshard scope="/, "", s); scope = s; sub(/".*/, "", scope); sub(/^[^"]*" /, "", s)
	n = split(s, f, " ")
	for (i = 1; i <= n; i++) { split(f[i], kv, "="); v[kv[1]] = kv[2] }
	split(v["shard"], sh, "/"); k = sh[1] + 0; m = sh[2] + 0
	if ((scope in mods) && mods[scope] != m) { bad[scope] = "shards of different m" }
	mods[scope] = m
	key = scope SUBSEP k SUBSEP (v["from"] + 0)
	if (!(key in nxt) || v["next"] + 0 >= nxt[key]) {
		nxt[key] = v["next"] + 0; chk[key] = v["checked"] + 0; idl[key] = v["inner"] + 0
		dn[key] = v["done"]; tot[key] = v["total"] + 0
	}
}
END {
	fail = 0
	for (scope in mods) {
		m = mods[scope]; sum = 0; ideals = 0; total = -1; why = bad[scope]
		for (k = 0; k < m && why == ""; k++) {
			pos = 0; done = 0
			while (!done) {
				key = scope SUBSEP k SUBSEP pos
				if (!(key in nxt)) { why = "shard " k "/" m " has no run from " pos; break }
				if (chk[key] != cnt(nxt[key], k, m) - cnt(pos, k, m)) { why = "shard " k "/" m " run from " pos " checked " chk[key] " jobs"; break }
				sum += chk[key]; ideals += idl[key]
				if (dn[key] == "true") {
					done = 1
					if (total >= 0 && tot[key] != total) { why = "shards disagree on the total" }
					total = tot[key]
				} else if (nxt[key] == pos) {
					why = "shard " k "/" m " stopped at " pos; break
				}
				pos = nxt[key]
			}
		}
		if (why == "" && sum != total) { why = "checked " sum " of " total }
		if (why == "") {
			printf "%s: complete, %d of %d jobs checked in %d shards, %d inner checks\n", scope, sum, total, m, ideals
		} else {
			printf "%s: NOT COMPLETE: %s\n", scope, why; fail = 1
		}
	}
	exit fail
}' "$@"
