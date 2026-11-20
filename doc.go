// Package kolobraid merges the entries of several members into one
// deterministic order that a user-supplied apply function turns into a view.
//
// Each member writes its own signed kolo-log, a strand. A [Braid] comes from
// [Create], [Open] or a join ([NewJoin], then [Joining.Complete]); members
// write with [Braid.Append] and read with [Braid.View] (settled plus tentative)
// or [Braid.Settled]. [Braid.ServeConn] syncs the braid over a [Conn], direct
// or relayed. Every peer runs the same [ApplyFunc] over the same order; the
// kolobraidtest package checks that an apply function is pure, and the kv
// package is a key-value helper on top of apply. docs/guide.md explains the
// rules.
//
// Experimental: the API may change before v1.0.0.
package kolobraid
