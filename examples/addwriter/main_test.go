package main

import (
	"strings"
	"testing"

	"github.com/ciranda-dev/kolo-braid/internal/exampletest"
)

func TestMain(m *testing.M) { exampletest.Main(m, main) }

func TestRuns(t *testing.T) {
	out, stderr, err := exampletest.Run(t, t.TempDir())
	if err != nil {
		t.Fatalf("exit: %v\n%s", err, stderr)
	}
	want := "admin: created the braid and a manual invite\n" +
		"joiner: sent its join request and received a welcome\n" +
		"admin: admitted the joiner with an AddMember entry\n" +
		"joiner: completed the join\n" +
		"both settled 2 messages in the same order: true\n"
	if out != want {
		t.Fatalf("output %q, want %q", out, want)
	}
}

func TestNeedsADirectory(t *testing.T) {
	out, stderr, err := exampletest.Run(t)
	if err == nil || out != "" || !strings.Contains(stderr, "usage: addwriter <temp dir>") {
		t.Fatalf("no argument: err %v, stdout %q, stderr %q", err, out, stderr)
	}
}
