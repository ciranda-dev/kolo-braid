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
	want := "B wrote a message and both settled it\n" +
		"B reads A's strand: ok\n" +
		"A removed B\n" +
		"B asks A for A's strand again, refused with ErrNotFound: true\n" +
		"A keeps B's earlier message: 1\n" +
		"B keeps what it received: 1\n" +
		"A admits B's old strand again, refused with ErrKnownStrand: true\n" +
		"A settled 2 messages after B joined with a new strand\n"
	if out != want {
		t.Fatalf("output %q, want %q", out, want)
	}
}

func TestNeedsADirectory(t *testing.T) {
	out, stderr, err := exampletest.Run(t)
	if err == nil || out != "" || !strings.Contains(stderr, "usage: removewriter <temp dir>") {
		t.Fatalf("no argument: err %v, stdout %q, stderr %q", err, out, stderr)
	}
}
