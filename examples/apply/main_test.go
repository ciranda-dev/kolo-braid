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
	if want := "1 settled message(s)\n1 rejected by apply\n"; out != want {
		t.Fatalf("output %q, want %q", out, want)
	}
}

func TestNeedsADirectory(t *testing.T) {
	out, stderr, err := exampletest.Run(t)
	if err == nil || out != "" || !strings.Contains(stderr, "usage: apply <temp dir>") {
		t.Fatalf("no argument: err %v, stdout %q, stderr %q", err, out, stderr)
	}
}
