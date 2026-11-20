package main

import (
	"os"
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
	if want := "A settled 2 messages\nB settled 2 messages\nsame order: true\n"; out != want {
		t.Fatalf("output %q, want %q", out, want)
	}
}

func TestNeedsADirectory(t *testing.T) {
	out, stderr, err := exampletest.Run(t)
	if err == nil || out != "" || !strings.Contains(stderr, "usage: twopeers <temp dir>") {
		t.Fatalf("no argument: err %v, stdout %q, stderr %q", err, out, stderr)
	}
}

// TestREADMEHoldsThisProgram: the quick start in the README is this file, byte for byte.
func TestREADMEHoldsThisProgram(t *testing.T) {
	readme, err := os.ReadFile("../../README.md")
	if err != nil {
		t.Fatal(err)
	}
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(readme), "```go\n"+string(src)+"```\n") {
		t.Fatal("README.md does not contain examples/twopeers/main.go as its Go block")
	}
}
