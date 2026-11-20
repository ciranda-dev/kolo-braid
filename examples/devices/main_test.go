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
	want := "laptop: founded the group and anchored its approver certificate\n" +
		"laptop: admitted \"Maria's phone\" without a new decision\n" +
		"the phone verifies as a device of the person: true\n" +
		"after the laptop revokes it, the phone verifies: false\n"
	if out != want {
		t.Fatalf("output %q, want %q", out, want)
	}
}

func TestNeedsADirectory(t *testing.T) {
	out, stderr, err := exampletest.Run(t)
	if err == nil || out != "" || !strings.Contains(stderr, "usage: devices <temp dir>") {
		t.Fatalf("no argument: err %v, stdout %q, stderr %q", err, out, stderr)
	}
}
