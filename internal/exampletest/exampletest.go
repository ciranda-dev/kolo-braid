// Package exampletest runs an example's main in a child process, so that its tests see the output and exit status a user sees.
package exampletest

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"testing"
	"time"
)

const childEnv = "KOLOBRAID_EXAMPLE_MAIN"

// Main is called from TestMain: in a child started by Run it runs main, otherwise it runs the tests.
func Main(m *testing.M, main func()) {
	if os.Getenv(childEnv) == "1" {
		main()
		return
	}
	os.Exit(m.Run())
}

// Run starts the example with args and returns what it printed to standard output and standard error, and its exit error.
func Run(t testing.TB, args ...string) (stdout, stderr string, err error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], args...)
	cmd.Env = append(os.Environ(), childEnv+"=1")
	var out, errOut bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errOut
	err = cmd.Run()
	return out.String(), errOut.String(), err
}
