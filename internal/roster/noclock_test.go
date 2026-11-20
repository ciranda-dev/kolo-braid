package roster

import (
	"go/parser"
	"go/token"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// TestNoClockImports fails when the package code imports a clock or a source of randomness: the order must depend on the
// entries alone.
func TestNoClockImports(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	checked := 0
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		af, err := parser.ParseFile(token.NewFileSet(), f, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		for _, im := range af.Imports {
			p, _ := strconv.Unquote(im.Path.Value)
			switch p {
			case "time", "math/rand", "math/rand/v2", "crypto/rand":
				t.Errorf("%s imports %s", f, p)
			}
		}
		checked++
	}
	if checked == 0 {
		t.Fatal("no package files")
	}
}
