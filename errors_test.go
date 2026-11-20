package kolobraid

import (
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"maps"
	"os"
	"slices"
	"strings"
	"testing"
)

// TestErrorsAreDistinctAndPrefixed checks every Err value the package declares: the source is parsed, so a new one cannot be
// left out of the check.
func TestErrorsAreDistinctAndPrefixed(t *testing.T) {
	all := map[string]error{
		"ErrClosed": ErrClosed, "ErrInvalidArgument": ErrInvalidArgument, "ErrInvalidConfig": ErrInvalidConfig,
		"ErrNotMember": ErrNotMember, "ErrNotAdmin": ErrNotAdmin, "ErrRemoved": ErrRemoved, "ErrTooLarge": ErrTooLarge,
		"ErrRejected": ErrRejected, "ErrViewLimit": ErrViewLimit, "ErrTxDone": ErrTxDone, "ErrNotFound": ErrNotFound,
		"ErrHalted": ErrHalted, "ErrDiverged": ErrDiverged, "ErrErased": ErrErased, "ErrEquivocation": ErrEquivocation,
		"ErrWrongApp": ErrWrongApp, "ErrWrongGroup": ErrWrongGroup, "ErrCorrupt": ErrCorrupt, "ErrProtocol": ErrProtocol,
		"ErrUnconfirmed": ErrUnconfirmed, "ErrStrandLost": ErrStrandLost, "ErrBadSeed": ErrBadSeed, "ErrReorged": ErrReorged,
		"ErrKnownStrand": ErrKnownStrand, "ErrActiveKey": ErrActiveKey, "ErrMalformed": ErrMalformed, "ErrGroupFull": ErrGroupFull,
	}
	declared := map[string]bool{}
	files, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	for _, f := range files {
		if !strings.HasSuffix(f.Name(), ".go") || strings.HasSuffix(f.Name(), "_test.go") {
			continue
		}
		af, err := parser.ParseFile(fset, f.Name(), nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range af.Decls {
			if g, ok := d.(*ast.GenDecl); ok && g.Tok == token.VAR {
				for _, s := range g.Specs {
					for _, n := range s.(*ast.ValueSpec).Names {
						if strings.HasPrefix(n.Name, "Err") {
							declared[n.Name] = true
						}
					}
				}
			}
		}
	}
	if got, want := slices.Sorted(maps.Keys(declared)), slices.Sorted(maps.Keys(all)); !slices.Equal(got, want) {
		t.Fatalf("declared errors %v, checked %v", got, want)
	}
	seen := map[string]bool{}
	for _, err := range all {
		if !strings.HasPrefix(err.Error(), "kolobraid: ") {
			t.Errorf("%q does not start with kolobraid:", err)
		}
		if seen[err.Error()] {
			t.Errorf("%q repeats", err)
		}
		seen[err.Error()] = true
		for _, other := range all {
			if other != err && errors.Is(err, other) {
				t.Errorf("%q matches %q", err, other)
			}
		}
	}
}
