package devices_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestNoIgnoredCryptoErrors: no call of a crypto function drops its error in the code of this package.
func TestNoIgnoredCryptoErrors(t *testing.T) {
	files, _ := filepath.Glob("*.go")
	names := map[string]bool{"GenerateKey": true, "Sign": true, "Verify": true, "VerifyDevice": true, "OfferDeviceCert": true,
		"AcceptDeviceCert": true, "DecodeDeviceCert": true, "DecodeRevocation": true, "Read": true}
	fset := token.NewFileSet()
	checked := 0
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		src, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		f, err := parser.ParseFile(fset, name, src, 0)
		if err != nil {
			t.Fatal(err)
		}
		checked++
		ast.Inspect(f, func(n ast.Node) bool {
			as, ok := n.(*ast.AssignStmt)
			if !ok || len(as.Rhs) != 1 || len(as.Lhs) < 2 {
				return true
			}
			c, ok := as.Rhs[0].(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := c.Fun.(*ast.SelectorExpr)
			if !ok || !names[sel.Sel.Name] {
				return true
			}
			if id, ok := as.Lhs[len(as.Lhs)-1].(*ast.Ident); ok && id.Name == "_" {
				t.Errorf("%s: the error of %s is dropped", fset.Position(as.Pos()), sel.Sel.Name)
			}
			return true
		})
	}
	if checked < 3 {
		t.Fatalf("checked %d files", checked)
	}
}
