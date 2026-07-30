// Command pointgen writes a point's client adapter from its Go interface.
//
// A point's contract is its provider interface and its message types. Every
// other part of the point is derived from those at runtime -- the protobuf
// descriptors, the messages, the gRPC dispatch -- except one: the value the
// daemon holds when the provider is in another process has to implement the
// point's Go interface, and Go can build a function at runtime but not a value
// implementing an interface. So that adapter has to exist in source.
//
// It is mechanical. Each method forwards to wire.Call or wire.Do with its own
// name, which is three lines that say nothing the interface did not already say.
// This writes them.
//
// Unlike a contract generator, nothing here can go stale unnoticed: the adapter
// has to satisfy the point's interface, so a contract change that outruns the
// generated file is a build failure in the package that owns it, not a
// difference some CI job has to notice.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
)

func main() {
	dir := flag.String("dir", ".", "package directory holding the point contract")
	service := flag.String("service", "", "gRPC service name for the point")
	out := flag.String("out", "wire_gen.go", "file to write, relative to -dir")
	flag.Parse()

	if *service == "" {
		fmt.Fprintln(os.Stderr, "pointgen: -service is required")
		os.Exit(2)
	}
	if err := run(*dir, *service, *out); err != nil {
		fmt.Fprintln(os.Stderr, "pointgen:", err)
		os.Exit(1)
	}
}

// method is one call on the point's interface.
type method struct {
	name     string
	request  string
	response string // empty when the method returns only an error
}

func run(dir, service, out string) error {
	pkg, iface, err := parseDir(dir)
	if err != nil {
		return err
	}
	methods, err := parseMethods(iface.decl)
	if err != nil {
		return err
	}
	src, err := emit(pkg, iface.name, service, methods)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, out), src, 0o644)
}

type ifaceDecl struct {
	name string
	decl *ast.InterfaceType
}

// parseDir finds the package's DefinePoint call and the interface it names.
func parseDir(dir string) (pkgName string, _ ifaceDecl, _ error) {
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, dir, func(fi os.FileInfo) bool {
		// Skip generated output so a rerun reads the contract, not itself.
		return !strings.HasSuffix(fi.Name(), "_test.go") && !strings.HasSuffix(fi.Name(), "_gen.go")
	}, 0)
	if err != nil {
		return "", ifaceDecl{}, err
	}

	var files []*ast.File
	for name, pkg := range pkgs {
		if strings.HasSuffix(name, "_test") {
			continue
		}
		pkgName = name
		for _, f := range pkg.Files {
			files = append(files, f)
		}
	}
	if pkgName == "" {
		return "", ifaceDecl{}, fmt.Errorf("no package in %s", dir)
	}

	name := findDefinePoint(files)
	if name == "" {
		return "", ifaceDecl{}, fmt.Errorf(`no extensions.DefinePoint[T]("id") call in %s`, dir)
	}
	it := findInterface(files, name)
	if it == nil {
		return "", ifaceDecl{}, fmt.Errorf("interface %q not found in %s", name, dir)
	}
	return pkgName, ifaceDecl{name: name, decl: it}, nil
}

// findDefinePoint returns the type argument of the extensions.DefinePoint call.
func findDefinePoint(files []*ast.File) string {
	var name string
	for _, f := range files {
		ast.Inspect(f, func(n ast.Node) bool {
			if name != "" {
				return false
			}
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			idx, ok := call.Fun.(*ast.IndexExpr)
			if !ok {
				return true
			}
			if sel, ok := idx.X.(*ast.SelectorExpr); !ok || sel.Sel.Name != "DefinePoint" {
				return true
			}
			if id, ok := idx.Index.(*ast.Ident); ok {
				name = id.Name
			}
			return false
		})
	}
	return name
}

func findInterface(files []*ast.File, name string) *ast.InterfaceType {
	for _, f := range files {
		for _, decl := range f.Decls {
			gd, ok := decl.(*ast.GenDecl)
			if !ok || gd.Tok != token.TYPE {
				continue
			}
			for _, spec := range gd.Specs {
				ts := spec.(*ast.TypeSpec)
				if ts.Name.Name != name {
					continue
				}
				if it, ok := ts.Type.(*ast.InterfaceType); ok {
					return it
				}
			}
		}
	}
	return nil
}

// parseMethods reads the point's calls off its interface, rejecting any shape
// the wire contract cannot carry -- the same shapes wire.NewContractFor accepts,
// checked here so the mismatch is a generator error rather than a panic when the
// point is registered.
func parseMethods(iface *ast.InterfaceType) ([]method, error) {
	var methods []method
	for _, m := range iface.Methods.List {
		ft, ok := m.Type.(*ast.FuncType)
		if !ok || len(m.Names) == 0 {
			continue
		}
		name := m.Names[0].Name
		if ft.Params == nil || len(ft.Params.List) != 2 {
			return nil, fmt.Errorf("method %s: want (context.Context, *Request)", name)
		}
		req, err := pointerName(ft.Params.List[1].Type)
		if err != nil {
			return nil, fmt.Errorf("method %s request: %w", name, err)
		}
		res := results(ft)
		switch {
		case len(res) == 1 && isIdent(res[0], "error"):
			methods = append(methods, method{name: name, request: req})
		case len(res) == 2 && isIdent(res[1], "error"):
			resp, err := pointerName(res[0])
			if err != nil {
				return nil, fmt.Errorf("method %s response: %w", name, err)
			}
			methods = append(methods, method{name: name, request: req, response: resp})
		default:
			return nil, fmt.Errorf("method %s: result must be error or (*Response, error)", name)
		}
	}
	return methods, nil
}

func emit(pkg, iface, service string, methods []method) ([]byte, error) {
	var b bytes.Buffer
	fmt.Fprintf(&b, "// Code generated by pointgen. DO NOT EDIT.\n\npackage %s\n\n", pkg)
	fmt.Fprintf(&b, "import (\n\t%q\n\n\t%q\n)\n", "context", "github.com/moby/moby/v2/internal/extensions/wire")

	fmt.Fprintf(&b, `
// Wire is the point's contract and both sides of its gRPC wiring, derived from
// [%[1]s] and its message types.
var Wire = wire.Bind(Point, %[2]q, func(c wire.Client) %[1]s {
	return client{c}
})

// client calls a provider of the point that lives in another process. It has to
// satisfy [%[1]s], so a method added to the point breaks the build here until
// this file is regenerated.
type client struct {
	wire.Client
}
`, iface, service)

	for _, m := range methods {
		if m.response == "" {
			fmt.Fprintf(&b, `
func (c client) %[1]s(ctx context.Context, req *%[2]s) error {
	return wire.Do(ctx, c.Client, %[1]q, req)
}
`, m.name, m.request)
			continue
		}
		fmt.Fprintf(&b, `
func (c client) %[1]s(ctx context.Context, req *%[2]s) (*%[3]s, error) {
	return wire.Call[%[3]s](ctx, c.Client, %[1]q, req)
}
`, m.name, m.request, m.response)
	}
	return format.Source(b.Bytes())
}

func pointerName(expr ast.Expr) (string, error) {
	star, ok := expr.(*ast.StarExpr)
	if !ok {
		return "", fmt.Errorf("expected a pointer type")
	}
	id, ok := star.X.(*ast.Ident)
	if !ok {
		return "", fmt.Errorf("expected a named type in this package")
	}
	return id.Name, nil
}

func isIdent(expr ast.Expr, name string) bool {
	id, ok := expr.(*ast.Ident)
	return ok && id.Name == name
}

func results(ft *ast.FuncType) []ast.Expr {
	if ft.Results == nil {
		return nil
	}
	var out []ast.Expr
	for _, f := range ft.Results.List {
		if len(f.Names) == 0 {
			out = append(out, f.Type)
			continue
		}
		for range f.Names {
			out = append(out, f.Type)
		}
	}
	return out
}
