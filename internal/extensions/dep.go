package extensions

import (
	"fmt"
	"sync"
)

// Dep is a declared dependency on a point and the handle used to read it.
//
// It exists because declaring a dependency and using one were previously two
// separate things: an entry in the declaration naming a point, and a lookup by
// point id during Init. Nothing checked that the two agreed, and because Init
// received the whole broker, a module could reach a point it never declared --
// and usually get away with it, until an unrelated change reordered
// initialization. Binding the declaration and the read into one typed value
// makes that impossible rather than discouraged: the only way to read a point is
// through a handle, and a handle only works once the broker has bound it, which
// happens only for handles the declaration lists.
//
// A handle is created from its point, stored on the module, and listed in the
// declaration:
//
//	type Module struct{ store *extensions.Dep[Store] }
//
//	func New() *Module { return &Module{store: storepoint.Point.Require()} }
//
//	func (m *Module) Declaration() extensions.Declaration {
//		return extensions.Declaration{ID: id, Deps: []extensions.AnyDep{m.store}, Init: m.init}
//	}
//
//	func (m *Module) init(ctx context.Context, cfg extensions.Config) error {
//		store, err := m.store.Get()
//		...
//	}
type Dep[T any] struct {
	point    PointID
	optional bool
	lazy     bool

	mu       sync.RWMutex
	resolver Resolver
}

// AnyDep is a dependency handle as the broker sees it, without its type
// parameter. A declaration lists these; the broker reads what each one requires
// and binds it before the dependent initializes.
type AnyDep interface {
	// Point is the point depended on.
	Point() PointID
	// Optional reports whether the dependent still loads with no provider.
	Optional() bool
	// Lazy reports whether the dependency is resolved at use time rather than
	// ordered before the dependent.
	Lazy() bool
	// Bind gives the handle its resolver. Only the broker calls it, and only for
	// handles a declaration lists, which is what makes an undeclared handle
	// unusable.
	Bind(Resolver)
}

// Require declares a required dependency on the point: at least one provider
// must exist, and every provider initializes before the dependent.
func (p Point[T]) Require() *Dep[T] {
	return &Dep[T]{point: p.id}
}

// Optional declares a dependency that may have no providers. The dependent
// still loads, ordered after any that do exist.
func (p Point[T]) Optional() *Dep[T] {
	return &Dep[T]{point: p.id, optional: true}
}

// Lazy declares a dependency resolved at use time rather than at init.
//
// A required or optional dependency is an ordering edge: every provider is
// initialized before the dependent, which is what lets Init call it. That is
// also what makes a cycle fatal, and the daemon's own subsystems genuinely do
// refer to each other -- containers to images, images to volumes, volumes back
// to containers. Decomposing them would deadlock the graph if every edge forced
// an order.
//
// A lazy dependency adds no edge. It says the module will call this point while
// serving a request, by which time everything is up, so nothing needs to be
// ordered. It is the primitive that lets a mutually-referential subsystem be
// split at all, and the one to reach for unless Init itself has to make the
// call.
func (p Point[T]) Lazy() *Dep[T] {
	return &Dep[T]{point: p.id, lazy: true}
}

// Point returns the point this handle depends on.
func (d *Dep[T]) Point() PointID { return d.point }

// Optional reports whether the dependency tolerates having no providers.
func (d *Dep[T]) Optional() bool { return d.optional }

// Lazy reports whether the dependency is resolved at use time.
func (d *Dep[T]) Lazy() bool { return d.lazy }

func (d *Dep[T]) Bind(r Resolver) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.resolver = r
}

// Get returns the single provider of the point.
//
// It fails if the handle was never declared, which is how an undeclared
// dependency surfaces: at the first use, with the point named, rather than as a
// silent success that depends on initialization order.
func (d *Dep[T]) Get() (T, error) {
	var zero T
	r, err := d.bound()
	if err != nil {
		return zero, err
	}
	provider, err := r.SingleProvider(d.point)
	if err != nil {
		return zero, err
	}
	return typedProvider[T](d.point, "", provider)
}

// All returns every provider of the point, for a fan-out dependency.
func (d *Dep[T]) All() ([]TypedProvider[T], error) {
	r, err := d.bound()
	if err != nil {
		return nil, err
	}
	return Point[T]{id: d.point}.All(r)
}

// ByExtension returns the provider implemented by a named extension.
func (d *Dep[T]) ByExtension(extension ExtensionID) (T, error) {
	var zero T
	r, err := d.bound()
	if err != nil {
		return zero, err
	}
	return Point[T]{id: d.point}.ByExtension(r, extension)
}

func (d *Dep[T]) bound() (Resolver, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.resolver == nil {
		return nil, fmt.Errorf("dependency on point %q was not declared: list the handle in the extension's Deps", d.point)
	}
	return d.resolver, nil
}
