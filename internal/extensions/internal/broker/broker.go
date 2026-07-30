package broker

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/moby/moby/v2/internal/extensions"
)

// Broker registers extensions, resolves dependencies, and exposes providers.
//
// It is safe for concurrent use: the read side (Provider, SingleProvider,
// Providers) is held by request handlers once the daemon is up, while the write
// side (Register, and marking extensions initialized during Init) mutates the
// registry. The lock is never held across a user Init or Shutdown callback --
// those receive the broker as a Resolver and would deadlock -- so mutations are
// applied around, not during, those calls.
type Broker struct {
	mu         sync.RWMutex
	extensions map[extensions.ExtensionID]*extensionState
	order      []extensions.ExtensionID
	initOrder  []extensions.ExtensionID
	// byPoint indexes providers by the point they implement, in registration
	// order. Lookup is on the request path -- a fan-out point is resolved on
	// every container start -- and the daemon is itself decomposed into
	// extensions, so scanning every extension's provider list per call would
	// grow with the number of modules in the daemon rather than with the number
	// of providers for the point being looked up.
	byPoint map[extensions.PointID][]extensions.ResolvedProvider
}

type extensionState struct {
	extension   extensions.Declaration
	initialized bool
}

// New creates an empty Broker.
func New() *Broker {
	return &Broker{
		extensions: make(map[extensions.ExtensionID]*extensionState),
		byPoint:    make(map[extensions.PointID][]extensions.ResolvedProvider),
	}
}

// Register adds an extension to the broker.
func (b *Broker) Register(ext extensions.Extension) error {
	decl := ext.Declaration()
	if err := extensions.ValidateExtensionID(decl.ID); err != nil {
		return err
	}
	seenPoints := make(map[extensions.PointID]struct{})
	for _, provider := range decl.Providers {
		if provider.Point == "" {
			return fmt.Errorf("extension %q has provider without point", decl.ID)
		}
		if provider.Impl == nil {
			return fmt.Errorf("extension %q provider for point %q is nil", decl.ID, provider.Point)
		}
		if _, ok := seenPoints[provider.Point]; ok {
			return fmt.Errorf("extension %q implements point %q more than once", decl.ID, provider.Point)
		}
		seenPoints[provider.Point] = struct{}{}
	}
	for _, dep := range decl.Dependencies {
		if dep.Point == "" && dep.Extension == "" {
			return fmt.Errorf("extension %q has dependency without point or extension", decl.ID)
		}
		if dep.Point != "" && dep.Extension != "" {
			return fmt.Errorf("extension %q dependency must name either point or extension", decl.ID)
		}
	}
	for _, dep := range decl.Deps {
		if dep == nil {
			return fmt.Errorf("extension %q has a nil dependency handle", decl.ID)
		}
		if dep.Point() == "" {
			return fmt.Errorf("extension %q has a dependency handle with no point", decl.ID)
		}
	}
	for _, conflict := range decl.Conflicts {
		if conflict == "" {
			return fmt.Errorf("extension %q has empty conflict id", decl.ID)
		}
		if conflict == decl.ID {
			return fmt.Errorf("extension %q conflicts with itself", decl.ID)
		}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.extensions[decl.ID]; ok {
		return fmt.Errorf("extension %q is already registered", decl.ID)
	}
	if err := b.checkConflicts(decl); err != nil {
		return err
	}

	b.extensions[decl.ID] = &extensionState{extension: decl}
	b.order = append(b.order, decl.ID)
	for _, provider := range decl.Providers {
		b.byPoint[provider.Point] = append(b.byPoint[provider.Point], extensions.ResolvedProvider{
			Extension: decl.ID,
			Impl:      provider.Impl,
		})
	}
	return nil
}

func (b *Broker) checkConflicts(ext extensions.Declaration) error {
	for _, existingID := range b.order {
		existing := b.extensions[existingID].extension
		if slices.Contains(ext.Conflicts, existing.ID) || slices.Contains(existing.Conflicts, ext.ID) {
			return fmt.Errorf("extension %q conflicts with extension %q", ext.ID, existing.ID)
		}
	}
	return nil
}

// Init resolves dependencies and initializes all registered extensions in
// order, delivering each its configuration from configs (keyed by extension id).
func (b *Broker) Init(ctx context.Context, configs map[extensions.ExtensionID]extensions.Config) error {
	b.mu.Lock()
	resolved, err := b.resolveOrder()
	if err != nil {
		b.mu.Unlock()
		return err
	}
	// Record the order before initializing, so a failure part-way through can be
	// unwound: Shutdown walks initOrder in reverse and skips extensions whose
	// Init never ran (see below).
	b.initOrder = resolved
	// Bind every declared handle before anything initializes. A lazy handle is
	// bound too -- it is read later, not ordered earlier -- so binding is about
	// what a module may reach, and the graph is about when it may reach it.
	for _, id := range resolved {
		for _, dep := range b.extensions[id].extension.Deps {
			dep.Bind(b)
		}
	}
	b.mu.Unlock()

	for _, id := range resolved {
		b.mu.RLock()
		state := b.extensions[id]
		initialized, initFn := state.initialized, state.extension.Init
		b.mu.RUnlock()
		if initialized {
			continue
		}
		// The extension's Init receives the broker as its Resolver, so the lock
		// must not be held here: it may resolve providers (RLock) and would
		// deadlock. Mark it initialized afterwards, under the lock.
		if initFn != nil {
			if err := initFn(ctx, configs[id]); err != nil {
				return fmt.Errorf("initialize extension %q: %w", id, err)
			}
		}
		b.mu.Lock()
		state.initialized = true
		b.mu.Unlock()
	}
	return nil
}

// Provider returns one provider for point implemented by extension.
func (b *Broker) Provider(point extensions.PointID, extension extensions.ExtensionID) (any, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	state, ok := b.extensions[extension]
	if !ok {
		return nil, fmt.Errorf("extension %q is not registered", extension)
	}
	for _, provider := range state.extension.Providers {
		if provider.Point == point {
			return provider.Impl, nil
		}
	}
	return nil, fmt.Errorf("extension %q does not provide point %q", extension, point)
}

// SingleProvider returns the only provider for point.
func (b *Broker) SingleProvider(point extensions.PointID) (any, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	providers := b.providersLocked(point)
	switch len(providers) {
	case 0:
		return nil, fmt.Errorf("point %q has no providers", point)
	case 1:
		return providers[0].Impl, nil
	default:
		return nil, fmt.Errorf("point %q has multiple providers", point)
	}
}

// Providers returns all providers for point. The order is unspecified and not
// part of any contract: callers must not rely on which provider comes first, and
// a point that needs ordering defines it in its own contract. (Providers are
// enumerated in registration order in practice, but that is an implementation
// detail, not a promise.)
func (b *Broker) Providers(point extensions.PointID) []extensions.ResolvedProvider {
	b.mu.RLock()
	defer b.mu.RUnlock()
	// Clone: the index's slice is shared by every caller, and appending to a
	// returned slice with spare capacity would write into it. Cloning a point
	// with no providers -- the common case on a fan-out point nothing implements
	// -- allocates nothing.
	return slices.Clone(b.providersLocked(point))
}

// providersLocked is Providers without locking, for callers that already hold
// the lock (the exported readers, and resolveOrder during Init).
//
// The returned slice is the index's own and must not be modified. It is only
// appended to, by Register, so a slice handed out earlier stays valid; and since
// the extension set is fixed once the daemon is up, no reader ever observes a
// partially built one.
func (b *Broker) providersLocked(point extensions.PointID) []extensions.ResolvedProvider {
	return b.byPoint[point]
}

// declaredDependencies is an extension's dependencies as ordering edges: those
// declared as data, plus its typed handles.
//
// A lazy handle is deliberately absent. It says the point is called while
// serving a request, not during Init, so it needs no provider to exist first --
// and so it adds no edge that could close a cycle. That is what lets subsystems
// that genuinely refer to each other be split apart.
func declaredDependencies(decl extensions.Declaration) []extensions.Dependency {
	deps := decl.Dependencies
	for _, d := range decl.Deps {
		if d.Lazy() {
			continue
		}
		deps = append(deps, extensions.Dependency{Point: d.Point(), Optional: d.Optional()})
	}
	return deps
}

func (b *Broker) resolveOrder() ([]extensions.ExtensionID, error) {
	dependencies := make(map[extensions.ExtensionID][]extensions.ExtensionID, len(b.extensions))
	for _, id := range b.order {
		state := b.extensions[id]
		for _, dep := range declaredDependencies(state.extension) {
			if dep.Extension != "" {
				if _, ok := b.extensions[dep.Extension]; !ok {
					if dep.Optional {
						continue
					}
					return nil, fmt.Errorf("extension %q requires missing extension %q", id, dep.Extension)
				}
				dependencies[id] = append(dependencies[id], dep.Extension)
				continue
			}

			// A point dependency orders the consumer after every current provider.
			// This lets Init safely resolve and call that point, but also makes
			// cycles visible when any provider depends back on the consumer.
			providers := b.providersLocked(dep.Point)
			if len(providers) == 0 {
				if dep.Optional {
					continue
				}
				return nil, fmt.Errorf("extension %q requires missing point %q", id, dep.Point)
			}
			for _, provider := range providers {
				dependencies[id] = append(dependencies[id], provider.Extension)
			}
		}
	}

	var resolved []extensions.ExtensionID
	permanent := make(map[extensions.ExtensionID]struct{}, len(b.extensions))
	temporary := make(map[extensions.ExtensionID]struct{}, len(b.extensions))
	var visit func(extensions.ExtensionID, []extensions.ExtensionID) error
	visit = func(id extensions.ExtensionID, stack []extensions.ExtensionID) error {
		if _, ok := permanent[id]; ok {
			return nil
		}
		if _, ok := temporary[id]; ok {
			cycleStart := slices.Index(stack, id)
			cycle := append(stack[cycleStart:], id)
			return fmt.Errorf("extension dependency cycle: %s", joinExtensionIDs(cycle))
		}
		temporary[id] = struct{}{}
		stack = append(stack, id)
		for _, dep := range dependencies[id] {
			if dep == id {
				continue
			}
			if err := visit(dep, stack); err != nil {
				return err
			}
		}
		delete(temporary, id)
		permanent[id] = struct{}{}
		resolved = append(resolved, id)
		return nil
	}
	for _, id := range b.order {
		if err := visit(id, nil); err != nil {
			return nil, err
		}
	}
	return resolved, nil
}

// Shutdown stops initialized extensions in reverse dependency order. An
// extension whose Init never ran is skipped: it has nothing to tear down, and
// calling Shutdown on it would unwind state that was never set up. So a broker
// whose Init failed part-way, or was never called, shuts down only the
// extensions that were actually initialized.
func (b *Broker) Shutdown(ctx context.Context) error {
	// Snapshot the shutdown hooks to run under the lock, then call them without
	// it: a Shutdown callback also gets the broker as a Resolver, so holding the
	// lock across it could deadlock.
	type hook struct {
		id extensions.ExtensionID
		fn func(context.Context) error
	}
	var hooks []hook
	b.mu.RLock()
	for i := len(b.initOrder) - 1; i >= 0; i-- {
		state := b.extensions[b.initOrder[i]]
		if !state.initialized || state.extension.Shutdown == nil {
			continue
		}
		hooks = append(hooks, hook{state.extension.ID, state.extension.Shutdown})
	}
	b.mu.RUnlock()

	var errs []error
	for _, h := range hooks {
		if err := h.fn(ctx); err != nil {
			errs = append(errs, fmt.Errorf("shutdown extension %q: %w", h.id, err))
		}
	}
	return errors.Join(errs...)
}

func joinExtensionIDs(ids []extensions.ExtensionID) string {
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = string(id)
	}
	return strings.Join(parts, " -> ")
}
