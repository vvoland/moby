package broker

import (
	"context"
	"testing"

	"github.com/moby/moby/v2/internal/extensions"
	"gotest.tools/v3/assert"
	is "gotest.tools/v3/assert/cmp"
)

type store interface{ Name() string }

type namedStore struct{ name string }

func (s namedStore) Name() string { return s.name }

var storePoint = extensions.DefinePoint[store]("org.mobyproject.test.store.v1")

// TestDeclaredHandleResolves is the ordinary path: a module declares a handle,
// the broker binds it, and Init reads the provider through it without ever
// naming a point id or touching a resolver.
func TestDeclaredHandleResolves(t *testing.T) {
	b := New()
	assert.NilError(t, b.Register(extensions.New(extensions.Declaration{
		ID:        "org.example.provider.v1",
		Providers: []extensions.Provider{storePoint.Provide(namedStore{"real"})},
	})))

	dep := storePoint.Require()
	var got string
	assert.NilError(t, b.Register(extensions.New(extensions.Declaration{
		ID:   "org.example.consumer.v1",
		Deps: []extensions.AnyDep{dep},
		Init: func(context.Context, extensions.Config) error {
			s, err := dep.Get()
			if err != nil {
				return err
			}
			got = s.Name()
			return nil
		},
	})))

	assert.NilError(t, b.Init(context.Background(), nil))
	assert.Check(t, is.Equal(got, "real"))
}

// TestUndeclaredHandleIsUnusable is the property the handles exist for. A module
// that reads a point it did not declare gets an error naming the point, rather
// than a provider it was never entitled to.
//
// Previously this could not fail: Init received the whole broker, so an
// undeclared point resolved fine as long as some other module happened to be
// initialized first -- which made the declaration advisory and the module
// boundary imaginary.
func TestUndeclaredHandleIsUnusable(t *testing.T) {
	b := New()
	assert.NilError(t, b.Register(extensions.New(extensions.Declaration{
		ID:        "org.example.provider.v1",
		Providers: []extensions.Provider{storePoint.Provide(namedStore{"real"})},
	})))

	// Created but deliberately left out of Deps.
	undeclared := storePoint.Require()
	assert.NilError(t, b.Register(extensions.New(extensions.Declaration{
		ID: "org.example.sneaky.v1",
		Init: func(context.Context, extensions.Config) error {
			_, err := undeclared.Get()
			return err
		},
	})))

	err := b.Init(context.Background(), nil)
	assert.ErrorContains(t, err, "was not declared")
	assert.ErrorContains(t, err, "org.mobyproject.test.store.v1")
}

// TestLazyDependencyBreaksACycle is the primitive that makes decomposing the
// daemon possible at all.
//
// Two modules that refer to each other -- as containers, images and volumes
// genuinely do -- cannot both be ordered first. With ordering edges on both
// sides the graph is unsatisfiable and the daemon refuses to start. Declaring
// one side lazy says that call happens while serving a request rather than
// during Init, so it constrains nothing about startup and the cycle disappears.
func TestLazyDependencyBreaksACycle(t *testing.T) {
	const (
		aPoint = extensions.PointID("org.mobyproject.test.a.v1")
		bPoint = extensions.PointID("org.mobyproject.test.b.v1")
	)
	pointA := extensions.DefinePoint[store](aPoint)
	pointB := extensions.DefinePoint[store](bPoint)

	build := func(aOnB, bOnA extensions.AnyDep) *Broker {
		br := New()
		assert.NilError(t, br.Register(extensions.New(extensions.Declaration{
			ID:        "org.example.a.v1",
			Providers: []extensions.Provider{pointA.Provide(namedStore{"a"})},
			Deps:      []extensions.AnyDep{aOnB},
		})))
		assert.NilError(t, br.Register(extensions.New(extensions.Declaration{
			ID:        "org.example.b.v1",
			Providers: []extensions.Provider{pointB.Provide(namedStore{"b"})},
			Deps:      []extensions.AnyDep{bOnA},
		})))
		return br
	}

	t.Run("both eager is a cycle", func(t *testing.T) {
		br := build(pointB.Require(), pointA.Require())
		err := br.Init(context.Background(), nil)
		assert.ErrorContains(t, err, "cycle")
	})

	t.Run("one lazy resolves", func(t *testing.T) {
		lazy := pointA.Lazy()
		br := build(pointB.Require(), lazy)
		assert.NilError(t, br.Init(context.Background(), nil))

		// The lazy handle is still bound and still typed -- it just did not
		// constrain startup order.
		s, err := lazy.Get()
		assert.NilError(t, err)
		assert.Check(t, is.Equal(s.Name(), "a"))
	})
}
