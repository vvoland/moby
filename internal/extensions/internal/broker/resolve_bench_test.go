package broker

import (
	"context"
	"testing"

	"github.com/moby/moby/v2/internal/extensions"
)

type svc interface{ Do(int) int }

type impl struct{}

func (impl) Do(n int) int { return n + 1 }

var benchPoint = extensions.DefinePoint[svc]("org.mobyproject.test.bench.v1")

func benchBroker(b *testing.B) *Broker {
	b.Helper()
	br := New()
	if err := br.Register(extensions.New(extensions.Declaration{
		ID:        "org.example.provider.v1",
		Providers: []extensions.Provider{benchPoint.Provide(impl{})},
	})); err != nil {
		b.Fatal(err)
	}
	return br
}

// BenchmarkDirectCall is the baseline: what a daemon subsystem costs today,
// holding its collaborator in a struct field.
func BenchmarkDirectCall(b *testing.B) {
	var s svc = impl{}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = s.Do(i)
	}
}

// BenchmarkDepGet resolves through a declared handle on every call, which is
// what a module does if it does not cache the result.
func BenchmarkDepGet(b *testing.B) {
	br := benchBroker(b)
	dep := benchPoint.Require()
	if err := br.Register(extensions.New(extensions.Declaration{
		ID:   "org.example.consumer.v1",
		Deps: []extensions.AnyDep{dep},
	})); err != nil {
		b.Fatal(err)
	}
	if err := br.Init(context.Background(), nil); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s, err := dep.Get()
		if err != nil {
			b.Fatal(err)
		}
		_ = s.Do(i)
	}
}

// BenchmarkPointSingle is the same through the untyped resolver.
func BenchmarkPointSingle(b *testing.B) {
	br := benchBroker(b)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		s, err := benchPoint.Single(br)
		if err != nil {
			b.Fatal(err)
		}
		_ = s.Do(i)
	}
}

// BenchmarkPointAll is a fan-out lookup, which is what a hook point does.
func BenchmarkPointAll(b *testing.B) {
	br := benchBroker(b)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		all, err := benchPoint.All(br)
		if err != nil {
			b.Fatal(err)
		}
		for _, p := range all {
			_ = p.Impl.Do(i)
		}
	}
}

// BenchmarkDepGetParallel is the contention question: every module resolving
// through the one broker at once.
func BenchmarkDepGetParallel(b *testing.B) {
	br := benchBroker(b)
	dep := benchPoint.Require()
	if err := br.Register(extensions.New(extensions.Declaration{
		ID:   "org.example.consumer.v1",
		Deps: []extensions.AnyDep{dep},
	})); err != nil {
		b.Fatal(err)
	}
	if err := br.Init(context.Background(), nil); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			s, err := dep.Get()
			if err != nil {
				b.Fatal(err)
			}
			_ = s.Do(i)
			i++
		}
	})
}
