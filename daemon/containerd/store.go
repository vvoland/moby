package containerd

import (
	"context"

	"github.com/containerd/containerd/content"
	cerrdefs "github.com/containerd/containerd/errdefs"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/sirupsen/logrus"
)

type lazyContentStore struct {
	s       content.Store
	sources map[digest.Digest]distributionSource
}

func newLazyContentStore(s content.Store, sources map[digest.Digest]distributionSource) lazyContentStore {
	return lazyContentStore{
		s:       s,
		sources: sources,
	}
}

// Delete implements content.Store
func (p lazyContentStore) Delete(ctx context.Context, dgst digest.Digest) error {
	return p.s.Delete(ctx, dgst)
}

// Info implements content.Store
func (p lazyContentStore) Info(ctx context.Context, dgst digest.Digest) (content.Info, error) {
	info, err := p.s.Info(ctx, dgst)
	if err != nil {
		if !cerrdefs.IsNotFound(err) {
			return info, err
		}
		s, ok := p.sources[dgst]
		if !ok {
			return info, err
		}

		logrus.WithField("digest", dgst).WithField("source", s).Debug("faking")
		return content.Info{
			Digest: dgst,
			Labels: map[string]string{
				s.key: s.value,
			},
		}, nil
	}

	return info, nil
}

// Update implements content.Store
func (p lazyContentStore) Update(ctx context.Context, info content.Info, fieldpaths ...string) (content.Info, error) {
	return p.s.Update(ctx, info, fieldpaths...)
}

// Walk implements content.Store
func (p lazyContentStore) Walk(ctx context.Context, fn content.WalkFunc, filters ...string) error {
	return p.s.Walk(ctx, fn, filters...)
}

// ReaderAt implements content.Store
func (p lazyContentStore) ReaderAt(ctx context.Context, desc ocispec.Descriptor) (content.ReaderAt, error) {
	return p.s.ReaderAt(ctx, desc)
}

// Abort implements content.Store
func (p lazyContentStore) Abort(ctx context.Context, ref string) error {
	return p.s.Abort(ctx, ref)
}

// ListStatuses implements content.Store
func (p lazyContentStore) ListStatuses(ctx context.Context, filters ...string) ([]content.Status, error) {
	return p.s.ListStatuses(ctx, filters...)
}

// Status implements content.Store
func (p lazyContentStore) Status(ctx context.Context, ref string) (content.Status, error) {
	return p.s.Status(ctx, ref)
}

// Writer implements content.Store
func (p lazyContentStore) Writer(ctx context.Context, opts ...content.WriterOpt) (content.Writer, error) {
	return p.s.Writer(ctx, opts...)
}
