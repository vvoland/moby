package peerstore

import (
	"context"
	"io"

	"github.com/containerd/containerd/log"
	"github.com/containerd/containerd/remotes"
	"github.com/docker/docker/errdefs"
	"github.com/docker/docker/pkg/ioutils"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

type P2PRemote struct {
	resolver remotes.Resolver
	net      *ContentNetwork
}

func (p *P2PRemote) Resolve(ctx context.Context, ref string) (string, ocispec.Descriptor, error) {
	name, desc, err := p.resolver.Resolve(ctx, ref)
	if err == nil {
		return name, desc, nil
	}

	desc, err = p.net.Resolve(ctx, ref)
	if err == nil {
		log.G(ctx).WithField("desc", desc).WithField("ref", ref).Info("[p2p] resolved image by p2p")
		return ref, desc, nil
	}

	if errdefs.IsNotFound(err) {
		log.G(ctx).WithField("ref", ref).Debug("[p2p] reference unknown to p2p network")
	} else {
		log.G(ctx).WithError(err).WithField("ref", ref).Debug("[p2p] fallback to regular resolver")
	}

	return ref, desc, err
}

type FallbackFetcher struct {
	fetchers []remotes.Fetcher
}

func (f *FallbackFetcher) Fetch(ctx context.Context, desc ocispec.Descriptor) (io.ReadCloser, error) {
	var rc io.ReadCloser
	var err error
	for _, fetcher := range f.fetchers {
		orgRc, err := fetcher.Fetch(ctx, desc)
		if err != nil {
			continue
		}

		log.G(ctx).WithField("desc", desc).Info("[p2p] fetching from p2p")
		digester := digest.Canonical.Digester()
		wrappedReader := io.TeeReader(orgRc, digester.Hash())

		rc = ioutils.NewReadCloserWrapper(wrappedReader, func() error {
			err := orgRc.Close()
			realDgst := digester.Digest()
			if realDgst.String() != desc.Digest.String() {
				log.G(ctx).Errorf("[p2p] !!!!!!! digest didn't match! Requested %s, got %s", desc.Digest, realDgst)
			}
			return err
		})

		return rc, err
	}

	return rc, err
}

// Fetcher returns a new fetcher for the provided reference.
// All content fetched from the returned fetcher will be
// from the namespace referred to by ref.
func (p *P2PRemote) Fetcher(ctx context.Context, ref string) (remotes.Fetcher, error) {
	orgFetcher, err := p.resolver.Fetcher(ctx, ref)
	if err != nil {
		return nil, err
	}

	_, err = p.net.Resolve(ctx, ref)
	if err == nil {
		return &FallbackFetcher{fetchers: []remotes.Fetcher{p.net, orgFetcher}}, nil
	}

	if errdefs.IsNotFound(err) {
		log.G(ctx).WithField("ref", ref).Debug("[p2p] reference unknown to p2p network")
	} else {
		log.G(ctx).WithError(err).WithField("ref", ref).Debug("[p2p] fallback to regular resolver")
	}

	return orgFetcher, nil
}

// Pusher returns a new pusher for the provided reference
// The returned Pusher should satisfy content.Ingester and concurrent attempts
// to push the same blob using the Ingester API should result in ErrUnavailable.
func (p *P2PRemote) Pusher(ctx context.Context, ref string) (remotes.Pusher, error) {
	return p.resolver.Pusher(ctx, ref)
}

func WrapResolver(resolver remotes.Resolver, net *ContentNetwork) remotes.Resolver {
	return &P2PRemote{
		resolver: resolver,
		net:      net,
	}
}
