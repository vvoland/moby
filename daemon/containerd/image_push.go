package containerd

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/containerd/containerd/content"
	cerrdefs "github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/images"
	containerdimages "github.com/containerd/containerd/images"
	"github.com/containerd/containerd/platforms"
	"github.com/containerd/containerd/remotes"
	"github.com/docker/distribution/reference"
	"github.com/docker/docker/api/types/registry"
	"github.com/docker/docker/errdefs"
	"github.com/docker/docker/pkg/streamformatter"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/semaphore"
)

// PushImage initiates a push operation of the image pointed to by targetRef.
// If not all content referenced by the image manifest list is available
// locally, it will attempt to cross-repo mount them or download if the former
// is not possible.
func (i *ImageService) PushImage(ctx context.Context, targetRef reference.Named, metaHeaders map[string][]string, authConfig *registry.AuthConfig, outStream io.Writer) error {
	if _, ok := targetRef.(reference.Tagged); !ok {
		return errdefs.NotImplemented(errors.New("push all tags is not implemented"))
	}

	leasedCtx, release, err := i.client.WithLease(ctx)
	if err != nil {
		return err
	}
	defer release(leasedCtx)
	out := streamformatter.NewJSONProgressOutput(outStream, false)

	img, err := i.client.ImageService().Get(ctx, targetRef.String())
	if err != nil {
		return errdefs.NotFound(err)
	}

	// TODO: Add `--platform` flag and allow user choose specific platform manifest
	target := img.Target
	store := i.client.ContentStore()

	resolver, tracker := i.newResolverFromAuthConfig(authConfig)
	progress := pushProgress{Tracker: tracker}
	jobs := newJobs()
	finishProgress := jobs.showProgress(ctx, out, combinedProgress([]progressUpdater{
		&progress,
		pullProgress{ShowExists: false, Store: store},
	}))
	defer finishProgress()

	var limiter *semaphore.Weighted = nil // TODO: Respect max concurrent downloads/uploads

	if containerdimages.IsIndexType(target.MediaType) {
		// When pushing manifest list we may not have all platforms available locally.
		// This will frequently be the case for pulled images, because by default
		// only host platform is pulled instead of all supported by the image.
		mountableBlobs, err := i.prepareMissing(ctx, store, jobs, resolver, target, targetRef, &progress, limiter)
		if err != nil {
			return err
		}

		// Create a store which fakes the local existence of blobs which are mountable.
		// Otherwise they can't be pushed at all.
		wrapped := wrapWithFakeMountableBlobs(store, mountableBlobs)
		store = wrapped
	}

	pusher, err := resolver.Pusher(ctx, targetRef.String())
	if err != nil {
		return err
	}

	addChildrenToJobs := func(h images.Handler) images.Handler {
		return containerdimages.Handlers(containerdimages.HandlerFunc(
			func(ctx context.Context, desc ocispec.Descriptor) ([]ocispec.Descriptor, error) {
				children, err := containerdimages.Children(ctx, store, desc)
				if err != nil {
					return nil, err
				}
				for _, c := range children {
					jobs.Add(c)
				}

				jobs.Add(desc)

				return nil, nil
			}), h)
	}
	return remotes.PushContent(ctx, pusher, target, store, limiter, platforms.All, addChildrenToJobs)
}

// prepareMissing will walk the target descriptor recursively and handles
// missing content which to make the target descriptor possible to be pushed.
//
// If target references any missing content it will be fetched from the source
// repository unless it can be cross-repo mounted.
//
// Manifests/configs can't be mounted and need to be downloaded - it shouldn't
// be an issue though, because they are relatively small json files.
// If we are pushing to the same registry then layer blobs can be
// cross-repo mounted and won't be downloaded.
func (i *ImageService) prepareMissing(ctx context.Context, store content.Store, jobs *jobs, resolver remotes.Resolver,
	target ocispec.Descriptor, targetRef reference.Named,
	progress *pushProgress, limiter *semaphore.Weighted,
) (map[digest.Digest]distributionSource, error) {
	mountableBlobs := map[digest.Digest]distributionSource{}
	source, err := getDigestSource(ctx, store, target.Digest)

	if err != nil {
		if !errdefs.IsNotFound(err) {
			return nil, err
		}
		logrus.WithField("target", target).Debug("distribution source label not found")
		return mountableBlobs, nil
	}

	handler := func(ctx context.Context, desc ocispec.Descriptor) ([]ocispec.Descriptor, error) {
		_, err := store.Info(ctx, desc.Digest)
		if err != nil {
			if !cerrdefs.IsNotFound(err) {
				return nil, errdefs.System(errors.Wrapf(err, "failed to get metadata of content %s", desc.Digest.String()))
			}

			ref, err := source.GetReference(desc.Digest)
			if err != nil {
				return nil, errdefs.InvalidParameter(errors.Wrapf(err, "failed to get reference to content %s", desc.Digest.String()))
			}

			jobs.Add(desc)
			if canBeMounted(desc, targetRef, i.registryService.IsInsecureRegistry, source) {
				dgst := desc.Digest
				mountableBlobs[dgst] = source
				progress.addMountable(dgst)
				return nil, nil
			} else {
				fetcher, err := resolver.Fetcher(ctx, ref.String())
				if err != nil {
					return nil, errdefs.InvalidParameter(errors.Wrapf(err, "failed to create fetcher for %s", ref.String()))
				}

				fetchHandler := remotes.FetchHandler(store, fetcher)
				_, err = fetchHandler(ctx, desc)
				if err != nil {
					return nil, errdefs.System(errors.Wrapf(err, "failed to fetch descriptor %s (type %s)", desc.Digest.String(), desc.MediaType))
				}
			}
		}

		return containerdimages.Children(ctx, store, desc)
	}

	err = containerdimages.Dispatch(ctx, containerdimages.HandlerFunc(handler), limiter, target)
	if err != nil {
		return nil, err
	}

	// Set GC labels recursively for all downloaded blobs.
	setLabels := containerdimages.SetChildrenLabels(store, containerdimages.ChildrenHandler(store))
	err = containerdimages.Dispatch(ctx, setLabels, nil, target)
	if err != nil {
		logrus.WithError(err).Warn("set children gc labels failed")
	}

	return mountableBlobs, nil
}

func getDigestSource(ctx context.Context, store content.Manager, digest digest.Digest) (distributionSource, error) {
	source := distributionSource{}

	info, err := store.Info(ctx, digest)
	if err != nil {
		if cerrdefs.IsNotFound(err) {
			return source, errdefs.NotFound(err)
		}
		return source, errdefs.System(err)
	}

	s := extractDistributionSource(info.Labels)
	if s == nil {
		return source, errdefs.NotFound(fmt.Errorf("label %q is not attached to %s", labelDistributionSource, digest.String()))
	}

	return *s, nil
}

const labelDistributionSource = "containerd.io/distribution.source."

func extractDistributionSource(labels map[string]string) *distributionSource {
	var source distributionSource

	// Check if this blob has a distributionSource label
	// if yes, read it as source
	for k, v := range labels {
		registry := strings.TrimPrefix(k, labelDistributionSource)
		if registry != k {
			ref, err := reference.ParseNamed(registry + "/" + v)
			if err != nil {
				continue
			}

			source.registryRef = ref
			return &source
		}
	}

	return nil
}

type distributionSource struct {
	registryRef reference.Named
}

func (source distributionSource) GetReference(dgst digest.Digest) (reference.Named, error) {
	return reference.WithDigest(source.registryRef, dgst)
}

// shouldDownload returns if the given descriptor can be cross-repo mounted
// when pushing it to a remote reference ref.
func canBeMounted(desc ocispec.Descriptor, targetRef reference.Named, isInsecureFunc func(string) bool, source distributionSource) bool {
	mediaType := desc.MediaType

	if containerdimages.IsManifestType(mediaType) {
		return false
	}
	if containerdimages.IsIndexType(mediaType) {
		return false
	}
	if containerdimages.IsConfigType(mediaType) {
		return false
	}

	registry := reference.Domain(targetRef)

	// Cross-repo mount doesn't seem to work with insecure registries.
	isInsecure := isInsecureFunc(registry)
	if isInsecure {
		return false
	}

	// If the source registry is the same as the one we are pushing to
	// then the cross-repo mount will work.
	return registry == reference.Domain(source.registryRef)
}
