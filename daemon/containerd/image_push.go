package containerd

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/containerd/containerd"
	"github.com/containerd/containerd/content"
	cerrdefs "github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/images"
	containerdimages "github.com/containerd/containerd/images"
	"github.com/containerd/containerd/images/converter"
	"github.com/containerd/containerd/platforms"
	"github.com/containerd/containerd/remotes"
	"github.com/docker/distribution/reference"
	"github.com/docker/docker/api/types/registry"
	"github.com/docker/docker/errdefs"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/semaphore"
)

// PushImage initiates a push operation on the repository named localName.
func (i *ImageService) PushImage(ctx context.Context, image, tag string, metaHeaders map[string][]string, authConfig *registry.AuthConfig, outStream io.Writer) error {
	// TODO: Pass this from user?
	platformMatcher := platforms.All

	ref, err := reference.ParseNormalizedNamed(image)
	if err != nil {
		return err
	}
	if tag != "" {
		// Push by digest is not supported, so only tags are supported.
		ref, err = reference.WithTag(ref, tag)
		if err != nil {
			return err
		}
	}

	is := i.client.ImageService()
	store := i.client.ContentStore()

	img, err := is.Get(ctx, ref.String())
	if err != nil {
		return errors.Wrap(err, "Failed to get image")
	}

	target := img.Target

	// Create a temporary image which is stripped from content that references other platforms.
	// We or the remote may not have them and referencing them will end with an error.
	if platformMatcher != platforms.All {
		tmpRef := ref.String() + "-tmp-platformspecific"
		platformImg, err := converter.Convert(ctx, i.client, tmpRef, ref.String(), converter.WithPlatform(platformMatcher))
		if err != nil {
			return errors.Wrap(err, "Failed to convert image to platform specific")
		}

		target = platformImg.Target
		defer i.client.ImageService().Delete(ctx, platformImg.Name, containerdimages.SynchronousDelete())
	}

	jobs := newJobs()

	imageHandler := containerdimages.HandlerFunc(func(ctx context.Context, desc ocispec.Descriptor) (subdescs []ocispec.Descriptor, err error) {
		logrus.WithField("desc", desc).Debug("Pushing")
		if desc.MediaType != containerdimages.MediaTypeDockerSchema1Manifest {
			children, err := containerdimages.Children(ctx, store, desc)
			if err != nil {
				return nil, err
			}
			for _, c := range children {
				jobs.Add(c)
			}

			jobs.Add(desc)
		}

		return nil, nil
	})
	imageHandler = remotes.SkipNonDistributableBlobs(imageHandler)

	resolver, tracker := newResolverFromAuthConfig(authConfig)

	finishProgress := showProgress(ctx, jobs, outStream, pushProgress(tracker))
	defer finishProgress()

	sources, err := collectSources(ctx, target, store)
	if err != nil {
		return err
	}

	return push(ctx, i.client, ref.String(), target, resolver, imageHandler, sources)
}

// Push uploads the provided content to a remote resource
func push(ctx context.Context, c *containerd.Client, ref string, desc ocispec.Descriptor, resolver remotes.Resolver, imagesHandler containerdimages.HandlerFunc, sources map[string]distributionSource) error {
	// Annotate ref with digest to push only push tag for single digest
	if !strings.Contains(ref, "@") {
		ref = ref + "@" + desc.Digest.String()
	}

	pusher, err := resolver.Pusher(ctx, ref)
	if err != nil {
		return err
	}

	wrapper := func(h images.Handler) images.Handler {
		h = images.Handlers(imagesHandler, h)
		return h
	}

	var limiter *semaphore.Weighted

	logrus.WithField("desc", desc).WithField("ref", ref).Info("Pushing desc to remote ref")
	return pushContent(ctx, pusher, desc, c.ContentStore(), limiter, platforms.All, wrapper, sources)
}

func pushContent(ctx context.Context, pusher remotes.Pusher, desc ocispec.Descriptor, store content.Store, limiter *semaphore.Weighted, platform platforms.MatchComparer, wrapper func(h images.Handler) images.Handler, sources map[string]distributionSource) error {
	var m sync.Mutex
	manifestStack := []ocispec.Descriptor{}

	filterHandler := images.HandlerFunc(func(ctx context.Context, desc ocispec.Descriptor) ([]ocispec.Descriptor, error) {
		switch desc.MediaType {
		case images.MediaTypeDockerSchema2Manifest, ocispec.MediaTypeImageManifest,
			images.MediaTypeDockerSchema2ManifestList, ocispec.MediaTypeImageIndex:
			m.Lock()
			manifestStack = append(manifestStack, desc)
			m.Unlock()
			return nil, images.ErrStopHandler
		default:
			return nil, nil
		}
	})

	pushHandler := remotes.PushHandler(pusher, store)

	platformFilterhandler := images.FilterPlatforms(images.ChildrenHandler(store), platform)

	annotateHandler := annotateDistributionSourceHandler(platformFilterhandler, store, sources)

	var handler images.Handler = images.Handlers(
		annotateHandler,
		filterHandler,
		pushHandler,
	)
	if wrapper != nil {
		handler = wrapper(handler)
	}

	if err := images.Dispatch(ctx, handler, limiter, desc); err != nil {
		return err
	}

	// Iterate in reverse order as seen, parent always uploaded after child
	for i := len(manifestStack) - 1; i >= 0; i-- {
		_, err := pushHandler(ctx, manifestStack[i])
		if err != nil {
			// TODO(estesp): until we have a more complete method for index push, we need to report
			// missing dependencies in an index/manifest list by sensing the "400 Bad Request"
			// as a marker for this problem
			if (manifestStack[i].MediaType == ocispec.MediaTypeImageIndex ||
				manifestStack[i].MediaType == images.MediaTypeDockerSchema2ManifestList) &&
				errors.Unwrap(err) != nil && strings.Contains(errors.Unwrap(err).Error(), "400 Bad Request") {
				return fmt.Errorf("manifest list/index references to blobs and/or manifests are missing in your target registry: %w", err)
			}
			return err
		}
	}

	return nil
}

// annotateDistributionSourceHandler add distribution source label into
// annotation of config or blob descriptor.
func annotateDistributionSourceHandler(f images.HandlerFunc, manager content.Manager, sources map[string]distributionSource) images.HandlerFunc {
	return func(ctx context.Context, desc ocispec.Descriptor) ([]ocispec.Descriptor, error) {
		children, err := f(ctx, desc)
		if err != nil {
			return nil, err
		}

		// only add distribution source for the config or blob data descriptor
		switch desc.MediaType {
		case images.MediaTypeDockerSchema2Manifest, ocispec.MediaTypeImageManifest,
			images.MediaTypeDockerSchema2ManifestList, ocispec.MediaTypeImageIndex:
		default:
			return children, nil
		}

		for i := range children {
			child := children[i]
			if child.Annotations == nil {
				child.Annotations = map[string]string{}
			}

			info, err := manager.Info(ctx, child.Digest)
			if err != nil {
				if cerrdefs.IsNotFound(err) {
					if s, ok := sources[child.Digest.String()]; ok {
						child.Annotations[s.key] = s.value
						continue
					}
				}
				return nil, err
			}

			for k, v := range info.Labels {
				if !strings.HasPrefix(k, "containerd.io/distribution.source.") {
					continue
				}

				child.Annotations[k] = v
			}

			children[i] = child
		}
		return children, nil
	}
}

func collectSources(ctx context.Context, desc ocispec.Descriptor, store content.Store) (map[string]distributionSource, error) {
	queue := []ocispec.Descriptor{desc}

	sources := make(map[string]distributionSource)

	for len(queue) > 0 {
		child := queue[0]
		queue = queue[1:]

		if containerdimages.IsNonDistributable(child.MediaType) {
			continue
		}

		if containerdimages.IsLayerType(child.MediaType) {
			_, err := store.ReaderAt(ctx, child)
			if err != nil && cerrdefs.IsNotFound(err) {
				source, err := findSource(ctx, store, child.Digest)

				if err != nil {
					return sources, err
				}
				if source.value == "" {
					logrus.WithField("digest", child.Digest).Error("failed to find source")
					return sources, errors.New("failed to find source")
				}

				sources[child.Digest.String()] = source
				continue
			}
		}

		newChildren, err := containerdimages.Children(ctx, store, child)
		if err != nil {
			return sources, err
		}

		if len(newChildren) > 0 {
			queue = append(queue, newChildren...)
		}
	}

	return sources, nil
}

type distributionSource struct {
	key   string
	value string
}

func findSource(ctx context.Context, store content.Store, orphan digest.Digest) (distributionSource, error) {
	var source distributionSource

	success := errors.New("success, found the source but can't return earlier without an error")

	err := store.Walk(ctx, func(i content.Info) error {
		var is distributionSource
		// Check if this blob has a distributionSource label
		for k, v := range i.Labels {
			registry := strings.TrimPrefix(k, "containerd.io/distribution.source.")

			if registry != k {
				is.key = k
				is.value = v
				break
			}
		}

		// Nah, we're looking for a parent of this poor orphan.
		// This blob will not provide us with the source.
		if is.value == "" {
			return nil
		}

		blob, err := content.ReadBlob(ctx, store, ocispec.Descriptor{Digest: i.Digest})
		if err != nil {
			return err
		}

		var manifest ocispec.Manifest
		err = json.Unmarshal(blob, &manifest)
		if err != nil {
			return nil
		}

		// Just in case, check if the manifest identifies itself as a manifest.
		if !containerdimages.IsManifestType(manifest.MediaType) {
			return nil
		}

		for _, layer := range manifest.Layers {
			if layer.Digest == orphan {
				source = is
				return success
			}
		}

		return nil
	})

	if err == success {
		err = nil
	}
	if source.value == "" {
		err = errdefs.NotFound(fmt.Errorf("missing blob %s and no source can be found", orphan))
	}

	return source, err
}
