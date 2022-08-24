package containerd

import (
	"context"
	"encoding/json"
	"io"
	"strings"

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

	return push(ctx, store, ref.String(), target, resolver, imageHandler)
}

// Push uploads the provided content to a remote resource
func push(ctx context.Context, store content.Store, ref string, desc ocispec.Descriptor, resolver remotes.Resolver, imagesHandler containerdimages.HandlerFunc) error {
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

	sources, err := collectSources(ctx, desc, store)
	if err != nil {
		return err
	}

	lazyStore := newLazyContentStore(store, sources)

	var limiter *semaphore.Weighted
	return remotes.PushContent(ctx, pusher, desc, lazyStore, limiter, platforms.All, wrapper)
}

func findLazyChildren(ctx context.Context, desc ocispec.Descriptor, store content.Store) ([]ocispec.Descriptor, error) {
	// Collect to hashset to remove duplicates
	set := map[string]ocispec.Descriptor{}

	// Do a breadth-first search starting from this descriptor
	queue := []ocispec.Descriptor{desc}
	for len(queue) > 0 {
		child := queue[0]
		queue = queue[1:]

		if containerdimages.IsNonDistributable(child.MediaType) {
			continue
		}

		if containerdimages.IsLayerType(child.MediaType) {
			_, err := store.ReaderAt(ctx, child)
			if err != nil && cerrdefs.IsNotFound(err) {
				set[child.Digest.String()] = child
				continue
			}
		}

		newChildren, err := containerdimages.Children(ctx, store, child)
		if err != nil {
			return nil, err
		}

		if len(newChildren) > 0 {
			queue = append(queue, newChildren...)
		}
	}

	result := []ocispec.Descriptor{}
	for _, desc := range set {
		result = append(result, desc)
	}

	return result, nil

}

func collectSources(ctx context.Context, desc ocispec.Descriptor, store content.Store) (map[string]distributionSource, error) {
	lazyChildren, err := findLazyChildren(ctx, desc, store)
	if err != nil {
		logrus.WithField("desc", desc).WithError(err).Error("failed to find lazy children referenced by descriptor")
		return nil, err
	}

	sources := map[string]distributionSource{}

	success := errors.New("success, found the source but can't return earlier without an error")
	err = store.Walk(ctx, func(i content.Info) error {
		source := extractDistributionSource(i.Labels)

		// Nah, we're looking for a parent of this lazy child.
		// This one will not provide us with the source.
		if source.value == "" {
			return nil
		}

		// Read the manifest
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
			for idx, wanted := range lazyChildren {
				if layer.Digest == wanted.Digest {
					// Found it!
					sources[wanted.Digest.String()] = source

					// Don't look for it anymore
					if len(lazyChildren) > 1 {
						lastIdx := len(lazyChildren) - 1
						lazyChildren[idx] = lazyChildren[lastIdx]
						lazyChildren = lazyChildren[:lastIdx]
					} else {
						// We found all lazy children, let's end the walk.
						lazyChildren = lazyChildren[:0]
						return success
					}
				}
			}
		}

		return nil
	})

	if err == success {
		err = nil
	}
	if len(lazyChildren) > 0 {
		msg := "missing blobs with no source: "
		for idx, c := range lazyChildren {
			if idx != 0 {
				msg += ", "
			}
			msg += c.Digest.String()
		}
		err = errdefs.NotFound(errors.New(msg))
	}

	return sources, err
}

func extractDistributionSource(labels map[string]string) distributionSource {
	var source distributionSource

	// Check if this blob has a distributionSource label
	// if yes, read it as source
	for k, v := range labels {
		registry := strings.TrimPrefix(k, "containerd.io/distribution.source.")

		if registry != k {
			source.key = k
			source.value = v
			break
		}
	}

	return source
}

type distributionSource struct {
	key   string
	value string
}
