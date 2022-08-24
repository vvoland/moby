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
		logrus.WithField("digest", desc.Digest.String()).
			WithField("mediaType", desc.MediaType).
			Debug("Pushing")
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

	return lazyPush(ctx, store, ref.String(), target, resolver, imageHandler)
}

// lazyPush uploads the provided content to a remote resource. It also attempts to
// handle push of content, which is not present locally in the store.
func lazyPush(ctx context.Context, store content.Store, ref string, desc ocispec.Descriptor, resolver remotes.Resolver, imagesHandler containerdimages.HandlerFunc) error {
	// Annotate ref with digest to push only push tag for single digest
	if !strings.Contains(ref, "@") {
		ref = ref + "@" + desc.Digest.String()
	}

	pusher, err := resolver.Pusher(ctx, ref)
	if err != nil {
		return err
	}

	wrapper := func(h images.Handler) images.Handler {
		return images.Handlers(imagesHandler, h)
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

		_, err := store.ReaderAt(ctx, child)
		if err != nil {
			if cerrdefs.IsNotFound(err) {
				set[child.Digest.String()] = child
				continue
			}
			return nil, err
		}

		newChildren, err := containerdimages.Children(ctx, store, child)
		if err != nil {
			return nil, err
		}

		if len(newChildren) > 0 {
			queue = append(queue, newChildren...)
		}
	}

	result := make([]ocispec.Descriptor, 0, len(set))
	for _, desc := range set {
		result = append(result, desc)
		logrus.WithField("digest", desc.Digest.String()).
			WithField("mediaType", desc.MediaType).
			Debug("lazy children found")
	}

	return result, nil
}

// peekNotJson  does a small peek of the content to check if content is definitely not JSON.
// It returns true if content is definitely not JSON, or false if it was unable to detect if it's
// JSON or not.
func peekNotJson(ctx context.Context, store content.Store, desc ocispec.Descriptor) (bool, error) {
	readerAt, err := store.ReaderAt(ctx, desc)
	if err != nil {
		logrus.WithError(err).WithField("digest", desc.Digest).Debug("failed to create reader to peek for json")
		return false, err
	}

	buffer := []byte{0}
	n, err := readerAt.ReadAt(buffer, 0)
	if n != 1 || err != nil {
		logrus.WithError(err).WithField("digest", desc.Digest).Debug("failed to peek json")
		return false, err
	}

	// It doesn't start with {, then it's not a json.
	return rune(buffer[0]) != '{', nil
}

func collectSources(ctx context.Context, desc ocispec.Descriptor, store content.Store) (map[digest.Digest]distributionSource, error) {
	lazyChildren, err := findLazyChildren(ctx, desc, store)
	if err != nil {
		logrus.WithField("digest", desc.Digest.String()).
			WithField("mediaType", desc.MediaType).
			WithError(err).Error("failed to find lazy children referenced by descriptor")
		return nil, err
	}

	sources := map[digest.Digest]distributionSource{}

	success := errors.New("success, found the source but can't return earlier without an error")
	err = store.Walk(ctx, func(i content.Info) error {
		source := extractDistributionSource(i.Labels)

		// Nah, we're looking for a parent of this lazy child.
		// This one will not provide us with the source.
		if source.value == "" {
			return nil
		}

		desc := ocispec.Descriptor{Digest: i.Digest}

		// Do a simple peek of the content to avoid big blobs which definitely aren't json.
		notJson, err := peekNotJson(ctx, store, desc)
		if err != nil {
			return err
		}
		if notJson {
			logrus.WithField("digest", i.Digest).Debug("skipping, definitely not a json")
			return nil
		}

		// Read the manifest
		blob, err := content.ReadBlob(ctx, store, desc)
		if err != nil {
			logrus.WithError(err).WithField("digest", i.Digest).Error("error reading blob")
			return err
		}

		// Manifests and indexes have different children.
		// Index stores other manifests and manifests store layers.
		// To avoid unmarshaling the blob separately as manifest and index
		// this holds fields that contains them both and the media type.
		var indexOrManifest struct {
			MediaType string               `json:"mediaType,omitempty"`
			Manifests []ocispec.Descriptor `json:"manifests,omitempty"`
			Layers    []ocispec.Descriptor `json:"layers,omitempty"`
		}

		err = json.Unmarshal(blob, &indexOrManifest)
		if err != nil {
			return nil
		}

		mediaType := indexOrManifest.MediaType
		// Just in case, check if it really is manifest or index.
		if !containerdimages.IsManifestType(mediaType) && !containerdimages.IsIndexType(mediaType) {
			return nil
		}
		if len(indexOrManifest.Layers) == 0 && len(indexOrManifest.Manifests) == 0 {
			return nil
		}

		// Look if this manifest/index specifies any of the lazy children
		children := append(indexOrManifest.Layers, indexOrManifest.Manifests...)
		for _, layer := range children {
			for idx, wanted := range lazyChildren {
				if layer.Digest == wanted.Digest {
					// Found it!
					sources[wanted.Digest] = source

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
		if strings.HasPrefix(k, "containerd.io/distribution.source.") {
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
