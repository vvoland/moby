package containerd

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/containerd/containerd/content"
	cerrdefs "github.com/containerd/containerd/errdefs"
	containerdimages "github.com/containerd/containerd/images"
	"github.com/containerd/containerd/platforms"
	"github.com/containerd/containerd/remotes"
	"github.com/containerd/containerd/remotes/docker"
	"github.com/docker/distribution/reference"
	"github.com/docker/docker/errdefs"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

type lazyPusher struct {
	store           content.Store
	resolver        remotes.Resolver
	jobs            *jobs
	downloadLimiter *semaphore.Weighted
	uploadLimiter   *semaphore.Weighted
}

func newLazyPusher(store content.Store, resolver remotes.Resolver, jobs *jobs,
	downloadLimiter, uploadLimiter *semaphore.Weighted) *lazyPusher {
	return &lazyPusher{
		store:           store,
		resolver:        resolver,
		jobs:            jobs,
		downloadLimiter: downloadLimiter,
		uploadLimiter:   uploadLimiter,
	}
}

const labelDistributionSource = "containerd.io/distribution.source."

// push uploads the provided content to a remote resource. If not all
// required content is present in the local content store, then it's fetched
// from the source repository or mounted on with cross-repo mounts.
func (p *lazyPusher) push(ctx context.Context, ref reference.Named, root ocispec.Descriptor, imagesHandler containerdimages.HandlerFunc) error {

	refDigest := ref.String()

	// Annotate ref with digest to push only push tag for single digest
	if !strings.Contains(refDigest, "@") {
		refDigest = refDigest + "@" + root.Digest.String()
	}

	pusher, err := p.resolver.Pusher(ctx, refDigest)
	if err != nil {
		return err
	}

	sources, err := p.fetchMissingContent(ctx, ref, root)
	if err != nil {
		return err
	}

	lazyStore := newLazyContentStore(p.store, sources)
	wrapper := func(h containerdimages.Handler) containerdimages.Handler {
		return containerdimages.Handlers(imagesHandler, h)
	}

	return remotes.PushContent(ctx, pusher, root, lazyStore, p.uploadLimiter, platforms.All, wrapper)
}

func (p *lazyPusher) fetchMissingContent(ctx context.Context, ref reference.Named, root ocispec.Descriptor) (map[digest.Digest]distributionSource, error) {
	sources := map[digest.Digest]distributionSource{}
	missing := []ocispec.Descriptor{}
	next := []ocispec.Descriptor{root}

	for len(next) > 0 {
		newMissing, err := findMissingContent(ctx, p.store, next...)
		if err != nil {
			return sources, err
		}
		missing = dedupDescriptors(append(missing, newMissing...))

		logrus.WithField("missing", missing).Debug("searching sources for missing descriptors")
		err = collectSources(ctx, missing, p.store, sources)
		if err != nil {
			return sources, err
		}

		// Create a slice of descriptors that can be fetched now.
		toFetch := []ocispec.Descriptor{}
		for _, desc := range missing {
			source, hasSource := sources[desc.Digest]
			if hasSource && shouldDownload(ref, source, desc) {
				toFetch = append(toFetch, desc)
				p.jobs.Add(desc)
			}
		}

		fetched, err := p.fetch(ctx, sources, toFetch)
		logrus.WithError(err).
			WithField("fetched", fetched).
			WithField("toFetch", toFetch).
			Debug("fetch")

		if err != nil {
			return sources, err
		}

		if len(toFetch) > 0 && len(fetched) == 0 {
			logrus.WithField("toFetch", toFetch).Error("failed to fetch any of the missing blobs")
			return sources, err
		}

		isFetched := func(desc ocispec.Descriptor) bool {
			for _, f := range fetched {
				if f.Digest == desc.Digest {
					return true
				}
			}
			return false
		}

		// Remove fetched content from missing
		missingMinusFetched := []ocispec.Descriptor{}
		for _, m := range missing {
			if !isFetched(m) {
				missingMinusFetched = append(missingMinusFetched, m)
			}
		}
		missing = missingMinusFetched

		next = fetched
	}

	return sources, nil
}

func dedupDescriptors(s []ocispec.Descriptor) []ocispec.Descriptor {
	m := map[digest.Digest]ocispec.Descriptor{}
	for _, d := range s {
		m[d.Digest] = d
	}

	out := []ocispec.Descriptor{}
	for _, v := range m {
		out = append(out, v)
	}

	return out
}

func shouldDownload(root reference.Named, source distributionSource, desc ocispec.Descriptor) bool {
	mediaType := desc.MediaType

	// Manifests/indexes/configs cannot be cross-repo mounted so we have to download them
	if containerdimages.IsManifestType(mediaType) {
		return true
	}
	if containerdimages.IsIndexType(mediaType) {
		return true
	}
	if containerdimages.IsConfigType(mediaType) {
		return true
	}

	registry := reference.Domain(root)

	// Cross-repo mount doesn't seem to work with insecure registries.
	// Maybe it's only Docker Hub?
	// TODO(vvoland): do the actual check when we support insecure registries
	isInsecure := false
	if isInsecure {
		return true
	}

	// If the source registry is the same as the one we are pushing to
	// then the cross-repo mount will work, and we don't need to download.
	return registry != source.Registry()
}

func (p *lazyPusher) fetch(ctx context.Context, sources map[digest.Digest]distributionSource, missing []ocispec.Descriptor) ([]ocispec.Descriptor, error) {
	fetched := []ocispec.Descriptor{}
	mutex := sync.Mutex{}
	eg, ctx := errgroup.WithContext(ctx)

	for _, desc := range missing {
		log := logrus.
			WithField("digest", desc.Digest.String()).
			WithField("mediaType", desc.MediaType)

		source, ok := sources[desc.Digest]
		if !ok {
			log.Debug("no source")
			continue
		}

		desc := desc
		eg.Go(func() error {
			if p.downloadLimiter != nil {
				if err := p.downloadLimiter.Acquire(ctx, 1); err != nil {
					return err
				}
				defer p.downloadLimiter.Release(1)
			}

			ref, err := source.GetReference(desc.Digest)
			if err != nil {
				return err
			}
			log = log.WithField("ref", ref.String())

			name, resolved, err := p.resolver.Resolve(ctx, ref.String())
			if err != nil {
				// If the size is set, we can just fallback to the original descriptor.
				if desc.Size > 0 {
					log.WithError(err).Debug("failed to resolve missing content, fallback to original")
					name = ref.String()
					resolved = desc
				} else {
					return err
				}
			}

			log.WithField("name", name).Debug("resolved missing content")
			fetcher, err := p.resolver.Fetcher(ctx, name)
			if err != nil {
				log.WithError(err).Debug("failed to create fetcher")
				return err
			}

			appendDistributionSourceLabel, err := docker.AppendDistributionSourceLabel(p.store, ref.String())
			if err != nil {
				return err
			}

			fetchHandler := containerdimages.Handlers(
				remotes.FetchHandler(p.store, fetcher),
				appendDistributionSourceLabel,
				appendLabelHandler(ctx, p.store, "docker.io/fetch.reason", "push"),
			)

			_, err = fetchHandler(ctx, resolved)
			if err != nil {
				log.WithError(err).Debug("failed to fetch")
				return err
			}

			log.Debug("fetched!")
			mutex.Lock()
			fetched = append(fetched, desc)
			mutex.Unlock()
			return nil
		})
	}

	return fetched, eg.Wait()
}

// appendLabelHandler returns a handler which adds a label with value to the handled content
func appendLabelHandler(ctx context.Context, manager content.Manager, key, value string) containerdimages.HandlerFunc {
	return containerdimages.HandlerFunc(func(ctx context.Context, desc ocispec.Descriptor) (subdescs []ocispec.Descriptor, err error) {
		info, err := manager.Info(ctx, desc.Digest)
		if err != nil {
			return nil, err
		}
		if info.Labels == nil {
			info.Labels = map[string]string{}
		}
		info.Labels[key] = value

		_, err = manager.Update(ctx, info, "labels."+key)
		return nil, err
	})
}

// contentDoesntExist returns true only if content is not present in store and
// there was no other error.
func contentDoesntExist(ctx context.Context, store content.Store, desc ocispec.Descriptor) (bool, error) {
	// Don't use store.Info to make this also work with the lazyContentStore
	// which doesn't return NotFound error
	r, err := store.ReaderAt(ctx, desc)
	if err == nil {
		r.Close()
	} else {
		if cerrdefs.IsNotFound(err) {
			return true, nil
		}
	}

	return false, err
}

// findMissingContent traverses the children of the given descriptors and returns
// descriptors of contents that are not present in the content store.
func findMissingContent(ctx context.Context, store content.Store, desc ...ocispec.Descriptor) ([]ocispec.Descriptor, error) {
	// Collect to hashset to remove duplicates
	set := map[digest.Digest]ocispec.Descriptor{}
	mutex := sync.Mutex{}

	err := containerdimages.Dispatch(ctx,
		containerdimages.HandlerFunc(func(ctx context.Context, desc ocispec.Descriptor) ([]ocispec.Descriptor, error) {
			mt := desc.MediaType

			if containerdimages.IsNonDistributable(mt) {
				return nil, containerdimages.ErrSkipDesc
			}

			doesntExist, err := contentDoesntExist(ctx, store, desc)
			if err != nil {
				return nil, err
			}
			if doesntExist {
				mutex.Lock()
				defer mutex.Unlock()
				set[desc.Digest] = desc
				return nil, nil
			}

			children, err := containerdimages.Children(ctx, store, desc)
			return children, err
		}),
		nil, desc...)

	if err != nil {
		return nil, err
	}

	result := make([]ocispec.Descriptor, 0, len(set))
	for _, desc := range set {
		result = append(result, desc)
		logrus.WithField("digest", desc.Digest.String()).
			WithField("mediaType", desc.MediaType).
			Debug("missing content")
	}

	return result, nil
}

// peekNotJson  does a small peek of the content to check if content is definitely not JSON.
// It returns true if content is definitely not JSON, or false if it was unable to detect if it's
// JSON or not.
func peekNotJson(ctx context.Context, store content.Provider, desc ocispec.Descriptor) (bool, error) {
	readerAt, err := store.ReaderAt(ctx, desc)
	if err != nil {
		logrus.WithError(err).WithField("digest", desc.Digest).Debug("failed to create reader to peek for json")
		return false, err
	}
	defer readerAt.Close()

	buffer := []byte{0}
	n, err := readerAt.ReadAt(buffer, 0)
	if n != 1 || err != nil {
		logrus.WithError(err).WithField("digest", desc.Digest).Debug("failed to peek json")
		return false, err
	}

	// It doesn't start with {, then it's not a json.
	return rune(buffer[0]) != '{', nil
}

// collectSources walks the content store and looks for content which can
// provide a source registry and repository for the provided descriptors from
// the containerd.io/distribution.source labels
func collectSources(ctx context.Context, toCollect []ocispec.Descriptor, store content.Store, sources map[digest.Digest]distributionSource) error {
	// Nothing to do.
	if len(toCollect) == 0 {
		return nil
	}

	// Make a copy of the missing descriptors as we will be removing
	// the descriptors that we found a source for.
	missing := make([]ocispec.Descriptor, len(toCollect))
	copy(missing, toCollect)

	success := errors.New("success, found the source but can't return earlier without an error")
	err := store.Walk(ctx, func(i content.Info) error {
		source := extractDistributionSource(i.Labels)
		log := logrus.
			WithField("digest", i.Digest)

		log.Debug("walk")

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
			log.Debug("skipping, definitely not a json")
			return nil
		}

		// Read the manifest
		blob, err := content.ReadBlob(ctx, store, desc)
		if err != nil {
			log.WithError(err).Error("error reading blob")
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
			Config    ocispec.Descriptor   `json:"config,omitempty"`
		}

		err = json.Unmarshal(blob, &indexOrManifest)
		if err != nil {
			log.WithError(err).Debug("unmarshal failed")
			return nil
		}

		mediaType := indexOrManifest.MediaType
		// Just in case, check if it really is manifest or index.
		if !containerdimages.IsManifestType(mediaType) && !containerdimages.IsIndexType(mediaType) {
			log.Debug("not a manifest/index")
			return nil
		}
		children := append(indexOrManifest.Layers, indexOrManifest.Manifests...)
		if indexOrManifest.Config.Digest != digest.Digest("") {
			children = append(children, indexOrManifest.Config)
		}

		if len(children) == 0 {
			log.Debug("empty a manifest/index")
			return nil
		}

		// Look if this manifest/index specifies any of the missing content
		for _, layer := range children {
			for idx := 0; idx < len(missing); idx += 1 {
				wanted := missing[idx]
				if layer.Digest == wanted.Digest {
					// Found it!
					sources[wanted.Digest] = source
					log.WithField("wanted", wanted.Digest.String()).Debug("found")

					// Don't look for it anymore
					if len(missing) > 1 {
						lastIdx := len(missing) - 1
						missing[idx] = missing[lastIdx]
						missing = missing[:lastIdx]
						idx -= 1
					} else {
						// We found all missing, let's end the walk.
						missing = missing[:0]
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
	if len(missing) > 0 {
		msg := "missing blobs with no source: "
		for idx, c := range missing {
			if idx != 0 {
				msg += ", "
			}
			msg += c.Digest.String()
		}
		err = errdefs.NotFound(errors.New(msg))
	}

	return err
}

func extractDistributionSource(labels map[string]string) distributionSource {
	var source distributionSource

	// Check if this blob has a distributionSource label
	// if yes, read it as source
	for k, v := range labels {
		if strings.HasPrefix(k, labelDistributionSource) {
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

func (source distributionSource) Registry() string {
	registry := strings.TrimPrefix(source.key, labelDistributionSource)
	if registry == source.key {
		return ""
	}
	return registry
}

func (source distributionSource) GetReference(dgst digest.Digest) (reference.Named, error) {
	registry := source.Registry()
	if registry == "" {
		return nil, fmt.Errorf("invalid distribution source label %s=%s", source.key, source.value)
	}

	ref, err := reference.ParseNamed(registry + "/" + source.value)
	if err != nil {
		return nil, err
	}

	return reference.WithDigest(ref, dgst)
}
