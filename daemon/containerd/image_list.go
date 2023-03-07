package containerd

import (
	"context"
	"fmt"
	"time"

	"github.com/containerd/containerd"
	"github.com/containerd/containerd/content"
	cerrdefs "github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/images"
	c8dplatforms "github.com/containerd/containerd/platforms"
	"github.com/docker/distribution/reference"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/filters"
	timetypes "github.com/docker/docker/api/types/time"
	"github.com/docker/docker/errdefs"
	"github.com/opencontainers/go-digest"
	"github.com/opencontainers/image-spec/identity"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/sirupsen/logrus"
)

var acceptedImageFilterTags = map[string]bool{
	"dangling":  true,
	"label":     true,
	"before":    true,
	"since":     true,
	"reference": true,
}

// Images returns a filtered list of images.
//
// TODO(thaJeztah): sort the results by created (descending); see https://github.com/moby/moby/issues/43848
// TODO(thaJeztah): implement opts.ContainerCount (used for docker system df); see https://github.com/moby/moby/issues/43853
// TODO(thaJeztah): add labels to results; see https://github.com/moby/moby/issues/43852
// TODO(thaJeztah): verify behavior of `RepoDigests` and `RepoTags` for images without (untagged) or multiple tags; see https://github.com/moby/moby/issues/43861
// TODO(thaJeztah): verify "Size" vs "VirtualSize" in images; see https://github.com/moby/moby/issues/43862
func (i *ImageService) Images(ctx context.Context, opts types.ImageListOptions) ([]*types.ImageSummary, error) {
	if err := opts.Filters.Validate(acceptedImageFilterTags); err != nil {
		return nil, err
	}

	filters, filterFn, err := i.setupFilters(ctx, opts.Filters)
	if err != nil {
		return nil, err
	}

	danglingFilter := opts.Filters.Contains("dangling")
	danglingValue := false
	if danglingFilter {
		if opts.Filters.ExactMatch("dangling", "true") {
			danglingValue = true
		} else if opts.Filters.ExactMatch("dangling", "false") {
			danglingValue = false
		} else {
			err := fmt.Errorf("invalid filter 'dangling=%s'", opts.Filters.Get("dangling"))
			return nil, errdefs.InvalidParameter(err)
		}
	}

	imgs, err := i.client.ImageService().List(ctx, filters...)
	if err != nil {
		return nil, err
	}

	snapshotter := i.client.SnapshotService(i.snapshotter)
	sizeCache := make(map[digest.Digest]int64)
	snapshotSizeFn := func(d digest.Digest) (int64, error) {
		if s, ok := sizeCache[d]; ok {
			return s, nil
		}
		usage, err := snapshotter.Usage(ctx, d.String())
		if err != nil {
			return 0, err
		}
		sizeCache[d] = usage.Size
		return usage.Size, nil
	}

	var (
		summaries = make([]*types.ImageSummary, 0, len(imgs))
		root      []*[]digest.Digest
		layers    map[digest.Digest]int
	)
	if opts.SharedSize {
		root = make([]*[]digest.Digest, 0, len(imgs))
		layers = make(map[digest.Digest]int)
	}

	contentStore := i.client.ContentStore()
	for _, img := range imgs {
		if !filterFn(img) {
			continue
		}

		if danglingFilter && danglingValue != isDanglingImage(img) {
			continue
		}

		platforms, err := images.Platforms(ctx, contentStore, img.Target)
		if err != nil {
			return nil, err
		}

		for _, platform := range platforms {
			image, chainIDs, err := i.singlePlatformImage(ctx, contentStore, img, platform)
			if err != nil {
				return nil, err
			}
			if image == nil {
				continue
			}

			summaries = append(summaries, image)

			if opts.SharedSize {
				root = append(root, &chainIDs)
				for _, id := range chainIDs {
					layers[id] = layers[id] + 1
				}
			}
		}
	}

	if opts.SharedSize {
		for n, chainIDs := range root {
			sharedSize, err := computeSharedSize(*chainIDs, layers, snapshotSizeFn)
			if err != nil {
				return nil, err
			}
			summaries[n].SharedSize = sharedSize
		}
	}

	return summaries, nil
}

func (i *ImageService) singlePlatformImage(ctx context.Context, contentStore content.Store, img images.Image, platform v1.Platform) (*types.ImageSummary, []digest.Digest, error) {
	platformMatcher := c8dplatforms.OnlyStrict(platform)
	available, _, _, missing, err := images.Check(ctx, contentStore, img.Target, platformMatcher)
	if !available || err != nil || len(missing) > 0 {
		return nil, nil, nil
	}

	image := containerd.NewImageWithPlatform(i.client, img, platformMatcher)

	diffIDs, err := image.RootFS(ctx)
	if err != nil {
		return nil, nil, err
	}
	chainIDs := identity.ChainIDs(diffIDs)

	size, err := image.Size(ctx)
	if err != nil {
		return nil, nil, err
	}
	snapshotter := i.client.SnapshotService(i.snapshotter)
	sizeCache := make(map[digest.Digest]int64)

	snapshotSizeFn := func(d digest.Digest) (int64, error) {
		if s, ok := sizeCache[d]; ok {
			return s, nil
		}
		usage, err := snapshotter.Usage(ctx, d.String())
		if err != nil {
			if cerrdefs.IsNotFound(err) {
				return 0, nil
			}
			return 0, err
		}
		sizeCache[d] = usage.Size
		return usage.Size, nil
	}
	virtualSize, err := computeVirtualSize(chainIDs, snapshotSizeFn)
	if err != nil {
		return nil, nil, err
	}

	var repoTags, repoDigests []string
	target := img.Target.Digest

	logger := logrus.WithFields(logrus.Fields{
		"name":   img.Name,
		"digest": target,
	})

	ref, err := reference.ParseNamed(img.Name)
	if err != nil {
		// If the image has unexpected name format (not a Named reference or a dangling image)
		// add the offending name to RepoTags but also log an error to make it clear to the
		// administrator that this is unexpected.
		// TODO: Reconsider when containerd is more strict on image names, see:
		//       https://github.com/containerd/containerd/issues/7986
		if !isDanglingImage(img) {
			logger.WithError(err).Error("failed to parse image name as reference")
			repoTags = append(repoTags, img.Name)
		}
	} else {
		repoTags = append(repoTags, reference.TagNameOnly(ref).String())

		digested, err := reference.WithDigest(reference.TrimNamed(ref), target)
		if err != nil {
			logger.WithError(err).Error("failed to create digested reference")
		} else {
			repoDigests = append(repoDigests, digested.String())
		}
	}

	summary := &types.ImageSummary{
		RepoDigests: repoDigests, // "hello-world@sha256:bfea6278a0a267fad2634554f4f0c6f31981eea41c553fdf5a83e95a41d40c38"},
		RepoTags:    repoTags,
		Containers:  -1,
		ParentID:    "",
		VirtualSize: virtualSize,
		ID:          image.Target().Digest.String(),
		Created:     image.Metadata().CreatedAt.Unix(),
		Size:        size,
		// 		// -1 indicates that the value has not been set (avoids ambiguity
		// 		// between 0 (default) and "not set". We cannot use a pointer (nil)
		// 		// for this, as the JSON representation uses "omitempty", which would
		// 		// consider both "0" and "nil" to be "empty".
		SharedSize: -1,
	}

	return summary, chainIDs, nil
}

type imageFilterFunc func(image images.Image) bool

// setupFilters constructs an imageFilterFunc from the given imageFilters.
//
// TODO(thaJeztah): reimplement filters using containerd filters: see https://github.com/moby/moby/issues/43845
func (i *ImageService) setupFilters(ctx context.Context, imageFilters filters.Args) ([]string, imageFilterFunc, error) {
	var (
		filters []string
		fltrs   []imageFilterFunc
	)
	err := imageFilters.WalkValues("before", func(value string) error {
		ref, err := reference.ParseDockerRef(value)
		if err != nil {
			return err
		}
		img, err := i.client.GetImage(ctx, ref.String())
		if img != nil {
			t := img.Metadata().CreatedAt
			fltrs = append(fltrs, func(image images.Image) bool {
				created := image.CreatedAt
				return created.Equal(t) || created.After(t)
			})
		}
		return err
	})
	if err != nil {
		return nil, nil, err
	}

	err = imageFilters.WalkValues("since", func(value string) error {
		ref, err := reference.ParseDockerRef(value)
		if err != nil {
			return err
		}
		img, err := i.client.GetImage(ctx, ref.String())
		if img != nil {
			t := img.Metadata().CreatedAt
			fltrs = append(fltrs, func(image images.Image) bool {
				created := image.CreatedAt
				return created.Equal(t) || created.Before(t)
			})
		}
		return err
	})
	if err != nil {
		return nil, nil, err
	}

	err = imageFilters.WalkValues("until", func(value string) error {
		ts, err := timetypes.GetTimestamp(value, time.Now())
		if err != nil {
			return err
		}
		seconds, nanoseconds, err := timetypes.ParseTimestamps(ts, 0)
		if err != nil {
			return err
		}
		until := time.Unix(seconds, nanoseconds)

		fltrs = append(fltrs, func(image images.Image) bool {
			created := image.CreatedAt
			return created.Before(until)
		})
		return err
	})
	if err != nil {
		return nil, nil, err
	}

	if imageFilters.Contains("label") {
		for _, l := range imageFilters.Get("label") {
			filters = append(filters, "label=="+l)
		}
	}

	if imageFilters.Contains("label!") {
		for _, l := range imageFilters.Get("label!") {
			filters = append(filters, "label!="+l)
		}
	}

	if imageFilters.Contains("reference") {
		for _, ref := range imageFilters.Get("reference") {
			filters = append(filters, "name~="+ref)
		}
	}

	return filters, func(image images.Image) bool {
		for _, filter := range fltrs {
			if !filter(image) {
				return false
			}
		}
		return true
	}, nil
}

func computeVirtualSize(chainIDs []digest.Digest, sizeFn func(d digest.Digest) (int64, error)) (int64, error) {
	var virtualSize int64
	for _, chainID := range chainIDs {
		size, err := sizeFn(chainID)
		if err != nil {
			return virtualSize, err
		}
		virtualSize += size
	}
	return virtualSize, nil
}

func computeSharedSize(chainIDs []digest.Digest, layers map[digest.Digest]int, sizeFn func(d digest.Digest) (int64, error)) (int64, error) {
	var sharedSize int64
	for _, chainID := range chainIDs {
		if layers[chainID] == 1 {
			continue
		}
		size, err := sizeFn(chainID)
		if err != nil {
			return 0, err
		}
		sharedSize += size
	}
	return sharedSize, nil
}
