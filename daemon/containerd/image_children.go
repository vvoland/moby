package containerd

import (
	"context"

	"github.com/containerd/containerd/content"
	cerrdefs "github.com/containerd/containerd/errdefs"
	containerdimages "github.com/containerd/containerd/images"
	"github.com/containerd/containerd/platforms"
	"github.com/docker/docker/image"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/sirupsen/logrus"
)

// Children returns the children image.IDs for a parent image.
// called from list.go to filter containers
func (i *ImageService) Children(id image.ID) []image.ID {
	ctx := context.Background()

	parentImg, _, err := i.getImage(ctx, id.String(), nil)
	if err != nil {
		logrus.WithError(err).Error("failed to get parent image")
		return []image.ID{}
	}

	log := logrus.WithField("id", id.String())

	allPlatforms, err := containerdimages.Platforms(ctx, parentImg.ContentStore(), parentImg.Target())
	if err != nil {
		log.WithError(err).Error("failed to list supported platorms of image")
		return []image.ID{}
	}

	is := i.client.ImageService()
	cs := i.client.ContentStore()

	imgs, err := is.List(ctx)
	if err != nil {
		log.WithError(err).Error("failed to list images")
		return []image.ID{}
	}

	parentRootFS := []ocispec.RootFS{}
	for _, platform := range allPlatforms {
		rootfs, err := platformRootfs(ctx, cs, parentImg.Target(), platform)
		if err != nil {
			continue
		}

		parentRootFS = append(parentRootFS, rootfs)
	}

	children := []image.ID{}
	for _, img := range imgs {
	nextImage:
		for _, platform := range allPlatforms {
			rootfs, err := platformRootfs(ctx, cs, parentImg.Target(), platform)
			if err != nil {
				continue
			}

			for _, parentRoot := range parentRootFS {
				if isRootfsChildOf(rootfs, parentRoot) {
					children = append(children, image.ID(img.Target.Digest))
					break nextImage
				}
			}
		}

	}

	return children
}

func platformRootfs(ctx context.Context, store content.Store, desc ocispec.Descriptor, platform ocispec.Platform) (ocispec.RootFS, error) {
	empty := ocispec.RootFS{}

	log := logrus.WithField("desc", desc.Digest).WithField("platform", platforms.Format(platform))
	configDesc, err := containerdimages.Config(ctx, store, desc, platforms.OnlyStrict(platform))
	if err != nil {
		if !cerrdefs.IsNotFound(err) {
			log.WithError(err).Warning("failed to get parent image config")
		}
		return empty, err
	}

	log = log.WithField("configDesc", configDesc)
	diffs, err := containerdimages.RootFS(ctx, store, configDesc)
	if err != nil {
		if !cerrdefs.IsNotFound(err) {
			log.WithError(err).Warning("failed to get parent image rootfs")
		}
		return empty, err
	}

	return ocispec.RootFS{
		Type:    "layers",
		DiffIDs: diffs,
	}, nil
}

func isRootfsChildOf(child ocispec.RootFS, parent ocispec.RootFS) bool {
	childLen := len(child.DiffIDs)
	parentLen := len(parent.DiffIDs)

	if childLen <= parentLen {
		return false
	}

	for i := 0; i < len(parent.DiffIDs); i++ {
		if child.DiffIDs[i] != parent.DiffIDs[i] {
			return false
		}
	}

	return true
}
