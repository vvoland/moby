package containerd

import (
	"context"
	"fmt"

	"github.com/containerd/containerd"
	"github.com/containerd/containerd/mount"
	"github.com/containerd/containerd/platforms"
	"github.com/docker/docker/container"
	"github.com/docker/docker/oci"
	"github.com/docker/docker/pkg/archive"
	"github.com/google/uuid"
	"github.com/opencontainers/image-spec/identity"
)

func (i *ImageService) Changes(ctx context.Context, container *container.Container) (changes []archive.Change, err error) {
	snapshotter := i.client.SnapshotService(i.snapshotter)
	mounts, uerr := snapshotter.Mounts(ctx, container.ID)
	if err != nil {
		return nil, uerr
	}

	cimg, _, uerr := i.getImage(ctx, container.Config.Image)
	if uerr != nil {
		return nil, uerr
	}
	baseImgWithoutPlatform, uerr := i.client.ImageService().Get(ctx, cimg.Name())
	if uerr != nil {
		return nil, uerr
	}
	baseImg := containerd.NewImageWithPlatform(i.client, baseImgWithoutPlatform, platforms.DefaultStrict())
	diffIDs, uerr := baseImg.RootFS(ctx)
	if uerr != nil {
		return nil, uerr
	}
	rnd, uerr := uuid.NewRandom()
	if uerr != nil {
		return nil, uerr
	}
	parent, uerr := snapshotter.View(ctx, rnd.String(), identity.ChainID(diffIDs).String())
	if uerr != nil {
		return nil, uerr
	}
	defer func() {
		uerr = snapshotter.Remove(ctx, rnd.String())
		if err == nil {
			err = uerr
		} else {
			err = fmt.Errorf("%s: %w", uerr.Error(), err)
		}
	}()

	err = mount.WithTempMount(ctx, oci.ReadonlyMounts(mounts), func(fs string) error {
		return mount.WithTempMount(ctx, parent, func(root string) error {
			changes, err = archive.ChangesDirs(fs, root)
			return err
		})
	})

	return changes, err
}
