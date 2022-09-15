package containerd

import (
	"context"
	"strings"

	"github.com/containerd/containerd"
	"github.com/containerd/containerd/mount"
	"github.com/containerd/containerd/platforms"
	"github.com/docker/docker/container"
	"github.com/docker/docker/pkg/archive"
	"github.com/google/uuid"
	"github.com/opencontainers/image-spec/identity"
)

func (i *ImageService) Changes(ctx context.Context, container *container.Container) ([]archive.Change, error) {
	snapshotter := i.client.SnapshotService(i.snapshotter)
	mounts, err := snapshotter.Mounts(ctx, container.ID)
	if err != nil {
		return nil, err
	}

	cimg, _, err := i.getImage(ctx, container.Config.Image)
	if err != nil {
		return nil, err
	}
	baseImgWithoutPlatform, err := i.client.ImageService().Get(ctx, cimg.Name())
	if err != nil {
		return nil, err
	}
	baseImg := containerd.NewImageWithPlatform(i.client, baseImgWithoutPlatform, platforms.DefaultStrict())
	diffIDs, err := baseImg.RootFS(ctx)
	rnd, err := uuid.NewRandom()
	if err != nil {
		return nil, err
	}
	parent, err := snapshotter.View(ctx, rnd.String(), identity.ChainID(diffIDs).String())
	if err != nil {
		return nil, err
	}
	defer snapshotter.Remove(ctx, rnd.String())

	var changes []archive.Change
	err = mount.WithTempMount(ctx, readOnly(mounts), func(fs string) error {
		return mount.WithTempMount(ctx, parent, func(root string) error {
			changes, err = archive.ChangesDirs(fs, root)
			return err
		})
		return err
	})
	return changes, err
}

func readOnly(mounts []mount.Mount) []mount.Mount {
	for i, m := range mounts {
		if m.Type == "overlay" {
			opts := make([]string, 0, len(m.Options))
			upper := ""
			for _, o := range m.Options {
				if strings.HasPrefix(o, "upperdir=") {
					upper = strings.TrimPrefix(o, "upperdir=")
				} else if !strings.HasPrefix(o, "workdir=") {
					opts = append(opts, o)
				}
			}
			if upper != "" {
				for i, o := range opts {
					if strings.HasPrefix(o, "lowerdir=") {
						opts[i] = "lowerdir=" + upper + ":" + strings.TrimPrefix(o, "lowerdir=")
					}
				}
			}
			mounts[i].Options = opts
			continue
		}
		opts := make([]string, 0, len(m.Options))
		for _, opt := range m.Options {
			if opt != "rw" {
				opts = append(opts, opt)
			}
		}
		opts = append(opts, "ro")
		mounts[i].Options = opts
	}
	return mounts
}
