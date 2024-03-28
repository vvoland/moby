package containerd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/containerd/containerd/content"
	cerrdefs "github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/images"
	"github.com/containerd/containerd/platforms"
	"github.com/containerd/log"
	"github.com/distribution/reference"
	imagetypes "github.com/docker/docker/api/types/image"
	"github.com/docker/docker/errdefs"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

func (i *ImageService) ImageConvert(ctx context.Context, src string, dst reference.NamedTagged, opts imagetypes.ConvertOptions) error {
	log.G(ctx).Debugf("converting: %+v", opts)

	srcImg, err := i.resolveImage(ctx, src)
	if err != nil {
		return err
	}

	if _, err := i.images.Get(ctx, dst.String()); !cerrdefs.IsNotFound(err) {
		err := i.images.Delete(ctx, dst.String(), images.SynchronousDelete())
		if err != nil {
			return errdefs.System(fmt.Errorf("failed to delete existing image: %w", err))
		}
	}

	if opts.OnlyAvailablePlatforms && len(opts.Platforms) > 0 {
		return errdefs.InvalidParameter(errors.New("specifying both explicit platform list and only-available-platforms is not allowed"))
	}

	srcMediaType := srcImg.Target.MediaType
	if !images.IsIndexType(srcMediaType) {
		return errdefs.InvalidParameter(errors.New("cannot convert non-index image"))
	}

	oldIndex, info, err := readIndex(ctx, i.content, srcImg.Target)
	if err != nil {
		return err
	}

	newImg := srcImg
	newImg.Name = dst.String()

	n, err := i.convertManifests(ctx, srcImg, opts)
	if err != nil {
		return err
	}
	if n != nil {
		newIndex := oldIndex
		newIndex.Manifests = n
		target, err := storeJson(ctx, i.content, newIndex.MediaType, newIndex, info.Labels)
		if err != nil {
			return errdefs.System(fmt.Errorf("failed to write modified image target: %w", err))
		}

		newImg.Target = target
		newImg.CreatedAt = time.Now()
		newImg.UpdatedAt = newImg.CreatedAt
	}

	if _, err := i.images.Create(ctx, newImg); err != nil {
		return errdefs.System(fmt.Errorf("failed to create image: %w", err))
	}
	return nil
}

func (i *ImageService) convertManifests(ctx context.Context, srcImg images.Image, opts imagetypes.ConvertOptions) ([]ocispec.Descriptor, error) {
	changed := false
	pm := platforms.All
	if len(opts.Platforms) > 0 {
		pm = platforms.Any(opts.Platforms...)
	}

	var newManifests []ocispec.Descriptor
	walker := i.walkReachableImageManifests
	if opts.OnlyAvailablePlatforms {
		walker = i.walkImageManifests
		changed = true
	}

	err := walker(ctx, srcImg, func(m *ImageManifest) error {
		if opts.NoAttestations && m.IsAttestation() {
			return nil
		}

		mtarget := m.Target()
		mplatform, err := m.ImagePlatform(ctx)
		if err != nil {
			return err
		}
		if !pm.Match(mplatform) {
			changed = true
			log.G(ctx).WithFields(log.Fields{
				"platform": mplatform,
				"digest":   mtarget.Digest,
			}).Debugf("skipping manifest %s due to platform mismatch", mtarget.Digest)
			return images.ErrSkipDesc
		}

		newManifests = append(newManifests, mtarget)

		return nil
	})
	if err != nil {
		return nil, err
	}

	if changed {
		return newManifests, nil
	}

	return nil, nil
}

func diffObj(old, new any) map[string]any {
	oldBytes, _ := json.Marshal(old)
	newBytes, _ := json.Marshal(new)

	oldMap := make(map[string]any)
	newMap := make(map[string]any)

	_ = json.Unmarshal(oldBytes, &newMap)
	_ = json.Unmarshal(newBytes, &oldMap)

	diff := map[string]any{}
	for k, v := range oldMap {
		if reflect.DeepEqual(newMap[k], v) {
			diff[k] = v
		}
	}
	for k, v := range newMap {
		if reflect.DeepEqual(oldMap[k], v) {
			diff[k] = v
		}
	}

	return diff
}

func readIndex(ctx context.Context, store content.InfoReaderProvider, desc ocispec.Descriptor) (ocispec.Index, content.Info, error) {
	info, err := store.Info(ctx, desc.Digest)
	if err != nil {
		return ocispec.Index{}, content.Info{}, err
	}

	p, err := content.ReadBlob(ctx, store, desc)
	if err != nil {
		return ocispec.Index{}, info, err
	}

	var idx ocispec.Index
	if err := json.Unmarshal(p, &idx); err != nil {
		return ocispec.Index{}, info, err
	}

	return idx, info, nil
}
