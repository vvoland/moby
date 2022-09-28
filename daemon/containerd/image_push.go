package containerd

import (
	"context"
	"io"

	containerdimages "github.com/containerd/containerd/images"
	"github.com/containerd/containerd/images/converter"
	"github.com/containerd/containerd/platforms"
	"github.com/containerd/containerd/remotes"
	"github.com/docker/distribution/reference"
	"github.com/docker/docker/api/types/registry"
	"github.com/docker/docker/pkg/progress"
	"github.com/docker/docker/pkg/streamformatter"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// PushImage initiates a push operation on the repository named localName.
func (i *ImageService) PushImage(ctx context.Context, image, tag string, metaHeaders map[string][]string, authConfig *registry.AuthConfig, outStream io.Writer) (outerr error) {
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

	// If user requested specific platforms to push, then create a manifest
	// list with only the matching platforms.
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

	resolver, tracker := i.newResolverFromAuthConfig(authConfig)

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

	out := streamformatter.NewJSONProgressOutput(outStream, false)
	finishProgress := showProgress(ctx, jobs, out, combineProgress(pushProgress(tracker), pullProgress(store, false)))
	defer func() {
		finishProgress()
		if outerr == nil {
			progress.Messagef(out, "", "%s: digest: %s, size: %d", tag, target.Digest.String(), target.Size)
		}
	}()

	pusher := newLazyPusher(store, resolver, jobs, i.downloadLimiter, i.uploadLimiter)

	leasedCtx, release, err := i.client.WithLease(ctx)
	if err != nil {
		return err
	}
	defer release(leasedCtx)

	return pusher.push(leasedCtx, ref, target, imageHandler)
}
