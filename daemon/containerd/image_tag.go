package containerd

import (
	"context"

	cerrdefs "github.com/containerd/containerd/errdefs"
	containerdimages "github.com/containerd/containerd/images"
	"github.com/docker/distribution/reference"
	"github.com/docker/docker/errdefs"
	"github.com/docker/docker/image"
	"github.com/sirupsen/logrus"
)

// TagImage creates the tag specified by newTag, pointing to the image named
// imageName (alternatively, imageName can also be an image ID).
func (i *ImageService) TagImage(ctx context.Context, imageName, repository, tag string) (string, error) {
	img, err := i.resolveImage(ctx, imageName, nil)
	if err != nil {
		return "", err
	}

	newTag, err := reference.ParseNormalizedNamed(repository)
	if err != nil {
		return "", err
	}
	if tag != "" {
		if newTag, err = reference.WithTag(reference.TrimNamed(newTag), tag); err != nil {
			return "", err
		}
	}

	err = i.TagImageWithReference(ctx, image.ID(img.Target.Digest), newTag)
	return reference.FamiliarString(newTag), err
}

// TagImageWithReference adds the given reference to the image ID provided.
func (i *ImageService) TagImageWithReference(ctx context.Context, imageID image.ID, newTag reference.Named) error {
	logger := logrus.WithFields(logrus.Fields{
		"imageID": imageID.String(),
		"tag":     newTag.String(),
	})

	cimg, err := i.GetContainerdImage(ctx, imageID.String(), nil)
	if err != nil {
		logger.WithError(err).Debug("failed to resolve image id to a descriptor")
		return err
	}

	img := containerdimages.Image{
		Name:   newTag.String(),
		Target: cimg.Target,
	}

	is := i.client.ImageService()
	_, err = is.Create(ctx, img)
	if err != nil {
		if !cerrdefs.IsAlreadyExists(err) {
			logger.WithError(err).Error("failed to create image")
			return errdefs.System(err)
		}

		// If there already exists an image with this tag, delete it
		err := is.Delete(ctx, img.Name)

		if err != nil {
			logger.WithError(err).Error("failed to delete old image")
			return errdefs.System(err)
		}

		_, err = is.Create(ctx, img)
		if err != nil {
			logger.WithError(err).Error("failed to create an image after deleting old image")
			return errdefs.System(err)
		}
	}

	if err == nil {
		if isDanglingImage(cimg) {
			delErr := is.Delete(ctx, cimg.Name)
			if delErr != nil {
				logrus.WithField("name", cimg.Name).Debug("failed to remove dangling image")
			}
		}
	}

	logger.Info("image created")
	return nil
}
