package containerd

import (
	"context"
	"errors"

	"github.com/docker/docker/container"
	"github.com/docker/docker/pkg/archive"
)

func (i *ImageService) Changes(ctx context.Context, container *container.Container) ([]archive.Change, error) {
	return nil, errors.New("not implemented (yet)")
}
