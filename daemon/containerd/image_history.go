package containerd

import (
	"context"
	"errors"

	imagetype "github.com/docker/docker/api/types/image"
)

// ImageHistory returns a slice of ImageHistory structures for the specified
// image name by walking the image lineage.
func (i *ImageService) ImageHistory(ctx context.Context, name string) ([]*imagetype.HistoryResponseItem, error) {
	return nil, errors.New("not implemented")
}
