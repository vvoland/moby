package containerdstore

import (
	"context"
	"fmt"
	"sync"

	containerdimages "github.com/containerd/containerd/images"
	"github.com/containerd/containerd/log"
	"github.com/containerd/containerd/platforms"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/filters"
	"github.com/pkg/errors"
)

type PruneImageResult struct {
	Error error
	ID    string
	Size  uint64
}

func (cs *containerdStore) ImagesPrune(ctx context.Context, pruneFilters filters.Args) (*types.ImagesPruneReport, error) {
	is := cs.client.ImageService()
	images, err := is.List(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "Failed to list images")
	}

	// Delete all images synchronously and post result
	results := make(chan PruneImageResult, len(images))
	wg := &sync.WaitGroup{}
	wg.Add(len(images))
	store := cs.client.ContentStore()
	for _, img := range images {
		img := img
		go func() {
			size, err := img.Size(ctx, store, platforms.DefaultStrict())
			if err != nil {
				log.G(ctx).WithError(err).WithField("image", img.Name).Error("Failed to calculate image size")
				size = 0
			}
			err = is.Delete(ctx, img.Name, containerdimages.SynchronousDelete())

			results <- PruneImageResult{
				Error: err,
				ID:    img.Target.Digest.String(),
				Size:  uint64(size),
			}
			wg.Done()
		}()
	}

	// Close channel when all deletions completed
	go func() {
		wg.Wait()
		close(results)
	}()

	// Prepare final report from individual results
	report := types.ImagesPruneReport{}
	errorString := ""
	for r := range results {
		if r.Error == nil {
			report.SpaceReclaimed += r.Size
			report.ImagesDeleted = append(report.ImagesDeleted,
				types.ImageDeleteResponseItem{
					Deleted: r.ID,
				},
			)
		} else {
			if errorString != "" {
				errorString += "\n"
			}
			errorString += fmt.Sprintf("Failed to delete image %s: %v", r.ID, r.Error)
		}
	}

	if errorString != "" {
		return &report, errors.New(errorString)
	}
	return &report, nil
}
