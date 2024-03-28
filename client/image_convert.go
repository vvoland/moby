package client // import "github.com/docker/docker/client"

import (
	"context"
	"net/url"

	"github.com/containerd/containerd/platforms"
	"github.com/distribution/reference"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/errdefs"
	"github.com/pkg/errors"
)

// ImageList converts image.
func (cli *Client) ImageConvert(ctx context.Context, src string, dst reference.NamedTagged, options image.ConvertOptions) error {
	query := url.Values{}

	if options.OnlyAvailablePlatforms && len(options.Platforms) > 0 {
		return errdefs.InvalidParameter(errors.New("specifying both explicit platform list and only-available-platforms is not allowed"))
	}

	if options.OnlyAvailablePlatforms {
		query.Set("only-available-platforms", "1")
	}

	if len(options.Platforms) > 0 {
		for _, p := range options.Platforms {
			query.Add("platforms", platforms.Format(p))
		}
	}

	if options.NoAttestations {
		query.Set("no-attestations", "1")
	}

	query.Set("from", src)
	query.Set("to", dst.String())

	serverResp, err := cli.post(ctx, "/images/convert", query, nil, nil)
	ensureReaderClosed(serverResp)

	return err
}
