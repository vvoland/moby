package platform

import (
	"strings"

	"github.com/containerd/containerd/platforms"
)

func ParsePlatform(platform string) (platforms.MatchComparer, error) {
	multiple := strings.Split(platform, ",")
	if len(multiple) > 1 {
	}
	return nil, nil
}
