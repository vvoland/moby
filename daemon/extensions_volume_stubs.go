//go:build !linux && !windows

package daemon

import (
	"github.com/moby/moby/v2/daemon/internal/idtools"
	"github.com/moby/moby/v2/internal/extensions"
)

// defaultVolumeExtensions is empty on platforms with no local volume driver.
func defaultVolumeExtensions(string, idtools.Identity) []extensions.Extension { return nil }
