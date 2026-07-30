//go:build linux || windows

package daemon

import (
	"github.com/moby/moby/v2/daemon/internal/extproviders/volumelocal"
	"github.com/moby/moby/v2/daemon/internal/idtools"
	"github.com/moby/moby/v2/internal/extensions"
)

// defaultVolumeExtensions is the built-in local volume driver, which exists
// only on platforms that can host local volumes.
//
// Selecting it with a build tag here replaces the build-tagged setup function
// the volume service used to call. Composing a build by choosing which modules
// go in the list is the same thing containerd does by choosing which packages
// to import, without the import having a side effect.
func defaultVolumeExtensions(root string, rootIdentity idtools.Identity) []extensions.Extension {
	return []extensions.Extension{volumelocal.Extension(root, rootIdentity)}
}
