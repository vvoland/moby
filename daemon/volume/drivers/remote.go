package drivers

import (
	"time"

	"github.com/moby/moby/v2/daemon/volume"
)

// VolumeInfo is a volume as its driver reports it.
type VolumeInfo struct {
	Name       string
	Mountpoint string
	CreatedAt  time.Time
	Status     map[string]any
}

// RemoteDriver is the flat, one-call-one-answer volume driver contract.
//
// It is the shape a volume driver has always had over the wire -- names rather
// than handles, so nothing needs to be kept alive between calls -- and it is
// what both the legacy plugin protocol and the volume driver extension point
// speak. Implementations reach the volume service through [NewRemoteDriver].
//
// It is declared in terms of ordinary types so this package's exported API does
// not depend on how a particular driver is delivered.
type RemoteDriver interface {
	Create(name string, options map[string]string) error
	Remove(name string) error
	Path(name string) (string, error)
	Mount(name, ref string) (string, error)
	Unmount(name, ref string) error
	List() ([]VolumeInfo, error)
	Get(name string) (*VolumeInfo, error)
	Capabilities() (volume.Capability, error)
}

// LiveRestorer is the optional part of [RemoteDriver] for a driver that keeps
// per-volume state -- a mount reference count, say -- which has to be rebuilt
// for containers that outlived the daemon.
type LiveRestorer interface {
	LiveRestore(name, ref string) error
}

// NewRemoteDriver adapts a flat driver to the volume service's driver
// interface, under the given name and scope.
//
// It is the same adaptation the legacy plugin path uses, so a driver reaching
// the volume service this way is indistinguishable from a plugin below this
// point -- which is the property that lets a driver move between the daemon and
// a separate process without the volume service noticing.
func NewRemoteDriver(name, scope string, d RemoteDriver) volume.Driver {
	return &volumeDriverAdapter{
		name:         name,
		scopePath:    func(s string) string { return s },
		capabilities: &volume.Capability{Scope: scope},
		proxy:        &remoteProxy{RemoteDriver: d},
	}
}

// remoteProxy presents a [RemoteDriver] as the internal driver interface the
// adapters speak. The two differ only in the volume type they carry.
type remoteProxy struct {
	RemoteDriver
}

func (p *remoteProxy) List() ([]*proxyVolume, error) {
	vols, err := p.RemoteDriver.List()
	if err != nil {
		return nil, err
	}
	out := make([]*proxyVolume, 0, len(vols))
	for i := range vols {
		out = append(out, toProxyVolume(&vols[i]))
	}
	return out, nil
}

func (p *remoteProxy) Get(name string) (*proxyVolume, error) {
	v, err := p.RemoteDriver.Get(name)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, errNoSuchVolume
	}
	return toProxyVolume(v), nil
}

// LiveRestore forwards to the driver when it keeps restorable state, and is
// otherwise a no-op, so [volumeAdapter.LiveRestoreVolume] does not have to know
// which kind of driver is behind it.
func (p *remoteProxy) LiveRestore(name, ref string) error {
	lr, ok := p.RemoteDriver.(LiveRestorer)
	if !ok {
		return nil
	}
	return lr.LiveRestore(name, ref)
}

func toProxyVolume(v *VolumeInfo) *proxyVolume {
	return &proxyVolume{
		Name:       v.Name,
		Mountpoint: v.Mountpoint,
		CreatedAt:  v.CreatedAt,
		Status:     v.Status,
	}
}
