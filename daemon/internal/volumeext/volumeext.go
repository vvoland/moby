// Package volumeext bridges volume drivers provided as extensions to the volume
// driver store.
//
// It is internal to the daemon because it is the only place the two vocabularies
// meet: extension points on one side, the volume service's driver contract on
// the other. Keeping the join here means neither public package has to name the
// other's types -- daemon/volume/drivers stays free of extension types, and the
// point stays free of volume-service types.
package volumeext

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/moby/moby/v2/daemon/volume"
	"github.com/moby/moby/v2/daemon/volume/drivers"
	"github.com/moby/moby/v2/internal/extensions"
	volumedriverv0 "github.com/moby/moby/v2/internal/extpoints/volumedriver/v0"
)

// Drivers returns the volume drivers provided as extensions, ready to register.
//
// The extension set is fixed when the daemon starts, so drivers are resolved
// once here rather than looked up per operation, and each is named by what it
// reports -- which is what `docker volume create -d <name>` selects.
//
// A driver that cannot report its capabilities is refused. Capabilities is the
// call that says whether a driver is usable at all, so failing it would mean
// registering a driver under a name it might not be able to serve, and loading
// is all-or-nothing.
func Drivers(ctx context.Context, r extensions.Resolver) ([]volume.Driver, error) {
	providers, err := volumedriverv0.Point.All(r)
	if err != nil {
		return nil, err
	}
	out := make([]volume.Driver, 0, len(providers))
	for _, p := range providers {
		caps, err := p.Impl.Capabilities(ctx, &volumedriverv0.CapabilitiesRequest{})
		if err != nil {
			return nil, fmt.Errorf("volume driver extension %q: capabilities: %w", p.Extension, err)
		}
		if caps.Name == "" {
			return nil, fmt.Errorf("volume driver extension %q reported no driver name", p.Extension)
		}
		out = append(out, drivers.NewRemoteDriver(caps.Name, caps.Scope, &pointDriver{ctx: ctx, driver: p.Impl}))
	}
	return out, nil
}

// pointDriver presents a volume driver point provider as the flat driver
// contract the volume service consumes, so an extension-provided driver and a
// legacy plugin reach exactly the same code below this line.
//
// The flat contract predates contexts, so the daemon's lifetime context is
// carried here. It bounds a call to the daemon running rather than to any one
// request.
type pointDriver struct {
	ctx    context.Context
	driver volumedriverv0.Driver
}

func (p *pointDriver) Create(name string, options map[string]string) error {
	return p.driver.Create(p.ctx, &volumedriverv0.CreateRequest{Name: name, Options: options})
}

func (p *pointDriver) Remove(name string) error {
	return p.driver.Remove(p.ctx, &volumedriverv0.NameRequest{Name: name})
}

func (p *pointDriver) Path(name string) (string, error) {
	resp, err := p.driver.Path(p.ctx, &volumedriverv0.NameRequest{Name: name})
	if err != nil {
		return "", err
	}
	return resp.Mountpoint, nil
}

func (p *pointDriver) Mount(name, ref string) (string, error) {
	resp, err := p.driver.Mount(p.ctx, &volumedriverv0.MountRequest{Name: name, Ref: ref})
	if err != nil {
		return "", err
	}
	return resp.Mountpoint, nil
}

func (p *pointDriver) Unmount(name, ref string) error {
	return p.driver.Unmount(p.ctx, &volumedriverv0.MountRequest{Name: name, Ref: ref})
}

func (p *pointDriver) LiveRestore(name, ref string) error {
	return p.driver.LiveRestore(p.ctx, &volumedriverv0.MountRequest{Name: name, Ref: ref})
}

func (p *pointDriver) List() ([]drivers.VolumeInfo, error) {
	resp, err := p.driver.List(p.ctx, &volumedriverv0.ListRequest{})
	if err != nil {
		return nil, err
	}
	out := make([]drivers.VolumeInfo, 0, len(resp.Volumes))
	for i := range resp.Volumes {
		v, err := toVolumeInfo(&resp.Volumes[i])
		if err != nil {
			return nil, err
		}
		out = append(out, *v)
	}
	return out, nil
}

func (p *pointDriver) Get(name string) (*drivers.VolumeInfo, error) {
	resp, err := p.driver.Get(p.ctx, &volumedriverv0.NameRequest{Name: name})
	if err != nil {
		return nil, err
	}
	if resp.Volume == nil {
		return nil, nil
	}
	return toVolumeInfo(resp.Volume)
}

func (p *pointDriver) Capabilities() (volume.Capability, error) {
	resp, err := p.driver.Capabilities(p.ctx, &volumedriverv0.CapabilitiesRequest{})
	if err != nil {
		return volume.Capability{}, err
	}
	return volume.Capability{Scope: resp.Scope}, nil
}

// toVolumeInfo converts a volume as the point reports it.
//
// The point carries CreatedAt as RFC 3339 text and Status as JSON, so a driver
// written in another language needs neither well-known types nor a schema for
// detail that is not the engine's to model. Both are decoded here, at the edge.
func toVolumeInfo(v *volumedriverv0.Volume) (*drivers.VolumeInfo, error) {
	out := &drivers.VolumeInfo{Name: v.Name, Mountpoint: v.Mountpoint}
	if v.CreatedAt != "" {
		t, err := time.Parse(time.RFC3339, v.CreatedAt)
		if err != nil {
			return nil, fmt.Errorf("volume %q: parse created_at: %w", v.Name, err)
		}
		out.CreatedAt = t
	}
	if len(v.Status) > 0 {
		if err := json.Unmarshal(v.Status, &out.Status); err != nil {
			return nil, fmt.Errorf("volume %q: parse status: %w", v.Name, err)
		}
	}
	return out, nil
}
