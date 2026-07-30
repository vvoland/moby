package drivers

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/moby/moby/v2/daemon/volume"
	volumedriverv0 "github.com/moby/moby/v2/extpoints/volumedriver/v0"
	"github.com/moby/moby/v2/internal/extensions"
)

// RegisterExtensions registers every volume driver provided as an extension.
//
// The extension set is fixed when the daemon starts, so drivers are resolved
// once here rather than looked up per operation, and each is registered under
// the name it reports -- which is what `docker volume create -d <name>` selects.
// From that point a driver provided by an extension is indistinguishable from
// any other: the same [Store] lookup, the same adapters, the same volume.Driver.
//
// A driver that cannot report its capabilities is refused. Capabilities is the
// call that says whether a driver is usable at all, so failing it means the
// driver would be registered under a name it might not be able to serve, and
// loading is all-or-nothing.
func (s *Store) RegisterExtensions(ctx context.Context, r extensions.Resolver) error {
	providers, err := volumedriverv0.Point.All(r)
	if err != nil {
		return err
	}
	for _, p := range providers {
		caps, err := p.Impl.Capabilities(ctx, &volumedriverv0.CapabilitiesRequest{})
		if err != nil {
			return fmt.Errorf("volume driver extension %q: capabilities: %w", p.Extension, err)
		}
		if caps.Name == "" {
			return fmt.Errorf("volume driver extension %q reported no driver name", p.Extension)
		}
		d := &volumeDriverAdapter{
			name:         caps.Name,
			scopePath:    func(s string) string { return s },
			capabilities: &volume.Capability{Scope: caps.Scope},
			proxy:        &extensionProxy{ctx: ctx, driver: p.Impl},
		}
		if err := validateDriver(d); err != nil {
			return fmt.Errorf("volume driver extension %q: %w", p.Extension, err)
		}
		if !s.Register(d, caps.Name) {
			return fmt.Errorf("volume driver extension %q: driver name %q is already registered", p.Extension, caps.Name)
		}
	}
	return nil
}

// extensionProxy presents a volume driver extension as the flat driver
// interface the existing adapters speak, so an extension-provided driver and a
// legacy plugin reach exactly the same code below this line.
//
// The flat interface predates contexts, so the daemon's lifetime context is
// carried here. It is the same context the driver was registered with, which
// bounds a call to the daemon running rather than to any one request.
type extensionProxy struct {
	ctx    context.Context
	driver volumedriverv0.Driver
}

func (p *extensionProxy) Create(name string, opts map[string]string) error {
	return p.driver.Create(p.ctx, &volumedriverv0.CreateRequest{Name: name, Options: opts})
}

func (p *extensionProxy) Remove(name string) error {
	return p.driver.Remove(p.ctx, &volumedriverv0.NameRequest{Name: name})
}

func (p *extensionProxy) Path(name string) (string, error) {
	resp, err := p.driver.Path(p.ctx, &volumedriverv0.NameRequest{Name: name})
	if err != nil {
		return "", err
	}
	return resp.Mountpoint, nil
}

func (p *extensionProxy) Mount(name, id string) (string, error) {
	resp, err := p.driver.Mount(p.ctx, &volumedriverv0.MountRequest{Name: name, Ref: id})
	if err != nil {
		return "", err
	}
	return resp.Mountpoint, nil
}

func (p *extensionProxy) Unmount(name, id string) error {
	return p.driver.Unmount(p.ctx, &volumedriverv0.MountRequest{Name: name, Ref: id})
}

func (p *extensionProxy) List() ([]*proxyVolume, error) {
	resp, err := p.driver.List(p.ctx, &volumedriverv0.ListRequest{})
	if err != nil {
		return nil, err
	}
	out := make([]*proxyVolume, 0, len(resp.Volumes))
	for i := range resp.Volumes {
		v, err := toProxyVolume(&resp.Volumes[i])
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

func (p *extensionProxy) Get(name string) (*proxyVolume, error) {
	resp, err := p.driver.Get(p.ctx, &volumedriverv0.NameRequest{Name: name})
	if err != nil {
		return nil, err
	}
	if resp.Volume == nil {
		return nil, errNoSuchVolume
	}
	return toProxyVolume(resp.Volume)
}

func (p *extensionProxy) Capabilities() (volume.Capability, error) {
	resp, err := p.driver.Capabilities(p.ctx, &volumedriverv0.CapabilitiesRequest{})
	if err != nil {
		return volume.Capability{}, err
	}
	return volume.Capability{Scope: resp.Scope}, nil
}

// toProxyVolume converts a volume as the point reports it into the form the
// existing adapters expect.
//
// The point carries CreatedAt as RFC 3339 text and Status as JSON, so a driver
// written in another language needs no well-known types and no schema for
// driver-defined detail. Both are decoded here, at the edge.
func toProxyVolume(v *volumedriverv0.Volume) (*proxyVolume, error) {
	out := &proxyVolume{Name: v.Name, Mountpoint: v.Mountpoint}
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
