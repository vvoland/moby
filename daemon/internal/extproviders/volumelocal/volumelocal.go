// Package volumelocal provides Moby's built-in local volume driver as an
// extension.
//
// It is the daemon's own code, registered the same way a third-party extension
// is. That is the point: the local driver was previously wired into the volume
// service by a dedicated setup function that knew its concrete type, and it is
// now one provider of the volume driver point among however many are loaded.
// Nothing about the driver had to change -- this package is an adapter over
// [local.Root], not a rewrite -- and nothing downstream of the driver store can
// tell it apart from a driver shipped as a separate binary.
package volumelocal

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/moby/moby/v2/daemon/internal/idtools"
	"github.com/moby/moby/v2/daemon/volume"
	"github.com/moby/moby/v2/daemon/volume/local"
	"github.com/moby/moby/v2/internal/extensions"
	volumedriverv0 "github.com/moby/moby/v2/internal/extpoints/volumedriver/v0"
)

// ID is the extension id of the built-in local volume driver.
const ID extensions.ExtensionID = "org.mobyproject.volume-local.v1"

// DriverName is the name users select with `docker volume create -d local`. It
// is part of the user-facing API and predates extensions, so it stays a short
// word rather than following the extension id.
const DriverName = "local"

// Extension returns the built-in local volume driver as an extension rooted at
// scope, owned by rootIdentity.
//
// The driver is constructed in Init rather than here, so a failure to create
// the volume root is reported through the extension lifecycle -- and fails
// daemon startup -- rather than at the first volume operation.
func Extension(scope string, rootIdentity idtools.Identity) extensions.Extension {
	d := &driver{scope: scope, rootIdentity: rootIdentity}
	return extensions.New(extensions.Declaration{
		ID:        ID,
		Providers: []extensions.Provider{volumedriverv0.Point.Provide(d)},
		Init:      d.init,
	})
}

// driver adapts [local.Root] to the volume driver point.
type driver struct {
	scope        string
	rootIdentity idtools.Identity
	root         *local.Root
}

func (d *driver) init(context.Context, extensions.Config) error {
	root, err := local.New(d.scope, d.rootIdentity)
	if err != nil {
		return fmt.Errorf("create local volume root: %w", err)
	}
	d.root = root
	return nil
}

func (d *driver) Capabilities(context.Context, *volumedriverv0.CapabilitiesRequest) (*volumedriverv0.CapabilitiesResponse, error) {
	return &volumedriverv0.CapabilitiesResponse{Name: DriverName, Scope: d.root.Scope()}, nil
}

func (d *driver) Create(_ context.Context, req *volumedriverv0.CreateRequest) error {
	_, err := d.root.Create(req.Name, req.Options)
	return err
}

func (d *driver) Remove(_ context.Context, req *volumedriverv0.NameRequest) error {
	v, err := d.root.Get(req.Name)
	if err != nil {
		return err
	}
	return d.root.Remove(v)
}

func (d *driver) Path(_ context.Context, req *volumedriverv0.NameRequest) (*volumedriverv0.PathResponse, error) {
	v, err := d.root.Get(req.Name)
	if err != nil {
		return nil, err
	}
	return &volumedriverv0.PathResponse{Mountpoint: v.Path()}, nil
}

func (d *driver) Mount(_ context.Context, req *volumedriverv0.MountRequest) (*volumedriverv0.PathResponse, error) {
	v, err := d.root.Get(req.Name)
	if err != nil {
		return nil, err
	}
	path, err := v.Mount(req.Ref)
	if err != nil {
		return nil, err
	}
	return &volumedriverv0.PathResponse{Mountpoint: path}, nil
}

func (d *driver) Unmount(_ context.Context, req *volumedriverv0.MountRequest) error {
	v, err := d.root.Get(req.Name)
	if err != nil {
		return err
	}
	return v.Unmount(req.Ref)
}

// LiveRestore restores the driver's per-volume state for a container that
// outlived a daemon restart. The local driver reference-counts mounts, so
// without this a volume still mounted by a live-restored container would look
// unused and could be unmounted from under it.
func (d *driver) LiveRestore(ctx context.Context, req *volumedriverv0.MountRequest) error {
	v, err := d.root.Get(req.Name)
	if err != nil {
		return err
	}
	lr, ok := v.(volume.LiveRestorer)
	if !ok {
		return nil
	}
	return lr.LiveRestoreVolume(ctx, req.Ref)
}

func (d *driver) Get(_ context.Context, req *volumedriverv0.NameRequest) (*volumedriverv0.GetResponse, error) {
	v, err := d.root.Get(req.Name)
	if err != nil {
		return nil, err
	}
	out, err := toPointVolume(v)
	if err != nil {
		return nil, err
	}
	return &volumedriverv0.GetResponse{Volume: out}, nil
}

func (d *driver) List(context.Context, *volumedriverv0.ListRequest) (*volumedriverv0.ListResponse, error) {
	vols, err := d.root.List()
	if err != nil {
		return nil, err
	}
	resp := &volumedriverv0.ListResponse{Volumes: make([]volumedriverv0.Volume, 0, len(vols))}
	for _, v := range vols {
		out, err := toPointVolume(v)
		if err != nil {
			return nil, err
		}
		resp.Volumes = append(resp.Volumes, *out)
	}
	return resp, nil
}

// toPointVolume converts a volume to the point's representation, encoding the
// timestamp as RFC 3339 and the driver-defined status as JSON so both cross a
// process boundary without a schema.
func toPointVolume(v volume.Volume) (*volumedriverv0.Volume, error) {
	out := &volumedriverv0.Volume{Name: v.Name(), Mountpoint: v.Path()}
	createdAt, err := v.CreatedAt()
	if err != nil {
		return nil, err
	}
	if !createdAt.IsZero() {
		out.CreatedAt = createdAt.Format(time.RFC3339)
	}
	if status := v.Status(); len(status) > 0 {
		encoded, err := json.Marshal(status)
		if err != nil {
			return nil, fmt.Errorf("encode status of volume %q: %w", v.Name(), err)
		}
		out.Status = encoded
	}
	return out, nil
}
