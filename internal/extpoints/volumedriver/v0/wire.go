package volumedriverv0

import (
	"context"

	"github.com/moby/moby/v2/internal/extensions/wire"
)

// Wire is the point's contract and both sides of its gRPC wiring, derived from
// [Driver] and its message types.
var Wire = wire.Bind(Point, "VolumeDriver", func(c wire.Client) Driver {
	return client{c}
})

// client calls an out-of-process driver.
//
// Go can build a function at runtime but not a value implementing an interface,
// so this adapter is the one part of a point that is not derived. The compiler
// checks it against [Driver]: a method added to the point fails the build here
// rather than going silently uncalled across the process boundary.
type client struct {
	wire.Client
}

func (c client) Create(ctx context.Context, req *CreateRequest) error {
	return wire.Do(ctx, c.Client, "Create", req)
}

func (c client) Remove(ctx context.Context, req *NameRequest) error {
	return wire.Do(ctx, c.Client, "Remove", req)
}

func (c client) Unmount(ctx context.Context, req *MountRequest) error {
	return wire.Do(ctx, c.Client, "Unmount", req)
}

func (c client) LiveRestore(ctx context.Context, req *MountRequest) error {
	return wire.Do(ctx, c.Client, "LiveRestore", req)
}

func (c client) Path(ctx context.Context, req *NameRequest) (*PathResponse, error) {
	return wire.Call[PathResponse](ctx, c.Client, "Path", req)
}

func (c client) Mount(ctx context.Context, req *MountRequest) (*PathResponse, error) {
	return wire.Call[PathResponse](ctx, c.Client, "Mount", req)
}

func (c client) List(ctx context.Context, req *ListRequest) (*ListResponse, error) {
	return wire.Call[ListResponse](ctx, c.Client, "List", req)
}

func (c client) Get(ctx context.Context, req *NameRequest) (*GetResponse, error) {
	return wire.Call[GetResponse](ctx, c.Client, "Get", req)
}

func (c client) Capabilities(ctx context.Context, req *CapabilitiesRequest) (*CapabilitiesResponse, error) {
	return wire.Call[CapabilitiesResponse](ctx, c.Client, "Capabilities", req)
}
