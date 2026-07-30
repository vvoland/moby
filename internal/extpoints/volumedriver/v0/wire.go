package volumedriverv0

import (
	"context"

	"github.com/moby/moby/v2/internal/extensions"
	"github.com/moby/moby/v2/internal/extensions/wire"
	"google.golang.org/grpc"
)

// Contract is the point's wire form, derived from [Driver] and its message
// types.
var Contract = wire.MustContract(Point, "VolumeDriver")

// ServerPoint serves the point for an out-of-process driver.
var ServerPoint = wire.ServerPoint{
	Point: Point.ID(),
	Serve: func(r grpc.ServiceRegistrar, impl any) error {
		return wire.Serve(r, Contract, impl)
	},
}

// ClientPoint builds an in-daemon [Driver] backed by an out-of-process one.
var ClientPoint = wire.ClientPoint{
	Point: Point.ID(),
	Build: func(conn grpc.ClientConnInterface) extensions.Provider {
		return Point.Provide(client{conn})
	},
}

// client calls an out-of-process driver over conn. It is the one part of a
// point that cannot be derived, and the compiler checks it against [Driver].
type client struct {
	conn grpc.ClientConnInterface
}

func (c client) Create(ctx context.Context, req *CreateRequest) error {
	return wire.Invoke(ctx, c.conn, Contract, "Create", req, nil)
}

func (c client) Remove(ctx context.Context, req *NameRequest) error {
	return wire.Invoke(ctx, c.conn, Contract, "Remove", req, nil)
}

func (c client) Unmount(ctx context.Context, req *MountRequest) error {
	return wire.Invoke(ctx, c.conn, Contract, "Unmount", req, nil)
}

func (c client) LiveRestore(ctx context.Context, req *MountRequest) error {
	return wire.Invoke(ctx, c.conn, Contract, "LiveRestore", req, nil)
}

func (c client) Path(ctx context.Context, req *NameRequest) (*PathResponse, error) {
	var resp PathResponse
	if err := wire.Invoke(ctx, c.conn, Contract, "Path", req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c client) Mount(ctx context.Context, req *MountRequest) (*PathResponse, error) {
	var resp PathResponse
	if err := wire.Invoke(ctx, c.conn, Contract, "Mount", req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c client) List(ctx context.Context, req *ListRequest) (*ListResponse, error) {
	var resp ListResponse
	if err := wire.Invoke(ctx, c.conn, Contract, "List", req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c client) Get(ctx context.Context, req *NameRequest) (*GetResponse, error) {
	var resp GetResponse
	if err := wire.Invoke(ctx, c.conn, Contract, "Get", req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c client) Capabilities(ctx context.Context, req *CapabilitiesRequest) (*CapabilitiesResponse, error) {
	var resp CapabilitiesResponse
	if err := wire.Invoke(ctx, c.conn, Contract, "Capabilities", req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}
