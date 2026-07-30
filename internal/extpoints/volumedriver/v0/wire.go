package volumedriverv0

import (
	"context"

	"github.com/moby/moby/v2/internal/extensions/wire"
	"google.golang.org/grpc"
)

// Wire is the point's contract and both sides of its gRPC wiring, derived from
// [Driver] and its message types.
var Wire = wire.Bind(Point, "VolumeDriver", func(conn grpc.ClientConnInterface) Driver {
	return client{conn}
})

// client calls an out-of-process driver over conn. It is the one part of a point
// that is not derived, and the compiler checks it against [Driver].
type client struct {
	conn grpc.ClientConnInterface
}

func (c client) Create(ctx context.Context, req *CreateRequest) error {
	return wire.Invoke(ctx, c.conn, Wire.Contract, "Create", req, nil)
}

func (c client) Remove(ctx context.Context, req *NameRequest) error {
	return wire.Invoke(ctx, c.conn, Wire.Contract, "Remove", req, nil)
}

func (c client) Unmount(ctx context.Context, req *MountRequest) error {
	return wire.Invoke(ctx, c.conn, Wire.Contract, "Unmount", req, nil)
}

func (c client) LiveRestore(ctx context.Context, req *MountRequest) error {
	return wire.Invoke(ctx, c.conn, Wire.Contract, "LiveRestore", req, nil)
}

func (c client) Path(ctx context.Context, req *NameRequest) (*PathResponse, error) {
	var resp PathResponse
	if err := wire.Invoke(ctx, c.conn, Wire.Contract, "Path", req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c client) Mount(ctx context.Context, req *MountRequest) (*PathResponse, error) {
	var resp PathResponse
	if err := wire.Invoke(ctx, c.conn, Wire.Contract, "Mount", req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c client) List(ctx context.Context, req *ListRequest) (*ListResponse, error) {
	var resp ListResponse
	if err := wire.Invoke(ctx, c.conn, Wire.Contract, "List", req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c client) Get(ctx context.Context, req *NameRequest) (*GetResponse, error) {
	var resp GetResponse
	if err := wire.Invoke(ctx, c.conn, Wire.Contract, "Get", req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c client) Capabilities(ctx context.Context, req *CapabilitiesRequest) (*CapabilitiesResponse, error) {
	var resp CapabilitiesResponse
	if err := wire.Invoke(ctx, c.conn, Wire.Contract, "Capabilities", req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}
