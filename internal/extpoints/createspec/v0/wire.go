package createspecv0

import (
	"context"

	"github.com/moby/moby/v2/internal/extensions"
	"github.com/moby/moby/v2/internal/extensions/wire"
	"google.golang.org/grpc"
)

// Contract is the point's wire form, derived from [Hook] and its message types.
// Building it at package scope means a contract this daemon cannot represent --
// a field shape the wire format does not support, say -- is a build-time panic
// in the point's own package rather than a failure when an extension first
// declares the point.
var Contract = wire.MustContract(Point, "CreateSpecHook")

// ServerPoint serves the point for an out-of-process extension. Dispatch is
// derived from the contract, so there is no generated server here: the provider
// is the same [Hook] value an in-process extension would register.
var ServerPoint = wire.ServerPoint{
	Point: Point.ID(),
	Serve: func(r grpc.ServiceRegistrar, impl any) error {
		return wire.Serve(r, Contract, impl)
	},
}

// ClientPoint builds an in-daemon [Hook] backed by an out-of-process provider.
var ClientPoint = wire.ClientPoint{
	Point: Point.ID(),
	Build: func(conn grpc.ClientConnInterface) extensions.Provider {
		return Point.Provide(client{conn})
	},
}

// client calls an out-of-process provider over conn.
//
// Go can build a function at runtime but not a value implementing an interface,
// so this adapter is the one part of a point that cannot be derived. It is
// written out rather than generated because the compiler already enforces what a
// generator would: it has to satisfy [Hook], so a method added to the point
// fails the build here instead of silently going uncalled across the process
// boundary.
type client struct {
	conn grpc.ClientConnInterface
}

func (c client) CreateSpec(ctx context.Context, req *SpecRequest) (*SpecAdjustment, error) {
	var resp SpecAdjustment
	if err := wire.Invoke(ctx, c.conn, Contract, "CreateSpec", req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c client) Validate(ctx context.Context, req *SpecRequest) error {
	return wire.Invoke(ctx, c.conn, Contract, "Validate", req, nil)
}
