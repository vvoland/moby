package createspecv0

import (
	"context"

	"github.com/moby/moby/v2/internal/extensions/wire"
	"google.golang.org/grpc"
)

// Wire is the point's contract and both sides of its gRPC wiring, derived from
// [Hook] and its message types.
var Wire = wire.Bind(Point, "CreateSpecHook", func(conn grpc.ClientConnInterface) Hook {
	return client{conn}
})

// client calls an out-of-process provider over conn.
//
// Go can build a function at runtime but not a value implementing an interface,
// so this adapter is the one part of a point that is not derived. It is written
// out rather than generated because the compiler already enforces what a
// generator would: it has to satisfy [Hook], so a method added to the point
// fails the build here instead of silently going uncalled across the process
// boundary.
type client struct {
	conn grpc.ClientConnInterface
}

func (c client) CreateSpec(ctx context.Context, req *SpecRequest) (*SpecAdjustment, error) {
	var resp SpecAdjustment
	if err := wire.Invoke(ctx, c.conn, Wire.Contract, "CreateSpec", req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c client) Validate(ctx context.Context, req *SpecRequest) error {
	return wire.Invoke(ctx, c.conn, Wire.Contract, "Validate", req, nil)
}
