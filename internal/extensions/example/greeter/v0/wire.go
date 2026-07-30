package greeterv0

import (
	"context"

	"github.com/moby/moby/v2/internal/extensions"
	"github.com/moby/moby/v2/internal/extensions/wire"
	"google.golang.org/grpc"
)

// Contract is the point's wire form, derived from [Greeter].
var Contract = wire.MustContract(Point, "Greeter")

// ServerPoint serves the point for an out-of-process extension.
var ServerPoint = wire.ServerPoint{
	Point: Point.ID(),
	Serve: func(r grpc.ServiceRegistrar, impl any) error {
		return wire.Serve(r, Contract, impl)
	},
}

// ClientPoint builds an in-daemon [Greeter] backed by an out-of-process
// provider.
var ClientPoint = wire.ClientPoint{
	Point: Point.ID(),
	Build: func(conn grpc.ClientConnInterface) extensions.Provider {
		return Point.Provide(client{conn})
	},
}

type client struct {
	conn grpc.ClientConnInterface
}

func (c client) Greet(ctx context.Context, req *HelloRequest) (*HelloReply, error) {
	var resp HelloReply
	if err := wire.Invoke(ctx, c.conn, Contract, "Greet", req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}
