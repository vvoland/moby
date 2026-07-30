package echov1

import (
	"context"

	"github.com/moby/moby/v2/internal/extensions"
	"github.com/moby/moby/v2/internal/extensions/clientpoint"
	"github.com/moby/moby/v2/internal/extensions/serverpoint"
	"github.com/moby/moby/v2/internal/extensions/wire"
	"google.golang.org/grpc"
)

// Contract is the point's wire form, derived from [EchoServer].
var Contract = wire.MustContract(Point, "Echo")

// ServerPoint serves the point for an out-of-process extension.
var ServerPoint = serverpoint.Registration{
	Point: Point.ID(),
	Register: func(r grpc.ServiceRegistrar, impl any) error {
		return wire.Serve(r, Contract, impl)
	},
}

// ClientPoint builds an in-daemon [EchoServer] backed by an out-of-process
// provider.
var ClientPoint = clientpoint.Registration{
	Point: Point.ID(),
	Provider: func(conn grpc.ClientConnInterface) extensions.Provider {
		return Point.Provide(client{conn})
	},
}

type client struct {
	conn grpc.ClientConnInterface
}

func (c client) Echo(ctx context.Context, req *EchoRequest) (*EchoResponse, error) {
	var resp EchoResponse
	if err := wire.Invoke(ctx, c.conn, Contract, "Echo", req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}
