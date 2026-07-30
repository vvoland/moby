package greeterv0

import (
	"context"

	"github.com/moby/moby/v2/internal/extensions/wire"
	"google.golang.org/grpc"
)

// Wire is the point's contract and both sides of its gRPC wiring.
var Wire = wire.Bind(Point, "Greeter", func(conn grpc.ClientConnInterface) Greeter {
	return client{conn}
})

type client struct {
	conn grpc.ClientConnInterface
}

func (c client) Greet(ctx context.Context, req *HelloRequest) (*HelloReply, error) {
	var resp HelloReply
	if err := wire.Invoke(ctx, c.conn, Wire.Contract, "Greet", req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}
