package echov1

import (
	"context"

	"github.com/moby/moby/v2/internal/extensions/wire"
	"google.golang.org/grpc"
)

// Wire is the point's contract and both sides of its gRPC wiring.
var Wire = wire.Bind(Point, "Echo", func(conn grpc.ClientConnInterface) EchoServer {
	return client{conn}
})

type client struct {
	conn grpc.ClientConnInterface
}

func (c client) Echo(ctx context.Context, req *EchoRequest) (*EchoResponse, error) {
	var resp EchoResponse
	if err := wire.Invoke(ctx, c.conn, Wire.Contract, "Echo", req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}
