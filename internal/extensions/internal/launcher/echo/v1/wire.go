package echov1

import (
	"context"

	"github.com/moby/moby/v2/internal/extensions/wire"
)

// Wire is the point's contract and both sides of its gRPC wiring.
var Wire = wire.Bind(Point, "Echo", func(c wire.Client) EchoServer {
	return client{c}
})

type client struct {
	wire.Client
}

func (c client) Echo(ctx context.Context, req *EchoRequest) (*EchoResponse, error) {
	return wire.Call[EchoResponse](ctx, c.Client, "Echo", req)
}
