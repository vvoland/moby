package greeterv0

import (
	"context"

	"github.com/moby/moby/v2/internal/extensions/wire"
)

// Wire is the point's contract and both sides of its gRPC wiring.
var Wire = wire.Bind(Point, "Greeter", func(c wire.Client) Greeter {
	return client{c}
})

type client struct {
	wire.Client
}

func (c client) Greet(ctx context.Context, req *HelloRequest) (*HelloReply, error) {
	return wire.Call[HelloReply](ctx, c.Client, "Greet", req)
}
