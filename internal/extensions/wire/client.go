package wire

import (
	"context"

	"google.golang.org/grpc"
)

// invoke calls one of the point's methods on an out-of-process provider over
// conn.
//
// req is a pointer to the method's request struct. resp is a pointer to its
// response struct, or nil for a method whose Go signature returns only error.
// The call is an ordinary unary gRPC request carrying an ordinary protobuf
// message, so nothing about the traffic reveals that the contract was derived
// rather than generated.
func invoke(ctx context.Context, conn grpc.ClientConnInterface, c *Contract, method string, req, resp any) error {
	reqMsg, err := c.ToDynamic(req)
	if err != nil {
		return err
	}
	respMsg, err := c.NewResponse(method)
	if err != nil {
		return err
	}
	if err := conn.Invoke(ctx, c.FullMethod(method), reqMsg, respMsg); err != nil {
		return err
	}
	if resp == nil {
		return nil
	}
	return c.FromDynamic(respMsg, resp)
}

// Client pairs a point's contract with a connection to a provider of it.
//
// A point's client adapter embeds one so that each of its methods is a single
// forwarding call: the contract and the connection are already bound, leaving
// only the method name and the request.
type Client struct {
	Contract *Contract
	Conn     grpc.ClientConnInterface
}

// Call invokes a method that answers with a response.
//
//	func (c client) Mount(ctx context.Context, req *MountRequest) (*PathResponse, error) {
//		return wire.Call[PathResponse](ctx, c.Client, "Mount", req)
//	}
func Call[R any](ctx context.Context, c Client, method string, req any) (*R, error) {
	var resp R
	if err := invoke(ctx, c.Conn, c.Contract, method, req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// Do invokes a method that answers with only an error.
func Do(ctx context.Context, c Client, method string, req any) error {
	return invoke(ctx, c.Conn, c.Contract, method, req, nil)
}
