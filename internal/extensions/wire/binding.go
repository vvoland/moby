package wire

import (
	"github.com/moby/moby/v2/internal/extensions"
	"google.golang.org/grpc"
)

// ClientPoint says how to reach a provider of a point that lives in another
// process: given a connection to it, build the in-daemon value that satisfies
// the point's Go interface.
//
// A host collects one per point it supports across the boundary. A point that a
// launched extension declares and the host has no entry for is refused, because
// nothing would be able to call it.
type ClientPoint struct {
	Point extensions.PointID
	// Build returns a provider backed by conn.
	Build func(conn grpc.ClientConnInterface) extensions.Provider
}

// ServerPoint says how to serve a provider of a point to another process.
//
// An out-of-process extension hands one per point it provides to the SDK.
// Serve reports an error rather than panicking when impl does not implement the
// point: an extension that declares a point it does not provide is a
// misconfiguration to report, and loading is all-or-nothing, so it has to fail
// that extension's startup rather than take the process down.
type ServerPoint struct {
	Point extensions.PointID
	// Serve registers the point's gRPC service for impl on r.
	Serve func(r grpc.ServiceRegistrar, impl any) error
}

// Binding is everything a point needs to cross a process boundary: its derived
// contract, plus the client and server sides bound to it.
type Binding[T any] struct {
	// Contract is the point's wire form, derived from its Go types.
	Contract *Contract
	// Client builds an in-daemon provider from a connection to one out of
	// process. A host lists it for each point it supports across the boundary.
	Client ClientPoint
	// Server serves a provider of the point to the daemon. An out-of-process
	// extension hands it to the SDK for each point it provides.
	Server ServerPoint
}

// Bind derives a point's contract and wires both sides of it.
//
// newClient is the only argument that carries real information: it adapts a
// bound [Client] to the point's Go interface, and it is the one part of a point
// that cannot be derived, because Go can build a function at runtime but not a
// value implementing an interface. Everything else -- the descriptors, the messages,
// the gRPC dispatch, the service registration -- follows from the point's types.
//
// It panics if the point's Go types cannot be represented on the wire, so a
// contract this daemon cannot carry fails at build time in the package that owns
// it, rather than when some extension first declares the point.
func Bind[T any](p extensions.Point[T], service string, newClient func(Client) T) Binding[T] {
	contract := MustContract(p, service)
	return Binding[T]{
		Contract: contract,
		Client: ClientPoint{
			Point: p.ID(),
			Build: func(conn grpc.ClientConnInterface) extensions.Provider {
				return p.Provide(newClient(Client{Contract: contract, Conn: conn}))
			},
		},
		Server: ServerPoint{
			Point: p.ID(),
			Serve: func(r grpc.ServiceRegistrar, impl any) error {
				return Serve(r, contract, impl)
			},
		},
	}
}
