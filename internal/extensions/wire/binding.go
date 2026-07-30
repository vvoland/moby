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
