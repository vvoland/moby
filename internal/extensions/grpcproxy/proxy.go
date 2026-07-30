// Package grpcproxy is a transparent gRPC proxy that forwards a call to a
// backend connection by gRPC service name, without knowing the service's proto.
// It is how the daemon publishes an extension's own gRPC service on its API
// socket: the daemon never imports the extension's generated code, it just
// forwards the bytes to the extension serving the service. Unary and streaming
// methods are forwarded the same way.
//
// Forwarding is installed on the daemon's own gRPC server as its unknown-service
// handler rather than on a second server of its own. That is what makes an
// exposed service behave the same wherever it runs: a proxied service gets the
// daemon's message size limits, tracing, and error interceptors, exactly as an
// in-process extension's service registered on the same server does.
package grpcproxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/encoding"
	_ "google.golang.org/grpc/encoding/proto" // register the proto codec Codec delegates to
	"google.golang.org/grpc/mem"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Codec forwards proxied messages as raw bytes and hands everything else to the
// codec it embeds.
//
// The proxy runs on the daemon's own gRPC server, which also serves ordinary
// typed services, so a codec installed there has to do both jobs: a proxied call
// is carried as an opaque frame, and every other call is decoded normally. It
// reports the name "proto" because the bytes really are proto -- the daemon just
// does not know their schema -- so the content type it forwards is the one the
// external client sent.
type Codec struct {
	encoding.CodecV2
}

// NewCodec returns the hybrid codec wrapping gRPC's registered proto codec.
func NewCodec() Codec { return Codec{CodecV2: encoding.GetCodecV2("proto")} }

// Marshal passes an already-encoded frame through untouched and encodes anything
// else with the embedded codec.
func (c Codec) Marshal(v any) (mem.BufferSlice, error) {
	if bs, ok := v.(mem.BufferSlice); ok {
		bs.Ref() // gRPC frees the returned slice; keep the caller's reference intact.
		return bs, nil
	}
	return c.CodecV2.Marshal(v)
}

// Unmarshal hands the caller the raw frame when it asked for one, and otherwise
// decodes with the embedded codec.
func (c Codec) Unmarshal(data mem.BufferSlice, v any) error {
	if dst, ok := v.(*mem.BufferSlice); ok {
		data.Ref() // data is freed when Unmarshal returns; take our own reference.
		*dst = data
		return nil
	}
	return c.CodecV2.Unmarshal(data, v)
}

// Routes maps gRPC service names to the backend serving each.
//
// It is created empty and filled once, before the server starts serving, because
// the daemon's gRPC server has to exist before the extensions whose services it
// will forward have been resolved.
type Routes struct {
	mu     sync.RWMutex
	routes map[string]grpc.ClientConnInterface
}

// Backend is one gRPC backend the proxy can forward to: the service names it
// serves and the connection to reach it, identified by ID for diagnostics.
type Backend struct {
	ID       string
	Conn     grpc.ClientConnInterface
	Services []string
}

// BuildRoutes assembles a service-name -> connection map from backends,
// rejecting conflicts rather than silently overriding. A service served by two
// backends, or one whose name is reserved -- already served elsewhere, e.g. by
// the host's own gRPC server -- is an error, so a backend can never shadow
// another backend's or a reserved service. Backends are processed in ID order,
// so the reported conflict is deterministic. The returned map may be empty.
func BuildRoutes(backends []Backend, reserved map[string]struct{}) (map[string]grpc.ClientConnInterface, error) {
	sorted := append([]Backend(nil), backends...)
	slices.SortFunc(sorted, func(a, b Backend) int { return strings.Compare(a.ID, b.ID) })

	routes := map[string]grpc.ClientConnInterface{}
	owner := map[string]string{}
	for _, be := range sorted {
		for _, svc := range be.Services {
			if _, taken := reserved[svc]; taken {
				return nil, fmt.Errorf("grpcproxy: backend %q cannot expose gRPC service %q: that name is reserved by an already-served service", be.ID, svc)
			}
			if other, taken := owner[svc]; taken {
				return nil, fmt.Errorf("grpcproxy: backends %q and %q both expose gRPC service %q", other, be.ID, svc)
			}
			owner[svc] = be.ID
			routes[svc] = be.Conn
		}
	}
	return routes, nil
}

// Set installs the routes. It is called once, after extensions are resolved and
// before the server serves.
func (r *Routes) Set(routes map[string]grpc.ClientConnInterface) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.routes = routes
}

func (r *Routes) lookup(service string) (grpc.ClientConnInterface, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	conn, ok := r.routes[service]
	return conn, ok
}

// Forward proxies a call -- unary or streaming -- to the backend serving its
// service. A unary call is just a stream carrying one message each way, so this
// single handler covers every method shape: it opens a bidirectional stream to
// the backend and pumps raw frames in both directions, forwarding the response
// header before the first reply and the trailer (with the backend's status)
// after the last.
func (r *Routes) Forward(_ any, serverStream grpc.ServerStream) error {
	fullMethod, ok := grpc.MethodFromServerStream(serverStream)
	if !ok {
		return status.Error(codes.Internal, "grpcproxy: no method in stream")
	}
	conn, ok := r.lookup(serviceName(fullMethod))
	if !ok {
		return status.Errorf(codes.Unimplemented, "grpcproxy: no backend for %s", fullMethod)
	}

	ctx, cancel := context.WithCancel(serverStream.Context())
	defer cancel()
	if md, ok := metadata.FromIncomingContext(serverStream.Context()); ok {
		ctx = metadata.NewOutgoingContext(ctx, md.Copy())
	}

	clientStream, err := conn.NewStream(ctx,
		&grpc.StreamDesc{ServerStreams: true, ClientStreams: true},
		fullMethod, grpc.ForceCodecV2(NewCodec()))
	if err != nil {
		return err
	}

	s2c := forwardServerToClient(serverStream, clientStream) // requests to the backend
	c2s := forwardClientToServer(clientStream, serverStream) // responses to the client

	// Only c2s ends the RPC: both its branches return. s2c fires at most once
	// (its goroutine sends a single value and exits), and its clean-EOF branch
	// does not return -- it half-closes the backend and loops, after which the
	// select can only wake on c2s. So this loop turns over at most twice and is
	// guaranteed to terminate via c2s.
	for {
		select {
		case err := <-s2c:
			if !errors.Is(err, io.EOF) {
				// The client failed or cancelled; tear the backend stream down.
				cancel()
				return status.Errorf(codes.Internal, "grpcproxy: forwarding request: %v", err)
			}
			// The client finished sending; half-close the backend so a
			// server-streaming method can run to completion.
			_ = clientStream.CloseSend()
		case err := <-c2s:
			// The backend finished or errored: forward its trailer and status.
			serverStream.SetTrailer(clientStream.Trailer())
			if !errors.Is(err, io.EOF) {
				return err
			}
			return nil
		}
	}
}

// forwardServerToClient pumps request frames from the client to the backend.
func forwardServerToClient(src grpc.ServerStream, dst grpc.ClientStream) <-chan error {
	ret := make(chan error, 1)
	go func() {
		for {
			var frame mem.BufferSlice
			if err := src.RecvMsg(&frame); err != nil {
				ret <- err // io.EOF once the client half-closes
				return
			}
			if err := dst.SendMsg(frame); err != nil {
				frame.Free()
				ret <- err
				return
			}
			frame.Free()
		}
	}()
	return ret
}

// forwardClientToServer pumps response frames from the backend to the client,
// forwarding the backend's header before the first frame.
func forwardClientToServer(src grpc.ClientStream, dst grpc.ServerStream) <-chan error {
	ret := make(chan error, 1)
	go func() {
		md, err := src.Header()
		if err != nil {
			ret <- err
			return
		}
		if err := dst.SendHeader(md); err != nil {
			ret <- err
			return
		}
		for {
			var frame mem.BufferSlice
			if err := src.RecvMsg(&frame); err != nil {
				ret <- err // io.EOF once the backend is done
				return
			}
			if err := dst.SendMsg(frame); err != nil {
				frame.Free()
				ret <- err
				return
			}
			frame.Free()
		}
	}()
	return ret
}

// serviceName extracts the service from a "/pkg.Service/Method" gRPC method.
func serviceName(fullMethod string) string {
	s := strings.TrimPrefix(fullMethod, "/")
	if i := strings.LastIndex(s, "/"); i >= 0 {
		return s[:i]
	}
	return s
}
