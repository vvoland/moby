package wire_test

import (
	"context"
	"net"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	createspecv0 "github.com/moby/moby/v2/extpoints/createspec/v0"
	protogen "github.com/moby/moby/v2/extpoints/createspec/v0/protogen"
	"github.com/moby/moby/v2/internal/extensions/wire"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"gotest.tools/v3/assert"
	is "gotest.tools/v3/assert/cmp"
)

// fakeHook is an in-process provider of the create-spec point. It is the plain
// Go interface a provider implements; nothing about it knows it is being reached
// over gRPC.
type fakeHook struct {
	gotContainerID string
	vetoErr        error
}

func (h *fakeHook) CreateSpec(_ context.Context, req *createspecv0.SpecRequest) (*createspecv0.SpecAdjustment, error) {
	h.gotContainerID = req.ContainerID
	return &createspecv0.SpecAdjustment{Spec: append([]byte("adjusted:"), req.Spec...)}, nil
}

func (h *fakeHook) Validate(_ context.Context, _ *createspecv0.SpecRequest) error {
	return h.vetoErr
}

// serveOn starts a gRPC server on a unix socket and returns a connection to it.
func serveOn(t *testing.T, register func(grpc.ServiceRegistrar)) *grpc.ClientConn {
	t.Helper()
	// A short socket name: the sun_path limit is 108 bytes, and t.TempDir() paths
	// derived from long test names get close to it.
	sock := filepath.Join(t.TempDir(), "s")
	lis, err := net.Listen("unix", sock)
	assert.NilError(t, err)

	srv := grpc.NewServer()
	register(srv)
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("unix:"+sock,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", sock)
		}),
	)
	assert.NilError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// generatedServer implements the protoc-generated server interface, standing in
// for an extension built from the point's .proto with ordinary stubs -- which is
// what an extension written in another language amounts to.
type generatedServer struct {
	protogen.UnimplementedCreateSpecHookServer
}

func (generatedServer) CreateSpec(_ context.Context, req *protogen.SpecRequest) (*protogen.SpecAdjustment, error) {
	return &protogen.SpecAdjustment{Spec: append([]byte("adjusted:"), req.GetSpec()...)}, nil
}

func (generatedServer) Validate(context.Context, *protogen.SpecRequest) (*protogen.ValidateResponse, error) {
	return &protogen.ValidateResponse{}, nil
}

// TestGenericServerServesGeneratedClient checks the direction that matters for
// the daemon calling an extension: a point served generically from its Go
// interface, with no generated server code, answers a client built from the
// generated stubs. If this holds, an extension author in another language needs
// only the point's .proto.
func TestGenericServerServesGeneratedClient(t *testing.T) {
	c, err := wire.NewContractFor(
		string(createspecv0.Point.ID()), "CreateSpecHook",
		reflect.TypeOf((*createspecv0.Hook)(nil)).Elem(),
	)
	assert.NilError(t, err)

	hook := &fakeHook{}
	conn := serveOn(t, func(r grpc.ServiceRegistrar) {
		assert.NilError(t, wire.Serve(r, c, hook))
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client := protogen.NewCreateSpecHookClient(conn)

	adj, err := client.CreateSpec(ctx, &protogen.SpecRequest{
		ContainerId: "deadbeef",
		Name:        "/brave_lion",
		Spec:        []byte(`{"ociVersion":"1.0.2"}`),
		Labels:      map[string]string{"k": "v"},
	})
	assert.NilError(t, err)
	assert.Check(t, is.Equal(string(adj.GetSpec()), `adjusted:{"ociVersion":"1.0.2"}`))
	assert.Check(t, is.Equal(hook.gotContainerID, "deadbeef"))

	_, err = client.Validate(ctx, &protogen.SpecRequest{ContainerId: "deadbeef"})
	assert.NilError(t, err)
}

// TestGeneratedServerServesGenericClient is the same interop in reverse: a
// server built from the generated stubs, called through the derived contract.
// Both the interface-adapter and the struct-of-funcs client shapes are
// exercised, since those are the two ways a point can be reached in process.
func TestGeneratedServerServesGenericClient(t *testing.T) {
	c, err := wire.NewContractFor(
		string(createspecv0.Point.ID()), "CreateSpecHook",
		reflect.TypeOf((*createspecv0.Hook)(nil)).Elem(),
	)
	assert.NilError(t, err)

	conn := serveOn(t, func(r grpc.ServiceRegistrar) {
		protogen.RegisterCreateSpecHookServer(r, generatedServer{})
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	t.Run("Invoke", func(t *testing.T) {
		var adj createspecv0.SpecAdjustment
		err := wire.Invoke(ctx, conn, c, "CreateSpec", &createspecv0.SpecRequest{
			ContainerID: "cafe",
			Spec:        []byte("spec"),
		}, &adj)
		assert.NilError(t, err)
		assert.Check(t, is.Equal(string(adj.Spec), "adjusted:spec"))
	})

	t.Run("bare-error method", func(t *testing.T) {
		err := wire.Invoke(ctx, conn, c, "Validate", &createspecv0.SpecRequest{ContainerID: "cafe"}, nil)
		assert.NilError(t, err)
	})

	// The struct-of-funcs shape: a contract expressed as function fields can be
	// bound entirely at runtime, with no per-point adapter, because reflect can
	// build a function even though it cannot build an interface implementation.
	t.Run("BindFuncs", func(t *testing.T) {
		var funcs struct {
			CreateSpec func(context.Context, *createspecv0.SpecRequest) (*createspecv0.SpecAdjustment, error)
			Validate   func(context.Context, *createspecv0.SpecRequest) error
		}
		assert.NilError(t, c.BindFuncs(conn, &funcs))

		adj, err := funcs.CreateSpec(ctx, &createspecv0.SpecRequest{
			ContainerID: "cafe",
			Spec:        []byte("spec"),
		})
		assert.NilError(t, err)
		assert.Check(t, is.Equal(string(adj.Spec), "adjusted:spec"))
		assert.NilError(t, funcs.Validate(ctx, &createspecv0.SpecRequest{ContainerID: "cafe"}))
	})
}

// TestProviderSignatureMismatch checks that a provider that does not implement
// the point is refused when the service is built, not when it is first called.
// Loading is all-or-nothing, so this has to be a startup error.
func TestProviderSignatureMismatch(t *testing.T) {
	c, err := wire.NewContractFor(
		string(createspecv0.Point.ID()), "CreateSpecHook",
		reflect.TypeOf((*createspecv0.Hook)(nil)).Elem(),
	)
	assert.NilError(t, err)

	_, err = wire.ServiceDesc(c, struct{}{})
	assert.ErrorContains(t, err, "has no method CreateSpec")
}
