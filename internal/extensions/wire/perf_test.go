package wire_test

import (
	"context"
	"net"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/moby/moby/v2/internal/extensions/wire"
	protogen "github.com/moby/moby/v2/internal/extensions/wire/internal/golden"
	createspecv0 "github.com/moby/moby/v2/internal/extpoints/createspec/v0"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

func benchSpec() *createspecv0.SpecRequest {
	return &createspecv0.SpecRequest{
		ContainerID: "9f2c1e4b7a03",
		Name:        "/eager_hopper",
		Spec:        make([]byte, 4096),
		Labels:      map[string]string{"a": "1", "b": "2"},
	}
}

// BenchmarkContractBuild measures deriving a point's descriptors, which happens
// once per point when the package is loaded.
func BenchmarkContractBuild(b *testing.B) {
	iface := reflect.TypeOf((*createspecv0.Hook)(nil)).Elem()
	id := string(createspecv0.Point.ID())
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := wire.NewContractFor(id, "CreateSpecHook", iface); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkMarshalDerived is the marshal+unmarshal the derived path performs on
// a call that crosses a process boundary.
func BenchmarkMarshalDerived(b *testing.B) {
	c := createspecv0.Wire.Contract
	src := benchSpec()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		data, err := c.Marshal(src)
		if err != nil {
			b.Fatal(err)
		}
		var out createspecv0.SpecRequest
		if err := c.Unmarshal(data, &out); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkMarshalGenerated is the same work through protoc-generated types,
// including the conversions a generated adapter would perform.
func BenchmarkMarshalGenerated(b *testing.B) {
	src := benchSpec()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		pb := &protogen.SpecRequest{
			ContainerId: src.ContainerID, Name: src.Name,
			Spec: src.Spec, Labels: src.Labels,
		}
		data, err := proto.Marshal(pb)
		if err != nil {
			b.Fatal(err)
		}
		var got protogen.SpecRequest
		if err := proto.Unmarshal(data, &got); err != nil {
			b.Fatal(err)
		}
		_ = &createspecv0.SpecRequest{
			ContainerID: got.GetContainerId(), Name: got.GetName(),
			Spec: got.GetSpec(), Labels: got.GetLabels(),
		}
	}
}

// BenchmarkGRPCRoundTrip is a whole out-of-process call: the marshalling above
// plus the gRPC round trip over a unix socket it is part of. It is the number
// the marshalling cost has to be read against, because nothing crosses a process
// boundary without it.
func BenchmarkGRPCRoundTrip(b *testing.B) {
	sock := filepath.Join(b.TempDir(), "s")
	lis, err := net.Listen("unix", sock)
	if err != nil {
		b.Fatal(err)
	}
	srv := grpc.NewServer()
	if err := wire.Serve(srv, createspecv0.Wire.Contract, &benchHook{}); err != nil {
		b.Fatal(err)
	}
	go srv.Serve(lis)
	defer srv.Stop()

	conn, err := grpc.NewClient("unix:"+sock,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", sock)
		}))
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close()

	client := createspecv0.Wire.Client.Build(conn).Impl.(createspecv0.Hook)
	ctx := context.Background()
	src := benchSpec()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := client.CreateSpec(ctx, src); err != nil {
			b.Fatal(err)
		}
	}
}

type benchHook struct{}

func (benchHook) CreateSpec(context.Context, *createspecv0.SpecRequest) (*createspecv0.SpecAdjustment, error) {
	return &createspecv0.SpecAdjustment{}, nil
}
func (benchHook) Validate(context.Context, *createspecv0.SpecRequest) error { return nil }

// BenchmarkInProcess is the same point called in process, which is what a
// module inside the daemon does. Nothing is marshalled.
func BenchmarkInProcess(b *testing.B) {
	var h createspecv0.Hook = benchHook{}
	ctx := context.Background()
	src := benchSpec()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := h.CreateSpec(ctx, src); err != nil {
			b.Fatal(err)
		}
	}
}
