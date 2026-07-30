package volumelocal_test

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/moby/moby/v2/daemon/extproviders/volumelocal"
	"github.com/moby/moby/v2/daemon/internal/idtools"
	"github.com/moby/moby/v2/internal/extensions"
	"github.com/moby/moby/v2/internal/extensions/host"
	volumedriverv0 "github.com/moby/moby/v2/internal/extpoints/volumedriver/v0"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"gotest.tools/v3/assert"
	is "gotest.tools/v3/assert/cmp"
	"gotest.tools/v3/skip"
)

// localDriver brings up the built-in local driver as an extension and returns
// the point provider, exactly as the daemon resolves it.
func localDriver(t *testing.T) volumedriverv0.Driver {
	t.Helper()
	root := t.TempDir()
	ctx := context.Background()

	h, err := host.New(ctx, host.Options{
		RuntimeDir: filepath.Join(root, "run"),
		Extensions: []extensions.Extension{
			volumelocal.Extension(filepath.Join(root, "volumes"), idtools.Identity{UID: os.Getuid(), GID: os.Getgid()}),
		},
	})
	assert.NilError(t, err)
	t.Cleanup(func() { assert.NilError(t, h.Shutdown(context.Background())) })

	d, err := volumedriverv0.Point.Single(h)
	assert.NilError(t, err)
	return d
}

// exercise runs the same sequence of driver operations against d and returns
// what the driver reported, so two placements of the same driver can be
// compared directly.
func exercise(t *testing.T, ctx context.Context, d volumedriverv0.Driver, name string) (scope, mountpoint string, listed int) {
	t.Helper()

	caps, err := d.Capabilities(ctx, &volumedriverv0.CapabilitiesRequest{})
	assert.NilError(t, err)
	assert.Check(t, is.Equal(caps.Name, volumelocal.DriverName))

	assert.NilError(t, d.Create(ctx, &volumedriverv0.CreateRequest{Name: name}))

	got, err := d.Get(ctx, &volumedriverv0.NameRequest{Name: name})
	assert.NilError(t, err)
	assert.Assert(t, got.Volume != nil)
	assert.Check(t, is.Equal(got.Volume.Name, name))

	path, err := d.Path(ctx, &volumedriverv0.NameRequest{Name: name})
	assert.NilError(t, err)

	ls, err := d.List(ctx, &volumedriverv0.ListRequest{})
	assert.NilError(t, err)

	return caps.Scope, path.Mountpoint, len(ls.Volumes)
}

// TestLocalDriverIsLocationTransparent is the modularization claim reduced to a
// check: Moby's own local volume driver, now a module rather than a hard-wired
// subsystem, behaves identically whether the daemon calls it directly or over
// gRPC.
//
// The in-process call is a plain Go method call on the driver. The
// out-of-process call goes through the derived wire contract to a gRPC server
// serving the very same value. Neither side has code specific to its placement,
// which is what makes moving a subsystem out of the daemon a deployment choice
// rather than a rewrite.
func TestLocalDriverIsLocationTransparent(t *testing.T) {
	skip.If(t, os.Getuid() != 0 && testing.Short(), "creating volume roots needs a writable root")
	ctx := context.Background()

	t.Run("in process", func(t *testing.T) {
		d := localDriver(t)
		scope, mountpoint, listed := exercise(t, ctx, d, "direct")
		assert.Check(t, is.Equal(scope, "local"))
		assert.Check(t, mountpoint != "")
		assert.Check(t, is.Equal(listed, 1))
	})

	t.Run("over gRPC", func(t *testing.T) {
		d := localDriver(t)

		sock := filepath.Join(t.TempDir(), "s")
		lis, err := net.Listen("unix", sock)
		assert.NilError(t, err)
		srv := grpc.NewServer()
		assert.NilError(t, volumedriverv0.ServerPoint.Register(srv, d))
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

		remote := volumedriverv0.ClientPoint.Provider(conn).Impl.(volumedriverv0.Driver)
		scope, mountpoint, listed := exercise(t, ctx, remote, "remote")
		assert.Check(t, is.Equal(scope, "local"))
		assert.Check(t, mountpoint != "")
		assert.Check(t, is.Equal(listed, 1))
	})
}
