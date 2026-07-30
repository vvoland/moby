package volumelocal_test

import (
	"context"
	"github.com/moby/moby/v2/daemon/internal/volumeext"
	"github.com/moby/moby/v2/daemon/volume"
	"github.com/moby/moby/v2/daemon/volume/drivers"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/moby/moby/v2/daemon/internal/extproviders/volumelocal"
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

// TestLiveRestoreSurvivesTheAdapter is a regression test for the local driver
// losing its live-restore path when it became an extension.
//
// The driver used to be registered directly, so the volume the service got back
// was a *localVolume and satisfied volume.LiveRestorer. Reached through the
// point it is a driver-store adapter instead, and the service's type assertion
// quietly took the "does not implement" branch -- so a volume still mounted by a
// container that outlived the daemon came back with its mount reference count
// lost, and a later unmount could tear it out from under that container.
//
// The assertion has to hold on what the volume service actually receives.
func TestLiveRestoreSurvivesTheAdapter(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()

	h, err := host.New(ctx, host.Options{
		RuntimeDir: filepath.Join(root, "run"),
		Extensions: []extensions.Extension{
			volumelocal.Extension(filepath.Join(root, "volumes"), idtools.Identity{UID: os.Getuid(), GID: os.Getgid()}),
		},
	})
	assert.NilError(t, err)
	t.Cleanup(func() { assert.NilError(t, h.Shutdown(context.Background())) })

	d := registeredDriver(t, ctx, h)

	created, err := d.Create("restored", nil)
	assert.NilError(t, err)

	// This is the assertion daemon/volume/service.LiveRestoreVolume makes.
	lr, ok := created.(volume.LiveRestorer)
	assert.Assert(t, ok, "volume %T does not implement volume.LiveRestorer, so live restore would be skipped", created)
	assert.NilError(t, lr.LiveRestoreVolume(ctx, "container-id"))
}

// TestCreatedAtSurvivesTheAdapter is a regression test for `docker volume
// create` reporting the zero time for local volumes.
//
// The driver protocol's Create answers with nothing, so the volume the driver
// store hands back after a create carries no detail. That did not matter while
// the local driver was registered directly -- the value was a *localVolume,
// which reads the creation time off the directory. Through the point it is an
// adapter, and the API rendered CreatedAt as "0001-01-01T00:00:00Z".
func TestCreatedAtSurvivesTheAdapter(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()

	h, err := host.New(ctx, host.Options{
		RuntimeDir: filepath.Join(root, "run"),
		Extensions: []extensions.Extension{
			volumelocal.Extension(filepath.Join(root, "volumes"), idtools.Identity{UID: os.Getuid(), GID: os.Getgid()}),
		},
	})
	assert.NilError(t, err)
	t.Cleanup(func() { assert.NilError(t, h.Shutdown(context.Background())) })

	d := registeredDriver(t, ctx, h)

	created, err := d.Create("createdatvol", nil)
	assert.NilError(t, err)

	at, err := created.CreatedAt()
	assert.NilError(t, err)
	assert.Assert(t, !at.IsZero(), "CreatedAt is the zero time, so the API would report 0001-01-01T00:00:00Z")
}

// registeredDriver resolves the local driver from h and registers it with a
// driver store, which is the path the daemon takes.
func registeredDriver(t *testing.T, ctx context.Context, h *host.Host) volume.Driver {
	t.Helper()
	ds, err := volumeext.Drivers(ctx, h)
	assert.NilError(t, err)
	assert.Assert(t, len(ds) == 1)

	store := drivers.NewStore(nil)
	assert.Assert(t, store.Register(ds[0], volumelocal.DriverName))
	d, err := store.GetDriver(volumelocal.DriverName)
	assert.NilError(t, err)
	return d
}

// TestCreateReportsMountpoint is a regression test for `docker volume create`
// returning an empty Mountpoint for the default driver.
//
// The API fills Mountpoint from CachedPath, which returns only what the volume
// already knows. A driver built into the daemon knew its own path; one reached
// through the driver store learns it from a Path call, and the volume returned
// straight from Create had not made one -- so the field came back empty for
// every `docker volume create` on the local driver.
func TestCreateReportsMountpoint(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()

	h, err := host.New(ctx, host.Options{
		RuntimeDir: filepath.Join(root, "run"),
		Extensions: []extensions.Extension{
			volumelocal.Extension(filepath.Join(root, "volumes"), idtools.Identity{UID: os.Getuid(), GID: os.Getgid()}),
		},
	})
	assert.NilError(t, err)
	t.Cleanup(func() { assert.NilError(t, h.Shutdown(context.Background())) })

	created, err := registeredDriver(t, ctx, h).Create("mountpointvol", nil)
	assert.NilError(t, err)

	// This is what daemon/volume/service.volumeToAPIType reads.
	cp, ok := created.(interface{ CachedPath() string })
	assert.Assert(t, ok, "%T does not expose CachedPath", created)
	assert.Assert(t, cp.CachedPath() != "", "CachedPath is empty, so the API would report no Mountpoint")
	assert.Check(t, is.Equal(cp.CachedPath(), created.Path()))
}
