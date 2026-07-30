// Package volumedriverv0 is the volume driver extension point.
//
// It is deliberately the same shape as the legacy Docker VolumeDriver plugin
// protocol -- flat names, one request and one response per call, no handles --
// because a volume driver has always been an out-of-process thing and the
// protocol has to survive the move. What changes is that the same contract now
// also describes a driver compiled into the daemon: the built-in local driver
// is an extension providing this point, reached by a direct Go call, and a
// third-party driver in a separate binary provides it over gRPC. Neither knows
// which it is.
//
// A driver is selected by name (docker volume create -d <name>), so the point's
// providers are looked up by extension id rather than fanned out.
package volumedriverv0

import (
	"context"

	"github.com/moby/moby/v2/internal/extensions"
)

// Driver is the volume driver provider interface.
//
// Errors are the driver's own: returning one fails the operation that provoked
// it. There is no veto semantics here and no ordering between drivers, because
// exactly one driver handles any given volume.
type Driver interface {
	// Create provisions a volume. Options are the driver-specific --opt values.
	Create(ctx context.Context, req *CreateRequest) error
	// Remove destroys a volume.
	Remove(ctx context.Context, req *NameRequest) error
	// Path returns a volume's mountpoint without mounting it.
	Path(ctx context.Context, req *NameRequest) (*PathResponse, error)
	// Mount makes a volume available and returns its mountpoint. Ref identifies
	// the consumer, so a driver can reference-count concurrent users.
	Mount(ctx context.Context, req *MountRequest) (*PathResponse, error)
	// Unmount releases one consumer's use of a volume.
	Unmount(ctx context.Context, req *MountRequest) error
	// List returns every volume the driver knows about.
	List(ctx context.Context, req *ListRequest) (*ListResponse, error)
	// Get returns one volume.
	Get(ctx context.Context, req *NameRequest) (*GetResponse, error)
	// Capabilities reports how the driver behaves. It is called before a driver
	// is used, so an unusable driver is rejected before it owns any data.
	Capabilities(ctx context.Context, req *CapabilitiesRequest) (*CapabilitiesResponse, error)
	// LiveRestore reattaches a volume to a consumer that outlived a daemon
	// restart.
	//
	// A driver that reference-counts mounts has no way to learn about those
	// consumers otherwise: the containers were never started by this daemon
	// process, so no Mount call was made for them, and the driver would consider
	// the volume unused while it is in fact mounted. It is called once per
	// live-restored container, after restart. A driver that keeps no such state
	// returns nil.
	LiveRestore(ctx context.Context, req *MountRequest) error
}

// CreateRequest names a volume to provision and its driver-specific options.
type CreateRequest struct {
	Name    string            `pb:"1"`
	Options map[string]string `pb:"2"`
}

// NameRequest names a volume, for the calls that take nothing else.
type NameRequest struct {
	Name string `pb:"1"`
}

// MountRequest names a volume and the consumer mounting it.
type MountRequest struct {
	Name string `pb:"1"`
	// Ref identifies the consumer, normally a container id. A driver that
	// reference-counts uses it to tell concurrent mounts apart.
	Ref string `pb:"2"`
}

// PathResponse carries a volume's mountpoint on the host.
type PathResponse struct {
	Mountpoint string `pb:"1"`
}

// ListRequest takes no arguments.
type ListRequest struct{}

// ListResponse carries every volume the driver knows about.
type ListResponse struct {
	Volumes []Volume `pb:"1"`
}

// GetResponse carries one volume, or none if the driver does not have it.
type GetResponse struct {
	Volume *Volume `pb:"1"`
}

// Volume is a volume as the driver reports it.
type Volume struct {
	Name       string `pb:"1"`
	Mountpoint string `pb:"2"`
	// CreatedAt is RFC 3339, not a timestamp type, so a driver in any language
	// can produce it without a well-known-type dependency. Empty means unknown.
	CreatedAt string `pb:"3"`
	// Status is driver-defined detail, JSON-encoded. It reaches the user
	// verbatim through the API, and its shape is the driver's business, so it
	// is carried as bytes rather than modelled here.
	Status []byte `pb:"4"`
}

// CapabilitiesRequest takes no arguments.
type CapabilitiesRequest struct{}

// CapabilitiesResponse reports how a driver behaves.
type CapabilitiesResponse struct {
	// Scope is "local" when volumes exist on one node, "global" when the driver
	// shares them across a cluster.
	Scope string `pb:"1"`
	// Name is the driver name users select with `docker volume create -d`, such
	// as "local". It is reported by the driver rather than derived from the
	// extension id, because the two namespaces are different: an extension id is
	// a versioned reverse-DNS name, while a driver name is a short word that is
	// part of the user-facing API and, for existing drivers, already fixed.
	Name string `pb:"2"`
}

// Point is the volume driver point.
var Point = extensions.DefinePoint[Driver]("org.mobyproject.extension.volume.driver.v0")
