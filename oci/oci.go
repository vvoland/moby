package oci // import "github.com/docker/docker/oci"

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"

	"github.com/containerd/containerd/containers"
	"github.com/containerd/containerd/mount"
	"github.com/containerd/continuity/fs"

	coci "github.com/containerd/containerd/oci"
	libcontainer "github.com/opencontainers/runc/libcontainer/user"
	specs "github.com/opencontainers/runtime-spec/specs-go"
)

// TODO verify if this regex is correct for "a" (all);
//
// The docs (https://github.com/torvalds/linux/blob/v5.10/Documentation/admin-guide/cgroup-v1/devices.rst) describe:
// "'all' means it applies to all types and all major and minor numbers", and shows an example
// that *only* passes `a` as value: `echo a > /sys/fs/cgroup/1/devices.allow, which would be
// the "implicit" equivalent of "a *:* rwm". Source-code also looks to confirm this, and returns
// early for "a" (all); https://github.com/torvalds/linux/blob/v5.10/security/device_cgroup.c#L614-L642
var deviceCgroupRuleRegex = regexp.MustCompile("^([acb]) ([0-9]+|\\*):([0-9]+|\\*) ([rwm]{1,3})$") //nolint: gosimple

// SetCapabilities sets the provided capabilities on the spec
// All capabilities are added if privileged is true.
func SetCapabilities(s *specs.Spec, caplist []string) error {
	// setUser has already been executed here
	if s.Process.User.UID == 0 {
		s.Process.Capabilities = &specs.LinuxCapabilities{
			Effective: caplist,
			Bounding:  caplist,
			Permitted: caplist,
		}
	} else {
		// Do not set Effective and Permitted capabilities for non-root users,
		// to match what execve does.
		s.Process.Capabilities = &specs.LinuxCapabilities{
			Bounding: caplist,
		}
	}
	return nil
}

// AppendDevicePermissionsFromCgroupRules takes rules for the devices cgroup to append to the default set
func AppendDevicePermissionsFromCgroupRules(devPermissions []specs.LinuxDeviceCgroup, rules []string) ([]specs.LinuxDeviceCgroup, error) {
	for _, deviceCgroupRule := range rules {
		ss := deviceCgroupRuleRegex.FindAllStringSubmatch(deviceCgroupRule, -1)
		if len(ss) == 0 || len(ss[0]) != 5 {
			return nil, fmt.Errorf("invalid device cgroup rule format: '%s'", deviceCgroupRule)
		}
		matches := ss[0]

		dPermissions := specs.LinuxDeviceCgroup{
			Allow:  true,
			Type:   matches[1],
			Access: matches[4],
		}
		if matches[2] == "*" {
			major := int64(-1)
			dPermissions.Major = &major
		} else {
			major, err := strconv.ParseInt(matches[2], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid major value in device cgroup rule format: '%s'", deviceCgroupRule)
			}
			dPermissions.Major = &major
		}
		if matches[3] == "*" {
			minor := int64(-1)
			dPermissions.Minor = &minor
		} else {
			minor, err := strconv.ParseInt(matches[3], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid minor value in device cgroup rule format: '%s'", deviceCgroupRule)
			}
			dPermissions.Minor = &minor
		}
		devPermissions = append(devPermissions, dPermissions)
	}
	return devPermissions, nil
}

// WithResetAdditionalGIDs resets additional GIDs
// This code is based  nerdctl, under Apache License
// https://github.com/containerd/nerdctl/blob/2bbd998a1c95e6682120918d9a07a24ccef4f5fb/cmd/nerdctl/run_user.go#L69
func WithResetAdditionalGIDs() coci.SpecOpts {
	return func(_ context.Context, _ coci.Client, _ *containers.Container, s *coci.Spec) error {
		s.Process.User.AdditionalGids = nil
		return nil
	}
}

// ReadonlyMounts is used by the options which are trying to get user/group
// information from container's rootfs. Since the option does read operation
// only, this helper will append ReadOnly mount option to prevent linux kernel
// from syncing whole filesystem in umount syscall. This also prevents the
// filesystem to end up in a state with undefined behavior.
func ReadonlyMounts(mounts []mount.Mount) []mount.Mount {
	for i, m := range mounts {
		if m.Type == "overlay" {
			mounts[i].Options = readonlyOverlay(m.Options)
		}
	}
	if len(mounts) == 1 && mounts[0].Type == "overlay" {
		mounts[0].Options = append(mounts[0].Options, "ro")
	}
	return mounts
}

func readonlyOverlay(opt []string) []string {
	out := make([]string, 0, len(opt))
	upper := ""
	for _, o := range opt {
		if strings.HasPrefix(o, "upperdir=") {
			upper = strings.TrimPrefix(o, "upperdir=")
		} else if !strings.HasPrefix(o, "workdir=") {
			out = append(out, o)
		}
	}
	if upper != "" {
		for i, o := range out {
			if strings.HasPrefix(o, "lowerdir=") {
				out[i] = "lowerdir=" + upper + ":" + strings.TrimPrefix(o, "lowerdir=")
			}
		}
	}
	return out
}

func isRootfsAbs(root string) bool {
	return filepath.IsAbs(root)
}

// setProcess sets Process to empty if unset
func setProcess(s *coci.Spec) {
	if s.Process == nil {
		s.Process = &specs.Process{}
	}
}

func getSupplementalGroupsFromPath(root string, filter func(libcontainer.Group) bool) ([]uint32, error) {
	gpath, err := fs.RootPath(root, "/etc/group")
	if err != nil {
		return []uint32{}, err
	}
	groups, err := libcontainer.ParseGroupFileFilter(gpath, filter)
	if err != nil {
		return []uint32{}, err
	}
	if len(groups) == 0 {
		// if there are no additional groups; just return an empty set
		return []uint32{}, nil
	}
	addlGids := []uint32{}
	for _, grp := range groups {
		addlGids = append(addlGids, uint32(grp.Gid))
	}
	return addlGids, nil
}

// WithUserID sets the correct UID and GID for the container based
// on the image's /etc/passwd contents. If /etc/passwd does not exist,
// or uid is not found in /etc/passwd, it sets the requested uid,
// additionally sets the gid to 0, and does not return an error.
func withUserID(uid uint32) coci.SpecOpts {
	return func(ctx context.Context, client coci.Client, c *containers.Container, s *coci.Spec) (err error) {
		setProcess(s)
		if c.Snapshotter == "" && c.SnapshotKey == "" {
			if !isRootfsAbs(s.Root.Path) {
				return errors.New("rootfs absolute path is required")
			}
			user, err := coci.UserFromPath(s.Root.Path, func(u libcontainer.User) bool {
				return u.Uid == int(uid)
			})
			if err != nil {
				if os.IsNotExist(err) || err == coci.ErrNoUsersFound {
					s.Process.User.UID, s.Process.User.GID = uid, 0
					return nil
				}
				return err
			}
			s.Process.User.UID, s.Process.User.GID = uint32(user.Uid), uint32(user.Gid)
			return nil

		}
		if c.Snapshotter == "" {
			return errors.New("no snapshotter set for container")
		}
		if c.SnapshotKey == "" {
			return errors.New("rootfs snapshot not created for container")
		}
		snapshotter := client.SnapshotService(c.Snapshotter)
		mounts, err := snapshotter.Mounts(ctx, c.SnapshotKey)
		if err != nil {
			return err
		}

		mounts = ReadonlyMounts(mounts)
		return mount.WithTempMount(ctx, mounts, func(root string) error {
			user, err := coci.UserFromPath(root, func(u libcontainer.User) bool {
				return u.Uid == int(uid)
			})
			if err != nil {
				if os.IsNotExist(err) || err == coci.ErrNoUsersFound {
					s.Process.User.UID, s.Process.User.GID = uid, 0
					return nil
				}
				return err
			}
			s.Process.User.UID, s.Process.User.GID = uint32(user.Uid), uint32(user.Gid)
			return nil
		})
	}
}

// WithUsername sets the correct UID and GID for the container
// based on the image's /etc/passwd contents. If /etc/passwd
// does not exist, or the username is not found in /etc/passwd,
// it returns error. On Windows this sets the username as provided,
// the operating system will validate the user when going to run
// the container.
func withUsername(username string) coci.SpecOpts {
	return func(ctx context.Context, client coci.Client, c *containers.Container, s *coci.Spec) (err error) {
		setProcess(s)
		if s.Linux != nil {
			if c.Snapshotter == "" && c.SnapshotKey == "" {
				if !isRootfsAbs(s.Root.Path) {
					return errors.New("rootfs absolute path is required")
				}
				user, err := coci.UserFromPath(s.Root.Path, func(u libcontainer.User) bool {
					return u.Name == username
				})
				if err != nil {
					return err
				}
				s.Process.User.UID, s.Process.User.GID = uint32(user.Uid), uint32(user.Gid)
				return nil
			}
			if c.Snapshotter == "" {
				return errors.New("no snapshotter set for container")
			}
			if c.SnapshotKey == "" {
				return errors.New("rootfs snapshot not created for container")
			}
			snapshotter := client.SnapshotService(c.Snapshotter)
			mounts, err := snapshotter.Mounts(ctx, c.SnapshotKey)
			if err != nil {
				return err
			}

			mounts = ReadonlyMounts(mounts)
			return mount.WithTempMount(ctx, mounts, func(root string) error {
				user, err := coci.UserFromPath(root, func(u libcontainer.User) bool {
					return u.Name == username
				})
				if err != nil {
					return err
				}
				s.Process.User.UID, s.Process.User.GID = uint32(user.Uid), uint32(user.Gid)
				return nil
			})
		} else if s.Windows != nil {
			s.Process.User.Username = username
		} else {
			return errors.New("spec does not contain Linux or Windows section")
		}
		return nil
	}
}

// WithUser sets the user to be used within the container.
// It accepts a valid user string in OCI Image Spec v1.0.0:
// user, uid, user:group, uid:gid, uid:group, user:gid
func WithUser(userstr string) coci.SpecOpts {
	return func(ctx context.Context, client coci.Client, c *containers.Container, s *coci.Spec) error {
		// For LCOW it's a bit harder to confirm that the user actually exists on the host as a rootfs isn't
		// mounted on the host and shared into the guest, but rather the rootfs is constructed entirely in the
		// guest itself. To accommodate this, a spot to place the user string provided by a client as-is is needed.
		// The `Username` field on the runtime spec is marked by Platform as only for Windows, and in this case it
		// *is* being set on a Windows host at least, but will be used as a temporary holding spot until the guest
		// can use the string to perform these same operations to grab the uid:gid inside.
		if s.Windows != nil && s.Linux != nil {
			s.Process.User.Username = userstr
			return nil
		}

		parts := strings.Split(userstr, ":")
		switch len(parts) {
		case 1:
			v, err := strconv.Atoi(parts[0])
			if err != nil {
				// if we cannot parse as a uint they try to see if it is a username
				return withUsername(userstr)(ctx, client, c, s)
			}
			return withUserID(uint32(v))(ctx, client, c, s)
		case 2:
			var (
				username  string
				groupname string
			)
			var uid, gid uint32
			v, err := strconv.Atoi(parts[0])
			if err != nil {
				username = parts[0]
			} else {
				uid = uint32(v)
			}
			if v, err = strconv.Atoi(parts[1]); err != nil {
				groupname = parts[1]
			} else {
				gid = uint32(v)
			}
			if username == "" && groupname == "" {
				s.Process.User.UID, s.Process.User.GID = uid, gid
				return nil
			}
			f := func(root string) error {
				if username != "" {
					user, err := coci.UserFromPath(root, func(u libcontainer.User) bool {
						return u.Name == username
					})
					if err != nil {
						return err
					}
					uid = uint32(user.Uid)
				}
				if groupname != "" {
					gid, err = coci.GIDFromPath(root, func(g libcontainer.Group) bool {
						return g.Name == groupname
					})
					if err != nil {
						return err
					}
				}
				s.Process.User.UID, s.Process.User.GID = uid, gid
				return nil
			}
			if c.Snapshotter == "" && c.SnapshotKey == "" {
				if !isRootfsAbs(s.Root.Path) {
					return errors.New("rootfs absolute path is required")
				}
				return f(s.Root.Path)
			}
			if c.Snapshotter == "" {
				return errors.New("no snapshotter set for container")
			}
			if c.SnapshotKey == "" {
				return errors.New("rootfs snapshot not created for container")
			}
			snapshotter := client.SnapshotService(c.Snapshotter)
			mounts, err := snapshotter.Mounts(ctx, c.SnapshotKey)
			if err != nil {
				return err
			}

			mounts = ReadonlyMounts(mounts)
			return mount.WithTempMount(ctx, mounts, f)
		default:
			return fmt.Errorf("invalid USER value %s", userstr)
		}
	}
}

// WithAdditionalGIDs sets the OCI spec's additionalGids array to any additional groups listed
// for a particular user in the /etc/groups file of the image's root filesystem
// The passed in user can be either a uid or a username.
func WithAdditionalGIDs(userstr string) coci.SpecOpts {
	return func(ctx context.Context, client coci.Client, c *containers.Container, s *coci.Spec) (err error) {
		// For LCOW or on Darwin additional GID's not supported
		if s.Windows != nil || runtime.GOOS == "darwin" {
			return nil
		}
		setProcess(s)
		setAdditionalGids := func(root string) error {
			var username string
			uid, err := strconv.Atoi(userstr)
			if err == nil {
				user, err := coci.UserFromPath(root, func(u libcontainer.User) bool {
					return u.Uid == uid
				})
				if err != nil {
					if os.IsNotExist(err) || err == coci.ErrNoUsersFound {
						return nil
					}
					return err
				}
				username = user.Name
			} else {
				username = userstr
			}
			gids, err := getSupplementalGroupsFromPath(root, func(g libcontainer.Group) bool {
				// we only want supplemental groups
				if g.Name == username {
					return false
				}
				for _, entry := range g.List {
					if entry == username {
						return true
					}
				}
				return false
			})
			if err != nil {
				if os.IsNotExist(err) {
					return nil
				}
				return err
			}
			s.Process.User.AdditionalGids = gids
			return nil
		}
		if c.Snapshotter == "" && c.SnapshotKey == "" {
			if !isRootfsAbs(s.Root.Path) {
				return errors.New("rootfs absolute path is required")
			}
			return setAdditionalGids(s.Root.Path)
		}
		if c.Snapshotter == "" {
			return errors.New("no snapshotter set for container")
		}
		if c.SnapshotKey == "" {
			return errors.New("rootfs snapshot not created for container")
		}
		snapshotter := client.SnapshotService(c.Snapshotter)
		mounts, err := snapshotter.Mounts(ctx, c.SnapshotKey)
		if err != nil {
			return err
		}

		mounts = ReadonlyMounts(mounts)
		return mount.WithTempMount(ctx, mounts, setAdditionalGids)
	}
}
