package daemon // import "github.com/docker/docker/daemon"

import (
	"context"

	coci "github.com/containerd/containerd/oci"
	"github.com/containerd/containerd/pkg/apparmor"
	"github.com/docker/docker/container"
	"github.com/docker/docker/oci"
	"github.com/docker/docker/oci/caps"
	specs "github.com/opencontainers/runtime-spec/specs-go"
)

func (daemon *Daemon) execSetPlatformOpt(ctx context.Context, c *container.Container, ec *container.ExecConfig, p *specs.Process) error {
	if len(ec.User) > 0 {
		if daemon.UsesSnapshotter() {
			cc, err := daemon.containerdCli.LoadContainer(ctx, c.ID)
			if err != nil {
				return err
			}
			spec, err := cc.Spec(ctx)
			if err != nil {
				return err
			}
			opts := []coci.SpecOpts{
				oci.WithUser(ec.User),
				oci.WithResetAdditionalGIDs(),
				oci.WithAdditionalGIDs(ec.User),
			}
			ci, err := cc.Info(ctx)
			if err != nil {
				return err
			}
			for _, opt := range opts {
				if err := opt(ctx, daemon.containerdCli, &ci, spec); err != nil {
					return err
				}
			}
			p.User = spec.Process.User
		} else {
			var err error
			p.User, err = getUser(c, ec.User)
			if err != nil {
				return err
			}
		}
	}
	if ec.Privileged {
		p.Capabilities = &specs.LinuxCapabilities{
			Bounding:  caps.GetAllCapabilities(),
			Permitted: caps.GetAllCapabilities(),
			Effective: caps.GetAllCapabilities(),
		}
	}
	if apparmor.HostSupports() {
		var appArmorProfile string
		if ec.Container.AppArmorProfile != "" {
			appArmorProfile = ec.Container.AppArmorProfile
		} else if ec.Container.HostConfig.Privileged {
			// `docker exec --privileged` does not currently disable AppArmor
			// profiles. Privileged configuration of the container is inherited
			appArmorProfile = unconfinedAppArmorProfile
		} else {
			appArmorProfile = defaultAppArmorProfile
		}

		if appArmorProfile == defaultAppArmorProfile {
			// Unattended upgrades and other fun services can unload AppArmor
			// profiles inadvertently. Since we cannot store our profile in
			// /etc/apparmor.d, nor can we practically add other ways of
			// telling the system to keep our profile loaded, in order to make
			// sure that we keep the default profile enabled we dynamically
			// reload it if necessary.
			if err := ensureDefaultAppArmorProfile(); err != nil {
				return err
			}
		}
		p.ApparmorProfile = appArmorProfile
	}
	s := &specs.Spec{Process: p}
	return WithRlimits(daemon, ec.Container)(ctx, nil, nil, s)
}
