package containerd

import (
	"github.com/containerd/containerd/remotes"
	"github.com/containerd/containerd/remotes/docker"
	registrytypes "github.com/docker/docker/api/types/registry"
)

func newResolverFromAuthConfig(authConfig *registrytypes.AuthConfig) remotes.Resolver {
	opts := []docker.RegistryOpt{}
	if authConfig != nil {
		authorizer := docker.NewDockerAuthorizer(docker.WithAuthCreds(func(_ string) (string, string, error) {
			if authConfig.IdentityToken != "" {
				return "", authConfig.IdentityToken, nil
			}
			return authConfig.Username, authConfig.Password, nil
		}))

		opts = append(opts, docker.WithAuthorizer(authorizer))
	}

	return docker.NewResolver(docker.ResolverOptions{
		Hosts: docker.ConfigureDefaultRegistries(opts...),
	})
}
