package containerd

import (
	"crypto/tls"
	"net/http"

	"github.com/containerd/containerd/remotes"
	"github.com/containerd/containerd/remotes/docker"
	registrytypes "github.com/docker/docker/api/types/registry"
	"github.com/docker/docker/registry"
	"github.com/sirupsen/logrus"
)

func (i *ImageService) newResolverFromAuthConfig(authConfig *registrytypes.AuthConfig) (remotes.Resolver, docker.StatusTracker) {
	tracker := docker.NewInMemoryTracker()
	hostsFn := i.registryHosts.RegistryHosts()

	hosts := hostsWrapper(hostsFn, authConfig, i.registryService)

	return docker.NewResolver(docker.ResolverOptions{
		Hosts:   hosts,
		Tracker: tracker,
	}), tracker
}

func hostsWrapper(hostsFn docker.RegistryHosts, authConfig *registrytypes.AuthConfig, regService *registry.Service) docker.RegistryHosts {
	return docker.RegistryHosts(func(n string) ([]docker.RegistryHost, error) {
		hosts, err := hostsFn(n)
		if err != nil {
			return nil, err
		}

		for idx, host := range hosts {
			if host.Authorizer == nil {
				var opts []docker.AuthorizerOpt
				if authConfig != nil {
					opts = append(opts, authorizationCredsFromAuthConfig(*authConfig))
				}
				host.Authorizer = docker.NewDockerAuthorizer(opts...)

				isInsecure := regService.IsInsecureRegistry(host.Host)
				if host.Client.Transport != nil && isInsecure {
					host.Client.Transport = httpFallback{super: host.Client.Transport}
				}
				hosts[idx] = host
			}
		}
		return hosts, nil
	})
}

func authorizationCredsFromAuthConfig(authConfig registrytypes.AuthConfig) docker.AuthorizerOpt {
	cfgHost := registry.ConvertToHostname(authConfig.ServerAddress)
	if cfgHost == registry.IndexHostname {
		cfgHost = registry.DefaultRegistryHost
	}

	return docker.WithAuthCreds(func(host string) (string, string, error) {
		if cfgHost != host {
			logrus.WithField("host", host).WithField("cfgHost", cfgHost).Warn("Host doesn't match")
			return "", "", nil
		}
		if authConfig.IdentityToken != "" {
			return "", authConfig.IdentityToken, nil
		}
		return authConfig.Username, authConfig.Password, nil
	})
}

type httpFallback struct {
	super http.RoundTripper
}

func (f httpFallback) RoundTrip(r *http.Request) (*http.Response, error) {
	resp, err := f.super.RoundTrip(r)
	if tlsErr, ok := err.(tls.RecordHeaderError); ok && string(tlsErr.RecordHeader[:]) == "HTTP/" {
		// server gave HTTP response to HTTPS client
		plainHttpUrl := *r.URL
		plainHttpUrl.Scheme = "http"

		plainHttpRequest := *r
		plainHttpRequest.URL = &plainHttpUrl

		return http.DefaultTransport.RoundTrip(&plainHttpRequest)
	}

	return resp, err
}
