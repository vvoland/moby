# Moby threat model

This document defines Moby's security scope and untrusted inputs.

Moby is the open-source container engine.
Its daemon, `dockerd`, usually runs as root, manages containers through containerd and runc, and exposes the Docker Engine REST API.

## Trust boundaries and untrusted input

Engine API access grants the daemon user's privileges: root by default, or an unprivileged user in rootless mode.
Clients can bind-mount `/`, start privileged containers, or add capabilities by design.
A bug that needs full API access is not a vulnerability, unless it bypasses the restrictions of an authorization (AuthZ) plugin or gains more privileges than the daemon's user has.

The client sets the container policy: privileges, capabilities, mounts, namespaces, devices, published ports, and security options.
The daemon doesn't limit what clients can ask for, but it must enforce every requested and default restriction.
It must never grant more than requested.
Isolation weaker than requested, or weaker than the defaults, is in scope even without a working escape.
For example: missing seccomp or AppArmor profiles, capabilities not dropped, masked or read-only paths skipped, a wrong SELinux label, or no-new-privileges dropped.

Image content is different.
Layers and config (labels, environment, `USER`, entrypoint, `VOLUME`, `EXPOSE`, `HEALTHCHECK`, `ONBUILD`) are attacker-controlled, even when a trusted client asks for the image.
Image config can control the workload inside the container.
It must not make host-side choices the client didn't ask for, such as host paths, mount sources, devices, capabilities, or host port bindings.

We treat these as untrusted:

- Processes in containers with default settings (no `--privileged`, default seccomp and AppArmor profiles, default capabilities, no host namespaces or sensitive bind mounts).
  Escapes and isolation bypasses that affect the host or access other containers are in scope.
  This includes symlink and TOCTOU races in container filesystem operations: `docker cp`, archive and export endpoints, volume and bind mount setup, and `docker commit`.
- Container and image files the daemon reads, such as `/etc/passwd` and `/etc/group` when resolving `USER`, `--user`, `--group-add`, and `--chown` (`daemon/oci_linux.go`, `daemon/exec.go`, `daemon/archive_tarcopyoptions_unix.go`, `daemon/builder/dockerfile/internals_linux.go`, `daemon/internal/usergroup`).
- Images and registries: manifests, configs, layer tarballs, and registry HTTP responses from `docker pull`, and image tarballs for `docker load`.
  Layer extraction that writes outside its target is in scope, and so is copying image content into `VOLUME` volumes.
- Build inputs: Dockerfiles and contexts for the classic builder (`daemon/builder`) and the BuildKit integration (`daemon/internal/builder-next`).
- Traffic from containers and remote hosts: the embedded DNS resolver (`daemon/libnetwork/resolver.go`), `docker-proxy` (`cmd/docker-proxy`), and iptables and nftables rules for isolation and port publishing (`daemon/libnetwork`).
  Bypasses include reaching unpublished ports from blocked networks, crossing isolated networks, or reaching localhost-bound ports from remote hosts.
  Allowed traffic is not a vulnerability, for example between containers on a bridge with inter-container communication enabled.
- Container logs parsed by logging drivers (`daemon/logger`).
- API clients restricted by an AuthZ plugin (`pkg/authorization`): bypassing the plugin's restrictions is in scope.
- Swarm nodes and network peers without valid join tokens or certificates (`daemon/cluster`).
- Rootless API clients and containers (`contrib/dockerd-rootless.sh`, `daemon/internal/rootless`): gaining more privileges than the daemon's unprivileged user has, such as host root, is in scope.
- Containers with user-namespace remapping (`--userns-remap`): escaping to an unmapped host user, such as root, is in scope.
- Local unprivileged host users: they must not traverse or read the data root (`/var/lib/docker`), `/run/docker`, or container filesystems, run programs from them, or connect to sockets they have no permission for.

Crashing, hanging, or exhausting the daemon from a container, or with image or registry content, is in scope.

## Components

Highest priority:

- Container isolation: OCI spec generation (`daemon/oci_linux.go`, `daemon/pkg/oci`), seccomp, AppArmor, capabilities, devices, mounts (`daemon/volume`), and exec.
- Container filesystem operations: `daemon/archive*.go`, layers and images (`daemon/images`, `daemon/containerd`, `daemon/internal/layer`, `daemon/internal/image`), and pulls (`daemon/internal/distribution`, `daemon/pkg/registry`).
- API server, routing, and request validation (`daemon/server`).
- Networking (`daemon/libnetwork`, `cmd/docker-proxy`).

Lower priority, but still in scope:

- Go client (`client/`) and API types (`api/`) talking to a malicious or compromised daemon.
- Swarm mode.
- Plugin installation and privileges (`daemon/pkg/plugin`, `pkg/plugins`): a plugin must not get more privileges than it declared and the user granted.

Vendored modules from the moby GitHub organization (`vendor/github.com/moby/`), such as SwarmKit, `go-archive`, `sys`, and `profiles`, are in scope.
Their issues get the priority of the Moby component that uses the affected code.
BuildKit is the exception: report BuildKit issues to BuildKit directly, under its own threat model.
Here, only the Moby-specific BuildKit integration (`daemon/internal/builder-next`) is in scope, such as how Moby configures BuildKit or exposes it through the Engine API.

Out of scope:

- Other vendored code (for example containerd and `golang.org/x/...` modules).
  Report bugs upstream, unless they're caused by how Moby uses the dependency.
- Bugs in containerd, runc, crun, or the Linux kernel, and the shared-kernel container model itself; stronger isolation needs a VM-based runtime.
  Moby is responsible only for issues caused by its OCI specs or use of these components.
- Attacks that need malicious or compromised plugins.
  Managed and legacy plugins, AuthZ plugins, logging, volume, and network drivers, and NRI plugins (`daemon/internal/nri`) are trusted.
- Tampering that needs write access to `daemon.json`, the data root, daemon binaries, or other files writable only by the daemon's user.
- Tests and helpers (`integration/`, `integration-cli/`, `*_test.go`, `internal/testutil`), `hack/`, `contrib/` (except the rootless scripts), and documentation.
- Windows-only issues.
- Expected effects of settings documented as insecure, such as TCP API access without TLS, `--privileged`, `--security-opt seccomp=unconfined`, or Docker socket mounts.
  Bypassing the restrictions that remain is still in scope: disabling seccomp doesn't disable mount isolation, capability restrictions, or AppArmor.

## Vulnerabilities and bugs

A bug is a vulnerability only when one of the untrusted inputs above can use it to:

- escape or weaken isolation requested by the client or set by the defaults,
- gain privileges beyond those it was given,
- access the host or other containers without permission,
- leak credentials or other secrets,
- crash, hang, or exhaust the daemon.

Everything else is a regular bug.
This includes anything that requires access to the Docker socket or an equivalent API endpoint, or privileged host access such as root or write access to the data root (`/var/lib/docker`); such access is already considered a compromise.
AuthZ plugin bypasses and gaining privileges beyond the daemon's user are the exceptions.
Report regular bugs in public issues as described in `CONTRIBUTING.md`, and report privately when in doubt.

Don't report these as vulnerabilities:

- Findings limited to unit tests, direct calls to internal functions, or unreachable code.
- Expected privileges from API access, or weaker isolation from `--privileged`, host namespaces, `CAP_SYS_ADMIN`, or socket mounts.
- Containers without resource limits using up host CPU, memory, disk, or PIDs; setting limits is up to the client.
  Not applying requested limits is in scope.
- Malicious binaries, vulnerable libraries, or secrets in images; only images that trick the daemon are in scope.
- Sharing between API clients: images, layers, build cache, volumes, and networks are not isolated per client, so an image pulled with one client's credentials is available to all clients.
  Bypassing an AuthZ plugin's restrictions on this is still in scope.
- Wrong cache matches from tampered `--cache-from` sources; cache sources must be trusted by the user.
- `client` or `api` panics or errors caused by invalid arguments from the calling program.
- Secrets in registry or plugin errors passed through by the daemon.
  Credentials the daemon itself writes to logs or errors are in scope.
- Outdated dependencies without a reachable, demonstrated impact on Moby.
- Deprecated components (for example `integration-cli/`) or behavior documented as insecure.

## Reporting

Report privately to security@docker.com as described in `SECURITY.md`.
Don't open public issues.
Show a reachable path in a running `dockerd` (API request, CLI command, container process, image, or network traffic) and the security impact.
