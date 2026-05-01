seccomp: Block AF_ALG sockets in default profile (CVE-2026-31431)

---

CVE-2026-31431 ("Copy Fail") is a logic flaw in the kernel's `algif_aead` module that allows any unprivileged user with access to `AF_ALG` sockets to perform a controlled 4-byte page-cache write, leading to reliable local privilege escalation. The exploit is a 732-byte Python script that works on every Linux distribution shipped since 2017.

Inside a container, this allows escalation to root within the container by corrupting setuid binaries in the page cache. Since the page cache is shared across the host, corruption of shared image-layer files is also visible to other containers using the same layers on the same node.

- https://copy.fail
- https://xint.io/blog/copy-fail-linux-distributions

## Seccomp profile changes

The previous default seccomp profile allowed `AF_ALG` sockets (only `AF_VSOCK` was denied). This update denies both `AF_ALG` (38) and `AF_VSOCK` (40) by allowing socket creation only for address families outside that range:

- `arg0 < 38` (AF_ALG) → allow
- `arg0 == 39` (the single family between them) → allow
- `arg0 > 40` (AF_VSOCK) → allow
- everything else (38 and 40) → falls through to default ERRNO

The previous socket rule used a single `arg0 != AF_VSOCK` condition. Naively adding a second `OpNotEqual` for AF_ALG does not work: seccomp evaluates multiple argument conditions within a single rule as a logical AND, so `arg0 != 38 AND arg0 != 40` requires two comparisons against the same argument index, which libseccomp does not support reliably in one rule. Splitting into separate deny-action rules also fails because any matching allow rule takes precedence in seccomp's first-match-wins evaluation.

See https://github.com/moby/profiles/pull/20 for more details.

Additionally, `socketcall(2)` is now explicitly denied to prevent bypassing the socket address family filters on architectures with the legacy socketcall multiplexer. See https://github.com/moby/profiles/releases/tag/seccomp%2Fv0.2.2 for details.

## Integration tests

Adds `TestExecSocketDenied` which compiles and runs small C programs inside a container to verify that:

- `AF_ALG` socket creation is denied
- `AF_VSOCK` socket creation is denied
- `AF_ALG` via `socketcall(2)` (using `int $0x80` from amd64) is denied

## Vendor

Bumps `github.com/moby/profiles/seccomp` to [`seccomp/v0.2.2`](https://github.com/moby/profiles/releases/tag/seccomp%2Fv0.2.2).

## Changelog

```markdown changelog

```
