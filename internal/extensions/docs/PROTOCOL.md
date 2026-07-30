# Extensions — the out-of-process protocol

This is what an extension binary has to implement. It is written for someone
working outside Go: nothing here requires the Go SDK, and everything the daemon
sends or expects is specified rather than left to be read off a struct.

An extension written in Go does not need this — `internal/extensions/sdk`
implements all of it — but the Go SDK is one implementation of this protocol,
not the definition of it.

There are four things to get right: the file name, the startup handshake, the
runtime service, and the point's own service.

## 1. The binary's name is the extension id

The daemon scans its extension directory and treats each file's name as an
extension id. The binary must be named exactly that:

```
/usr/libexec/docker/moby-extensions/org.example.myext.v1
```

A file whose name is not a well-formed extension id is skipped, with a warning
in the daemon log and nothing else — so an extension named after its package
rather than its id simply never loads. A file whose name *is* a valid id but
does not match the id the binary reports at `Describe` is a hard error that
fails daemon startup.

The directory is a root-code-execution boundary. The daemon refuses
world-writable files and files owned by a user other than root or itself.

## 2. Startup handshake

On launch the daemon writes one JSON object to the extension's **stdin** and
closes it:

```json
{
  "endpoint": "/run/docker/extensions/org.example.myext.v1.sock",
  "protocolVersion": 1,
  "config": {"key": "value"},
  "callbackEndpoint": "/run/docker/extensions/callback.sock"
}
```

| field | meaning |
|---|---|
| `endpoint` | the Unix socket path the extension must listen on |
| `protocolVersion` | currently `1`; an extension must refuse anything else |
| `config` | the extension's own configuration, verbatim from the `extension-config` entry keyed by its id in `daemon.json`. Absent when none is configured. |
| `callbackEndpoint` | a Unix socket where the daemon serves the points this extension declared as dependencies. Absent when it has none. |

`config` and `callbackEndpoint` are omitted entirely when empty, so an extension
must treat them as optional. The exact encoding is pinned by
`TestStartupConfigWireForm`.

The extension then listens on `endpoint` and writes exactly this to **stdout**:

```
ready\n
```

The daemon compares that line exactly and gives up after five seconds. Nothing
else may be written to stdout before it; anything written after it is logged,
as is everything on stderr.

The readiness line means *listening*, not *ready to serve*. Start-up work
belongs in `Initialize`, which is not time-bounded by that budget.

## 3. The runtime service

The extension serves `internal/extensions/sdk/sdkpb/runtime.proto` on
`endpoint`, over ordinary gRPC:

- **`Describe`** — returns the extension's declaration: its id, the points it
  provides, the gRPC service names it serves for each, its dependencies, and any
  extensions it conflicts with. The id must equal the binary's file name.
- **`Initialize`** — called once, after every extension has been launched and
  described, in dependency order. Dependencies are reachable by the time it is
  called, so this is where start-up work goes.

## 4. The point's own service

For each point it provides, the extension serves that point's gRPC service on
the same socket. Generate stubs from the point's `.proto`, which is checked in
beside its Go contract, for example
`internal/extpoints/volumedriver/v0/volume_driver.proto`.

That file is rendered from the Go contract and compared against it by a test, so
it cannot drift from what the daemon speaks. The traffic is ordinary gRPC with
the standard `application/grpc+proto` content type — the daemon derives its own
side from Go types rather than from generated stubs, but the bytes are identical,
which `TestWireCompatibility` checks in both directions against real
`protoc`-generated code.

## Calling back into the daemon

If the extension declared dependencies, it dials `callbackEndpoint` and calls the
point's service there exactly as the daemon calls it: the daemon routes the call
to whichever provider actually implements it, in this process or another.

## Shutdown

The daemon signals the process (`SIGTERM` on Unix) and waits five seconds before
killing it. An extension should stop serving and exit on that signal.

## What is not covered yet

- No health checking, reconnection, or restart. If the process dies, calls fail
  until the daemon restarts.
- The declaration carries no contract fingerprint, so two peers that both claim
  a point id are assumed to agree about its schema.
- Loading is all-or-nothing: an extension that fails to launch, describe, or
  initialize fails daemon startup rather than being skipped.
