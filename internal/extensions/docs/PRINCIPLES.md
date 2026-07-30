# Extensions — Principles

- **Everything is an extension point.**
  A point is a versioned, namespaced interface id, such as `org.mobyproject.extension.container.create_hook.v1`.
  There is no separate hook concept.
  A hook is just a point the engine calls during one of its flows.
  An extension is anything that implements one or more points.
- **Points are uniform.**
  A point works the same way no matter who defines it or who calls it.
  The same interface, provider model, and routing path are used for engine calls and extension-to-extension calls.
- **Socket exposure is opt-in, and it is also a point.**
  By default, an extension is reachable only inside the daemon.
  An extension can publish its own gRPC services on `docker.sock` by implementing `org.mobyproject.extension.service.grpc.v0`.
  The daemon forwards those services by name.
  It does not need to know their proto files.
  This lets an extension add API surface without making it a built-in daemon API.
  These services are served on the raw gRPC endpoint alongside the daemon's own gRPC services (such as BuildKit), so, like those, they are not gated by authorization plugins — those apply to the REST API.
  An extension that needs access control must enforce it itself.
- **Extensions do not depend on their location.**
  Extension code should not care whether it is compiled into the daemon or runs as a separate process.
  The runtime owns the transport choice.
- **Dependencies are typed handles.**
  A dependency is created from the point it depends on and listed in the extension's declaration.
  Listing it is what binds it, so an extension cannot reach a point it did not declare.
  `Init` receives no resolver, which makes that a structural property rather than a rule.
  A required or optional dependency also orders its providers first; a lazy one does not, which is what lets subsystems that refer to each other be split apart.
  The same handle works in a launched extension, bound to a callback channel to the daemon instead of to the broker.
- **Broker plus dependency injection.**
  Like containerd, extensions register, declare dependencies, and initialize in dependency order.
  Unlike containerd, those dependencies can point to out-of-process extensions without changing the caller.
- **Registration is explicit.**
  There is no package-level `func init()` registration.
  A host chooses what to run by passing extension values to the runtime.
  Importing an extension package does nothing by itself.
  This keeps the active set clear, testable, and free of import-order side effects.
- **Extensions replace legacy plugins.**
  Network, volume, and log drivers become extension points.
  The old plugin system goes away.
