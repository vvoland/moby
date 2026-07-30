// Package protogen holds protoc-generated stubs for this point, kept as a
// fixture rather than as part of the framework.
//
// Points no longer need generated code: the daemon and the SDK derive a point's
// wire form from its Go types. What is still worth having is a generated client,
// because that is what an external caller actually uses -- an extension author
// in another language, or a client dialling an exposed service on docker.sock.
// Tests use these stubs from that side, so the derived server is exercised
// against a real generated peer instead of only against itself.
package protogen
