// Package golden holds protoc-generated code for the create-spec point, kept
// only as a test fixture.
//
// The wire package's claim is that deriving a point's contract from its Go types
// produces exactly what protoc would have produced. Checking that claim needs
// something protoc actually produced, so one point's generated output is kept
// here, frozen, and compared against the derived form -- messages, descriptors,
// and service name. It is deliberately unreachable from the daemon: nothing
// outside this package's tests imports it, and no point depends on generated
// code any more.
//
// Do not rewrite paths inside it. The generated file embeds the file descriptor
// as a length-prefixed byte string, so a search-and-replace that changes a path
// in those bytes without fixing the lengths corrupts the descriptor, and the
// package panics at init rather than failing to compile. The stale-looking
// go_package path in there is part of the frozen artifact and is correct.
//
// Regenerating it is not part of any build. If the create-spec contract changes,
// this fixture goes stale and the comparison tests fail, which is the intended
// signal: the wire format of a released point is not supposed to change.
package golden
