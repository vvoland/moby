package sdk

import (
	"encoding/json"
	"testing"

	"github.com/moby/moby/v2/internal/extensions"
	"gotest.tools/v3/assert"
	is "gotest.tools/v3/assert/cmp"
)

// TestStartupConfigWireForm pins the bytes the daemon writes to an extension's
// stdin.
//
// This is the first thing any out-of-process extension has to parse, and an
// extension written in another language parses it from the description in
// docs/PROTOCOL.md rather than from this struct. A field renamed here, or a tag
// changed, would break every such extension silently -- the daemon would still
// launch them, they would just not find their endpoint. So the encoding is
// asserted rather than left to whatever the tags happen to say.
func TestStartupConfigWireForm(t *testing.T) {
	got, err := json.Marshal(StartupConfig{
		Endpoint:         "/run/docker/extensions/org.example.myext.v1.sock",
		ProtocolVersion:  ProtocolVersion,
		Config:           extensions.Config{"key": "value"},
		CallbackEndpoint: "/run/docker/extensions/callback.sock",
	})
	assert.NilError(t, err)

	const want = `{` +
		`"endpoint":"/run/docker/extensions/org.example.myext.v1.sock",` +
		`"protocolVersion":1,` +
		`"config":{"key":"value"},` +
		`"callbackEndpoint":"/run/docker/extensions/callback.sock"` +
		`}`
	assert.Equal(t, string(got), want)
}

// TestStartupConfigOmitsAbsentFields checks the shape an extension sees when the
// daemon has no configuration for it and offers it no dependencies, which is the
// common case and the one a new extension is written against first.
func TestStartupConfigOmitsAbsentFields(t *testing.T) {
	got, err := json.Marshal(StartupConfig{
		Endpoint:        "/run/docker/extensions/org.example.myext.v1.sock",
		ProtocolVersion: ProtocolVersion,
	})
	assert.NilError(t, err)
	assert.Equal(t, string(got),
		`{"endpoint":"/run/docker/extensions/org.example.myext.v1.sock","protocolVersion":1}`)
}

// TestReadinessAck pins the line an extension writes to stdout once it is
// listening. The daemon compares it exactly, so a trailing-newline change here
// would hang every launch until the readiness timeout.
func TestReadinessAck(t *testing.T) {
	assert.Check(t, is.Equal(ReadinessAck, "ready\n"))
}
