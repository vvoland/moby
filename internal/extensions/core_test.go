package extensions

import (
	"testing"

	"gotest.tools/v3/assert"
)

func TestDefinePointValidatesID(t *testing.T) {
	valid := []PointID{
		"org.mobyproject.extension.volume.driver.v1",
		"org.mobyproject.extension.container.create_hook.v0",
		"com.docker.compose.api.v12",
		"a.b.v0",
	}
	for _, id := range valid {
		t.Run("valid/"+string(id), func(t *testing.T) {
			assert.Equal(t, DefinePoint[any](id).ID(), id)
		})
	}

	invalid := []PointID{
		"",
		"org.mobyproject.greeter",            // no version
		"greeter.v1",                         // only one segment before the version
		"org.mobyproject.extension.v1.thing", // version not last
		"Org.Mobyproject.Greeter.v1",         // uppercase
		"org.mobyproject.greeter.v",          // no version number
	}
	for _, id := range invalid {
		t.Run("invalid/"+string(id), func(t *testing.T) {
			assert.Assert(t, panics(func() { DefinePoint[any](id) }), "DefinePoint(%q) should panic", id)
		})
	}
}

// panics reports whether f panics.
func panics(f func()) (panicked bool) {
	defer func() { panicked = recover() != nil }()
	f()
	return false
}

func TestValidateExtensionID(t *testing.T) {
	valid := []ExtensionID{
		"org.example.no-privileged.v1",
		"com.docker.compose.v1",
		"com.docker.mobyextension.nri.v1",
		"org.example.s3-volume.v2",
		"org.mobyproject.example.greeter.v0",
	}
	for _, id := range valid {
		if err := ValidateExtensionID(id); err != nil {
			t.Errorf("ValidateExtensionID(%q) = %v, want nil", id, err)
		}
	}

	invalid := []ExtensionID{
		"",
		"single",                     // not reverse-DNS (one segment)
		"org.example.no-privileged",  // missing version segment
		"com.docker.compose",         // missing version segment
		"foo.v1",                     // version but only one name segment
		"Org.Example.Ext.v1",         // uppercase
		"org.example/evil.v1",        // path separator
		"org.example.../etc",         // path traversal shape
		"org.example.-bad.v1",        // segment leads with hyphen
		"org.example.bad-.v1",        // segment trails with hyphen
		"org.example.a b.v1",         // whitespace
		"org..example.v1",            // empty segment
		"org.example.under_score.v1", // underscore not allowed in extension ids
	}
	for _, id := range invalid {
		if err := ValidateExtensionID(id); err == nil {
			t.Errorf("ValidateExtensionID(%q) = nil, want error", id)
		}
	}
}
