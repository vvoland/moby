package volumedriverv0_test

import (
	"os"
	"testing"

	volumedriverv0 "github.com/moby/moby/v2/internal/extpoints/volumedriver/v0"
	"gotest.tools/v3/assert"
	"gotest.tools/v3/golden"
)

const schemaFile = "volume_driver.proto"

// TestSchemaMatchesContract keeps the published .proto in step with the Go
// contract, so a driver author generating stubs from it gets stubs that work.
// Re-run with -update after an intentional contract change.
func TestSchemaMatchesContract(t *testing.T) {
	got := volumedriverv0.Contract.Proto()
	if golden.FlagUpdate() {
		assert.NilError(t, os.WriteFile(schemaFile, []byte(got), 0o644))
		return
	}
	want, err := os.ReadFile(schemaFile)
	assert.NilError(t, err)
	assert.Equal(t, got, string(want),
		"%s is out of date with the Go contract; re-run with -update", schemaFile)
}
