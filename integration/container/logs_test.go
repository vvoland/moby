package container // import "github.com/docker/docker/integration/container"

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/daemon/logger/jsonfilelog"
	"github.com/docker/docker/daemon/logger/local"
	"github.com/docker/docker/integration/internal/container"
	"github.com/docker/docker/pkg/stdcopy"
	"gotest.tools/v3/assert"
	"gotest.tools/v3/poll"
	"gotest.tools/v3/skip"
)

// Regression test for #35370
// Makes sure that when following we don't get an EOF error when there are no logs
func TestLogsFollowTailEmpty(t *testing.T) {
	// FIXME(vdemeester) fails on a e2e run on linux...
	skip.If(t, testEnv.IsRemoteDaemon)
	defer setupTest(t)()
	client := testEnv.APIClient()
	ctx := context.Background()

	id := container.Run(ctx, t, client, container.WithCmd("sleep", "100000"))

	logs, err := client.ContainerLogs(ctx, id, types.ContainerLogsOptions{ShowStdout: true, Tail: "2"})
	if logs != nil {
		defer logs.Close()
	}
	assert.Check(t, err)

	_, err = stdcopy.StdCopy(io.Discard, io.Discard, logs)
	assert.Check(t, err)
}

func TestLogs(t *testing.T) {
	drivers := []string{local.Name, jsonfilelog.Name}

	for _, logDriver := range drivers {
		t.Run("driver "+logDriver, func(t *testing.T) {
			testLogs(t, logDriver)
		})
	}
}

func testLogs(t *testing.T, logDriver string) {
	defer setupTest(t)()
	client := testEnv.APIClient()
	ctx := context.Background()

	testCases := []struct {
		desc        string
		logOps      types.ContainerLogsOptions
		expectedOut string
		expectedErr string
		tty         bool
	}{
		// TTY, only one output streamś
		{
			desc: "tty/stdout and stderr",
			tty:  true,
			logOps: types.ContainerLogsOptions{
				ShowStdout: true,
				ShowStderr: true,
			},
			expectedOut: "this is fineaccidents happen",
		},
		{
			desc: "tty/only stdout",
			tty:  true,
			logOps: types.ContainerLogsOptions{
				ShowStdout: true,
				ShowStderr: false,
			},
			expectedOut: "this is fineaccidents happen",
		},
		{
			desc: "tty/only stderr",
			tty:  true,
			logOps: types.ContainerLogsOptions{
				ShowStdout: false,
				ShowStderr: true,
			},
			expectedOut: "",
		},
		// Without TTY, both stdout and stderrś
		{
			desc: "without tty/stdout and stderr",
			tty:  false,
			logOps: types.ContainerLogsOptions{
				ShowStdout: true,
				ShowStderr: true,
			},
			expectedOut: "this is fine",
			expectedErr: "accidents happen",
		},
		{
			desc: "without tty/only stdout",
			tty:  false,
			logOps: types.ContainerLogsOptions{
				ShowStdout: true,
				ShowStderr: false,
			},
			expectedOut: "this is fine",
			expectedErr: "",
		},
		{
			desc: "without tty/only stderr",
			tty:  false,
			logOps: types.ContainerLogsOptions{
				ShowStdout: false,
				ShowStderr: true,
			},
			expectedOut: "",
			expectedErr: "accidents happen",
		},
	}

	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			t.Parallel()
			tty := tC.tty
			id := container.Run(ctx, t, client,
				container.WithCmd("sh", "-c", "echo -n this is fine; echo -n accidents happen >&2"),
				container.WithTty(tty),
				container.WithLogDriver(logDriver),
			)
			defer client.ContainerRemove(ctx, id, types.ContainerRemoveOptions{Force: true})

			poll.WaitOn(t, container.IsStopped(ctx, client, id), poll.WithDelay(time.Millisecond*100))

			logs, err := client.ContainerLogs(ctx, id, tC.logOps)
			assert.NilError(t, err)
			defer logs.Close()

			// TTY, only one output stream
			if tty {
				out := new(bytes.Buffer)

				_, err = io.Copy(out, logs)
				assert.NilError(t, err)
				assert.DeepEqual(t, out.String(), tC.expectedOut)
			} else {
				stdout := new(bytes.Buffer)
				stderr := new(bytes.Buffer)

				_, err = stdcopy.StdCopy(stdout, stderr, logs)
				assert.NilError(t, err)
				assert.DeepEqual(t, stdout.String(), tC.expectedOut)
				assert.DeepEqual(t, stderr.String(), tC.expectedErr)
			}
		})
	}
}
