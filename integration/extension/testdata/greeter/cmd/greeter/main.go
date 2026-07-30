// Command greeter serves the greeter fixture as an out-of-process extension
// that opts into socket exposure, for the integration test.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/moby/moby/v2/integration/extension/testdata/greeter"
	"github.com/moby/moby/v2/internal/extensions/sdk"
	servicegrpcv0 "github.com/moby/moby/v2/internal/extpoints/servicegrpc/v0"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// The SDK serves all provider gRPC services on the extension socket. Because
	// this provider is service.grpc, the daemon publishes those service names on
	// the API socket.
	srv := sdk.NewServer()
	if err := srv.Register(greeter.Extension, servicegrpcv0.ServerPoint); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := srv.Listen(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
