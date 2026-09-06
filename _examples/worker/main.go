package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/kgantsov/celerity"
)

// shutdownTimeout bounds how long we wait for all components (broker,
// dispatcher, workers, task loop) to finish once a shutdown signal is
// received, so a second Ctrl+C or a hung dependency can never block the
// process from exiting.
const shutdownTimeout = 10 * time.Second

func main() {
	ctx, stop := signal.NotifyContext(
		context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	celerity := celerity.NewCelerity(
		"amqp://guest:guest@localhost:5672/",
		[]string{"celery"},
		celerity.WithWorkers(5),
		celerity.WithPrefetchCount(5),
		celerity.WithAcksLate(true),
	)

	celerity.RegisterTask(
		"hello.add", AddTask, "a", "b",
	)
	celerity.RegisterTask(
		"hello.update_metadata", UpdateMetadataTask, "asset_id", "mode", "user_id", "metadata",
	)

	log.Println("Starting task processor loop...")
	celerity.Start(ctx)

	<-ctx.Done()
	// Stop relaying further signals so a second Ctrl+C falls back to Go's
	// default (immediate) signal handling instead of being swallowed.
	stop()
	log.Println("Shutdown signal received, stopping gracefully...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	celerity.Stop(shutdownCtx)
	log.Println("Shutdown complete.")
}

func AddTask(a, b int) (int, error) {
	return a + b, nil
}

func UpdateMetadataTask(
	assetID string, mode string, user_id string, metadata map[string]any,
) error {
	log.Printf(
		"Updating metadata for assetID: %s, mode: %s, user_id: %s, metadata: %v\n",
		assetID,
		mode,
		user_id,
		metadata,
	)
	time.Sleep(5 * time.Second) // Simulate some processing time
	// return fmt.Errorf("Simulated error for assetID: %s", assetID) // Uncomment to simulate an error
	return nil
}
