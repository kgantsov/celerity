package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
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
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	ctx, stop := signal.NotifyContext(
		context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	c := celerity.NewCelerity(
		"amqp://guest:guest@localhost:5672/",
		[]string{"celery"},
		celerity.WithWorkers(5),
		celerity.WithPrefetchCount(5),
		celerity.WithAcksLate(true),
		celerity.WithLogger(logger),
		celerity.WithBackendURL("amqp://guest:guest@localhost:5672/"),
	)

	c.RegisterTask("hello.add", AddTask, []string{"a", "b"})
	c.RegisterTask("hello.append", AppendTask, []string{"a", "b"})
	c.RegisterTask("hello.reindex_file", ReindexFileTask, []string{"file_id"})
	c.RegisterTask(
		"hello.update_metadata",
		UpdateMetadataTask,
		[]string{"assetID", "mode", "user_id", "metadata"},
	)

	logger.Info("starting task processor")
	if err := c.Start(ctx); err != nil {
		logger.Error("failed to start", "err", err)
		os.Exit(1)
	}

	<-ctx.Done()
	// Stop relaying further signals so a second Ctrl+C falls back to Go's
	// default (immediate) signal handling instead of being swallowed.
	stop()
	logger.Info("shutdown signal received, stopping gracefully")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	c.Stop(shutdownCtx)
	logger.Info("shutdown complete")
}

func AddTask(a, b int) (int, error) {
	return a + b, nil
}

func AppendTask(a []string, b string) ([]string, error) {
	return append(a, b), nil
}

func UpdateMetadataTask(
	ctx context.Context,
	assetID string, mode string, userID string, metadata map[string]any,
) error {
	log := celerity.Logger(ctx)
	log.Info("updating metadata", "asset_id", assetID, "mode", mode, "user_id", userID)
	time.Sleep(5 * time.Second) // Simulate some processing time
	return &celerity.RetryError{
		Err:        fmt.Errorf("simulated error for assetID: %s", assetID),
		MaxRetries: 3,
	}
}

func ReindexFileTask(ctx context.Context, fileID string) error {
	log := celerity.Logger(ctx)
	log.Info("reindexing file", "file_id", fileID)
	time.Sleep(3 * time.Second) // Simulate some processing time
	log.Info("reindexing complete", "file_id", fileID)
	return nil
}
