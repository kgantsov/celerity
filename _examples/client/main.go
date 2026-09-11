package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/kgantsov/celerity"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	client := celerity.NewClient(
		"amqp://guest:guest@localhost:5672/",
		celerity.WithClientLogger(logger),
	)
	if err := client.Connect(); err != nil {
		logger.Error("failed to connect", "err", err)
		os.Exit(1)
	}
	defer client.Close()

	id, err := client.Publish(context.Background(), "hello.add", celerity.PublishOptions{
		Args:  []any{5, 3},
		Queue: "celery",
	})
	if err != nil {
		logger.Error("failed to publish", "err", err)
		os.Exit(1)
	}

	logger.Info("published task", "id", id)
}
