package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/kgantsov/celerity"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	const brokerURL = "amqp://guest:guest@localhost:5672/"

	client := celerity.NewClient(
		brokerURL,
		celerity.WithClientLogger(logger),
		celerity.WithClientBackendURL(brokerURL),
	)
	if err := client.Connect(); err != nil {
		logger.Error("failed to connect", "err", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	defer client.Close(ctx)

	task, err := client.Publish(ctx, "hello.add", celerity.PublishOptions{
		Args:  []any{5, 3},
		Queue: "celery",
	})
	if err != nil {
		logger.Error("failed to publish", "err", err)
		os.Exit(1)
	}

	logger.Info("published task", "id", task.ID)

	result, err := celerity.GetResult[int](ctx, task)
	if err != nil {
		logger.Error("failed to get result", "err", err)
		os.Exit(1)
	}

	logger.Info("task result", "id", task.ID, "result", result)
}
