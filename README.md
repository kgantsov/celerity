# Celerity

A Go implementation of a [Celery](https://docs.celeryq.dev/)-compatible task queue worker. Celerity consumes Celery v2 messages from RabbitMQ and dispatches them to registered Go handler functions, letting Python Celery producers talk to Go consumers.

## Features

- Consumes tasks published by Python Celery (v2 message format)
- Publishes tasks to Celery queues from Go (broker URL auto-selects the backend)
- Positional and keyword argument support with automatic type coercion from JSON
- Configurable worker pool and AMQP prefetch count
- Optional late acknowledgement (`AcksLate`)
- Automatic retries with configurable per-task retry limit
- Graceful shutdown with bounded timeouts
- Structured logging via `log/slog` with task-scoped context

## Requirements

- Go 1.22+
- RabbitMQ (via Docker or standalone)

## Quick start

**1. Start RabbitMQ:**

```bash
docker-compose up -d
```

**2. Register tasks and start the worker:**

```go
package main

import (
    "context"
    "log"
    "os/signal"
    "syscall"
    "time"

    "github.com/kgantsov/celerity"
)

func main() {
    ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer stop()

    c := celerity.NewCelerity(
        "amqp://guest:guest@localhost:5672/",
        []string{"celery"},
        celerity.WithWorkers(5),
        celerity.WithPrefetchCount(5),
        celerity.WithAcksLate(true),
    )

    c.RegisterTask("hello.add", AddTask, []string{"a", "b"})

    c.Start(ctx)
    <-ctx.Done()
    stop()

    shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()
    c.Stop(shutdownCtx)
}

func AddTask(a, b int) (int, error) {
    return a + b, nil
}
```

**3. Submit tasks from Python:**

```python
from celery import Celery

app = Celery('hello', broker='amqp://guest:guest@localhost:5672//')

@app.task
def add(a, b):
    return a + b

add.delay(5, 3)
```

See [`_examples/worker/`](./_examples/worker/) for a full working example.

## Publishing tasks from Go

Use `Client` to enqueue tasks without running a worker. The broker URL determines the backend (currently `amqp://` / `amqps://`):

```go
client := celerity.NewClient("amqp://guest:guest@localhost:5672/")
if err := client.Connect(); err != nil {
    log.Fatal(err)
}
defer client.Close()

id, err := client.Publish(ctx, "hello.add", celerity.PublishOptions{
    Args:  []any{5, 3},
    Queue: "celery", // defaults to "celery" if omitted
})
```

`PublishOptions` fields:

| Field | Default | Description |
|---|---|---|
| `Queue` | `"celery"` | Destination queue |
| `Args` | `nil` | Positional arguments |
| `Kwargs` | `nil` | Keyword arguments |
| `TaskID` | auto UUID | Celery task ID |

If the connection drops, `Publish` reconnects automatically and retries once before returning an error.

See [`_examples/client/`](./_examples/client/) for a runnable example.

## Configuration

`NewCelerity` accepts functional options:

| Option | Default | Description |
|---|---|---|
| `WithWorkers(n)` | `5` | Number of concurrent worker goroutines |
| `WithPrefetchCount(n)` | `5` | AMQP QoS prefetch count |
| `WithAcksLate(bool)` | `false` | Acknowledge messages after the handler returns instead of on delivery |
| `WithLogger(logger)` | `slog.Default()` | Structured logger used by all internal components |

## Registering tasks

```go
c.RegisterTask("task.name", HandlerFunc, []string{"param1", "param2"})
```

- The parameter names must match the kwargs keys sent by the Python producer.
- Arguments are matched positionally first, then by name from kwargs.
- JSON numeric types (`float64`) are automatically coerced to the Go parameter type (`int`, `float32`, etc.).
- Complex types (`map[string]any`, structs, slices) are handled via JSON round-trip.

## Logging

Celerity uses `log/slog` internally. Pass any `*slog.Logger` via `WithLogger`; all internal components (broker, dispatcher, workers) inherit it with their own `component` attribute pre-set.

```go
logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
c := celerity.NewCelerity(url, queues, celerity.WithLogger(logger))
```

### Logging inside task handlers

If a handler accepts `context.Context` as its first parameter, Celerity injects a logger pre-tagged with the task name and ID. Retrieve it with `celerity.Logger(ctx)`:

```go
func ReindexFileTask(ctx context.Context, fileID string) error {
    log := celerity.Logger(ctx)
    log.Info("reindexing file", "file_id", fileID)
    // every line automatically carries task= and id=
    return nil
}

// paramNames does not include ctx
c.RegisterTask("hello.reindex_file", ReindexFileTask, []string{"file_id"})
```

Handlers that don't need logging can omit `ctx` entirely — the signature is unchanged and the feature is strictly opt-in.

## Retries

To retry a failed task, return a `*celerity.RetryError` from the handler:

```go
func UpdateMetadataTask(assetID string) error {
    if err := elastic.Update(assetID); err != nil {
        return &celerity.RetryError{Err: err, MaxRetries: 3}
    }
    return nil
}
```

The task is republished to the same queue and retried up to `MaxRetries` times. Returning a plain `error` (not wrapped in `RetryError`) will not trigger a retry. Retries require `WithAcksLate(true)`.

## Project layout

```
celerity.go              # Worker API: NewCelerity, Start, Stop, RegisterTask, Logger
client.go                # Publisher API: NewClient, Connect, Publish, Close
internal/
  broker/                # RabbitMQ AMQP consumer + publisher; Publisher/Broker interfaces
  ctxlog/                # Context key for task-scoped logger propagation
  protocol/celery/       # Celery v2 message parser/serialiser
  registry/              # Task name → handler function mapping (reflection-based)
  worker/                # Dispatcher + worker pool
  task/                  # Task struct
_examples/worker/        # Runnable worker example
_examples/client/        # Runnable publisher example
```

## Development

```bash
# Run tests
go test ./...

# Run a single test
go test -run TestName ./internal/...

# Vet
go vet ./...

# Tidy modules
go mod tidy

# Start infrastructure
docker-compose up -d
```

## Architecture

```
RabbitMQ → Broker → Protocol parser → Registry.Execute → handler function
```

The broker spawns one consumer goroutine per queue (Qos=1 by default). The dispatcher fans tasks out to a fixed pool of worker goroutines via a shared job channel. Shutdown is driven by context cancellation and propagates through each layer with bounded timeouts: 5 s for the broker connection, 10 s total for the full shutdown sequence.

## License

MIT
