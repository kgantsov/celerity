# Celerity

A Go implementation of a [Celery](https://docs.celeryq.dev/)-compatible task queue worker. Celerity consumes Celery v1 messages from RabbitMQ and dispatches them to registered Go handler functions, letting Python Celery producers talk to Go consumers.

## Features

- Consumes tasks published by Python Celery (v1 message format)
- Positional and keyword argument support with automatic type coercion from JSON
- Configurable worker pool and AMQP prefetch count
- Optional late acknowledgement (`AcksLate`)
- Automatic retries with configurable per-task retry limit
- Graceful shutdown with bounded timeouts

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

## Configuration

`NewCelerity` accepts functional options:

| Option | Default | Description |
|---|---|---|
| `WithWorkers(n)` | `5` | Number of concurrent worker goroutines |
| `WithPrefetchCount(n)` | `5` | AMQP QoS prefetch count |
| `WithAcksLate(bool)` | `false` | Acknowledge messages after the handler returns instead of on delivery |

## Registering tasks

```go
c.RegisterTask("task.name", HandlerFunc, []string{"param1", "param2"})
```

- The parameter names must match the kwargs keys sent by the Python producer.
- Arguments are matched positionally first, then by name from kwargs.
- JSON numeric types (`float64`) are automatically coerced to the Go parameter type (`int`, `float32`, etc.).
- Complex types (`map[string]any`, structs, slices) are handled via JSON round-trip.

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
celerity.go              # Public API: NewCelerity, Start, Stop, RegisterTask
internal/
  broker/                # RabbitMQ AMQP consumer with auto-reconnect
  protocol/celeryv2/     # Celery v1 message parser
  registry/              # Task name → handler function mapping (reflection-based)
  worker/                # Dispatcher + worker pool
  task/                  # Task struct
_examples/worker/        # Runnable example
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
