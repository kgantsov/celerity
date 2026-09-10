package ctxlog

import (
	"context"
	"log/slog"
)

type key struct{}

func With(ctx context.Context, logger *slog.Logger) context.Context {
	return context.WithValue(ctx, key{}, logger)
}

func From(ctx context.Context) *slog.Logger {
	if l, ok := ctx.Value(key{}).(*slog.Logger); ok {
		return l
	}
	return slog.Default()
}
