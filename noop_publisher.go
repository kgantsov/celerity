package celerity

import "github.com/kgantsov/celerity/internal/broker"

type noopPublisher struct{}

func (noopPublisher) PublishMessage(_ *broker.RawMessage) error { return nil }
func (noopPublisher) Close()                                    {}
