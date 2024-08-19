package openlineage

import (
	"context"
	"fmt"

	"github.com/OpenLineage/openlineage/client/go/pkg/transport"
	"github.com/google/uuid"
)

var DefaultClient, _ = NewClient(ClientConfig{
	Transport: transport.Config{
		Type: transport.TransportTypeConsole,
		Console: transport.ConsoleConfig{
			PrettyPrint: true,
		},
	},
})

type ClientConfig struct {
	Transport transport.Config

	// Namespace for events. Defaults to "default"
	Namespace string

	// When true, OpenLineage will not emit events (default: false)
	Disabled bool
}

func NewClient(cfg ClientConfig) (*Client, error) {
	if cfg.Disabled {
		return &Client{
			disabled: true,
		}, nil
	}

	transport, err := transport.New(cfg.Transport)
	if err != nil {
		return nil, fmt.Errorf("create transport: %w", err)
	}

	namespace := cfg.Namespace
	if cfg.Namespace == "" {
		namespace = "default"
	}

	return &Client{
		transport: transport,
		namespace: namespace,
	}, nil
}

type Client struct {
	disabled  bool
	transport Transport
	namespace string
}

type Emittable interface {
	AsEmittable() Event
}

func (olc *Client) Emit(ctx context.Context, event Emittable) error {
	if olc.disabled {
		return nil
	}

	return olc.transport.Emit(ctx, event.AsEmittable())
}

func (c *Client) NewRunContext(ctx context.Context, job string) (context.Context, RunContext) {
	rctx := runContext{
		client:       c,
		runID:        uuid.New(),
		jobName:      job,
		jobNamespace: c.namespace,
	}

	parent := RunContextFromContext(ctx)
	if _, isNoop := parent.(*noopRunContext); !isNoop {
		rctx.parent = parent
	}

	return ContextWithRun(ctx, &rctx), &rctx
}

func (c *Client) ExistingRunContext(ctx context.Context, job string, runID uuid.UUID) (context.Context, RunContext) {
	rctx := runContext{
		client:       c,
		runID:        runID,
		jobName:      job,
		jobNamespace: c.namespace,
	}

	parent := RunContextFromContext(ctx)
	if _, isNoop := parent.(*noopRunContext); !isNoop {
		rctx.parent = parent
	}

	return ContextWithRun(ctx, &rctx), &rctx
}
