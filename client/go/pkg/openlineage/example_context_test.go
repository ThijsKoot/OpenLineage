package openlineage_test

import (
	"context"
	"errors"
	"log/slog"

	ol "github.com/OpenLineage/openlineage/client/go/pkg/openlineage"
	"github.com/OpenLineage/openlineage/client/go/pkg/transport"
)

func ExampleRunContext() {
	ctx := context.Background()

	cfg := ol.ClientConfig{
		Transport: transport.Config{
			Type: transport.TransportTypeConsole,
			Console: transport.ConsoleConfig{
				PrettyPrint: true,
			},
		},
	}
	client, err := ol.NewClient(cfg)
	if err != nil {
		slog.Error("ol.NewClient failed", "error", err)
	}

	ctx, runCtx := client.NewRunContext(ctx, "ingest")
	defer runCtx.Finish()

	runCtx.Event(ol.EventTypeStart).Emit()

	if err := ChildFunction(ctx); err != nil {
		runCtx.RecordError(err)

		slog.Warn("child function failed", "error", err)
	}

}

func ChildFunction(ctx context.Context) error {
	_, childRun := ol.RunContextFromContext(ctx).Child(ctx, "child")
	defer childRun.Finish()

	childRun.Event(ol.EventTypeStart).Emit()

	if err := DoWork(); err != nil {
		childRun.RecordError(err)

		return err
	}

	return nil
}

func DoWork() error {
	return errors.New("did not do work")
}
