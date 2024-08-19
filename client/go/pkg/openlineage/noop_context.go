package openlineage

import (
	"bytes"
	"context"

	"github.com/google/uuid"
)

var _ RunContext = (*noopRunContext)(nil)

type noopRunContext struct{}

// HasFailed implements RunContext.
func (n *noopRunContext) HasFailed() bool {
	return false
}

// Child implements RunContext.
func (n *noopRunContext) Child(ctx context.Context, jobName string) (context.Context, RunContext) {
	return ctx, &noopRunContext{}
}

// Finish implements RunContext.
func (n *noopRunContext) Finish() {}

// Emit implements RunContext.
func (n *noopRunContext) Emit(context.Context, Emittable) error {
	return nil
}

// RecordError implements RunContext.
func (n *noopRunContext) RecordError(error) {}

// Event implements RunContext.
func (n *noopRunContext) Event(EventType) *RunEvent {
	return &RunEvent{}
}

// JobName implements RunContext.
func (n *noopRunContext) JobName() string {
	return ""
}

// JobNamespace implements RunContext.
func (n *noopRunContext) JobNamespace() string {
	return ""
}

// Parent implements RunContext.
func (n *noopRunContext) Parent() RunContext {
	return &noopRunContext{}
}

// RunID implements RunContext.
func (n *noopRunContext) RunID() uuid.UUID {
	empty := bytes.Repeat([]byte{0}, 16)
	id, _ := uuid.FromBytes(empty)
	return id
}
