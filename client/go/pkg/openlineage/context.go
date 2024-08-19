package openlineage

import (
	"context"
	"runtime"

	"github.com/OpenLineage/openlineage/client/go/pkg/facets"
	"github.com/go-stack/stack"
	"github.com/google/uuid"
)

type runContextKeyType int

const currentRunKey runContextKeyType = iota

func RunContextFromContext(ctx context.Context) RunContext {
	if ctx == nil {
		return &noopRunContext{}
		// return noopSpanInstance
	}
	if r, ok := ctx.Value(currentRunKey).(RunContext); ok {
		return r
	}

	return &noopRunContext{}
}

func ContextWithRun(parent context.Context, run RunContext) context.Context {
	return context.WithValue(parent, currentRunKey, run)
}

type RunContext interface {
	Parent() RunContext
	RunID() uuid.UUID
	JobName() string
	JobNamespace() string

	Child(ctx context.Context, jobName string) (context.Context, RunContext)
	Emit(context.Context, Emittable) error
	Event(EventType) *RunEvent
	Finish()
	HasFailed() bool
	RecordError(error)
}

type runContext struct {
	parent       RunContext
	runID        uuid.UUID
	jobName      string
	jobNamespace string

	hasFailed bool
	client    *Client
}

// JobName implements RunContext.
func (rc *runContext) JobName() string {
	return rc.jobName
}

// JobNamespace implements RunContext.
func (rc *runContext) JobNamespace() string {
	return rc.jobNamespace
}

// RunID implements RunContext.
func (rc *runContext) RunID() uuid.UUID {
	return rc.runID
}

func (rc *runContext) Parent() RunContext {
	return rc.parent
}

func (rc *runContext) Event(eventType EventType) *RunEvent {
	run := NewNamespacedRunEvent(eventType, rc.runID, rc.jobName, rc.jobNamespace)

	if rc.Parent() != nil {
		parent := facets.NewParent(
			facets.Job{
				Name:      rc.parent.JobName(),
				Namespace: rc.parent.JobNamespace(),
			},
			facets.Run{
				RunID: rc.parent.RunID().String(),
			},
		)

		run = run.WithRunFacets(parent)
	}

	return run
}

func (rc *runContext) Child(ctx context.Context, jobName string) (context.Context, RunContext) {
	return rc.client.NewRunContext(ctx, jobName)
}

// Emit uses its openlineage.Client to emit an event
func (rc *runContext) Emit(ctx context.Context, event Emittable) error {
	return rc.client.Emit(ctx, event)
}

// RecordError emits an OTHER event with an ErrorMessage facet.
// Once this is called, the run is considered to have failed.
func (rc *runContext) RecordError(err error) {
	rc.hasFailed = true

	errorMessage := err.Error()

	stacktrace := stack.Caller(1).String()
	language := runtime.Version()

	errorFacet := facets.NewErrorMessage(errorMessage, language).
		WithStackTrace(stacktrace)

	errorEvent := rc.Event(EventTypeOther).WithRunFacets(errorFacet)

	_ = rc.client.Emit(context.Background(), errorEvent)
}

// Finish will emit a COMPLETE event if no error has occurred.
// Otherwise, it will emit a FAIL event.
func (rc *runContext) Finish() {
	eventType := EventTypeComplete
	if rc.hasFailed {
		eventType = EventTypeFail
	}

	_ = rc.client.Emit(context.Background(), rc.Event(eventType))
}

func (rc *runContext) HasFailed() bool {
	return rc.hasFailed
}
