package pipeline

import (
	"context"
	"errors"

	"reliable-orchestration/internal/agent"
	"reliable-orchestration/internal/job"
	"reliable-orchestration/internal/route"
)

var (
	ErrNoAgent     = errors.New("agent is required")
	ErrNoPublisher = errors.New("publisher is required")
)

// Publisher hands a failed job to the retry topic or the dead-letter topic.
type Publisher interface {
	Publish(ctx context.Context, dest route.Destination, env job.Envelope) error
}

// Handler runs one delivery of a job.
// MaxAttempts counts the live attempt. A budget of 2 allows the live run
// and one retry.
type Handler struct {
	Agent       agent.Agent
	MaxAttempts int
	Publisher   Publisher
}

// Result is a conclusive handling of the current delivery.
// Ack means the caller must acknowledge that delivery.
// Published is set when the job was handed to another destination.
type Result struct {
	Ack         bool
	Destination route.Destination
	Published   *job.Envelope
}

// Handle runs the agent once. Success acknowledges the delivery and publishes
// nothing. A failed attempt is published to retry or the dead-letter
// destination and then acknowledged. If that publish fails, the delivery is
// left unacknowledged so it can be delivered again.
func (h Handler) Handle(ctx context.Context, env job.Envelope) (Result, error) {
	if h.Agent == nil {
		return Result{}, ErrNoAgent
	}
	if err := env.Validate(); err != nil {
		return Result{}, err
	}

	runErr := h.Agent.Run(ctx, env)
	if runErr != nil && ctx.Err() != nil {
		return Result{}, ctx.Err()
	}
	if runErr == nil {
		decision, err := route.Success(env.Attempt)
		if err != nil {
			return Result{}, err
		}
		return Result{Ack: decision.Ack}, nil
	}

	decision, err := route.Failure(env.Attempt, h.MaxAttempts)
	if err != nil {
		return Result{}, err
	}
	if h.Publisher == nil {
		return Result{}, ErrNoPublisher
	}

	next := route.NextEnvelope(env, decision, runErr)
	if err := h.Publisher.Publish(ctx, decision.Destination, next); err != nil {
		return Result{}, err
	}
	return Result{
		Ack:         decision.Ack,
		Destination: decision.Destination,
		Published:   &next,
	}, nil
}
