package route

import (
	"errors"

	"reliable-orchestration/internal/job"
)

var ErrBadBudget = errors.New("max attempts must be at least 1")

type Destination string

const (
	DestNone  Destination = ""
	DestRetry Destination = "retry"
	DestDLQ   Destination = "dlq"
)

// Ack is true for both success and failure: a failure is finished on this
// topic once the job has been handed to the retry topic or the dead-letter topic.
type Decision struct {
	Ack         bool
	Destination Destination
	NextAttempt int
}

func Success(attempt int) (Decision, error) {
	if attempt < 1 {
		return Decision{}, job.ErrBadAttempt
	}
	return Decision{Ack: true, NextAttempt: attempt}, nil
}

func Failure(attempt, maxAttempts int) (Decision, error) {
	if attempt < 1 {
		return Decision{}, job.ErrBadAttempt
	}
	if maxAttempts < 1 {
		return Decision{}, ErrBadBudget
	}
	if attempt >= maxAttempts {
		return Decision{
			Ack:         true,
			Destination: DestDLQ,
			NextAttempt: attempt,
		}, nil
	}
	return Decision{
		Ack:         true,
		Destination: DestRetry,
		NextAttempt: attempt + 1,
	}, nil
}

// NextEnvelope is the record published to the retry or dead-letter topic.
func NextEnvelope(current job.Envelope, d Decision, cause error) job.Envelope {
	next := current
	next.Attempt = d.NextAttempt
	if cause != nil {
		next.Error = cause.Error()
	}
	return next
}
