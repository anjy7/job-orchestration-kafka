package agent

import (
	"context"
	"errors"
	"sync"

	"reliable-orchestration/internal/job"
)

var ErrNoSteps = errors.New("agent has no steps")

// Agent runs one attempt of a job from the first step through the last.
type Agent interface {
	Run(context.Context, job.Envelope) error
}

// Call is one step that an attempt started. A later error from Run means
// that step did not finish. The next attempt starts again at step 1.
type Call struct {
	JobID   string
	Step    int
	Attempt int
}

// Failure makes Run return Err when that attempt reaches Step.
type Failure struct {
	Attempt int
	Step    int
	Err     error
}

// Script is a fake agent. It records every step it starts and can fail
// at a chosen step. Nothing is kept between attempts, so a retry repeats
// the earlier steps.
type Script struct {
	Steps    int
	Failures []Failure

	mu  sync.Mutex
	log []Call
}

func (s *Script) Run(ctx context.Context, env job.Envelope) error {
	if err := env.Validate(); err != nil {
		return err
	}
	if s.Steps < 1 {
		return ErrNoSteps
	}

	for step := 1; step <= s.Steps; step++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		s.record(Call{JobID: env.ID, Step: step, Attempt: env.Attempt})
		if err := s.failure(env.Attempt, step); err != nil {
			return err
		}
	}
	return nil
}

func (s *Script) Calls() []Call {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Call, len(s.log))
	copy(out, s.log)
	return out
}

func (s *Script) record(c Call) {
	s.mu.Lock()
	s.log = append(s.log, c)
	s.mu.Unlock()
}

func (s *Script) failure(attempt, step int) error {
	for _, f := range s.Failures {
		if f.Attempt == attempt && f.Step == step && f.Err != nil {
			return f.Err
		}
	}
	return nil
}
