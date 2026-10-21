package agent

import (
	"context"
	"errors"
	"testing"

	"reliable-orchestration/internal/job"
)

func TestRunRecordsEachStep(t *testing.T) {
	script := &Script{Steps: 3}
	env := job.Envelope{ID: "email-1", Attempt: 1}

	if err := script.Run(context.Background(), env); err != nil {
		t.Fatal(err)
	}

	got := script.Calls()
	want := []Call{
		{JobID: "email-1", Step: 1, Attempt: 1},
		{JobID: "email-1", Step: 2, Attempt: 1},
		{JobID: "email-1", Step: 3, Attempt: 1},
	}
	if !sameCalls(got, want) {
		t.Fatalf("calls = %+v, want %+v", got, want)
	}
}

func TestRetryStartsAtStepOne(t *testing.T) {
	stepErr := errors.New("analysis unavailable")
	script := &Script{
		Steps: 3,
		Failures: []Failure{
			{Attempt: 1, Step: 3, Err: stepErr},
		},
	}

	first := job.Envelope{ID: "email-1", Attempt: 1}
	if err := script.Run(context.Background(), first); !errors.Is(err, stepErr) {
		t.Fatalf("first run error = %v, want %v", err, stepErr)
	}

	second := job.Envelope{ID: "email-1", Attempt: 2}
	if err := script.Run(context.Background(), second); err != nil {
		t.Fatal(err)
	}

	got := script.Calls()
	want := []Call{
		{JobID: "email-1", Step: 1, Attempt: 1},
		{JobID: "email-1", Step: 2, Attempt: 1},
		{JobID: "email-1", Step: 3, Attempt: 1},
		{JobID: "email-1", Step: 1, Attempt: 2},
		{JobID: "email-1", Step: 2, Attempt: 2},
		{JobID: "email-1", Step: 3, Attempt: 2},
	}
	if !sameCalls(got, want) {
		t.Fatalf("calls = %+v, want %+v", got, want)
	}
}

func TestFailureSkipsRemainingSteps(t *testing.T) {
	script := &Script{
		Steps: 3,
		Failures: []Failure{
			{Attempt: 1, Step: 2, Err: errors.New("boom")},
		},
	}
	env := job.Envelope{ID: "email-1", Attempt: 1}

	if err := script.Run(context.Background(), env); err == nil {
		t.Fatal("expected failure")
	}

	got := script.Calls()
	want := []Call{
		{JobID: "email-1", Step: 1, Attempt: 1},
		{JobID: "email-1", Step: 2, Attempt: 1},
	}
	if !sameCalls(got, want) {
		t.Fatalf("calls = %+v, want %+v", got, want)
	}
}

func TestRunRejectsBadInput(t *testing.T) {
	script := &Script{}
	err := script.Run(context.Background(), job.Envelope{ID: "email-1", Attempt: 1})
	if !errors.Is(err, ErrNoSteps) {
		t.Fatalf("error = %v, want %v", err, ErrNoSteps)
	}

	script.Steps = 1
	err = script.Run(context.Background(), job.Envelope{Attempt: 1})
	if !errors.Is(err, job.ErrEmptyID) {
		t.Fatalf("error = %v, want %v", err, job.ErrEmptyID)
	}
}

func TestRunStopsWhenContextIsCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	script := &Script{Steps: 3}
	err := script.Run(ctx, job.Envelope{ID: "email-1", Attempt: 1})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want %v", err, context.Canceled)
	}
	if len(script.Calls()) != 0 {
		t.Fatalf("calls = %+v, want none", script.Calls())
	}
}

func sameCalls(got, want []Call) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
