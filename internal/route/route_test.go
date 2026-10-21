package route

import (
	"errors"
	"testing"

	"reliable-orchestration/internal/job"
)

func TestFailure(t *testing.T) {
	tests := []struct {
		name        string
		attempt     int
		maxAttempts int
		wantDest    Destination
		wantNext    int
		wantErr     error
	}{
		{name: "first failure retries", attempt: 1, maxAttempts: 2, wantDest: DestRetry, wantNext: 2},
		{name: "last failure dead letters", attempt: 2, maxAttempts: 2, wantDest: DestDLQ, wantNext: 2},
		{name: "budget of one dead letters immediately", attempt: 1, maxAttempts: 1, wantDest: DestDLQ, wantNext: 1},
		{name: "middle failure retries", attempt: 2, maxAttempts: 3, wantDest: DestRetry, wantNext: 3},
		{name: "zero attempt", attempt: 0, maxAttempts: 2, wantErr: job.ErrBadAttempt},
		{name: "zero budget", attempt: 1, maxAttempts: 0, wantErr: ErrBadBudget},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Failure(tt.attempt, tt.maxAttempts)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("error = %v, want %v", err, tt.wantErr)
			}
			if tt.wantErr != nil {
				return
			}
			if !got.Ack {
				t.Fatal("failure must ack the current topic")
			}
			if got.Destination != tt.wantDest {
				t.Fatalf("destination = %q, want %q", got.Destination, tt.wantDest)
			}
			if got.NextAttempt != tt.wantNext {
				t.Fatalf("next attempt = %d, want %d", got.NextAttempt, tt.wantNext)
			}
		})
	}
}

func TestSuccess(t *testing.T) {
	got, err := Success(2)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Ack || got.Destination != DestNone || got.NextAttempt != 2 {
		t.Fatalf("unexpected decision: %+v", got)
	}

	if _, err := Success(0); !errors.Is(err, job.ErrBadAttempt) {
		t.Fatalf("error = %v, want %v", err, job.ErrBadAttempt)
	}
}

func TestNextEnvelope(t *testing.T) {
	current := job.Envelope{ID: "email-1", Payload: []byte("msg"), Attempt: 1}
	decision, err := Failure(1, 2)
	if err != nil {
		t.Fatal(err)
	}

	next := NextEnvelope(current, decision, errors.New("model timeout"))
	if next.ID != current.ID || string(next.Payload) != string(current.Payload) {
		t.Fatalf("identity changed: %+v", next)
	}
	if next.Attempt != 2 {
		t.Fatalf("attempt = %d, want 2", next.Attempt)
	}
	if next.Error != "model timeout" {
		t.Fatalf("error = %q", next.Error)
	}
	if current.Attempt != 1 || current.Error != "" {
		t.Fatal("current envelope was mutated")
	}
}
