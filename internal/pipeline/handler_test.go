package pipeline

import (
	"context"
	"errors"
	"testing"

	"reliable-orchestration/internal/agent"
	"reliable-orchestration/internal/job"
	"reliable-orchestration/internal/route"
)

func TestSuccessAcksWithoutPublish(t *testing.T) {
	script := &agent.Script{Steps: 3}
	pub := &memPublisher{}
	h := Handler{Agent: script, MaxAttempts: 2, Publisher: pub}

	got, err := h.Handle(context.Background(), job.Envelope{ID: "email-1", Attempt: 1})
	if err != nil {
		t.Fatal(err)
	}
	if !got.Ack || got.Published != nil || got.Destination != route.DestNone {
		t.Fatalf("result = %+v", got)
	}
	if len(pub.got) != 0 {
		t.Fatalf("published = %+v, want none", pub.got)
	}
}

func TestFailurePublishesRetry(t *testing.T) {
	cause := errors.New("model timeout")
	script := &agent.Script{
		Steps:    3,
		Failures: []agent.Failure{{Attempt: 1, Step: 2, Err: cause}},
	}
	pub := &memPublisher{}
	h := Handler{Agent: script, MaxAttempts: 2, Publisher: pub}

	got, err := h.Handle(context.Background(), job.Envelope{
		ID:      "email-1",
		Payload: []byte("body"),
		Attempt: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !got.Ack || got.Destination != route.DestRetry {
		t.Fatalf("result = %+v", got)
	}
	if got.Published == nil {
		t.Fatal("missing published envelope")
	}
	if got.Published.ID != "email-1" || got.Published.Attempt != 2 || got.Published.Error != cause.Error() {
		t.Fatalf("published = %+v", got.Published)
	}
	if string(got.Published.Payload) != "body" {
		t.Fatalf("payload = %q", got.Published.Payload)
	}
	if len(pub.got) != 1 || pub.got[0].dest != route.DestRetry {
		t.Fatalf("published = %+v", pub.got)
	}
}

func TestExhaustedAttemptPublishesDeadLetter(t *testing.T) {
	script := &agent.Script{
		Steps:    1,
		Failures: []agent.Failure{{Attempt: 2, Step: 1, Err: errors.New("still down")}},
	}
	pub := &memPublisher{}
	h := Handler{Agent: script, MaxAttempts: 2, Publisher: pub}

	got, err := h.Handle(context.Background(), job.Envelope{ID: "email-1", Attempt: 2})
	if err != nil {
		t.Fatal(err)
	}
	if !got.Ack || got.Destination != route.DestDLQ {
		t.Fatalf("result = %+v", got)
	}
	if got.Published == nil || got.Published.Attempt != 2 {
		t.Fatalf("published = %+v", got.Published)
	}
}

func TestRetryThenSuccessRestartsAtStepOne(t *testing.T) {
	script := &agent.Script{
		Steps:    3,
		Failures: []agent.Failure{{Attempt: 1, Step: 2, Err: errors.New("down")}},
	}
	h := Handler{Agent: script, MaxAttempts: 2, Publisher: &memPublisher{}}

	first, err := h.Handle(context.Background(), job.Envelope{ID: "email-1", Attempt: 1})
	if err != nil {
		t.Fatal(err)
	}
	second, err := h.Handle(context.Background(), *first.Published)
	if err != nil {
		t.Fatal(err)
	}
	if !second.Ack || second.Published != nil {
		t.Fatalf("second result = %+v", second)
	}

	got := script.Calls()
	want := []agent.Call{
		{JobID: "email-1", Step: 1, Attempt: 1},
		{JobID: "email-1", Step: 2, Attempt: 1},
		{JobID: "email-1", Step: 1, Attempt: 2},
		{JobID: "email-1", Step: 2, Attempt: 2},
		{JobID: "email-1", Step: 3, Attempt: 2},
	}
	if len(got) != len(want) {
		t.Fatalf("calls = %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("calls = %+v, want %+v", got, want)
		}
	}
}

func TestPublishErrorDoesNotAck(t *testing.T) {
	script := &agent.Script{
		Steps:    1,
		Failures: []agent.Failure{{Attempt: 1, Step: 1, Err: errors.New("down")}},
	}
	pub := &memPublisher{err: errors.New("broker unavailable")}
	h := Handler{Agent: script, MaxAttempts: 2, Publisher: pub}

	got, err := h.Handle(context.Background(), job.Envelope{ID: "email-1", Attempt: 1})
	if err == nil {
		t.Fatal("expected publish error")
	}
	if got.Ack || got.Published != nil {
		t.Fatalf("result = %+v", got)
	}
	if len(pub.got) != 0 {
		t.Fatalf("published = %+v", pub.got)
	}
}

func TestCanceledAgentDoesNotPublish(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	script := &agent.Script{Steps: 1}
	pub := &memPublisher{}

	got, err := (Handler{Agent: script, MaxAttempts: 2, Publisher: pub}).Handle(ctx, job.Envelope{ID: "email-1", Attempt: 1})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v", err)
	}
	if got.Ack || got.Published != nil || len(pub.got) != 0 {
		t.Fatalf("result = %+v, published = %+v", got, pub.got)
	}
	if len(script.Calls()) != 0 {
		t.Fatalf("calls = %+v", script.Calls())
	}
}

func TestHandleRejectsBadInput(t *testing.T) {
	_, err := (Handler{}).Handle(context.Background(), job.Envelope{ID: "email-1", Attempt: 1})
	if !errors.Is(err, ErrNoAgent) {
		t.Fatalf("error = %v, want %v", err, ErrNoAgent)
	}

	script := &agent.Script{Steps: 1}
	_, err = (Handler{Agent: script}).Handle(context.Background(), job.Envelope{Attempt: 1})
	if !errors.Is(err, job.ErrEmptyID) {
		t.Fatalf("error = %v, want %v", err, job.ErrEmptyID)
	}
	if len(script.Calls()) != 0 {
		t.Fatalf("agent ran: %+v", script.Calls())
	}

	_, err = (Handler{Agent: script, MaxAttempts: 0}).Handle(context.Background(), job.Envelope{ID: "email-1", Attempt: 1})
	if err != nil {
		t.Fatal(err)
	}

	script.Failures = []agent.Failure{{Attempt: 1, Step: 1, Err: errors.New("down")}}
	_, err = (Handler{Agent: script, MaxAttempts: 0}).Handle(context.Background(), job.Envelope{ID: "email-1", Attempt: 1})
	if !errors.Is(err, route.ErrBadBudget) {
		t.Fatalf("error = %v, want %v", err, route.ErrBadBudget)
	}
}

type published struct {
	dest route.Destination
	env  job.Envelope
}

type memPublisher struct {
	err error
	got []published
}

func (m *memPublisher) Publish(_ context.Context, dest route.Destination, env job.Envelope) error {
	if m.err != nil {
		return m.err
	}
	m.got = append(m.got, published{dest: dest, env: env})
	return nil
}
