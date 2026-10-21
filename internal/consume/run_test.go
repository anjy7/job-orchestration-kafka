package consume

import (
	"context"
	"errors"
	"testing"

	"reliable-orchestration/internal/agent"
	"reliable-orchestration/internal/codec"
	"reliable-orchestration/internal/job"
	"reliable-orchestration/internal/pipeline"
	"reliable-orchestration/internal/route"
	"reliable-orchestration/internal/topics"
)

func TestRunCommitsASuccessfulJob(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	records := testRecords(t)
	reader := &scriptReader{}
	reader.msgs = []Message{mustMessage(t, records, job.Envelope{ID: "email-1", Attempt: 1})}
	reader.onCommit = func() { cancel() }

	err := Run(ctx, reader, records, pipeline.Handler{
		Agent:       &agent.Script{Steps: 3},
		MaxAttempts: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(reader.commits) != 1 || reader.commits[0].Offset != reader.msgs[0].Offset {
		t.Fatalf("commits = %+v", reader.commits)
	}
}

func TestRunPublishesRetryThenCommits(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	records := testRecords(t)
	reader := &scriptReader{
		msgs: []Message{mustMessage(t, records, job.Envelope{ID: "email-1", Payload: []byte("body"), Attempt: 1})},
	}
	reader.onCommit = func() { cancel() }
	pub := &memPublisher{}

	err := Run(ctx, reader, records, pipeline.Handler{
		Agent: &agent.Script{
			Steps:    3,
			Failures: []agent.Failure{{Attempt: 1, Step: 2, Err: errors.New("down")}},
		},
		MaxAttempts: 2,
		Publisher:   pub,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(reader.commits) != 1 {
		t.Fatalf("commits = %d", len(reader.commits))
	}
	if len(pub.got) != 1 || pub.got[0].dest != route.DestRetry {
		t.Fatalf("published = %+v", pub.got)
	}
	if pub.got[0].env.Attempt != 2 || pub.got[0].env.ID != "email-1" || string(pub.got[0].env.Payload) != "body" {
		t.Fatalf("retry envelope = %+v", pub.got[0].env)
	}
}

func TestRunDoesNotCommitWhenPublishFails(t *testing.T) {
	records := testRecords(t)
	reader := &scriptReader{
		msgs: []Message{
			mustMessage(t, records, job.Envelope{ID: "email-1", Attempt: 1}),
			mustMessage(t, records, job.Envelope{ID: "email-2", Attempt: 1}),
		},
	}
	pub := &memPublisher{err: errors.New("broker down")}
	err := Run(context.Background(), reader, records, pipeline.Handler{
		Agent: &agent.Script{
			Steps:    1,
			Failures: []agent.Failure{{Attempt: 1, Step: 1, Err: errors.New("down")}},
		},
		MaxAttempts: 2,
		Publisher:   pub,
	})
	if err == nil {
		t.Fatal("expected publish error")
	}
	if len(reader.commits) != 0 {
		t.Fatalf("commits = %+v", reader.commits)
	}
	if reader.n != 1 {
		t.Fatalf("read %d messages, want 1", reader.n)
	}
}

func TestRunDeadLettersUndecodableMessage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	records := testRecords(t)
	reader := &scriptReader{
		msgs: []Message{
			{Topic: topics.Live, Partition: 0, Offset: 3, Key: []byte("email-9"), Value: []byte("{")},
			mustMessage(t, records, job.Envelope{ID: "email-1", Attempt: 1}),
		},
	}
	reader.msgs[1].Offset = 4
	reader.onCommit = func() {
		if len(reader.commits) == 2 {
			cancel()
		}
	}
	pub := &memPublisher{}

	err := Run(ctx, reader, records, pipeline.Handler{
		Agent:       &agent.Script{Steps: 1},
		MaxAttempts: 2,
		Publisher:   pub,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(pub.got) != 1 || pub.got[0].dest != route.DestDLQ {
		t.Fatalf("published = %+v", pub.got)
	}
	if pub.got[0].env.ID != "email-9" || pub.got[0].env.Attempt != 1 || string(pub.got[0].env.Payload) != "{" {
		t.Fatalf("dead letter = %+v", pub.got[0].env)
	}
	if len(reader.commits) != 2 || reader.commits[0].Offset != 3 || reader.commits[1].Offset != 4 {
		t.Fatalf("commits = %+v", reader.commits)
	}
}

func TestRunReturnsCommitError(t *testing.T) {
	records := testRecords(t)
	reader := &scriptReader{
		msgs:      []Message{mustMessage(t, records, job.Envelope{ID: "email-1", Attempt: 1})},
		commitErr: errors.New("commit failed"),
	}
	err := Run(context.Background(), reader, records, pipeline.Handler{
		Agent:       &agent.Script{Steps: 1},
		MaxAttempts: 2,
	})
	if err == nil || err.Error() != "commit failed" {
		t.Fatalf("error = %v", err)
	}
}

type scriptReader struct {
	msgs      []Message
	commits   []Message
	commitErr error
	readErr   error
	n         int
	onCommit  func()
}

func (s *scriptReader) Read(ctx context.Context) (Message, error) {
	if err := ctx.Err(); err != nil {
		return Message{}, err
	}
	if s.n >= len(s.msgs) {
		if s.readErr != nil {
			return Message{}, s.readErr
		}
		<-ctx.Done()
		return Message{}, ctx.Err()
	}
	msg := s.msgs[s.n]
	s.n++
	return msg, nil
}

func (s *scriptReader) Commit(_ context.Context, msg Message) error {
	if s.commitErr != nil {
		return s.commitErr
	}
	s.commits = append(s.commits, msg)
	if s.onCommit != nil {
		s.onCommit()
	}
	return nil
}

func testRecords(t *testing.T) *codec.Codec {
	t.Helper()
	c, err := codec.New("mock://local")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func mustMessage(t *testing.T, records *codec.Codec, env job.Envelope) Message {
	t.Helper()
	b, err := records.Marshal(topics.Live, env)
	if err != nil {
		t.Fatal(err)
	}
	return Message{Topic: topics.Live, Key: []byte(env.ID), Value: b}
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
