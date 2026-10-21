package replay

import (
	"context"
	"errors"
	"testing"

	"reliable-orchestration/internal/codec"
	"reliable-orchestration/internal/consume"
	"reliable-orchestration/internal/job"
	"reliable-orchestration/internal/topics"
)

func TestRunPublishesToRetryAndCommits(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	records := testRecords(t)
	first := job.Envelope{ID: "email-1", Payload: []byte("one"), Attempt: 2, Error: "down"}
	second := job.Envelope{ID: "email-2", Payload: []byte("two"), Attempt: 2, Error: "down"}
	reader := &scriptReader{
		msgs: []consume.Message{
			mustDLQ(t, records, first),
			mustDLQ(t, records, second),
		},
	}
	reader.onCommit = func() {
		if len(reader.commits) == 2 {
			cancel()
		}
	}
	pub := &memProducer{}

	if err := Run(ctx, reader, records, pub); err != nil {
		t.Fatal(err)
	}
	if len(reader.commits) != 2 {
		t.Fatalf("commits = %d", len(reader.commits))
	}
	if len(pub.got) != 2 {
		t.Fatalf("published = %+v", pub.got)
	}
	for i, env := range []job.Envelope{first, second} {
		if pub.got[i].topic != topics.Retry {
			t.Fatalf("topic = %s", pub.got[i].topic)
		}
		got := pub.got[i].env
		if got.ID != env.ID || got.Attempt != env.Attempt || got.Error != env.Error || string(got.Payload) != string(env.Payload) {
			t.Fatalf("published = %+v, want %+v", got, env)
		}
	}
}

func TestRunDoesNotCommitWhenPublishFails(t *testing.T) {
	records := testRecords(t)
	reader := &scriptReader{
		msgs: []consume.Message{
			mustDLQ(t, records, job.Envelope{ID: "email-1", Attempt: 2}),
			mustDLQ(t, records, job.Envelope{ID: "email-2", Attempt: 2}),
		},
	}
	pub := &memProducer{err: errors.New("broker down")}

	err := Run(context.Background(), reader, records, pub)
	if err == nil {
		t.Fatal("expected publish error")
	}
	if len(reader.commits) != 0 {
		t.Fatalf("commits = %d", len(reader.commits))
	}
	if reader.n != 1 {
		t.Fatalf("read %d messages, want 1", reader.n)
	}
	if len(pub.got) != 0 {
		t.Fatalf("published = %+v", pub.got)
	}
}

func TestRunStopsOnUndecodableRecord(t *testing.T) {
	records := testRecords(t)
	reader := &scriptReader{
		msgs: []consume.Message{
			{Topic: topics.DLQ, Key: []byte("email-9"), Value: []byte("{")},
		},
	}
	pub := &memProducer{}

	err := Run(context.Background(), reader, records, pub)
	if err == nil {
		t.Fatal("expected a decode error")
	}
	if len(reader.commits) != 0 || len(pub.got) != 0 {
		t.Fatalf("commits = %d, published = %d", len(reader.commits), len(pub.got))
	}
}

type scriptReader struct {
	msgs     []consume.Message
	commits  []consume.Message
	n        int
	onCommit func()
}

func (s *scriptReader) Read(ctx context.Context) (consume.Message, error) {
	if err := ctx.Err(); err != nil {
		return consume.Message{}, err
	}
	if s.n >= len(s.msgs) {
		<-ctx.Done()
		return consume.Message{}, ctx.Err()
	}
	msg := s.msgs[s.n]
	s.n++
	return msg, nil
}

func (s *scriptReader) Commit(_ context.Context, msg consume.Message) error {
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

func mustDLQ(t *testing.T, records *codec.Codec, env job.Envelope) consume.Message {
	t.Helper()
	b, err := records.Marshal(topics.DLQ, env)
	if err != nil {
		t.Fatal(err)
	}
	return consume.Message{Topic: topics.DLQ, Key: []byte(env.ID), Value: b}
}

type published struct {
	topic string
	env   job.Envelope
}

type memProducer struct {
	err error
	got []published
}

func (m *memProducer) Produce(_ context.Context, topic string, env job.Envelope) error {
	if m.err != nil {
		return m.err
	}
	m.got = append(m.got, published{topic: topic, env: env})
	return nil
}
