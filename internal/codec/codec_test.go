package codec

import (
	"testing"

	"reliable-orchestration/internal/job"
	"reliable-orchestration/internal/topics"
)

func TestRoundTrip(t *testing.T) {
	c := newTestCodec(t)
	in := job.Envelope{
		ID:      "email-1",
		Payload: []byte("hello"),
		Attempt: 2,
		Error:   "model timeout",
	}
	b, err := c.Marshal(topics.Live, in)
	if err != nil {
		t.Fatal(err)
	}
	if len(b) < 6 || b[0] != 0 {
		t.Fatalf("value = %v, want a schema id prefix", b)
	}
	got, err := c.Unmarshal(topics.Live, b)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != in.ID || got.Attempt != in.Attempt || got.Error != in.Error || string(got.Payload) != string(in.Payload) {
		t.Fatalf("got %+v", got)
	}
}

func TestRoundTripUsesTheTopicSubject(t *testing.T) {
	c := newTestCodec(t)
	in := job.Envelope{ID: "email-1", Attempt: 2, Payload: []byte("body")}
	b, err := c.Marshal(topics.Retry, in)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Unmarshal(topics.Live, b); err == nil {
		t.Fatal("expected the live subject to reject a retry-topic record")
	}
	got, err := c.Unmarshal(topics.Retry, b)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != in.ID || string(got.Payload) != "body" || got.Attempt != 2 {
		t.Fatalf("got %+v", got)
	}
}

func TestUnmarshalRejectsBadMessages(t *testing.T) {
	c := newTestCodec(t)
	if _, err := c.Unmarshal(topics.Live, []byte("{")); err == nil {
		t.Fatal("expected plain json to fail")
	}

	raw, err := c.ser.Serialize(topics.Live, wire{ID: "", Attempt: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Unmarshal(topics.Live, raw); err == nil {
		t.Fatal("expected an empty id to fail")
	}

	raw, err = c.ser.Serialize(topics.Live, wire{ID: "email-1", Attempt: 0})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Unmarshal(topics.Live, raw); err == nil {
		t.Fatal("expected attempt 0 to fail")
	}
}

func TestNewRequiresURL(t *testing.T) {
	if _, err := New(""); err == nil {
		t.Fatal("expected an error")
	}
}

func newTestCodec(t *testing.T) *Codec {
	t.Helper()
	c, err := New("mock://local")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}
