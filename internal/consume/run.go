package consume

import (
	"context"
	"errors"
	"fmt"

	"reliable-orchestration/internal/job"
	"reliable-orchestration/internal/pipeline"
	"reliable-orchestration/internal/route"
)

var (
	ErrNotAcknowledged = errors.New("delivery was not acknowledged")
	ErrNoDecoder       = errors.New("decoder is required")
)

// Decoder turns a Kafka value into a job. The topic selects the schema subject.
type Decoder interface {
	Unmarshal(topic string, value []byte) (job.Envelope, error)
}

// Message is one Kafka record. Token is private data the reader uses to commit.
type Message struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	Token     any
}

// Reader is the piece of the consumer that talks to the broker.
type Reader interface {
	Read(ctx context.Context) (Message, error)
	Commit(ctx context.Context, msg Message) error
}

// Run reads deliveries until ctx is canceled.
// A successful handler result, or a message that cannot be decoded, is committed.
// A failed publish is not committed, and Run returns so a later start reads it again.
func Run(ctx context.Context, r Reader, dec Decoder, h pipeline.Handler) error {
	if dec == nil {
		return ErrNoDecoder
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		msg, err := r.Read(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}

		env, err := dec.Unmarshal(msg.Topic, msg.Value)
		if err != nil {
			if err := deadLetter(ctx, h.Publisher, msg, err); err != nil {
				return err
			}
			if err := r.Commit(ctx, msg); err != nil {
				return err
			}
			continue
		}

		result, err := h.Handle(ctx, env)
		if err != nil {
			return err
		}
		if !result.Ack {
			return ErrNotAcknowledged
		}
		if err := r.Commit(ctx, msg); err != nil {
			return err
		}
	}
}

func deadLetter(ctx context.Context, pub pipeline.Publisher, msg Message, cause error) error {
	if pub == nil {
		return pipeline.ErrNoPublisher
	}
	id := string(msg.Key)
	if id == "" {
		id = fmt.Sprintf("undecodable-%s-%d-%d", msg.Topic, msg.Partition, msg.Offset)
	}
	env := job.Envelope{
		ID:      id,
		Payload: append([]byte(nil), msg.Value...),
		Attempt: 1,
		Error:   cause.Error(),
	}
	return pub.Publish(ctx, route.DestDLQ, env)
}
