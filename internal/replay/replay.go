package replay

import (
	"context"
	"errors"

	"reliable-orchestration/internal/consume"
	"reliable-orchestration/internal/job"
	"reliable-orchestration/internal/topics"
)

var ErrNoDecoder = errors.New("decoder is required")

// Producer publishes a job to an explicit topic.
type Producer interface {
	Produce(ctx context.Context, topic string, env job.Envelope) error
}

// Run moves dead-letter records onto the retry topic until ctx is canceled.
// The live topic is never written. A record is committed only after the
// retry publish succeeds.
func Run(ctx context.Context, r consume.Reader, dec consume.Decoder, pub Producer) error {
	if dec == nil {
		return ErrNoDecoder
	}
	if pub == nil {
		return errors.New("producer is required")
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
			return err
		}
		if err := pub.Produce(ctx, topics.Retry, env); err != nil {
			return err
		}
		if err := r.Commit(ctx, msg); err != nil {
			return err
		}
	}
}
