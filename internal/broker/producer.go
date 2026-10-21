package broker

import (
	"context"
	"errors"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"

	"reliable-orchestration/internal/codec"
	"reliable-orchestration/internal/job"
	"reliable-orchestration/internal/route"
	"reliable-orchestration/internal/topics"
)

var ErrNoBrokers = errors.New("brokers are required")

// Producer publishes jobs. It implements pipeline.Publisher.
type Producer struct {
	producer *kafka.Producer
	records  *codec.Codec
}

func NewProducer(brokers string, records *codec.Codec) (*Producer, error) {
	if brokers == "" {
		return nil, ErrNoBrokers
	}
	if records == nil {
		return nil, errors.New("schema registry codec is required")
	}
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
		"acks":              "all",
	})
	if err != nil {
		return nil, err
	}
	return &Producer{producer: producer, records: records}, nil
}

func (p *Producer) Publish(ctx context.Context, dest route.Destination, env job.Envelope) error {
	topic, err := topics.For(dest)
	if err != nil {
		return err
	}
	return p.Produce(ctx, topic, env)
}

// Produce sends one job to an explicit topic. The live topic uses this path
// because a brand-new job is not a retry or a dead letter.
func (p *Producer) Produce(ctx context.Context, topic string, env job.Envelope) error {
	value, err := p.records.Marshal(topic, env)
	if err != nil {
		return err
	}
	delivery := make(chan kafka.Event, 1)
	err = p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(env.ID),
		Value:          value,
	}, delivery)
	if err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		go func() { <-delivery }()
		return ctx.Err()
	case event := <-delivery:
		message, ok := event.(*kafka.Message)
		if !ok {
			return fmt.Errorf("unexpected delivery event %T", event)
		}
		if message.TopicPartition.Error != nil {
			return message.TopicPartition.Error
		}
		return nil
	}
}

func (p *Producer) Close() {
	p.producer.Flush(5_000)
	p.producer.Close()
}
