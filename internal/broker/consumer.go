package broker

import (
	"context"
	"errors"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"

	"reliable-orchestration/internal/consume"
)

var ErrNoGroup = errors.New("consumer group is required")

// Consumer reads one or more topics and commits offsets explicitly.
type Consumer struct {
	consumer *kafka.Consumer
}

func NewConsumer(brokers, group string, subscribe []string) (*Consumer, error) {
	if brokers == "" {
		return nil, ErrNoBrokers
	}
	if group == "" {
		return nil, ErrNoGroup
	}
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  brokers,
		"group.id":           group,
		"enable.auto.commit": false,
		"auto.offset.reset":  "earliest",
	})
	if err != nil {
		return nil, err
	}
	if err := consumer.SubscribeTopics(subscribe, nil); err != nil {
		consumer.Close()
		return nil, err
	}
	return &Consumer{consumer: consumer}, nil
}

func (c *Consumer) Read(ctx context.Context) (consume.Message, error) {
	for {
		if err := ctx.Err(); err != nil {
			return consume.Message{}, err
		}
		msg, err := c.consumer.ReadMessage(200 * time.Millisecond)
		if err != nil {
			var kafkaErr kafka.Error
			if errors.As(err, &kafkaErr) && kafkaErr.IsTimeout() {
				continue
			}
			return consume.Message{}, err
		}
		topic := ""
		if msg.TopicPartition.Topic != nil {
			topic = *msg.TopicPartition.Topic
		}
		return consume.Message{
			Topic:     topic,
			Partition: msg.TopicPartition.Partition,
			Offset:    int64(msg.TopicPartition.Offset),
			Key:       msg.Key,
			Value:     msg.Value,
			Token:     msg,
		}, nil
	}
}

func (c *Consumer) Commit(_ context.Context, msg consume.Message) error {
	raw, ok := msg.Token.(*kafka.Message)
	if !ok || raw == nil {
		return errors.New("message is missing its broker token")
	}
	_, err := c.consumer.CommitMessage(raw)
	return err
}

func (c *Consumer) Close() error {
	return c.consumer.Close()
}
