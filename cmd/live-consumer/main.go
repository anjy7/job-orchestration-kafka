package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"reliable-orchestration/internal/broker"
	"reliable-orchestration/internal/codec"
	"reliable-orchestration/internal/consume"
	"reliable-orchestration/internal/job"
	"reliable-orchestration/internal/pipeline"
	"reliable-orchestration/internal/topics"
)

func main() {
	brokers := flag.String("brokers", "localhost:9092", "Kafka bootstrap servers")
	group := flag.String("group", "agent-live", "consumer group for the live topic")
	attempts := flag.Int("attempts", 2, "attempts including the first run")
	registry := flag.String("schema-registry", "http://localhost:8081", "schema registry url")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	records, err := codec.New(*registry)
	if err != nil {
		log.Fatal(err)
	}
	defer records.Close()

	producer, err := broker.NewProducer(*brokers, records)
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	consumer, err := broker.NewConsumer(*brokers, *group, []string{topics.Live})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	err = consume.Run(ctx, consumer, records, pipeline.Handler{
		Agent:       stubAgent{},
		MaxAttempts: *attempts,
		Publisher:   producer,
	})
	if err != nil {
		log.Fatal(err)
	}
}

// stubAgent stands in for a real classifier. A payload of "fail" makes the
// delivery take the retry path.
type stubAgent struct{}

func (stubAgent) Run(_ context.Context, env job.Envelope) error {
	if string(env.Payload) == "fail" {
		return errors.New("agent failed")
	}
	return nil
}
