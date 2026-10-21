package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"reliable-orchestration/internal/broker"
	"reliable-orchestration/internal/codec"
	"reliable-orchestration/internal/replay"
	"reliable-orchestration/internal/topics"
)

func main() {
	brokers := flag.String("brokers", "localhost:9092", "Kafka bootstrap servers")
	group := flag.String("group", "agent-replay", "consumer group for the dead-letter topic")
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

	consumer, err := broker.NewConsumer(*brokers, *group, []string{topics.DLQ})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	if err := replay.Run(ctx, consumer, records, producer); err != nil {
		log.Fatal(err)
	}
}
