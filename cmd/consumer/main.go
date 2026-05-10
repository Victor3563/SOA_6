package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/victor3563/soa6/internal/avroconfluent"
	"github.com/victor3563/soa6/internal/cassandra"
	"github.com/victor3563/soa6/internal/config"
	"github.com/victor3563/soa6/internal/events"
	"github.com/victor3563/soa6/internal/handler"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	cfg := config.ConsumerFromEnv()

	repo, err := cassandra.WaitReady(cfg.CassandraHosts, cfg.CassandraKeyspace, 60)
	if err != nil {
		log.Fatalf("cassandra: %v", err)
	}
	defer repo.Close()
	log.Printf("INFO Cassandra is ready")

	srClient, err := schemaregistry.NewClient(schemaregistry.NewConfig(cfg.SchemaRegistryURL))
	if err != nil {
		log.Fatalf("schema registry: %v", err)
	}
	codec, err := avroconfluent.New(srClient, cfg.Topic)
	if err != nil {
		log.Fatalf("avro codec: %v", err)
	}

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  cfg.KafkaBootstrap,
		"group.id":           cfg.ConsumerGroup,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false,
		"session.timeout.ms": 10000,
	})
	if err != nil {
		log.Fatalf("kafka consumer: %v", err)
	}
	defer consumer.Close()

	if err := consumer.SubscribeTopics([]string{cfg.Topic}, nil); err != nil {
		log.Fatalf("subscribe: %v", err)
	}

	h := handler.New(repo)
	log.Printf("INFO Consumer started: group=%s topic=%s bootstrap=%s", cfg.ConsumerGroup, cfg.Topic, cfg.KafkaBootstrap)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	running := true

	go func() {
		<-sigCh
		log.Printf("INFO Shutdown signal received")
		running = false
	}()

	for running {
		msg, err := consumer.ReadMessage(1 * time.Second)
		if err != nil {
			if kafkaErr, ok := err.(kafka.Error); ok && kafkaErr.Code() == kafka.ErrTimedOut {
				continue
			}
			log.Printf("ERROR poll: %v", err)
			continue
		}

		partition := msg.TopicPartition.Partition
		offset := int64(msg.TopicPartition.Offset)

		if err := processMessage(codec, repo, h, consumer, msg, partition, offset); err != nil {
			log.Printf("ERROR process partition=%d offset=%d: %v", partition, offset, err)
			time.Sleep(time.Second)
		}
	}

	log.Printf("INFO Closing consumer...")
}

func processMessage(
	codec *avroconfluent.Codec,
	repo *cassandra.Repository,
	h *handler.Handler,
	consumer *kafka.Consumer,
	msg *kafka.Message,
	partition int32,
	offset int64,
) error {
	var ev events.WarehouseEvent
	if err := codec.Decode(msg.Value, &ev); err != nil {
		return err
	}

	processed, err := repo.IsEventProcessed(ev.EventID)
	if err != nil {
		return err
	}
	if processed {
		log.Printf("INFO SKIP duplicate event_id=%s event_type=%s partition=%d offset=%d",
			ev.EventID, ev.EventType, partition, offset)
		_, err := consumer.CommitMessage(msg)
		return err
	}

	if err := h.Handle(&ev); err != nil {
		return err
	}
	if err := repo.MarkEventProcessed(ev.EventID, ev.EventType, partition, offset); err != nil {
		return err
	}

	log.Printf("INFO PROCESSED event_id=%s event_type=%s partition=%d offset=%d",
		ev.EventID, ev.EventType, partition, offset)

	_, err = consumer.CommitMessage(msg)
	return err
}
