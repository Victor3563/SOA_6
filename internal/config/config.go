package config

import (
	"fmt"
	"os"
	"strings"
)

type Consumer struct {
	KafkaBootstrap    string
	SchemaRegistryURL string
	CassandraHosts    []string
	CassandraKeyspace string
	ConsumerGroup     string
	Topic             string
}

type Producer struct {
	KafkaBootstrap    string
	SchemaRegistryURL string
	Topic             string
	SchemaPath        string
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func ConsumerFromEnv() Consumer {
	return Consumer{
		KafkaBootstrap:    env("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
		SchemaRegistryURL: env("SCHEMA_REGISTRY_URL", "http://schema-registry:8081"),
		CassandraHosts:    strings.Split(env("CASSANDRA_HOSTS", "cassandra"), ","),
		CassandraKeyspace: env("CASSANDRA_KEYSPACE", "warehouse"),
		ConsumerGroup:     env("CONSUMER_GROUP", "warehouse-state-consumer"),
		Topic:             env("TOPIC", "warehouse-events"),
	}
}

func ProducerFromEnv() Producer {
	return Producer{
		KafkaBootstrap:    env("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
		SchemaRegistryURL: env("SCHEMA_REGISTRY_URL", "http://schema-registry:8081"),
		Topic:             env("TOPIC", "warehouse-events"),
		SchemaPath:        env("SCHEMA_PATH", "/schemas/warehouse_event.avsc"),
	}
}

func MustReadFile(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		panic(fmt.Errorf("read schema %s: %w", path, err))
	}
	return string(data)
}
