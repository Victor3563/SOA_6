package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	"github.com/victor3563/soa6/internal/avroconfluent"
	"github.com/victor3563/soa6/internal/config"
	"github.com/victor3563/soa6/internal/events"
)

func main() {
	eventType := flag.String("event-type", "", "PRODUCT_RECEIVED, ...")
	eventID := flag.String("event-id", "", "reuse for idempotency demo")
	productID := flag.String("product-id", "", "")
	zoneID := flag.String("zone-id", "", "")
	fromZone := flag.String("from-zone", "", "")
	toZone := flag.String("to-zone", "", "")
	quantity := flag.Int("quantity", 0, "")
	countedQty := flag.Int("counted-quantity", 0, "")
	orderID := flag.String("order-id", "", "")
	orderItemsJSON := flag.String("order-items-json", "", `JSON array`)
	timestamp := flag.Int64("timestamp", 0, "unix ms")
	flag.Parse()

	if *eventType == "" {
		fmt.Fprintln(os.Stderr, "required: --event-type")
		os.Exit(2)
	}

	cfg := config.ProducerFromEnv()
	ev := buildEvent(*eventType, *eventID, *productID, *zoneID, *fromZone, *toZone,
		*quantity, *countedQty, *orderID, *orderItemsJSON, *timestamp)

	payload, err := avroconfluent.EncodeFromRegistry(cfg.SchemaRegistryURL, cfg.Topic, &ev)
	if err != nil {
		log.Fatalf("encode: %v", err)
	}

	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": cfg.KafkaBootstrap})
	if err != nil {
		log.Fatalf("kafka producer: %v", err)
	}
	defer producer.Close()

	topic := cfg.Topic
	delivery := make(chan kafka.Event, 1)
	if err := producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          payload,
	}, delivery); err != nil {
		log.Fatalf("produce: %v", err)
	}

	e := <-delivery
	m := e.(*kafka.Message)
	if m.TopicPartition.Error != nil {
		log.Fatalf("delivery: %v", m.TopicPartition.Error)
	}

	out, _ := json.MarshalIndent(map[string]any{
		"status":    "sent",
		"topic":     cfg.Topic,
		"partition": m.TopicPartition.Partition,
		"offset":    m.TopicPartition.Offset,
		"event":     ev,
	}, "", "  ")
	fmt.Println(string(out))
	producer.Flush(10 * 1000)
}

func buildEvent(
	eventType, eventID, productID, zoneID, fromZone, toZone string,
	quantity, countedQty int,
	orderID, orderItemsJSON string,
	timestamp int64,
) events.WarehouseEvent {
	if eventID == "" {
		eventID = uuid.NewString()
	}
	if timestamp == 0 {
		timestamp = time.Now().UnixMilli()
	}

	ev := events.WarehouseEvent{
		EventID:    eventID,
		EventType:  eventType,
		Timestamp:  timestamp,
		ProductID:  events.StrPtr(productID),
		ZoneID:     events.StrPtr(zoneID),
		FromZoneID: events.StrPtr(fromZone),
		ToZoneID:   events.StrPtr(toZone),
		OrderID:    events.StrPtr(orderID),
		OrderItems: []events.OrderItem{},
	}
	if quantity != 0 {
		ev.Quantity = events.IntPtr(quantity)
	}
	if countedQty != 0 {
		ev.CountedQuantity = events.IntPtr(countedQty)
	}
	if orderItemsJSON != "" {
		var items []events.OrderItem
		if err := json.Unmarshal([]byte(orderItemsJSON), &items); err != nil {
			log.Fatalf("order-items-json: %v", err)
		}
		ev.OrderItems = items
	}
	return ev
}
