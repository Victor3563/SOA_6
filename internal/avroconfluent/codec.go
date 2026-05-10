package avroconfluent

import (
	"encoding/binary"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/hamba/avro/v2"
)

const valueSubjectSuffix = "-value"

type Codec struct {
	client schemaregistry.Client
	schema avro.Schema
	schemaID int
}

func New(client schemaregistry.Client, topic string) (*Codec, error) {
	subject := topic + valueSubjectSuffix
	meta, err := client.GetLatestSchemaMetadata(subject)
	if err != nil {
		return nil, fmt.Errorf("get schema for %s: %w", subject, err)
	}
	parsed, err := avro.Parse(meta.Schema)
	if err != nil {
		return nil, fmt.Errorf("parse schema: %w", err)
	}
	return &Codec{client: client, schema: parsed, schemaID: meta.ID}, nil
}

func (c *Codec) Encode(msg interface{}) ([]byte, error) {
	payload, err := avro.Marshal(c.schema, msg)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 5+len(payload))
	out[0] = 0
	binary.BigEndian.PutUint32(out[1:5], uint32(c.schemaID))
	copy(out[5:], payload)
	return out, nil
}

func (c *Codec) Decode(data []byte, msg interface{}) error {
	if len(data) < 5 {
		return fmt.Errorf("payload too short")
	}
	if data[0] != 0 {
		return fmt.Errorf("unknown magic byte %d", data[0])
	}
	return avro.Unmarshal(c.schema, data[5:], msg)
}

func EncodeFromRegistry(registryURL, topic string, msg interface{}) ([]byte, error) {
	client, err := schemaregistry.NewClient(schemaregistry.NewConfig(registryURL))
	if err != nil {
		return nil, err
	}
	codec, err := New(client, topic)
	if err != nil {
		return nil, err
	}
	return codec.Encode(msg)
}

