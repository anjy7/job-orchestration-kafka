package codec

import (
	"errors"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/jsonschema"

	"reliable-orchestration/internal/job"
	"reliable-orchestration/internal/topics"
)

var ErrNoURL = errors.New("schema registry url is required")

// envelopeSchema is the value schema registered for every job topic.
// payload is the base64 string encoding/json produces for a []byte field.
const envelopeSchema = `{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "JobEnvelope",
  "type": "object",
  "additionalProperties": false,
  "required": ["id", "attempt"],
  "properties": {
    "id": {"type": "string", "minLength": 1},
    "payload": {"type": "string"},
    "attempt": {"type": "integer", "minimum": 1},
    "error": {"type": "string"}
  }
}`

type wire struct {
	ID      string `json:"id"`
	Payload []byte `json:"payload,omitempty"`
	Attempt int    `json:"attempt"`
	Error   string `json:"error,omitempty"`
}

// Codec reads and writes envelopes through the schema registry.
// The Kafka value is a schema id followed by JSON.
type Codec struct {
	client schemaregistry.Client
	ser    *jsonschema.Serializer
	deser  *jsonschema.Deserializer
}

func New(registryURL string) (*Codec, error) {
	if registryURL == "" {
		return nil, ErrNoURL
	}
	client, err := schemaregistry.NewClient(schemaregistry.NewConfig(registryURL))
	if err != nil {
		return nil, err
	}

	serConf := jsonschema.NewSerializerConfig()
	serConf.AutoRegisterSchemas = false
	serConf.UseLatestVersion = true
	ser, err := jsonschema.NewSerializer(client, serde.ValueSerde, serConf)
	if err != nil {
		client.Close()
		return nil, err
	}
	deserConf := jsonschema.NewDeserializerConfig()
	deserConf.EnableValidation = true
	deser, err := jsonschema.NewDeserializer(client, serde.ValueSerde, deserConf)
	if err != nil {
		client.Close()
		return nil, err
	}

	c := &Codec{client: client, ser: ser, deser: deser}
	for _, topic := range []string{topics.Live, topics.Retry, topics.DLQ} {
		if err := c.register(topic); err != nil {
			client.Close()
			return nil, err
		}
	}
	return c, nil
}

func (c *Codec) register(topic string) error {
	info := schemaregistry.SchemaInfo{
		Schema:     envelopeSchema,
		SchemaType: jsonschema.SchemaType,
	}
	subject, err := serde.TopicNameStrategy(topic, serde.ValueSerde, info)
	if err != nil {
		return err
	}
	if _, err := c.client.Register(subject, info, false); err != nil {
		return fmt.Errorf("register %s: %w", subject, err)
	}
	return nil
}

// Marshal writes env as a schema-registry value for topic.
func (c *Codec) Marshal(topic string, env job.Envelope) ([]byte, error) {
	if err := env.Validate(); err != nil {
		return nil, err
	}
	return c.ser.Serialize(topic, wire{
		ID:      env.ID,
		Payload: env.Payload,
		Attempt: env.Attempt,
		Error:   env.Error,
	})
}

func (c *Codec) Unmarshal(topic string, b []byte) (job.Envelope, error) {
	if len(b) == 0 {
		return job.Envelope{}, errors.New("empty message")
	}
	var decoded wire
	if err := c.deser.DeserializeInto(topic, b, &decoded); err != nil {
		return job.Envelope{}, err
	}
	env := job.Envelope{
		ID:      decoded.ID,
		Payload: decoded.Payload,
		Attempt: decoded.Attempt,
		Error:   decoded.Error,
	}
	if err := env.Validate(); err != nil {
		return job.Envelope{}, err
	}
	return env, nil
}

func (c *Codec) Close() error {
	return c.client.Close()
}
