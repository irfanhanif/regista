package main

import (
	"context"

	"github.com/dchest/uniuri"
	kafka "github.com/segmentio/kafka-go"
)

func main() {
	w := KafkaWriter()
	for i := 0; i < 100000; i++ {
		w.WriteMessages(context.Background(), KafkaMessages()...)
	}
	w.Close()
}

func KafkaWriter() *kafka.Writer {
	return kafka.NewWriter(kafka.WriterConfig{
		Brokers:      []string{"localhost:9092"},
		Topic:        "order",
		Async:        true,
		RequiredAcks: 0,
	})
}

func KafkaMessages() []kafka.Message {
	return []kafka.Message{
		kafka.Message{
			Value: []byte(uniuri.NewLen(300)),
		},
	}
}
