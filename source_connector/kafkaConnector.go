package sourceConnector

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
)

type KafkaConnect struct {
	kafkaReader *kafka.Reader
}

var ctx = context.Background()

func (kc *KafkaConnect) Init() {

	mechanism, err := scram.Mechanism(scram.SHA256, "username", "password")
	if err != nil {
		panic(err)
	}

	dialer := &kafka.Dialer{
		Timeout:       10 * time.Second,
		DualStack:     true,
		SASLMechanism: mechanism,
	}

	kc.kafkaReader = kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{partionLeader},
		Topic:   topic,
		GroupID: "proccess-read",
		Dialer:  dialer,
	})

}

func (kc *KafkaConnect) Connect() {

	go kc.read()

}

func (kc *KafkaConnect) read() {

	for {
		// Read a message from the topic
		msg, err := kc.kafkaReader.ReadMessage(context.Background())

		if err != nil {
			log.Fatalf("Error reading message: %s", err)
		}

		// Print the message value
		fmt.Printf("Message received: %s\n", string(msg.Value))
	}

}
