package sourceConnector

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	dbDriver "github.com/ali-pourmoghadam/kafka-elastic/db_drivers"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
)

type KafkaConnect struct {
	kafkaReader *kafka.Reader
	Target      dbDriver.DbDriver
}

func (kc *KafkaConnect) Init() {

	mechanism, err := scram.Mechanism(scram.SHA256, kafkaUsername, kafkaPassword)
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

	log.Println("kafka connected")
}

func (kc *KafkaConnect) Connect() {

	kc.Read()

}

func (kc *KafkaConnect) Read() {

	for {
		// Read a message from the topic
		msg, err := kc.kafkaReader.ReadMessage(context.Background())

		if err != nil {
			log.Fatalf("Error reading message: %s", err)
		}

		// Print the message value
		fmt.Printf("Message received: %s\n", string(msg.Value))

		kc.Target.Write(os.Getenv("INDEX_NAME_EXTERNAL"), msg.Value)
	}

}
