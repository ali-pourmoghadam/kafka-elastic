package sourceConnector

import "os"

var topic = os.Getenv("TOPIC_NAME")

var partionLeader = os.Getenv("KAFKA_ADDR")

var kafkaUsername = os.Getenv("KAFKA_USERNAME")

var kafkaPassword = os.Getenv("KAFKA_PASSWORD")

type SourceInterface interface {
	Init()
	Read()
	Connect()
}

type SourceRepo struct {
	Source SourceInterface
}

func AssignRepo(source SourceInterface) *SourceRepo {
	return &SourceRepo{
		Source: source,
	}
}
