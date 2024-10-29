package main

import (
	"log"

	dbDriver "github.com/ali-pourmoghadam/kafka-elastic/db_drivers"
	sourceConnector "github.com/ali-pourmoghadam/kafka-elastic/source_connector"
)

func main() {

	log.Println("START PROGRAM")

	// ############### CREATE DB_DRVIERS REPOSITORY ################

	elasticRepo := dbDriver.GetElasticRepository()

	// ############### ESTABLISH DB_DRIVER CONNECTIONS ################

	elasticRepo.Connect()

	// ############### GRAB DATA FROM SOURCES ################

	kafka := &sourceConnector.KafkaConnect{
		Target: elasticRepo,
	}

	kafkaConnectionRepo := sourceConnector.AssignRepo(kafka)

	// ############### INDEX DATA INTO DESTINATION DB ################

	kafkaConnectionRepo.Source.Init()

	kafkaConnectionRepo.Source.Connect()

}
