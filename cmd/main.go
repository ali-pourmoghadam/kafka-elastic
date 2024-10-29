package main

import (
	"log"

	dbDriver "github.com/ali-pourmoghadam/kafka-elastic/db_drivers"
)

func main() {

	log.Println("START PROGRAM")

	// ############### CREATE DB_DRVIERS REPOSITORY ################
	elasticRepo := dbDriver.GetElasticRepository()

	mongoRepo := dbDriver.GetMongoRepository(elasticRepo)

	// ############### ESTABLISH DB_DRIVER CONNECTIONS ################
	mongoRepo.Connect()

	elasticRepo.Connect()

	// ############### GRAB DATA FROM SOURCES ################

	mongoRepo.Read()

	// ############### INDEX DATA INTO DESTINATION DB ################

	mongoRepo.FanInConsumer()

}
