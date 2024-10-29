package dbDriver

import (
	"log"
	"os"
	"sync"
)

var (
	elasticRepo     *ElasticDriver
	mongoRepo       *MongoDriver
	elasticRepoOnce sync.Once
	mongoRepoOnce   sync.Once
)

var (
	mongoURI         = os.Getenv("MONGO_URI")
	databaseName     = os.Getenv("DB_NAME")
	mongoCollection  = os.Getenv("COLLECTION")
	elasticIndexName = os.Getenv("INDEX_NAME")
	elasticAddress   = os.Getenv("ELASTIC_ADDR")
	elasticUsername   = os.Getenv("ELASTIC_USERNAME")
	elasticPassword   = os.Getenv("ELASTIC_PASSWORD")
)

type DbDriver interface {
	Connect()
	Write(dbName string, data interface{})
	Read() error
	Delete(data interface{}, mutex sync.Mutex) error
	FanInConsumer()
}

func GetElasticRepository() DbDriver {

	elasticRepoOnce.Do(func() {
		elasticRepo = &ElasticDriver{}
		log.Println("Elastic instance created")
	})

	return elasticRepo
}

func GetMongoRepository(driver DbDriver) DbDriver {

	mongoRepoOnce.Do(func() {
		mongoRepo = &MongoDriver{
			targetDriver: driver,
		}
		log.Println("MongoDB instance created")
	})

	return mongoRepo
}
