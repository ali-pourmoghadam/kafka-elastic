package dbDriver

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoDriver struct {
	mongoClient  *mongo.Client
	targetDriver DbDriver
}

var Docchannel1 = make(chan bson.M, 20000)
var Docchannel2 = make(chan bson.M, 20000)
var Docchannel3 = make(chan bson.M, 20000)
var Docchannel4 = make(chan bson.M, 20000)
var Docchannel5 = make(chan bson.M, 20000)

var channels = []chan bson.M{Docchannel1, Docchannel2, Docchannel3, Docchannel4, Docchannel5}

var accChannel = make(chan bson.M, 100000)

func (mg *MongoDriver) Connect() {

	clientOptions := options.Client().ApplyURI(mongoURI)

	client, err := mongo.Connect(context.TODO(), clientOptions)

	if err != nil {

		log.Printf("failed to create mongo client: %v", err)
		return

	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if err := client.Ping(ctx, nil); err != nil {
		log.Printf("failed to ping mongo: %v", err)
		return
	}

	mg.mongoClient = client

	log.Println("MongoDriver connect")
}

/*
############ WRITE FROM MONGODB ############
*/

func (mg *MongoDriver) Write(dbName string, data interface{}) {
	log.Println("MongoDriver write")
}

/*
############ DELETE FROM MONGODB ############
*/

func (mg *MongoDriver) Delete(data interface{}, mutex sync.Mutex) error {

	mutex.Lock()

	defer mutex.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

	defer cancel()

	collection := mg.mongoClient.Database(databaseName).Collection(mongoCollection)

	result, err := collection.DeleteOne(ctx, data.(bson.M))

	if err != nil {
		return fmt.Errorf("failed to delete document: %w", err)
	}

	log.Printf("Deleted %d document(s)\n", result.DeletedCount)

	return nil

}

func (mg *MongoDriver) Read() error {

	collection := mg.mongoClient.Database(databaseName).Collection(mongoCollection)

	// Find all documents in the collection
	cursor, err := collection.Find(context.TODO(), bson.D{})

	if err != nil {
		return fmt.Errorf("failed to find users: %v", err)

	}

	defer cursor.Close(context.TODO())

	for cursor.Next(context.TODO()) {

		var document bson.M

		if err := cursor.Decode(&document); err != nil {

			return fmt.Errorf("failed to decode document: %v", err)

		}

		accChannel <- document

	}

	log.Printf("data accumulated : %v", len(accChannel))

	// var wg sync.WaitGroup
	var counter int
	var counterMutex sync.Mutex
	var wg sync.WaitGroup

	for i := 0; i < len(channels); i++ {
		wg.Add(1)
		go func() {

			defer wg.Done()
			for doc := range accChannel {
				counterMutex.Lock()
				idx := counter % len(channels)
				counter++
				counterMutex.Unlock()

				channels[idx] <- doc
				log.Printf("Worker %d processed a document\n", idx)

			}

		}()

	}

	if err := cursor.Err(); err != nil {
		return fmt.Errorf("error occurred during iteration: %v", err)

	}

	close(accChannel)

	wg.Wait()

	log.Println("data is fanout")

	return nil
}

func (mg *MongoDriver) FanInConsumer() {

	var wg sync.WaitGroup
	var Mutex sync.Mutex

	for i, ch := range channels {
		wg.Add(1)

		go func(workerID int, channel chan bson.M) {
			defer wg.Done()

			for document := range channel {

				mg.targetDriver.Write(elasticIndexName, document)

				log.Printf("Worker %d processing document: %v\n", workerID, document)

				mg.Delete(document, Mutex)
			}
		}(i, ch)
	}

	for _, ch := range channels {
		close(ch)
	}

	wg.Wait()
	log.Println("All workers finished processing.")
}
