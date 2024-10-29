package dbDriver

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type ElasticDriver struct {
	client *elasticsearch.Client
}

// type externalMsg struct {
// 	Collector string `json:"Collector"`
// 	Timestamp string `json:"Timestamp"`
// 	Source    string `json:"Source"`
// 	Hostname  string `json:"Hostname"`
// 	IP        string `json:"IP"`
// 	Log       string `json:"Log"`
// }

func (el *ElasticDriver) Connect() {

	tlsConfig := &tls.Config{

		InsecureSkipVerify: true,
	}
	transport := &http.Transport{

		TLSClientConfig: tlsConfig,
	}

	var err error

	el.client, err = elasticsearch.NewClient(elasticsearch.Config{

		Addresses: []string{elasticAddress},

		Username: elasticUsername,

		Password: elasticPassword,

		Transport: transport,
	})

	if err != nil {
		log.Printf("error in connection %v", err)
		return
	}

	log.Println("ElasticDriver connect")

	res, err := el.client.Info()

	if err != nil {
		log.Fatalf("Error getting response: %s", err)
	}
	defer res.Body.Close()

	log.Println("Connected to Elasticsearch:", res.Status())

	el.setupStreamProcessIndex()
	el.setupStreamProcessIndexExternal()

}

func (el *ElasticDriver) Write(dbanme string, data interface{}) {

	log.Println("ElasticDriver write")

	jsonData, err := el.normalized(data)

	if err != nil {

		log.Printf("failed to marshal document to JSON: %v", err)
		return
	}

	req := esapi.IndexRequest{
		Index:   dbanme,
		Body:    strings.NewReader(string(jsonData)),
		Refresh: "true",
	}

	res, err := req.Do(context.Background(), el.client)

	if err != nil {

		log.Printf("failed to push data into elastic: %v", err)
		return
	}

	defer res.Body.Close()

	if res.IsError() {

		log.Printf("error indexing document: %s", res.String())

		return
	}

	log.Printf("Document indexed successfully: %s", res.String())

}

/*
############ READ FROM ELASTICSEARCH ############
*/

func (el *ElasticDriver) Read() error {
	log.Println("ElasticDriver Read")
	return nil
}

/*
############ FanIN FROM ELASTICSEARCH ############
*/

func (el *ElasticDriver) FanInConsumer() {
	log.Println("ElasticDriver FanInTarget")
}

/*
############ Delete FROM ELASTICSEARCH ############
*/

func (el *ElasticDriver) Delete(data interface{}, mutex sync.Mutex) error {
	log.Println("ElasticDriver Delete")
	return nil
}

func (el *ElasticDriver) checkIndexExists(indexName string) bool {

	exists, err := el.client.Indices.Exists([]string{indexName})

	if err != nil {
		log.Fatalf("Error checking if index exists: %v", err)
	}

	defer exists.Body.Close()

	if exists.StatusCode == 200 {

		log.Printf("Index '%s' already exists.\n", indexName)

		return false

	} else if exists.StatusCode == 404 {

		log.Println("you can create index")

		return true

	}

	fmt.Printf("Unexpected status code %d: %s\n", exists.StatusCode, exists)

	return false

}

func (el *ElasticDriver) createIndex(indexName string, Body string) error {

	indexBody := Body

	res, err := el.client.Indices.Create(
		indexName,
		el.client.Indices.Create.WithBody(strings.NewReader(indexBody)),
	)

	if err != nil {
		log.Fatalf("Error creating index: %v", err)
	}

	defer res.Body.Close()

	if res.IsError() {

		log.Printf("Error response from Elasticsearch: %s\n", res)

	} else {
		log.Printf("Index created successfully: %s\n", res)
	}

	return nil
}

func (el *ElasticDriver) setupStreamProcessIndex() {

	checkRes := el.checkIndexExists(elasticIndexName)

	log.Printf("checkRes %v:", checkRes)

	if !checkRes {
		log.Printf("Skipping index setup for logs as it already exists.")
		return
	}

	streamProcessIndex := `{
        "settings": {
            "number_of_shards": 2,
            "number_of_replicas": 3
        },
        "mappings": {
            "properties": {
                "Collector": {
                    "type": "keyword"
                },
                "Timestamp": {
                    "type": "date"
                },
                "Source": {
                    "type": "text"
                },
                "Hostname": {
                    "type": "keyword"
                },
                "IP": {
                    "type": "ip"
                },
                "Log": {
                    "type": "text"
                }
            }
        }
    }`

	err := el.createIndex(elasticIndexName, streamProcessIndex)

	if err != nil {
		log.Fatalf("Error creating logs index: %s", err)
	}
}

func (el *ElasticDriver) setupStreamProcessIndexExternal() {

	checkRes := el.checkIndexExists(elasticIndexNameExternal)

	log.Printf("checkRes %v:", checkRes)

	if !checkRes {
		log.Printf("Skipping index setup for logs as it already exists.")
		return
	}

	streamProcessIndex := `{
        "settings": {
            "number_of_shards": 2,
            "number_of_replicas": 3
        },
        "mappings": {
            "properties": {
                "collector": {
                    "type": "keyword"
                },
                "timestamp": {
                    "type": "date"
                },
                "source": {
                    "type": "text"
                },
                "hostname": {
                    "type": "keyword"
                },
                "ip": {
                    "type": "ip"
                },
                "log": {
                    "type": "text"
                }
            }
        }
    }`

	err := el.createIndex(elasticIndexNameExternal, streamProcessIndex)

	if err != nil {
		log.Fatalf("Error creating logs index: %s", err)
	}
}

func (el *ElasticDriver) normalized(data interface{}) ([]byte, error) {

	document, isBson := data.(bson.M)

	if !isBson {

		log.Println("input data is not of type bson.M")

		return data.([]byte), nil
	}

	_, ok := document["_id"].(primitive.ObjectID)

	if ok {

		delete(document, "_id")

		jsonData, err := bson.MarshalExtJSON(document, false, false)

		if err != nil {

			log.Println("error in json marshal from elastic writer")

			return nil, err
		}

		return jsonData, nil
	}

	jsonData, _ := bson.MarshalExtJSON(document, false, false)

	return jsonData, nil
}
