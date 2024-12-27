package main

import (
	cl "clusterlite/lib"
	"clusterlite/models"
	"context"
	"log"
	"net/http"
	"path/filepath"
)

const (
	DB_PATH   = "./db/db.sqlite"
	KAFKA_URL = "redpanda:9092"
	HTTP_PORT = ":8082"
)

func main() {
	ctx := context.Background()
	kafkaBrokers := []string{KAFKA_URL}
	dbPath, err := filepath.Abs(DB_PATH)

	if err != nil {
		log.Fatal(err)
	}

	db, err := cl.NewDatabase(dbPath, kafkaBrokers)
	if err != nil {
		log.Fatal(err)
	}

	// Register User Table
	if err := db.Registry.Register(models.NewUserOperations(db.Db)); err != nil {
		log.Fatal(err)
	}

	//TODO: Register other tables using the same principle

	consumer, err := cl.NewDatabaseConsumer(kafkaBrokers, db.Registry)
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		if err := consumer.Start(ctx); err != nil {
			log.Printf("Consumer error: %v", err)
		}
	}()

	log.Printf("Starting HTTP server on %s", HTTP_PORT)
	if err := http.ListenAndServe(HTTP_PORT, cl.StartRouter(db.Registry)); err != nil {
		log.Fatal(err)
	}
}
