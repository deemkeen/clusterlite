package lib

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/gorilla/mux"
	_ "github.com/mattn/go-sqlite3"
)

const (
	PRAGMAS = `
    PRAGMA journal_mode=WAL;
    PRAGMA synchronous=NORMAL;
    PRAGMA busy_timeout=5000;           -- Important for concurrent access
    PRAGMA cache_size=-2000;            -- Use 2MB of memory for cache
    PRAGMA mmap_size=268435456;         -- Memory-mapped I/O (256MB)
    `
	TOPIC          = "db_events"
	CONSUMER_GROUP = "db-consumer-group"
)

type Entity interface {
	GetTableName() string
	Validate() error
}

type TableOperations interface {
	HandleUpsert(ctx context.Context, tx *sql.Tx, event DatabaseEvent) error
	HandleDelete(ctx context.Context, tx *sql.Tx, event DatabaseEvent) error
	Get(ctx context.Context, id string) (map[string]any, error)
	GetAll(ctx context.Context) ([]map[string]any, error)
	CreateEvent(data interface{}) (*DatabaseEvent, error)
	UpdateEvent(id string, data interface{}) (*DatabaseEvent, error)
	DeleteEvent(id string) (*DatabaseEvent, error)
	GetTableName() string
	CreateTable(ctx context.Context) error
}

type EntityRegistry struct {
	operations map[string]TableOperations
	db         *sql.DB
	producer   sarama.SyncProducer
	mu         sync.RWMutex
}

func NewEntityRegistry(db *sql.DB, producer sarama.SyncProducer) *EntityRegistry {
	return &EntityRegistry{
		operations: make(map[string]TableOperations),
		db:         db,
		producer:   producer,
	}
}

func (er *EntityRegistry) Register(ops TableOperations) error {
	er.mu.Lock()
	defer er.mu.Unlock()
	if err := ops.CreateTable(context.Background()); err != nil {
		return err
	}
	er.operations[ops.GetTableName()] = ops
	return nil
}

type DatabaseEvent struct {
	ID        string         `json:"id"`
	Operation string         `json:"operation"`
	TableName string         `json:"table_name"`
	Data      map[string]any `json:"data"`
	Timestamp time.Time      `json:"timestamp"`
}

type Database struct {
	Db       *sql.DB
	producer sarama.SyncProducer
	Registry *EntityRegistry
}

func createTopic(brokers []string) error {
	config := sarama.NewConfig()
	config.Version = sarama.V2_0_0_0
	admin, err := sarama.NewClusterAdmin(brokers, config)
	if err != nil {
		return err
	}
	defer func(admin sarama.ClusterAdmin) {
		err := admin.Close()
		if err != nil {
			fmt.Println("Error closing cluster admin:", err)
		}
	}(admin)

	topics, err := admin.ListTopics()
	if err != nil {
		return err
	}

	if _, exists := topics[TOPIC]; exists {
		return nil
	}

	topicDetail := &sarama.TopicDetail{
		NumPartitions:     3,
		ReplicationFactor: 1,
	}

	err = admin.CreateTopic(TOPIC, topicDetail, false)
	if err != nil && !strings.Contains(err.Error(), sarama.ErrTopicAlreadyExists.Error()) {
		return err
	}
	return nil
}

func NewDatabase(dbPath string, kafkaBrokers []string) (*Database, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, err
	}

	if _, err := db.Exec(PRAGMAS); err != nil {
		return nil, err
	}

	if err := createTopic(kafkaBrokers); err != nil {
		return nil, err
	}

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewRoundRobinPartitioner
	config.Version = sarama.V2_0_0_0

	producer, err := sarama.NewSyncProducer(kafkaBrokers, config)
	if err != nil {
		err := db.Close()
		if err != nil {
			return nil, err
		}
		return nil, err
	}

	database := &Database{
		Db:       db,
		producer: producer,
		Registry: NewEntityRegistry(db, producer),
	}

	return database, nil
}

func (d *Database) PublishEvent(event DatabaseEvent) error {
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}

	_, _, err = d.producer.SendMessage(&sarama.ProducerMessage{
		Topic: TOPIC,
		Key:   sarama.StringEncoder(event.TableName),
		Value: sarama.ByteEncoder(data),
	})
	return err
}

type DatabaseConsumer struct {
	Registry      *EntityRegistry
	consumerGroup sarama.ConsumerGroup
	groupID       string
}

func NewDatabaseConsumer(kafkaBrokers []string, registry *EntityRegistry) (*DatabaseConsumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRoundRobin()
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Version = sarama.V2_0_0_0

	group, err := sarama.NewConsumerGroup(kafkaBrokers, CONSUMER_GROUP, config)
	if err != nil {
		return nil, err
	}

	return &DatabaseConsumer{
		Registry:      registry,
		consumerGroup: group,
	}, nil
}

type ConsumerGroupHandler struct {
	registry *EntityRegistry
}

func (h *ConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (h *ConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

func (h *ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message := <-claim.Messages():
			log.Printf("Received message: %s", string(message.Value))

			var event DatabaseEvent
			if err := json.Unmarshal(message.Value, &event); err != nil {
				log.Printf("Error unmarshaling event: %v", err)
				continue
			}

			log.Printf("Processing event: %+v", event)
			if err := h.processEvent(session.Context(), event); err != nil {
				log.Printf("Error processing event: %v", err)
			}

			session.MarkMessage(message, "")
			log.Printf("Successfully processed event")

		case <-session.Context().Done():
			return nil
		}
	}
}

func (h *ConsumerGroupHandler) processEvent(ctx context.Context, event DatabaseEvent) error {
	ops, exists := h.registry.operations[event.TableName]
	if !exists {
		return fmt.Errorf("no operations registered for table: %s", event.TableName)
	}

	tx, err := h.registry.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func(tx *sql.Tx) {
		err := tx.Rollback()
		if err != nil {
			fmt.Println(err.Error())
		}
	}(tx)

	switch event.Operation {
	case "INSERT", "UPDATE":
		if err := ops.HandleUpsert(ctx, tx, event); err != nil {
			return err
		}
	case "DELETE":
		if err := ops.HandleDelete(ctx, tx, event); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (dc *DatabaseConsumer) Start(ctx context.Context) error {
	topics := []string{TOPIC}
	handler := &ConsumerGroupHandler{registry: dc.Registry}

	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		for {
			if err := dc.consumerGroup.Consume(ctx, topics, handler); err != nil {
				log.Printf("Error from consumer: %v", err)
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()

	<-ctx.Done()
	wg.Wait()
	return ctx.Err()
}

type EntityHandler struct {
	registry *EntityRegistry
}

func NewEntityHandler(registry *EntityRegistry) *EntityHandler {
	return &EntityHandler{registry: registry}
}

func (h *EntityHandler) HandleCreate(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	tableName := vars["table"]

	ops, exists := h.registry.operations[tableName]
	if !exists {
		http.Error(w, "Table not found", http.StatusNotFound)
		return
	}

	var data interface{}
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	event, err := ops.CreateEvent(data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	eventJSON, err := json.Marshal(event)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Printf("Publishing event: %s", string(eventJSON))

	if _, _, err := h.registry.producer.SendMessage(&sarama.ProducerMessage{
		Topic: TOPIC,
		Key:   sarama.StringEncoder(event.TableName),
		Value: sarama.ByteEncoder(eventJSON),
	}); err != nil {
		log.Printf("Failed to publish event: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Printf("Successfully published event")
	w.WriteHeader(http.StatusAccepted)
}

func (h *EntityHandler) HandleUpdate(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	tableName := vars["table"]
	id := vars["id"]

	ops, exists := h.registry.operations[tableName]
	if !exists {
		http.Error(w, "Table not found", http.StatusNotFound)
		return
	}

	var data interface{}
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	event, err := ops.UpdateEvent(id, data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	eventJSON, err := json.Marshal(event)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if _, _, err := h.registry.producer.SendMessage(&sarama.ProducerMessage{
		Topic: TOPIC,
		Key:   sarama.StringEncoder(event.TableName),
		Value: sarama.ByteEncoder(eventJSON),
	}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}

func (h *EntityHandler) HandleDelete(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	tableName := vars["table"]
	id := vars["id"]

	ops, exists := h.registry.operations[tableName]
	if !exists {
		http.Error(w, "Table not found", http.StatusNotFound)
		return
	}

	event, err := ops.DeleteEvent(id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	eventJSON, err := json.Marshal(event)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if _, _, err := h.registry.producer.SendMessage(&sarama.ProducerMessage{
		Topic: TOPIC,
		Key:   sarama.StringEncoder(event.TableName),
		Value: sarama.ByteEncoder(eventJSON),
	}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}

func (h *EntityHandler) HandleGet(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	tableName := vars["table"]
	id := vars["id"]

	ops, exists := h.registry.operations[tableName]
	if !exists {
		http.Error(w, "Table not found", http.StatusNotFound)
		return
	}

	entity, err := ops.Get(r.Context(), id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if entity == nil {
		http.Error(w, "Entity not found", http.StatusNotFound)
		return
	}

	json.NewEncoder(w).Encode(entity)
}

func (h *EntityHandler) HandleGetAll(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	tableName := vars["table"]

	ops, exists := h.registry.operations[tableName]
	if !exists {
		http.Error(w, "Table not found", http.StatusNotFound)
		return
	}

	entities, err := ops.GetAll(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err = json.NewEncoder(w).Encode(entities)
	if err != nil {
		fmt.Println(err.Error())
	}
}

func enableCORS(handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "*")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		handler(w, r)
	}
}

func StartRouter(registry *EntityRegistry) *mux.Router {
	router := mux.NewRouter()
	entityHandler := NewEntityHandler(registry)

	router.HandleFunc("/{table}", enableCORS(entityHandler.HandleCreate)).Methods("POST", "OPTIONS")
	router.HandleFunc("/{table}", enableCORS(entityHandler.HandleGetAll)).Methods("GET", "OPTIONS")
	router.HandleFunc("/{table}/{id}", enableCORS(entityHandler.HandleUpdate)).Methods("PUT", "OPTIONS")
	router.HandleFunc("/{table}/{id}", enableCORS(entityHandler.HandleDelete)).Methods("DELETE", "OPTIONS")
	router.HandleFunc("/{table}/{id}", enableCORS(entityHandler.HandleGet)).Methods("GET", "OPTIONS")

	return router
}
