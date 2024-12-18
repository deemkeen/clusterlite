package main

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
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	_ "github.com/mattn/go-sqlite3"
)

const (
	DB_PATH        = "./db/db.sqlite"
	PRAGMAS        = "PRAGMA journal_mode=WAL; PRAGMA SYNCHRONOUS=NORMAL;"
	KAFKA_URL      = "redpanda:9092"
	TOPIC          = "db_events"
	CONSUMER_GROUP = "db-consumer-group"
	HTTP_PORT      = ":8082"
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
	db       *sql.DB
	producer sarama.SyncProducer
	registry *EntityRegistry
}

func createTopic(brokers []string) error {
	config := sarama.NewConfig()
	config.Version = sarama.V2_0_0_0
	admin, err := sarama.NewClusterAdmin(brokers, config)
	if err != nil {
		return err
	}
	defer admin.Close()

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
		db.Close()
		return nil, err
	}

	database := &Database{
		db:       db,
		producer: producer,
		registry: NewEntityRegistry(db, producer),
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

// User Definition
type User struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Email string `json:"email"`
}

func (u User) GetTableName() string {
	return "users"
}

func (u User) Validate() error {
	if u.Name == "" || u.Email == "" {
		return fmt.Errorf("name and email are required")
	}
	return nil
}

type UserOperations struct {
	db *sql.DB
}

// Declaraction of implentation of TableOperations type
var _ TableOperations = (*UserOperations)(nil)

func NewUserOperations(db *sql.DB) *UserOperations {
	return &UserOperations{db: db}
}

func (uo *UserOperations) GetTableName() string {
	return "users"
}

func (uo *UserOperations) CreateTable(ctx context.Context) error {
	_, err := uo.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)
	return err
}

func (uo *UserOperations) HandleUpsert(ctx context.Context, tx *sql.Tx, event DatabaseEvent) error {
	_, err := tx.ExecContext(ctx,
		`INSERT INTO users (id, name, email)
		VALUES (?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
		name = excluded.name,
		email = excluded.email`,
		event.Data["id"],
		event.Data["name"],
		event.Data["email"],
	)
	return err
}

func (uo *UserOperations) HandleDelete(ctx context.Context, tx *sql.Tx, event DatabaseEvent) error {
	_, err := tx.ExecContext(ctx,
		"DELETE FROM users WHERE id = ?",
		event.Data["id"],
	)
	return err
}

func (uo *UserOperations) CreateEvent(data interface{}) (*DatabaseEvent, error) {
	var input struct {
		Name  string `json:"name"`
		Email string `json:"email"`
	}

	bytes, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("invalid input data format")
	}

	if err := json.Unmarshal(bytes, &input); err != nil {
		return nil, fmt.Errorf("invalid input data format")
	}

	if input.Name == "" || input.Email == "" {
		return nil, fmt.Errorf("name and email are required")
	}

	return &DatabaseEvent{
		ID:        uuid.New().String(),
		Operation: "INSERT",
		TableName: "users",
		Data: map[string]any{
			"id":    uuid.New().String(),
			"name":  input.Name,
			"email": input.Email,
		},
		Timestamp: time.Now(),
	}, nil
}

func (uo *UserOperations) UpdateEvent(id string, data interface{}) (*DatabaseEvent, error) {
	var input struct {
		Name  string `json:"name"`
		Email string `json:"email"`
	}

	bytes, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("invalid input data format")
	}

	if err := json.Unmarshal(bytes, &input); err != nil {
		return nil, fmt.Errorf("invalid input data format")
	}

	if input.Name == "" || input.Email == "" {
		return nil, fmt.Errorf("name and email are required")
	}

	return &DatabaseEvent{
		ID:        uuid.New().String(),
		Operation: "UPDATE",
		TableName: "users",
		Data: map[string]any{
			"id":    id,
			"name":  input.Name,
			"email": input.Email,
		},
		Timestamp: time.Now(),
	}, nil
}

func (uo *UserOperations) DeleteEvent(id string) (*DatabaseEvent, error) {
	return &DatabaseEvent{
		ID:        uuid.New().String(),
		Operation: "DELETE",
		TableName: "users",
		Data: map[string]any{
			"id": id,
		},
		Timestamp: time.Now(),
	}, nil
}

func (uo *UserOperations) Get(ctx context.Context, id string) (map[string]any, error) {
	var name, email string
	err := uo.db.QueryRowContext(ctx,
		"SELECT name, email FROM users WHERE id = ?",
		id,
	).Scan(&name, &email)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"id":    id,
		"name":  name,
		"email": email,
	}, nil
}

func (uo *UserOperations) GetAll(ctx context.Context) ([]map[string]any, error) {
	rows, err := uo.db.QueryContext(ctx, "SELECT id, name, email FROM users")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var users []map[string]any
	for rows.Next() {
		var id, name, email string
		if err := rows.Scan(&id, &name, &email); err != nil {
			return nil, err
		}
		users = append(users, map[string]any{
			"id":    id,
			"name":  name,
			"email": email,
		})
	}
	return users, nil
}

type DatabaseConsumer struct {
	registry      *EntityRegistry
	consumerGroup sarama.ConsumerGroup
	groupID       string
}

func NewDatabaseConsumer(dbPath string, kafkaBrokers []string, groupID string, registry *EntityRegistry) (*DatabaseConsumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRoundRobin()
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Version = sarama.V2_0_0_0

	group, err := sarama.NewConsumerGroup(kafkaBrokers, groupID, config)
	if err != nil {
		return nil, err
	}

	return &DatabaseConsumer{
		registry:      registry,
		consumerGroup: group,
		groupID:       groupID,
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
	defer tx.Rollback()

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
	handler := &ConsumerGroupHandler{registry: dc.registry}

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

	json.NewEncoder(w).Encode(entities)
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

func main() {
	ctx := context.Background()
	kafkaBrokers := []string{KAFKA_URL}

	db, err := NewDatabase(DB_PATH, kafkaBrokers)
	if err != nil {
		log.Fatal(err)
	}

	if err := db.registry.Register(NewUserOperations(db.db)); err != nil {
		log.Fatal(err)
	}

	consumer, err := NewDatabaseConsumer(DB_PATH, kafkaBrokers, CONSUMER_GROUP, db.registry)
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		if err := consumer.Start(ctx); err != nil {
			log.Printf("Consumer error: %v", err)
		}
	}()

	router := mux.NewRouter()
	entityHandler := NewEntityHandler(db.registry)

	router.HandleFunc("/{table}", enableCORS(entityHandler.HandleCreate)).Methods("POST", "OPTIONS")
	router.HandleFunc("/{table}", enableCORS(entityHandler.HandleGetAll)).Methods("GET", "OPTIONS")
	router.HandleFunc("/{table}/{id}", enableCORS(entityHandler.HandleUpdate)).Methods("PUT", "OPTIONS")
	router.HandleFunc("/{table}/{id}", enableCORS(entityHandler.HandleDelete)).Methods("DELETE", "OPTIONS")
	router.HandleFunc("/{table}/{id}", enableCORS(entityHandler.HandleGet)).Methods("GET", "OPTIONS")

	log.Printf("Starting HTTP server on %s", HTTP_PORT)
	if err := http.ListenAndServe(HTTP_PORT, router); err != nil {
		log.Fatal(err)
	}
}
