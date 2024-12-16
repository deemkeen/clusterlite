package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	_ "github.com/mattn/go-sqlite3"
)

// DatabaseEvent represents a database modification event
type DatabaseEvent struct {
	ID        string         `json:"id"`
	Operation string         `json:"operation"`
	TableName string         `json:"table_name"`
	Data      map[string]any `json:"data"`
	Timestamp time.Time      `json:"timestamp"`
}

// Database represents our SQLite database wrapper
type Database struct {
	db       *sql.DB
	producer sarama.SyncProducer
}

func createTopic(brokers []string) error {
	config := sarama.NewConfig()
	admin, err := sarama.NewClusterAdmin(brokers, config)
	if err != nil {
		return err
	}
	defer admin.Close()

	// Check if topic exists first
	topics, err := admin.ListTopics()
	if err != nil {
		return err
	}

	if _, exists := topics["db_events"]; exists {
		return nil
	}

	topicDetail := &sarama.TopicDetail{
		NumPartitions:     3,
		ReplicationFactor: 1,
	}

	err = admin.CreateTopic("db_events", topicDetail, false)
	if err != nil && err != sarama.ErrTopicAlreadyExists {
		return err
	}
	return nil
}

func NewDatabase(dbPath string, kafkaBrokers []string) (*Database, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, err
	}

	// Enable WAL mode
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		return nil, err
	}

	// Create topic if not exists
	if err := createTopic(kafkaBrokers); err != nil {
		return nil, err
	}

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewRoundRobinPartitioner

	producer, err := sarama.NewSyncProducer(kafkaBrokers, config)
	if err != nil {
		db.Close()
		return nil, err
	}

	return &Database{
		db:       db,
		producer: producer,
	}, nil
}

func (d *Database) PublishEvent(event DatabaseEvent) error {
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}

	_, _, err = d.producer.SendMessage(&sarama.ProducerMessage{
		Topic: "db_events",
		Key:   sarama.StringEncoder(event.TableName),
		Value: sarama.ByteEncoder(data),
	})
	return err
}

func (d *Database) CreateUser(ctx context.Context, name, email string) error {
	event := DatabaseEvent{
		ID:        uuid.New().String(),
		Operation: "INSERT",
		TableName: "users",
		Data: map[string]any{
			"id":    uuid.New().String(),
			"name":  name,
			"email": email,
		},
		Timestamp: time.Now(),
	}

	return d.PublishEvent(event)
}

func (d *Database) UpdateUser(ctx context.Context, id, name, email string) error {
	event := DatabaseEvent{
		ID:        uuid.New().String(),
		Operation: "UPDATE",
		TableName: "users",
		Data: map[string]any{
			"id":    id,
			"name":  name,
			"email": email,
		},
		Timestamp: time.Now(),
	}

	return d.PublishEvent(event)
}

func (d *Database) DeleteUser(ctx context.Context, id string) error {
	event := DatabaseEvent{
		ID:        uuid.New().String(),
		Operation: "DELETE",
		TableName: "users",
		Data: map[string]any{
			"id": id,
		},
		Timestamp: time.Now(),
	}

	return d.PublishEvent(event)
}

func (d *Database) GetUser(ctx context.Context, id string) (map[string]any, error) {
	var name, email string
	err := d.db.QueryRowContext(ctx,
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

func (d *Database) GetAllUsers(ctx context.Context) ([]map[string]any, error) {
	rows, err := d.db.QueryContext(ctx, "SELECT id, name, email FROM users")
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

// DatabaseConsumer handles database modifications from Kafka events
type DatabaseConsumer struct {
	db            *sql.DB
	consumerGroup sarama.ConsumerGroup
	groupID       string
}

func NewDatabaseConsumer(dbPath string, kafkaBrokers []string, groupID string) (*DatabaseConsumer, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, err
	}

	// Enable WAL mode
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		return nil, err
	}

	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRoundRobin()
	config.Consumer.Offsets.Initial = sarama.OffsetNewest

	group, err := sarama.NewConsumerGroup(kafkaBrokers, groupID, config)
	if err != nil {
		db.Close()
		return nil, err
	}

	return &DatabaseConsumer{
		db:            db,
		consumerGroup: group,
		groupID:       groupID,
	}, nil
}

// ConsumerGroupHandler implements sarama.ConsumerGroupHandler
type ConsumerGroupHandler struct {
	db *sql.DB
}

func (h *ConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (h *ConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

func (h *ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message := <-claim.Messages():
			var event DatabaseEvent
			if err := json.Unmarshal(message.Value, &event); err != nil {
				log.Printf("Error unmarshaling event: %v", err)
				continue
			}

			if err := h.processEvent(session.Context(), event); err != nil {
				log.Printf("Error processing event: %v", err)
			}

			session.MarkMessage(message, "")

		case <-session.Context().Done():
			return nil
		}
	}
}

func (h *ConsumerGroupHandler) processEvent(ctx context.Context, event DatabaseEvent) error {
	tx, err := h.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	switch event.Operation {
	case "INSERT", "UPDATE":
		if err := h.handleUpsert(ctx, tx, event); err != nil {
			return err
		}
	case "DELETE":
		if err := h.handleDelete(ctx, tx, event); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (h *ConsumerGroupHandler) handleUpsert(ctx context.Context, tx *sql.Tx, event DatabaseEvent) error {
	switch event.TableName {
	case "users":
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
	default:
		return nil
	}
}

func (h *ConsumerGroupHandler) handleDelete(ctx context.Context, tx *sql.Tx, event DatabaseEvent) error {
	switch event.TableName {
	case "users":
		_, err := tx.ExecContext(ctx,
			"DELETE FROM users WHERE id = ?",
			event.Data["id"],
		)
		return err
	default:
		return nil
	}
}

func (dc *DatabaseConsumer) Start(ctx context.Context) error {
	topics := []string{"db_events"}
	handler := &ConsumerGroupHandler{db: dc.db}

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

type Server struct {
	db *Database
}

func (s *Server) handleCreateUser(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Name  string `json:"name"`
		Email string `json:"email"`
	}

	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := s.db.CreateUser(r.Context(), input.Name, input.Email); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}

func (s *Server) handleUpdateUser(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	var input struct {
		Name  string `json:"name"`
		Email string `json:"email"`
	}

	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := s.db.UpdateUser(r.Context(), id, input.Name, input.Email); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}

func (s *Server) handleDeleteUser(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	if err := s.db.DeleteUser(r.Context(), id); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}

func (s *Server) handleGetUser(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	user, err := s.db.GetUser(r.Context(), id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if user == nil {
		http.Error(w, "User not found", http.StatusNotFound)
		return
	}

	json.NewEncoder(w).Encode(user)
}

func (s *Server) handleGetAllUsers(w http.ResponseWriter, r *http.Request) {
	users, err := s.db.GetAllUsers(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(users)
}

func main() {
	ctx := context.Background()
	kafkaBrokers := []string{"kafka:8092", "kafka2:8092"}

	db, err := NewDatabase("./db.sqlite", kafkaBrokers)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.db.Exec(`
		CREATE TABLE IF NOT EXISTS users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)
	if err != nil {
		log.Fatal(err)
	}

	consumer, err := NewDatabaseConsumer("./db.sqlite", kafkaBrokers, "db-consumer-group")
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		if err := consumer.Start(ctx); err != nil {
			log.Printf("Consumer error: %v", err)
		}
	}()

	server := &Server{db: db}
	router := mux.NewRouter()

	router.HandleFunc("/users", server.handleCreateUser).Methods("POST")
	router.HandleFunc("/users", server.handleGetAllUsers).Methods("GET")
	router.HandleFunc("/users/{id}", server.handleUpdateUser).Methods("PUT")
	router.HandleFunc("/users/{id}", server.handleDeleteUser).Methods("DELETE")
	router.HandleFunc("/users/{id}", server.handleGetUser).Methods("GET")

	log.Printf("Starting HTTP server on :8082")
	if err := http.ListenAndServe(":8082", router); err != nil {
		log.Fatal(err)
	}
}
