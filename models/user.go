package models

import (
	cl "clusterlite/lib"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

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

// Make sure the interface is implemented correctly
var _ cl.TableOperations = (*UserOperations)(nil)

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

func (uo *UserOperations) HandleUpsert(ctx context.Context, tx *sql.Tx, event cl.DatabaseEvent) error {
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

func (uo *UserOperations) HandleDelete(ctx context.Context, tx *sql.Tx, event cl.DatabaseEvent) error {
	_, err := tx.ExecContext(ctx,
		"DELETE FROM users WHERE id = ?",
		event.Data["id"],
	)
	return err
}

func (uo *UserOperations) CreateEvent(data interface{}) (*cl.DatabaseEvent, error) {
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

	return &cl.DatabaseEvent{
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

func (uo *UserOperations) UpdateEvent(id string, data interface{}) (*cl.DatabaseEvent, error) {
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

	return &cl.DatabaseEvent{
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

func (uo *UserOperations) DeleteEvent(id string) (*cl.DatabaseEvent, error) {
	return &cl.DatabaseEvent{
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
