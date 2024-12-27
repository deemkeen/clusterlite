# ClusterLite

ClusterLite is a distributed SQLite database system that implements event sourcing using RedPanda (Kafka-compatible) for synchronization across multiple nodes.
It provides a generic HTTP API for managing entities while maintaining eventual consistency across all nodes in the cluster.

## Features

- Generic entity management system with table operation abstractions
- Event-sourced architecture using RedPanda/Kafka
- Multi-node SQLite database synchronization
- RESTful API with CORS support
- Horizontal scalability with Nginx load balancing
- WAL (Write-Ahead Logging) enabled SQLite
- Docker-based deployment
- Sample User entity implementation
- Web-based UI for the User entity

## Architecture

The system consists of:

- Multiple ClusterLite nodes running SQLite databases
- RedPanda message broker for event distribution
- Nginx load balancer
- Shared volume for SQLite database files
- Generic entity registry and operation handlers

### Components

- `lib/clusterlite.go`: Core functionality including entity registry, event handling, and HTTP routing
- `models/user.go`: Sample User entity implementation
- `main.go`: Application entry point and configuration
- `index.html`: Web-based management interface

## Prerequisites

- Docker and Docker Compose
- Go 1.23.3 or later (for development)

## Quick Start

1. Create required Docker network:
```bash
docker network create vputest
```

2. Build and start the cluster:
```bash
chmod +x build.sh
./build.sh
docker-compose up --build
```

3. Access the web interface, by opening the index.html in your browser

## API Endpoints

Generic entity endpoints:

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/{table}` | Create entity |
| GET | `/{table}` | List entities |
| GET | `/{table}/{id}` | Get entity |
| PUT | `/{table}/{id}` | Update entity |
| DELETE | `/{table}/{id}` | Delete entity |

### Example User Operations

Create user:
```bash
curl -X POST http://localhost:8082/users \
  -H "Content-Type: application/json" \
  -d '{"name": "John Doe", "email": "john@example.com"}'
```

List users:
```bash
curl http://localhost:8082/users
```

## Implementation Details

### Entity Operations Interface

Entities must implement the `TableOperations` interface:

```go
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
```

### Event Structure

```go
type DatabaseEvent struct {
    ID        string
    Operation string
    TableName string
    Data      map[string]any
    Timestamp time.Time
}
```

## Development

Adding a new entity type:

1. Create a new model file in `models/`
2. Implement the `TableOperations` interface
3. Register the entity in `main.go`

## Contributing

1. Fork the repository
2. Create a feature branch
3. Implement changes
4. Submit a Pull Request

## License

MIT License

## Acknowledgments

- RedPanda for Kafka-compatible message broker
- SQLite for embedded database
- Go community for excellent libraries
