# ClusterLite

ClusterLite is a distributed SQLite database system that uses Apache Kafka (via RedPanda) for event synchronization across multiple nodes. It provides a simple HTTP API for managing user data while maintaining consistency across all nodes in the cluster.

## Features

- Distributed SQLite database with event-driven synchronization
- REST API for CRUD operations
- Event sourcing using Kafka/RedPanda
- Load balancing with Nginx
- Docker-based deployment
- Horizontal scalability
- CORS support
- WAL (Write-Ahead Logging) enabled for better concurrency

## Architecture

The system consists of the following components:

- Multiple ClusterLite nodes running SQLite databases
- RedPanda (Kafka-compatible message broker) for event distribution
- Nginx load balancer for distributing requests
- Shared volume for SQLite database files
- Event-driven consistency model

## Prerequisites

- Docker and Docker Compose
- Go 1.23.3 or later (for development)
- A Docker network named "vputest"

## Quick Start

1. Create the required Docker network:
```bash
docker network create vputest
```

2. Build and start the cluster:
```bash
docker-compose up --build
```

The service will be available at `http://localhost:80`

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/users` | Create a new user |
| GET | `/users` | Get all users |
| GET | `/users/{id}` | Get a specific user |
| PUT | `/users/{id}` | Update a user |
| DELETE | `/users/{id}` | Delete a user |

### Example Requests

Create a user:
```bash
curl -X POST http://localhost/users \
  -H "Content-Type: application/json" \
  -d '{"name": "John Doe", "email": "john@example.com"}'
```

Get all users:
```bash
curl http://localhost/users
```

## Configuration

The system can be configured through the following files:

- `docker-compose.yml`: Container configuration
- `nginx.conf`: Load balancer settings
- Environment variables in the Docker Compose file

## Development

To build the project locally:

```bash
go build -o clusterlite
```

## Project Structure

```
.
├── clusterlite.go      # Main application code
├── Dockerfile          # Docker build instructions
├── docker-compose.yml  # Container orchestration
├── go.mod             # Go dependencies
├── go.sum             # Go dependencies checksums
├── nginx.conf         # Nginx configuration
└── README.md          # This file
```

## How It Works

1. Each write operation (CREATE/UPDATE/DELETE) generates an event
2. Events are published to RedPanda (Kafka)
3. All nodes consume events and apply them to their local SQLite database
4. Read operations are served directly from the local SQLite database
5. Nginx distributes incoming requests across all nodes

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- RedPanda for providing a Kafka-compatible message broker
- SQLite for the embedded database
- The Go community for excellent libraries and tools

## Disclaimer

This project is meant for educational purposes and may need additional hardening for production use.
