## Service Template - Duct Framed Monolith or Microservices

This is a template for a Duct framed monolith or microservices.

It is designed to be configurable to support different features using multitude of different services like Postgres, Redis, Kafka, etc but also none as well.

Its a JVM based service written in Clojure using the Duct framework. It is designed to be a base for a monolith or microservices.

The big core design decision here is that general use of features are decoupled from underlying implementation details or dependecies, allowing for easy swapping of dependencies or even implementing new features with different dependencies without even requiring touching logic code, ideally.

The main abstractions are:
- [] Storage
- [] Messaging Queues, Bus & or Event Bus and Deadletters
- [] Database
- [] Text Search
- [] Cache
- [] Rate Limiting
- [] Monitoring

The supported clients give way to common protocols that can be implemented by different services, like:
- [] Cache Protocol
- [] Storage Protocol
- [] Messaging Protocol (Producers and Consumers)
- [] Database Protocol
- [] Text Search Protocol

This is meant to both allow for same system to run with mostly the same feature set to run on dev or staging evnrioments but with wildly different dependencies (infra), lets way you use Kafka for the events in prod, but in dev you use in-memory for the events, this way you can test the system with different dependencies without even requiring touching logic code, ideally.

## Development

### Running Locally
```bash
clojure -M:duct
```

### Docker

Build the optimized Docker image:
```bash
docker build -t core-service:latest .
```

Run the container:
```bash
docker run -p 3000:3000 core-service:latest
```

Run with environment variables:
```bash
docker run -p 3000:3000 core-service
```

Supported clients:
- [ ] Cache
- [ ] Storage
- [ ] Messaging Queues, Bus & or Event Bus
- [ ] Database
- [ ] Text Search

Supported protocols:
- [ ] Cache Protocol
- [ ] Storage Protocol
- [ ] Messaging Protocol
- [ ] Database Protocol
- [ ] Text Search Protocol

### Databases

- [ ] Postgres
- [ ] Redis

### Storage

- [ ] S3
- [ ] Minio

### Messaging Queues, Bus & or Event Bus

- [ ] Redis
- [ ] Kafka
- [ ] RabbitMQ
- [ ] In-memory
