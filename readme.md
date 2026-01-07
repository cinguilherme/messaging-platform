## Core Service - Message Platform

This is the core service for the message platform. It is responsible for the core functionality of the message platform.

Its desgined to be configurable to support different features using multitude of different services like Postgres, Redis, Kafka, etc but also none as well.

Its a JVM based service written in Clojure using the Duct framework.

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
docker run -p 3000:3000 -e DUCT_ENV=production core-service:latest
```

The Docker image uses:
- **Java 21 (Eclipse Temurin JRE)** for optimal performance
- **Multi-stage build** to minimize image size
- **Non-root user** for enhanced security
- **Health checks** for container orchestration
- **Dependency caching** for faster rebuilds
