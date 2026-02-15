## Service Template - Duct Framed Monolith or Microservices

This is a sample application built using Duct + Integrant + D-core
Its a POC showcasing the abstractions of the Duct+Integrat+D-core stack mainly. 

One such goal is to present Clojure in a more inviting way, specifiually to new audiences and new developers still learning. So building a simple but complete application is a good way to show how the pieces fit together, and how to build something real with Clojure. Not those old school "hello world", whatever API etc fromt he old (NodeJS+Express) days.

## Introduction

The application is a simple messaging platform. Designed to be low cost to operate with a sisable feature set, it can be used as a base for building more complex messaging solutions. It includes features like:
- User registration and authentication (via Keycloak)
- Creating and managing conversations
- Sending and receiving messages in real-time (via WebSockets)
- Handling attachments and media messages
- Read receipts and message status tracking
- Unread message indexing for efficient retrieval of unread counts

There are some exploratory things yet to be cleaned up.

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
docker run  -p 3000:3000 -p 3001:3001 core-service:latest
```
