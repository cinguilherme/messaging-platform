## Core Service - Message Platform

This is the core service for the message platform. It is responsible for the core functionality of the message platform.

There are two main use cases for this platform:
- Power a messaging application like WhatsApp or Telegram.
- Power any number of chats between userts more meant to be used by a Backoffice or a CRM that can have many chats with users or even other bots.

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
docker run -p 3000:3000 core-service
```

### Core ServiceContext

The core service is the main service powering the message platform and its design filosofy is around cost to serve and customization of features or even enabling features.
This decisions are made in a way to garantee extreme leaness and cost effectiveness.

From the abosolute bare bones, the app alone wihtout even an Database for persistence of any kind, although, to have uses accounted for at least one storage is required.

Another thing is that storage may be a biased expression since we can use the postgres database for persistence, but also can be the engine behind messages queues. Its up to the deployer of the platform to configure it how ever he wants.

A combination of ENV variables and configuration files and the system map of Duct are the Lego building blocks.


### Features requirements

- [ ] Persistence of messages
- [ ] Persistence of users
- [ ] Persistence of groups
- [ ] Persistence of channels
- [ ] Persistence of messages
- [ ] Persistence of users
- [ ] Persistence of groups
- [ ] Persistence of channels