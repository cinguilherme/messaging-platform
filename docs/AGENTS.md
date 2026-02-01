## Core service is a monolith that is designed to be a base for a monolith or microservices.

It is designed to be configurable to support different features using multitude of different services like Postgres, Redis, Kafka, etc but also none as well.

Its a JVM based service written in Clojure using the Duct framework. It is designed to be a base for a monolith or microservices.

The big core design decision here is that general use of features are decoupled from underlying implementation details or dependecies, allowing for easy swapping of dependencies or even implementing new features with different dependencies without even requiring touching logic code, ideally.

The main abstractions are:
- [] Storage
- [] Messaging Queues, Bus & or Event Bus and Deadletters
- [] Database
- [] Text Search
- [] Cache

Is's purpose is to be a testing service for the D-Core library, allowing for easy testing of the D-Core library with different dependencies or even implementing new features with different dependencies without even requiring touching logic code, ideally.

So some API's will be implemented here:

- Real Time messaging API (meant to power Chat like Apps such as WhatsApp, Telegram, etc) and also isolated chats and group chats that can be used in Backoffice.
