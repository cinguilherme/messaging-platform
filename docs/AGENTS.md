## Core service is a monolith that is designed to be a base for a monolith or microservices.

It is designed to be configurable to support different features using multitude of different services like Postgres, Redis, Kafka, etc but also none as well.

Its a JVM based service written in Clojure using the Duct framework. It is designed to be a base for a monolith or microservices.

## Core design decisions

the adr 00 defines the core design decisions to use d-core abstractions where possible.

The big core design decision here is that general use of features are decoupled from underlying implementation details or dependecies, allowing for easy swapping of dependencies or even implementing new features with different dependencies without even requiring touching logic code, ideally.

Refer to /docs/dcore-docs/supported.md for a list of supported and unsupported features.

## To consider when writing new or modifying existing code

Always validate the changes using the "make tests" and "INTEGRATION=1 make tests" commands.

Modularization is key, try to keep the codebase modular, specially avoiding logic code in the surface handler functions like HTTP handlers, GraphQL resolvers, Async messages consumer, etc.

## General Coding Rules

- Modularization is extremely important.
- Avoid too much nesting, extract functions to avoid too much nesting. If more than just one let binding is needed, extract to a function.
- Use pure functions whenever possible. They should be easy to unit test. 
- Always test in between changes to avoid regressions. "make tests" or clojure -M:test -n "target test name" to run a specific test.
- Always validate the changes using the "make tests" and "INTEGRATION=1 make tests" commands.