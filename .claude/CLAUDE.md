# Claude Code Instructions — Kafka Backend (Java 21 / Spring Boot)

## Project Context

This project is a Kafka-based backend service built with Java 21 and Spring Boot.
All code assistance must follow the conventions below.

---

## Language & Runtime

- **Java 21** is the minimum target version. Prefer modern Java features:
  - Records for immutable data carriers
  - Sealed classes and interfaces for closed type hierarchies
  - Pattern matching (`instanceof`, `switch` expressions)
  - Text blocks for multi-line strings (e.g., JSON templates, SQL)
  - Virtual threads (Project Loom) for I/O-bound tasks where appropriate
- Do **not** use deprecated or pre-Java 11 APIs (e.g., raw types, old `Date`/`Calendar`).

---

## Google Java Style Guide

All code must conform to the [Google Java Style Guide](https://google.github.io/styleguide/javaguide.html). Key rules:

- **Indentation**: 2 spaces, no tabs.
- **Column limit**: 100 characters per line.
- **Braces**: Always use braces for `if`, `else`, `for`, `while` — even single-line bodies.
- **Naming conventions**:
  - Classes/interfaces: `UpperCamelCase`
  - Methods/variables: `lowerCamelCase`
  - Constants: `UPPER_SNAKE_CASE`
  - Packages: all lowercase, no underscores (e.g., `com.example.kafka.consumer`)
- **Imports**: No wildcard imports. Organize in groups: static, then java, javax, then third-party, then internal.
- **Javadoc**: Required on all public classes and public/protected methods. Use `@param`, `@return`, `@throws`.
- **Annotations**: Place each annotation on its own line above the element.

---

## Spring Boot Conventions

- Use **constructor injection** — never field injection (`@Autowired` on fields is prohibited).
- Declare beans in `@Configuration` classes; avoid overloading `@SpringBootApplication`.
- Use `application.yml` (not `application.properties`) for configuration. Group related keys under namespaces.
- Externalize all secrets and environment-specific values via environment variables or Spring Cloud Config — never hardcode.
- Follow the **layered architecture**:
  - `controller` — REST/event entry points only; no business logic.
  - `service` — business logic; orchestrates calls to repositories and external systems.
  - `repository` — data access only.
  - `model` / `dto` — data structures; prefer records for DTOs.
  - `config` — Spring `@Configuration` and `@Bean` definitions.
  - `kafka` — producers, consumers, and serializers in a dedicated sub-package.

---

## Apache Kafka Best Practices

### Producers

- Use `KafkaTemplate<K, V>` with explicit key and value serializer configuration.
- Always handle `CompletableFuture` / `ListenableFuture` results — log or propagate errors.
- Set idempotence (`enable.idempotence=true`) and configure `acks=all` for exactly-once or at-least-once guarantees.
- Provide a `ProducerFactory` bean configured via `@ConfigurationProperties`-bound settings.

### Consumers

- Annotate listener methods with `@KafkaListener`; declare `groupId`, `topics`, and `containerFactory` explicitly.
- Use `ConsumerRecord<K, V>` as the method parameter to access headers, partition, and offset metadata.
- Implement a `DefaultErrorHandler` (or `SeekToCurrentErrorHandler`) with a `DeadLetterPublishingRecoverer` for failed messages.
- Commit offsets manually when strong delivery guarantees are required (`AckMode.MANUAL_IMMEDIATE`).
- Never perform blocking I/O inside a listener without moving work to a separate thread pool.

### Serialization

- Use **Spring Kafka's JSON serializer/deserializer** (`JsonSerializer` / `JsonDeserializer`) backed by Jackson.
- Register a `KafkaJacksonAutoConfiguration`-compatible `ObjectMapper` bean; enable `FAIL_ON_UNKNOWN_PROPERTIES=false` for consumer resilience.
- For Avro/Protobuf schemas, register schemas in a Schema Registry and use a typed deserializer.

### Schema & Topics

- Topic names follow the convention: `<domain>.<entity>.<event>` (e.g., `inventory.product.created`).
- Define partition count and replication factor in infrastructure-as-code, not in application code.
- Document each topic, its key/value schema, and retention policy in `docs/topics.md`.

---

## Testing

- Unit tests use **JUnit 5** + **Mockito**; no Spring context loading for pure unit tests.
- Integration tests use `@SpringBootTest` + **Testcontainers** (`kafka` container) to spin up a real broker.
- Use `EmbeddedKafkaBroker` only for lightweight slice tests (`@EmbeddedKafka`).
- Test class naming: `<ClassUnderTest>Test` for unit tests, `<ClassUnderTest>IT` for integration tests.
- Aim for **80%+ line coverage** on `service` and `kafka` packages.
- Assert on message content, headers, partition assignment, and DLT routing in Kafka integration tests.

---

## Observability

- Expose metrics via **Spring Boot Actuator** + **Micrometer** (Prometheus endpoint at `/actuator/prometheus`).
- Instrument Kafka consumer lag using `KafkaMetrics` or a custom `MeterBinder`.
- Use **structured logging** (JSON via Logback + `logstash-logback-encoder`). Log at `INFO` for business events, `DEBUG` for message payloads (guarded by `log.isDebugEnabled()`).
- Propagate trace context (OpenTelemetry or Spring Cloud Sleuth) through Kafka message headers.

---

## Security

- Never log message payloads that may contain PII or secrets.
- Use TLS for all broker connections in non-local environments (`security.protocol=SSL` or `SASL_SSL`).
- Validate all inbound message schemas before processing; reject and route malformed messages to the DLT.

---

## Dependency & Build

- Build tool: **Maven** (use `spring-boot-starter-parent` as parent POM).
- Dependency versions must be managed in `<dependencyManagement>`; no version pinning in individual `<dependency>` blocks unless overriding the BOM.
- Run `mvn verify` (unit + integration tests) before committing.
- Use the **Google Error Prone** compiler plugin and **Checkstyle** (Google ruleset) in the build.

---

## General Coding Principles (Google Engineering Culture)

- **Readability first**: code is read far more than it is written.
- **Prefer clarity over cleverness**: explicit is better than implicit.
- **Small, focused methods**: each method does one thing; keep methods under ~40 lines.
- **Fail fast**: validate inputs at public API boundaries; throw `IllegalArgumentException` or domain-specific exceptions early.
- **No magic numbers**: extract constants with meaningful names.
- **DRY but not prematurely**: only abstract when there are at least three concrete cases.
- **Every TODO must include a tracking ticket**: `// TODO(JIRA-123): description`.
