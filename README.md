# Kafka Topology Explorer

A Spring Boot service that discovers the full event topology of a Kafka platform and turns it
into an interactive graph: every topic, consumer group, Kafka Streams application, and the
microservices that deploy them. It answers *"who consumes this topic?"* and *"who produces here?"*
without reading a hundred codebases.

This repository is a self-contained demo. `docker compose up` starts a broker, seeds a fictional
e-commerce topology ("Acme Shop"), and launches the explorer against it.

![Kafka Topology Explorer showing the Acme Shop graph: topics, consumer groups, a Kafka Streams app and the services that deploy them](docs/images/topology-explorer.png)

## The problem

Kafka exposes topics and consumer groups through the AdminClient, but:

- there is **no producer registry** — the broker cannot tell you who writes to a topic;
- there is **no application layer** — a consumer group id is just a string, not a service;
- there is **no end-to-end view** — tracing an event from its source topic through every
  downstream consumer means stitching together configuration from many repositories.

The explorer fills those gaps by combining live broker metadata with each service's own Spring
`application.yml`, read either from a local directory or from named repositories in a GitHub
organisation.

## Quick start

Prerequisites: Docker with Compose v2.

```bash
git clone https://github.com/swatibansal/kafka-topology-explorer
cd kafka-topology-explorer
docker compose up
```

Then open <http://localhost:8083/>. The first start builds the image (a few minutes) and pulls
`apache/kafka:3.9.1`. The image build downloads Maven and every dependency inside Docker, so the
first `docker compose up` needs internet access. The broker is ephemeral by design:
`docker compose down` discards the topology and the next `docker compose up` re-seeds it in about
a minute.

What you will see:

- the Acme Shop graph: 18 topics (two of them Kafka Streams internal topics), 7 consumer groups,
  1 Kafka Streams app, 9 services;
- **search** — type `payment` to see every node and edge touching payments;
- **trace** — pick `orders.order.created` to follow it through every service and the topics
  those services produce in turn: 25 paths, the longest 14 hops deep;
- **export** — the same graph as Mermaid, Graphviz DOT, or JSON.

![Tracing orders.order.created: the downstream paths are highlighted in the graph and listed in the side panel](docs/images/topology-explorer-trace.png)

Developer loop (broker in Docker, app from Maven, Java 21+):

```bash
docker compose up -d kafka seed
ENRICHMENT_LOCAL_DIR="$PWD/demo/apps" ./mvnw spring-boot:run
```

The Maven loop needs Java 21 or newer on `PATH`, or `JAVA_HOME` pointing at such a JDK; the
Docker build ships its own.

## How it works

```
Kafka broker (AdminClient)
  ├─ topics + partition metadata + message counts
  └─ consumer groups + committed offsets
          │
          ▼
  TopologyDiscoveryService ──builds──▶ TopologyGraph
          │                              (TOPIC, CONSUMER_GROUP, STREAMS_APP nodes;
          │                               CONSUMES edges)
          ▼
  ApplicationEnrichmentService
          │  reads application.yml per service via an ApplicationSource:
          │    • LocalAppYmlSource   — a directory, one sub-folder per service
          │    • GitHubAppYmlClient  — named repositories in a GitHub organisation
          │  adds APPLICATION nodes + DEPLOYS / PRODUCES edges
          ▼
  TopologyCacheService (background refresh)
          │
          ▼
  REST API + browser UI (port 8083)
```

### Node types

| Type | Represents |
|------|------------|
| `TOPIC` | Kafka topic with partition count, replication factor, message count |
| `CONSUMER_GROUP` | Consumer group that has committed offsets |
| `STREAMS_APP` | Consumer group that also owns `-changelog` / `-repartition` topics |
| `APPLICATION` | Service discovered from its `application.yml` |

### Edge types

| Type | Meaning | Source |
|------|---------|--------|
| `CONSUMES` | Group reads from a topic | broker committed offsets |
| `DEPLOYS` | Application owns a consumer group or Streams app | `application.yml` group id |
| `PRODUCES` | Application writes to a topic | `application.yml` topic names |

## REST API

| Endpoint | Description |
|----------|-------------|
| `GET /api/topology` | Full cached graph as JSON |
| `GET /api/topology/search?q=<text>` | Nodes whose name contains `text`, plus neighbours |
| `GET /api/topology/trace?topic=<name>` | All data-flow paths from a topic: topic → group → owning service → produced topic → … |
| `GET /api/topology/export?format=mermaid\|dot\|json` | Graph export |
| `GET /api/topology/refresh` | Force an immediate re-discovery |
| `GET /api/topology/applications` | Enrichment summary from the last refresh |
| `GET /mgmt/health`, `GET /mgmt/prometheus` | Actuator health and metrics |

`/mgmt/health` shows component details (broker connectivity, cache state) and is meant for local
use; restrict or disable it before exposing the service beyond your machine.

## Configuration

Every setting is an environment variable with a local-friendly default.

| Variable | Default | Purpose |
|----------|---------|---------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:19092` | Broker list |
| `KAFKA_SECURITY_PROTOCOL` | `PLAINTEXT` | `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, `SASL_SSL` |
| `KAFKA_SASL_MECHANISM` | *(blank)* | e.g. `SCRAM-SHA-512` |
| `KAFKA_SASL_JAAS_CONFIG` | *(blank)* | Full JAAS string, or use the two below |
| `KAFKA_SASL_USERNAME` / `KAFKA_SASL_PASSWORD` | *(blank)* | Credentials from which a JAAS string is built |
| `KAFKA_TOPIC_FILTER` | *(blank = all)* | Comma-separated substrings a topic or consumer-group name must contain |
| `KAFKA_EXCLUDE_NUMERIC_TOPICS` | `true` | Drop topics whose names are purely numeric |
| `KAFKA_TOPOLOGY_REFRESH_MS` | `60000` | Background refresh interval |
| `KAFKA_ADMIN_REQUEST_TIMEOUT_MS` | `30000` | AdminClient per-request timeout (ms) |
| `KAFKA_ADMIN_DEFAULT_API_TIMEOUT_MS` | `30000` | AdminClient overall API timeout (ms) |
| `ENRICHMENT_ACTIVE_PROFILE` | `prod` | Spring profile block to read from each `application.yml` |
| `ENRICHMENT_LOCAL_DIR` | *(blank)* | Directory of `<service>/application.yml`; enables the local source |
| `GITHUB_ENRICHMENT_ENABLED` | `false` | Enables the GitHub source (takes precedence) |
| `GITHUB_ORG`, `GITHUB_TOKEN`, `GITHUB_REPOS` | *(blank)* | Organisation, token with read access to repository contents (classic `repo` scope or fine-grained Contents: read), comma-separated repos |
| `SERVER_PORT` | `8083` | HTTP port |

`ENRICHMENT_ACTIVE_PROFILE` is the profile block read from *each discovered service's*
`application.yml`; it is unrelated to the explorer's own Spring profiles.

## Pointing it at your own services

Create a directory with one sub-folder per service containing its `application.yml` (either at the
top level or under `src/main/resources/`), set `ENRICHMENT_LOCAL_DIR` to it, and set
`ENRICHMENT_ACTIVE_PROFILE` to the profile your deployments use. The extractor looks for:

- consumer groups: any key containing `kafka` and `group` (e.g. `spring.kafka.consumer.group-id`)
  and Kafka Streams `spring.kafka.streams.application-id`;
- produced topics: any key containing `kafka` and `topic`;
- Spring `${VAR:default}` expressions (the default is used) and cross-document references such as
  `"${app.kafka.consumer.group-id-base}-cg"`.

`demo/apps/` shows the common shapes; `src/main/resources/application.yml`,
`application-<profile>.yml` and `.yaml` are also read. To use GitHub instead, set the `GITHUB_*`
variables.

## Known limitations

- Producers are invisible at the broker level, so `PRODUCES` edges come from configuration, not
  from live metadata.
- Kafka Streams sink topics cannot be inferred from the broker; only source topics appear as
  `CONSUMES` edges.
- Consumer groups that have never committed offsets have no edges and are dropped as orphans.

## How this was built

This tool was built with AI-assisted development: a short design spec, then an implementation
plan executed task by task with tests written first. The conventions the assistant followed are
in [`.claude/CLAUDE.md`](.claude/CLAUDE.md); the architecture and demo design are in
[`docs/DESIGN.md`](docs/DESIGN.md). `scripts/check-redaction.sh` runs in CI to keep this repository
free of anything environment-specific.

## License

Apache License 2.0 — see [LICENSE](LICENSE). Copyright 2026 Collectors Universe, Inc.; see
[NOTICE](NOTICE).

Third-party: the vendored Cytoscape.js (MIT) is listed in
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).
