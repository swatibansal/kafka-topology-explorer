# Design Notes

## Problem

Kafka's AdminClient reports topics and consumer groups but has no notion of producers,
applications, or end-to-end flow. Teams running many services wired through Kafka end up with
no single picture of who publishes what and who depends on it.

## Discovery pipeline

1. **Topics** — `listTopics` + `describeTopics`; purely numeric names are dropped; an optional
   substring filter narrows the set. Message counts come from end offsets.
2. **Consumer groups** — `listConsumerGroups`, then `listConsumerGroupOffsets` per group. Every
   topic-partition with a committed offset yields a `CONSUMES` edge from the topic to the group.
3. **Streams detection** — a group is classified `STREAMS_APP` when a topic named
   `<group>-...-changelog` or `<group>-...-repartition` exists. Edges to those internal topics are
   suppressed so infrastructure topics do not clutter the graph. The internal topics themselves
   remain in the graph as topic nodes without edges.
4. **Enrichment** — an `ApplicationSource` returns one `DiscoveredApplication` per service with
   candidate group ids and topic names extracted from its `application.yml`. Candidates matching
   live nodes become `DEPLOYS` (app → group) and `PRODUCES` (app → topic) edges. A topic the
   application's own group already consumes never becomes a `PRODUCES` edge.
5. **Orphan removal** — groups with no `CONSUMES` edge, applications with no edges, and
   edgeless retry / dead-letter topics are removed. Edges that touch a removed node are dropped
   and pruning repeats until the graph is stable.

## Tracing

`GraphTraversalService` runs a depth-first search from the chosen topic over the *data-flow*
view of the graph (`TopologyGraph.downstream`): `CONSUMES` and `PRODUCES` edges are followed in
their stored direction, and `DEPLOYS` is followed in reverse, from a consumer group to the
service that runs it. A trace therefore continues through each service to the topics it
produces and on to their consumers, so `orders.order.created → payment-service-cg →
payment-service → payments.payment.captured → shipping-service-cg → …` is one path. A service is
never stepped forwards into the other groups it deploys, since no message travels that way. A
path ends at a service with no produced topics, at a produced topic with no consumers, or at a
group with no owning service. Nodes already on the current path are skipped, and a path whose
every onward edge loops back is flagged `cycleDetected`. The structural view (`neighbors`) is
unchanged and still drives the search sub-graph. `DemoTopologyTraceTest` rebuilds the demo graph
from the seed files and pins the 25 paths that tracing `orders.order.created` yields.

## application.yml extraction heuristics

`SpringAppYmlGroupIdExtractor` parses multi-document YAML, picks the document whose
`spring.config.activate.on-profile` matches the active profile (falling back to the default
document), flattens it to dotted keys, and keeps:

- group ids: keys containing `kafka` and `group`, plus Kafka Streams `application-id` keys;
- topics: keys containing `kafka` and `topic`.

Values pass through placeholder resolution: `${VAR:default}` yields `default`; `${a.b.c}` is
looked up across the default and profile documents (so a `group-id-base` in the default document
can be referenced from the profile document); unresolvable or `CHANGE_ME` values are dropped.

Two sources feed the same parser: `LocalAppYmlSource` (a directory, used by the demo) and
`GitHubAppYmlClient` (the GitHub Contents API).

## Demo topology: Acme Shop

Nine services, eighteen topics, eight groups. Seeded by `demo/seed.sh` using only the Kafka CLI:
topics are created, a realistic number of JSON events (a few hundred to a few thousand, listed per
topic in `demo/topics.txt`) is produced into each business topic, and committed
offsets are created for each group with `kafka-consumer-groups.sh --reset-offsets`, which works
for groups with no live members.

| Service | Group | Produces |
|---------|-------|----------|
| order-service | — | orders.order.{created,updated,cancelled} |
| payment-service | payment-service-cg | payments.payment.{requested,captured,failed} |
| inventory-service | inventory-service-cg | inventory.stock.{reserved,released} |
| shipping-service | shipping-service-cg | shipping.shipment.{created,delivered}, notifications.email.requested |
| notification-service | notification-service-cg | — |
| search-indexer | search-indexer-cg | search.product.indexed |
| catalog-service | — | catalog.product.updated |
| fraud-detection-service | fraud-detection-cg | payments.payment.failed |
| order-analytics (Streams) | order-analytics | analytics.order.totals |
| (none) | dlt-replay-cg | — (replays the two `.DLT` topics; no owning service in the demo) |

The `dlt-replay-cg` group keeps the two dead-letter topics out of orphan removal, which drops
edgeless retry/DLT topics.

Expected graph: 35 nodes (18 topics, 7 groups, 1 Streams app, 9 applications) and 41 edges
(19 CONSUMES, 7 DEPLOYS, 15 PRODUCES). `DemoAppsSmokeTest` pins the extraction side of this.

## Testing strategy

- Unit tests (JUnit 5, Mockito, AssertJ) for the extractor, parser, the local source, enrichment
  service, traversal, output formats, and cache; no Spring context.
- Web-layer tests with `MockMvc` and mocked services for the controller and static UI.
- Integration tests with `@EmbeddedKafka` for the admin service and discovery against a real
  in-process broker.
- A redaction scan (`scripts/check-redaction.sh`) runs in CI so environment-specific vocabulary
  cannot creep into this public repository.
