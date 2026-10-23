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
5. **Orphan removal** — groups with no `CONSUMES` edge (`UNCONSUMED_GROUP`), applications with
   no edges (`UNLINKED_APPLICATION`), and edgeless retry / dead-letter topics
   (`UNUSED_RETRY_OR_DLT_TOPIC`) are removed. Edges that touch a removed node are dropped and
   pruning repeats until the graph is stable. Every removed node is kept, with its reason and
   its pre-pruning metadata, in an `OrphanReport` next to the cached graph and served by
   `GET /api/topology/orphans`; the report is replaced on each refresh.

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
from the seed files and pins the 10 paths that tracing `orders.order.created` yields.

Because the trace is structural, a service that consumes several topics and produces several
topics links each of its inputs to each of its outputs. The demo data is arranged so this never
produces nonsense: every service consumes exactly one business stage, and the two fan-in
consumers (`notification-service`, `order-analytics`) map every input to the same kind of output.
That is why stock release lives in its own `inventory-release-service` rather than in
`inventory-service`: a single inventory service consuming both `orders.order.created` and
`payments.payment.failed` would make every failed payment appear to lead to a reservation and a
shipment, and every new order to a release. The walkthrough is a linear saga, reserve → charge →
ship → deliver, with `fraud-detection-service` as an asynchronous veto on `payments.payment.requested`
(approvals are silent, a decline is published as `payments.payment.failed`).

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

Twelve services, eighteen topics, ten groups, of which one service and one topic are planted as
orphans. Seeded by `demo/seed.sh` using only the Kafka CLI:
topics are created, a realistic number of JSON events (a few hundred to a few thousand, listed per
topic in `demo/topics.txt`) is produced into each business topic, and committed
offsets are created for each group with `kafka-consumer-groups.sh --reset-offsets`, which works
for groups with no live members.

| Service | Group (consumes) | Produces |
|---------|------------------|----------|
| order-service | — | orders.order.{created,updated,cancelled} |
| inventory-service | inventory-service-cg (orders.order.created) | inventory.stock.reserved |
| payment-service | payment-service-cg (inventory.stock.reserved) | payments.payment.{captured,failed,requested} |
| fraud-detection-service | fraud-detection-cg (payments.payment.requested) | payments.payment.failed |
| shipping-service | shipping-service-cg (payments.payment.captured) | shipping.shipment.created |
| delivery-tracking-service | delivery-tracking-cg (shipping.shipment.created) | shipping.shipment.delivered |
| inventory-release-service | inventory-release-cg (payments.payment.failed, orders.order.cancelled) | inventory.stock.released |
| notification-service | notification-service-cg (created, cancelled, captured, failed, shipment created, delivered) | — |
| search-indexer | search-indexer-cg (catalog.product.updated) | search.product.indexed |
| catalog-service | — | catalog.product.updated |
| order-analytics (Streams) | order-analytics (created, updated, captured) | analytics.order.totals |
| (none) | dlt-replay-cg | — (replays `orders.order.created.DLT` and `payments.payment.requested.DLT`; no owning service in the demo) |
| pricing-service | pricing-service-cg (does not exist) | pricing.price.updated (does not exist) |

The `dlt-replay-cg` group keeps two of the three dead-letter topics out of orphan removal. The
third, `orders.order.updated.DLT`, is seeded with five messages and no consumer group, and
`pricing-service` is a decommissioned service whose configuration survives in `demo/apps`. Both
are pruned and show up in `GET /api/topology/orphans`:

| Orphan | Reason |
|--------|--------|
| `orders.order.updated.DLT` (topic) | `UNUSED_RETRY_OR_DLT_TOPIC` |
| `pricing-service` (application) | `UNLINKED_APPLICATION`, metadata keeps `candidateGroupIds` and `candidateTopicNames` |

The third rule, `UNCONSUMED_GROUP`, needs a group that exists on the broker without committed
offsets on any graph topic. The Kafka CLI cannot create one (offsets are what makes an idle group
exist), so it is exercised by unit tests rather than by the seed data.

Expected graph after pruning: 38 nodes (17 topics, 9 groups, 1 Streams app, 11 applications) and
42 edges (19 CONSUMES, 9 DEPLOYS, 14 PRODUCES). `DemoAppsSmokeTest` pins the extraction side of
this and `DemoTopologyTraceTest` the graph, the orphans and the trace.

## Testing strategy

- Unit tests (JUnit 5, Mockito, AssertJ) for the extractor, parser, the local source, enrichment
  service, traversal, output formats, and cache; no Spring context.
- Web-layer tests with `MockMvc` and mocked services for the controller and static UI.
- Integration tests with `@EmbeddedKafka` for the admin service and discovery against a real
  in-process broker.
- A redaction scan (`scripts/check-redaction.sh`) runs in CI so environment-specific vocabulary
  cannot creep into this public repository.
