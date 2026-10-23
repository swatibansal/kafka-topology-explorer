package io.github.swatibansal.kafkatopology.kafka.traversal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import io.github.swatibansal.kafkatopology.enrichment.AppYmlApplicationParser;
import io.github.swatibansal.kafkatopology.enrichment.ApplicationEnrichmentService;
import io.github.swatibansal.kafkatopology.enrichment.LocalAppYmlSource;
import io.github.swatibansal.kafkatopology.enrichment.SpringAppYmlGroupIdExtractor;
import io.github.swatibansal.kafkatopology.kafka.discovery.StreamsTopologyAnalyzer;
import io.github.swatibansal.kafkatopology.model.EdgeType;
import io.github.swatibansal.kafkatopology.model.NodeType;
import io.github.swatibansal.kafkatopology.model.OrphanNode;
import io.github.swatibansal.kafkatopology.model.OrphanReason;
import io.github.swatibansal.kafkatopology.model.TopologyEdge;
import io.github.swatibansal.kafkatopology.model.TopologyGraph;
import io.github.swatibansal.kafkatopology.model.TopologyNode;
import io.github.swatibansal.kafkatopology.model.TracedPath;
import io.github.swatibansal.kafkatopology.model.TraversalResult;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Traces the Acme Shop demo topology end to end, rebuilding the graph the explorer would discover
 * from {@code demo/topics.txt}, {@code demo/groups.txt} and {@code demo/apps} without a broker.
 * Pins the node and edge counts quoted in the README and DESIGN docs, the two orphans the seed
 * data deliberately plants, and the multi-hop paths the trace panel shows for
 * {@code orders.order.created} and {@code orders.order.cancelled}.
 */
class DemoTopologyTraceTest {

  private static final Path DEMO = Path.of(System.getProperty("basedir", "."), "demo");
  private static final String START_TOPIC = "orders.order.created";

  private static TopologyGraph graph;
  private static List<OrphanNode> orphans;

  @BeforeAll
  static void buildDemoGraph() throws IOException {
    graph = new TopologyGraph();
    Set<String> topics = new LinkedHashSet<>();
    for (String line : dataLines(DEMO.resolve("topics.txt"))) {
      String topic = line.split("\\s+")[0];
      topics.add(topic);
      graph.addNode(TopologyNode.forTopic(topic));
    }

    Map<String, List<String>> groups = new LinkedHashMap<>();
    for (String line : dataLines(DEMO.resolve("groups.txt"))) {
      String[] parts = line.split("\\s+");
      groups.put(parts[0], Arrays.asList(parts[1].split(",")));
    }

    var analyzer = new StreamsTopologyAnalyzer();
    Set<String> streamsApps = analyzer.detectStreamsApps(groups.keySet(), topics);
    groups.forEach((groupId, consumed) -> {
      TopologyNode groupNode = streamsApps.contains(groupId)
          ? TopologyNode.forStreamsApp(groupId)
          : TopologyNode.forConsumerGroup(groupId);
      graph.addNode(groupNode);
      consumed.stream()
          .filter(topic -> !analyzer.isStreamsInternalTopic(topic, groupId))
          .forEach(topic -> graph.addEdge(TopologyEdge.of(
              TopologyNode.topicId(topic), groupNode.id(), EdgeType.CONSUMES,
              topic + " → " + groupId)));
    });

    var source = new LocalAppYmlSource(
        DEMO.resolve("apps"),
        new AppYmlApplicationParser(new SpringAppYmlGroupIdExtractor(), "prod"));
    new ApplicationEnrichmentService(source, true).enrich(graph);
    orphans = graph.removeOrphanNodes();
  }

  private static List<String> dataLines(Path file) throws IOException {
    return Files.readAllLines(file).stream()
        .map(String::strip)
        .filter(line -> !line.isEmpty() && !line.startsWith("#"))
        .toList();
  }

  @Test
  void demoGraph_matchesDocumentedCounts() {
    assertThat(graph.nodeCount()).isEqualTo(38);
    assertThat(graph.edgeCount()).isEqualTo(42);
    assertThat(graph.nodesByType(NodeType.TOPIC)).hasSize(17);
    assertThat(graph.nodesByType(NodeType.CONSUMER_GROUP)).hasSize(9);
    assertThat(graph.nodesByType(NodeType.STREAMS_APP)).hasSize(1);
    assertThat(graph.nodesByType(NodeType.APPLICATION)).hasSize(11);
    assertThat(graph.getEdges()).extracting(TopologyEdge::type)
        .filteredOn(t -> t == EdgeType.CONSUMES).hasSize(19);
    assertThat(graph.getEdges()).extracting(TopologyEdge::type)
        .filteredOn(t -> t == EdgeType.DEPLOYS).hasSize(9);
    assertThat(graph.getEdges()).extracting(TopologyEdge::type)
        .filteredOn(t -> t == EdgeType.PRODUCES).hasSize(14);
  }

  @Test
  void demoGraph_prunesTheTwoSeededOrphansAndReportsWhy() {
    // orders.order.updated.DLT is seeded with no consumer group, and pricing-service's
    // application.yml names a group and a topic that no longer exist on the broker.
    assertThat(orphans).extracting(o -> o.node().name(), OrphanNode::reason)
        .containsExactlyInAnyOrder(
            tuple("orders.order.updated.DLT", OrphanReason.UNUSED_RETRY_OR_DLT_TOPIC),
            tuple("pricing-service", OrphanReason.UNLINKED_APPLICATION));
    OrphanNode pricing = orphans.stream()
        .filter(o -> o.node().name().equals("pricing-service")).findFirst().orElseThrow();
    assertThat(pricing.node().metadata())
        .containsEntry("candidateGroupIds", "pricing-service-cg")
        .containsEntry("candidateTopicNames", "pricing.price.updated");
  }

  @Test
  void traceOrderCreated_paymentFailureOnlyLeadsToNotificationOrStockRelease() {
    TraversalResult result = new GraphTraversalService().trace(START_TOPIC, graph);

    // A failed payment is followed by the customer notice or the compensating stock release,
    // never by a reservation or a shipment.
    List<List<String>> allowedTails = List.of(
        List.of("notification-service-cg", "notification-service"),
        List.of("inventory-release-cg", "inventory-release-service", "inventory.stock.released"));
    int failedPaths = 0;
    for (TracedPath path : result.paths()) {
      List<String> names = path.nodes().stream().map(TopologyNode::name).toList();
      int failed = names.indexOf("payments.payment.failed");
      if (failed >= 0) {
        failedPaths++;
        assertThat(names.subList(failed + 1, names.size()))
            .as("after payments.payment.failed in %s", names)
            .isIn(allowedTails);
      }
    }
    assertThat(failedPaths).isEqualTo(4);
  }

  @Test
  void traceOrderCreated_followsTheHappyPathEndToEnd() {
    TraversalResult result = new GraphTraversalService().trace(START_TOPIC, graph);

    // order → stock reserved → payment captured → shipment created → delivered → notification
    // is reported as one continuous path, and it is the first one listed.
    List<List<String>> paths = result.paths().stream()
        .map(path -> path.nodes().stream().map(TopologyNode::name).toList())
        .toList();
    assertThat(paths.get(0)).containsExactly(
        "orders.order.created",
        "inventory-service-cg", "inventory-service",
        "inventory.stock.reserved",
        "payment-service-cg", "payment-service",
        "payments.payment.captured",
        "shipping-service-cg", "shipping-service",
        "shipping.shipment.created",
        "delivery-tracking-cg", "delivery-tracking-service",
        "shipping.shipment.delivered",
        "notification-service-cg", "notification-service");
    // Every path either passes through the single payment stage or is a side effect of the
    // order itself (confirmation e-mail, analytics).
    for (List<String> names : paths) {
      boolean viaPayment = names.contains("payment-service");
      boolean sideEffect = List.of("notification-service-cg", "order-analytics")
          .contains(names.get(1));
      assertThat(viaPayment || sideEffect).as("path %s", names).isTrue();
    }
  }

  @Test
  void traceOrderCancelled_onlyNotifiesAndReleasesStock() {
    TraversalResult result = new GraphTraversalService().trace("orders.order.cancelled", graph);

    assertThat(result.paths())
        .map(path -> path.nodes().stream().map(TopologyNode::name).toList())
        .containsExactlyInAnyOrder(
            List.of("orders.order.cancelled",
                "inventory-release-cg", "inventory-release-service", "inventory.stock.released"),
            List.of("orders.order.cancelled",
                "notification-service-cg", "notification-service"));
  }

  @Test
  void traceOrderCreated_neverEndsOnConsumerGroup() {
    TraversalResult result = new GraphTraversalService().trace(START_TOPIC, graph);

    // Every group reachable from orders.order.created is owned by a service, so a path only ends
    // at that service or at a topic it produces that nobody consumes yet.
    assertThat(result.finalDestinations())
        .extracting(TopologyNode::type)
        .containsOnly(NodeType.APPLICATION, NodeType.TOPIC);
    assertThat(result.finalDestinations())
        .extracting(TopologyNode::name)
        .containsExactlyInAnyOrder(
            "notification-service",
            "inventory.stock.released",
            "analytics.order.totals");
    assertThat(result.paths()).noneMatch(TracedPath::cycleDetected);
  }

  @Test
  void traceOrderCreated_pathCountAndDepth() {
    TraversalResult result = new GraphTraversalService().trace(START_TOPIC, graph);

    assertThat(result.pathCount()).isEqualTo(10);
    assertThat(result.paths()).extracting(TracedPath::hopCount).contains(2, 14);
    assertThat(result.paths()).extracting(TracedPath::hopCount).allMatch(h -> h <= 14);
  }
}
