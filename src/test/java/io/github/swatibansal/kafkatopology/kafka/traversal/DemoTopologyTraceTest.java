package io.github.swatibansal.kafkatopology.kafka.traversal;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.swatibansal.kafkatopology.enrichment.AppYmlApplicationParser;
import io.github.swatibansal.kafkatopology.enrichment.ApplicationEnrichmentService;
import io.github.swatibansal.kafkatopology.enrichment.LocalAppYmlSource;
import io.github.swatibansal.kafkatopology.enrichment.SpringAppYmlGroupIdExtractor;
import io.github.swatibansal.kafkatopology.kafka.discovery.StreamsTopologyAnalyzer;
import io.github.swatibansal.kafkatopology.model.EdgeType;
import io.github.swatibansal.kafkatopology.model.NodeType;
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
 * Pins the node and edge counts quoted in the README and DESIGN docs, and the multi-hop paths the
 * trace panel shows for {@code orders.order.created}.
 */
class DemoTopologyTraceTest {

  private static final Path DEMO = Path.of(System.getProperty("basedir", "."), "demo");
  private static final String START_TOPIC = "orders.order.created";

  private static TopologyGraph graph;

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
    graph.removeOrphanNodes();
  }

  private static List<String> dataLines(Path file) throws IOException {
    return Files.readAllLines(file).stream()
        .map(String::strip)
        .filter(line -> !line.isEmpty() && !line.startsWith("#"))
        .toList();
  }

  @Test
  void demoGraph_matchesDocumentedCounts() {
    assertThat(graph.nodeCount()).isEqualTo(35);
    assertThat(graph.edgeCount()).isEqualTo(41);
    assertThat(graph.nodesByType(NodeType.TOPIC)).hasSize(18);
    assertThat(graph.nodesByType(NodeType.CONSUMER_GROUP)).hasSize(7);
    assertThat(graph.nodesByType(NodeType.STREAMS_APP)).hasSize(1);
    assertThat(graph.nodesByType(NodeType.APPLICATION)).hasSize(9);
  }

  @Test
  void traceOrderCreated_followsServicesAcrossTheirProducedTopics() {
    TraversalResult result = new GraphTraversalService().trace(START_TOPIC, graph);

    // The payment → shipping → notification chain is reported as one continuous path.
    assertThat(result.paths())
        .map(path -> path.nodes().stream().map(TopologyNode::name).toList())
        .contains(List.of(
            "orders.order.created",
            "payment-service-cg", "payment-service",
            "payments.payment.captured",
            "shipping-service-cg", "shipping-service",
            "notifications.email.requested",
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
            "shipping.shipment.created",
            "inventory.stock.released",
            "analytics.order.totals");
    assertThat(result.paths()).noneMatch(TracedPath::cycleDetected);
  }

  @Test
  void traceOrderCreated_pathCountAndDepth() {
    TraversalResult result = new GraphTraversalService().trace(START_TOPIC, graph);

    assertThat(result.pathCount()).isEqualTo(25);
    assertThat(result.paths()).extracting(TracedPath::hopCount).contains(2, 14);
  }
}
