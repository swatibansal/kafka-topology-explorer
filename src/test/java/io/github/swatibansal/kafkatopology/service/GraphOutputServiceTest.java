package io.github.swatibansal.kafkatopology.service;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.swatibansal.kafkatopology.model.EdgeType;
import io.github.swatibansal.kafkatopology.model.ExportFormat;
import io.github.swatibansal.kafkatopology.model.TopologyEdge;
import io.github.swatibansal.kafkatopology.model.TopologyGraph;
import io.github.swatibansal.kafkatopology.model.TopologyNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link GraphOutputService}. No Spring context required. */
class GraphOutputServiceTest {

  private GraphOutputService service;

  @BeforeEach
  void setUp() {
    service = new GraphOutputService(new ObjectMapper());
  }

  // ── helpers ──────────────────────────────────────────────────────────────────

  private static TopologyGraph twoNodeGraph() {
    var graph = new TopologyGraph();
    graph.addNode(TopologyNode.forTopic("orders.created"));
    graph.addNode(TopologyNode.forConsumerGroup("order-service"));
    graph.addEdge(
        TopologyEdge.of(
            TopologyNode.topicId("orders.created"),
            TopologyNode.groupNodeId("order-service"),
            EdgeType.CONSUMES,
            "orders.created → order-service"));
    return graph;
  }

  // ── Mermaid ───────────────────────────────────────────────────────────────────

  @Test
  void toMermaid_emptyGraph_returnsHeaderOnly() {
    String result = service.toMermaid(new TopologyGraph());

    assertThat(result).isEqualTo("flowchart LR\n");
  }

  @Test
  void toMermaid_topicNode_usesRectangleShape() {
    var graph = new TopologyGraph();
    graph.addNode(TopologyNode.forTopic("orders.created"));

    String result = service.toMermaid(graph);

    // TOPIC → rectangle: id["label"]
    assertThat(result).contains("topic_orders_created[\"");
  }

  @Test
  void toMermaid_consumerGroupNode_usesRoundedShape() {
    var graph = new TopologyGraph();
    graph.addNode(TopologyNode.forConsumerGroup("order-service"));

    String result = service.toMermaid(graph);

    // CONSUMER_GROUP → rounded: id("label")
    assertThat(result).contains("group_order_service(\"");
  }

  @Test
  void toMermaid_streamsAppNode_usesDiamondShape() {
    var graph = new TopologyGraph();
    graph.addNode(TopologyNode.forStreamsApp("order-processor"));

    String result = service.toMermaid(graph);

    // STREAMS_APP → diamond: id{"label"}
    assertThat(result).contains("streams_order_processor{\"");
  }

  @Test
  void toMermaid_withEdge_containsArrowWithLabel() {
    String result = service.toMermaid(twoNodeGraph());

    assertThat(result)
        .contains("topic_orders_created")
        .contains("group_order_service")
        .contains("-->|CONSUMES|");
  }

  @Test
  void toMermaid_sanitisesSpecialCharactersInNodeIds() {
    var graph = new TopologyGraph();
    graph.addNode(TopologyNode.forTopic("my.domain:event-v1"));

    String result = service.toMermaid(graph);

    // All non-alphanumeric characters must be replaced with underscores.
    assertThat(result).contains("topic_my_domain_event_v1");
    assertThat(result).doesNotContain("topic:my.domain:event-v1");
  }

  // ── DOT ───────────────────────────────────────────────────────────────────────

  @Test
  void toDot_emptyGraph_returnsValidDotSkeleton() {
    String result = service.toDot(new TopologyGraph());

    assertThat(result).startsWith("digraph KafkaTopology {");
    assertThat(result).contains("rankdir=LR;");
    assertThat(result).endsWith("}\n");
  }

  @Test
  void toDot_topicNode_hasCylinderShapeAndBlueColor() {
    var graph = new TopologyGraph();
    graph.addNode(TopologyNode.forTopic("orders.created"));

    String result = service.toDot(graph);

    assertThat(result)
        .contains("\"topic:orders.created\"")
        .contains("shape=cylinder")
        .contains("fillcolor=lightblue");
  }

  @Test
  void toDot_consumerGroupNode_hasBoxShape() {
    var graph = new TopologyGraph();
    graph.addNode(TopologyNode.forConsumerGroup("order-service"));

    String result = service.toDot(graph);

    assertThat(result).contains("shape=box").contains("fillcolor=lightyellow");
  }

  @Test
  void toDot_streamsAppNode_hasHexagonShape() {
    var graph = new TopologyGraph();
    graph.addNode(TopologyNode.forStreamsApp("order-processor"));

    String result = service.toDot(graph);

    assertThat(result).contains("shape=hexagon").contains("fillcolor=lightgreen");
  }

  @Test
  void toDot_withEdge_containsArrow() {
    String result = service.toDot(twoNodeGraph());

    assertThat(result)
        .contains("\"topic:orders.created\" -> \"group:order-service\"")
        .contains("label=\"CONSUMES\"");
  }

  // ── JSON ──────────────────────────────────────────────────────────────────────

  @Test
  void toJson_containsNodesAndEdges() {
    String result = service.toJson(twoNodeGraph());

    assertThat(result).contains("\"nodes\"");
    assertThat(result).contains("\"edges\"");
    assertThat(result).contains("orders.created");
    assertThat(result).contains("order-service");
  }

  @Test
  void export_delegatesToCorrectFormatter() {
    var graph = twoNodeGraph();

    assertThat(service.export(graph, ExportFormat.MERMAID)).startsWith("flowchart LR");
    assertThat(service.export(graph, ExportFormat.DOT)).startsWith("digraph KafkaTopology");
    assertThat(service.export(graph, ExportFormat.JSON)).contains("\"nodes\"");
  }

  // ── APPLICATION node ──────────────────────────────────────────────

  @Test
  void toMermaid_applicationNode_usesPillShape() {
    var graph = new TopologyGraph();
    graph.addNode(TopologyNode.forApplication("order-service", java.util.Map.of()));

    String result = service.toMermaid(graph);

    // APPLICATION → stadium/pill: id(["label"])
    assertThat(result).contains("app_order_service([\"");
  }

  @Test
  void toDot_applicationNode_hasComponentShapeAndSalmonColor() {
    var graph = new TopologyGraph();
    graph.addNode(TopologyNode.forApplication("order-service", java.util.Map.of()));

    String result = service.toDot(graph);

    assertThat(result).contains("shape=component").contains("fillcolor=lightsalmon");
  }

  @Test
  void toMermaid_deploysEdge_containsArrow() {
    var graph = new TopologyGraph();
    var appNode = TopologyNode.forApplication("order-service", java.util.Map.of());
    var groupNode = TopologyNode.forConsumerGroup("order-service-cg");
    graph.addNode(appNode);
    graph.addNode(groupNode);
    graph.addEdge(TopologyEdge.of(
        appNode.id(), groupNode.id(), EdgeType.DEPLOYS, "order-service → order-service-cg"));

    String result = service.toMermaid(graph);

    assertThat(result)
        .contains("app_order_service")
        .contains("group_order_service_cg")
        .contains("-->|DEPLOYS|");
  }
}
