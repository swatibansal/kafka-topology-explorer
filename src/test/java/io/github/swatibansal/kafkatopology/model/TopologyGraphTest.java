package io.github.swatibansal.kafkatopology.model;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link TopologyGraph}. No Spring context required. */
class TopologyGraphTest {

  @Test
  void addNode_storesNodeById() {
    var graph = new TopologyGraph();
    var node = TopologyNode.forTopic("orders.created");

    graph.addNode(node);

    assertThat(graph.getNodes()).containsKey("topic:orders.created");
    assertThat(graph.getNodes().get("topic:orders.created")).isEqualTo(node);
  }

  @Test
  void addEdge_buildsForwardAdjacency() {
    var graph = new TopologyGraph();
    var topic = TopologyNode.forTopic("orders.created");
    var group = TopologyNode.forConsumerGroup("order-service");
    graph.addNode(topic);
    graph.addNode(group);

    graph.addEdge(TopologyEdge.of(topic.id(), group.id(), EdgeType.CONSUMES, "test"));

    assertThat(graph.neighbors(topic.id())).containsExactly(group.id());
    assertThat(graph.neighbors(group.id())).isEmpty();
  }

  @Test
  void addEdge_multipleOutgoingEdgesFromSameSource() {
    var graph = new TopologyGraph();
    var topic = TopologyNode.forTopic("orders.created");
    var groupA = TopologyNode.forConsumerGroup("service-a");
    var groupB = TopologyNode.forConsumerGroup("service-b");
    graph.addNode(topic);
    graph.addNode(groupA);
    graph.addNode(groupB);

    graph.addEdge(TopologyEdge.of(topic.id(), groupA.id(), EdgeType.CONSUMES, "a"));
    graph.addEdge(TopologyEdge.of(topic.id(), groupB.id(), EdgeType.CONSUMES, "b"));

    assertThat(graph.neighbors(topic.id())).containsExactlyInAnyOrder(groupA.id(), groupB.id());
    assertThat(graph.edgeCount()).isEqualTo(2);
  }

  @Test
  void nodesByType_filtersCorrectly() {
    var graph = new TopologyGraph();
    graph.addNode(TopologyNode.forTopic("orders.created"));
    graph.addNode(TopologyNode.forTopic("inventory.updated"));
    graph.addNode(TopologyNode.forConsumerGroup("order-service"));
    graph.addNode(TopologyNode.forStreamsApp("order-processor"));

    assertThat(graph.nodesByType(NodeType.TOPIC)).hasSize(2);
    assertThat(graph.nodesByType(NodeType.CONSUMER_GROUP)).hasSize(1);
    assertThat(graph.nodesByType(NodeType.STREAMS_APP)).hasSize(1);
  }

  @Test
  void addNode_overwritesDuplicateId() {
    var graph = new TopologyGraph();
    graph.addNode(TopologyNode.forTopic("orders.created"));
    var withMeta =
        new TopologyNode(
            TopologyNode.topicId("orders.created"),
            NodeType.TOPIC,
            "orders.created",
            Map.of("partitions", "3"));

    graph.addNode(withMeta);

    assertThat(graph.nodeCount()).isEqualTo(1);
    assertThat(graph.getNodes().get(TopologyNode.topicId("orders.created")).metadata())
        .containsEntry("partitions", "3");
  }

  @Test
  void neighbors_returnsEmptyListForUnknownNode() {
    var graph = new TopologyGraph();

    assertThat(graph.neighbors("topic:nonexistent")).isEmpty();
  }

  @Test
  void getEdges_returnsUnmodifiableView() {
    var graph = new TopologyGraph();
    var edges = graph.getEdges();

    assertThat(edges).isEmpty();
    // Verify the returned list is unmodifiable.
    org.assertj.core.api.Assertions.assertThatThrownBy(
            () ->
                edges.add(
                    TopologyEdge.of("a", "b", EdgeType.CONSUMES, "should fail")))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  // ── removeOrphanNodes — CONSUMER_GROUP / STREAMS_APP ──────────────────────

  @Test
  void removeOrphanNodes_removesConsumerGroupWithNoConsumeEdge() {
    var graph = new TopologyGraph();
    var group = TopologyNode.forConsumerGroup("lonely-group");
    graph.addNode(group);

    List<TopologyNode> removed = graph.removeOrphanNodes();

    assertThat(removed).containsExactly(group);
    assertThat(graph.getNodes()).doesNotContainKey(group.id());
  }

  @Test
  void removeOrphanNodes_keepsConsumerGroupTargetedByConsumeEdge() {
    var graph = new TopologyGraph();
    var topic = TopologyNode.forTopic("orders.created");
    var group = TopologyNode.forConsumerGroup("order-service");
    graph.addNode(topic);
    graph.addNode(group);
    graph.addEdge(TopologyEdge.of(topic.id(), group.id(), EdgeType.CONSUMES, "test"));

    List<TopologyNode> removed = graph.removeOrphanNodes();

    assertThat(removed).isEmpty();
    assertThat(graph.getNodes()).containsKey(group.id());
  }

  // ── removeOrphanNodes — APPLICATION ───────────────────────────────────────

  @Test
  void removeOrphanNodes_removesApplicationNodeWithNoEdges() {
    var graph = new TopologyGraph();
    var app = TopologyNode.forApplication("orphan-app", Map.of());
    graph.addNode(app);

    List<TopologyNode> removed = graph.removeOrphanNodes();

    assertThat(removed).containsExactly(app);
    assertThat(graph.getNodes()).doesNotContainKey(app.id());
  }

  @Test
  void removeOrphanNodes_retainsApplicationNodeWithOutgoingEdge() {
    var graph = new TopologyGraph();
    var topic = TopologyNode.forTopic("orders.created");
    var app = TopologyNode.forApplication("order-service", Map.of());
    var group = TopologyNode.forConsumerGroup("order-group");
    graph.addNode(topic);
    graph.addNode(app);
    graph.addNode(group);
    graph.addEdge(TopologyEdge.of(topic.id(), group.id(), EdgeType.CONSUMES, "test"));
    graph.addEdge(TopologyEdge.of(app.id(), group.id(), EdgeType.DEPLOYS, "test"));

    List<TopologyNode> removed = graph.removeOrphanNodes();

    assertThat(removed).isEmpty();
    assertThat(graph.getNodes()).containsKey(app.id());
    assertThat(graph.edgeCount()).isEqualTo(2);
  }

  @Test
  void removeOrphanNodes_dropsEdgesToPrunedNodesAndThenIsolatedApplication() {
    var graph = new TopologyGraph();
    var app = TopologyNode.forApplication("order-service", Map.of());
    var group = TopologyNode.forConsumerGroup("never-committed-group");
    graph.addNode(app);
    graph.addNode(group);
    graph.addEdge(TopologyEdge.of(app.id(), group.id(), EdgeType.DEPLOYS, "test"));

    List<TopologyNode> removed = graph.removeOrphanNodes();

    // The group has no CONSUMES edge, so it goes first; its DEPLOYS edge is then dangling and is
    // dropped, which leaves the application isolated, so it goes too.
    assertThat(removed).containsExactly(group, app);
    assertThat(graph.getNodes()).isEmpty();
    assertThat(graph.getEdges()).isEmpty();
    assertThat(graph.neighbors(app.id())).isEmpty();
  }

  @Test
  void removeOrphanNodes_dropsEdgesFromPrunedGroupAndThenEdgelessDltTopic() {
    var graph = new TopologyGraph();
    var group = TopologyNode.forConsumerGroup("never-committed-group");
    var dltTopic = TopologyNode.forTopic("orders.order.created.DLT");
    graph.addNode(group);
    graph.addNode(dltTopic);
    graph.addEdge(TopologyEdge.of(group.id(), dltTopic.id(), EdgeType.PRODUCES, "test"));

    List<TopologyNode> removed = graph.removeOrphanNodes();

    // The group has no CONSUMES edge, so it goes first; without its PRODUCES edge the DLT topic
    // is edge-less and goes in the second pass.
    assertThat(removed).containsExactly(group, dltTopic);
    assertThat(graph.getNodes()).isEmpty();
    assertThat(graph.getEdges()).isEmpty();
  }

  @Test
  void removeOrphanNodes_retainsApplicationStillReferencedAfterPruningOneTarget() {
    var graph = new TopologyGraph();
    var topic = TopologyNode.forTopic("orders.created");
    var app = TopologyNode.forApplication("order-service", Map.of());
    var liveGroup = TopologyNode.forConsumerGroup("order-group");
    var deadGroup = TopologyNode.forConsumerGroup("never-committed-group");
    graph.addNode(topic);
    graph.addNode(app);
    graph.addNode(liveGroup);
    graph.addNode(deadGroup);
    graph.addEdge(TopologyEdge.of(topic.id(), liveGroup.id(), EdgeType.CONSUMES, "test"));
    graph.addEdge(TopologyEdge.of(app.id(), liveGroup.id(), EdgeType.DEPLOYS, "live"));
    graph.addEdge(TopologyEdge.of(app.id(), deadGroup.id(), EdgeType.DEPLOYS, "dead"));

    List<TopologyNode> removed = graph.removeOrphanNodes();

    assertThat(removed).containsExactly(deadGroup);
    assertThat(graph.getNodes()).containsKeys(topic.id(), app.id(), liveGroup.id());
    assertThat(graph.getEdges())
        .allMatch(
            e -> graph.getNodes().containsKey(e.sourceId())
                && graph.getNodes().containsKey(e.targetId()));
    assertThat(graph.neighbors(app.id())).containsExactly(liveGroup.id());
  }

  @Test
  void removeOrphanNodes_retainsApplicationNodeWithIncomingEdge() {
    var graph = new TopologyGraph();
    var topic = TopologyNode.forTopic("orders.created");
    var app = TopologyNode.forApplication("order-service", Map.of());
    graph.addNode(topic);
    graph.addNode(app);
    graph.addEdge(TopologyEdge.of(topic.id(), app.id(), EdgeType.CONSUMES, "test"));

    List<TopologyNode> removed = graph.removeOrphanNodes();

    assertThat(removed).doesNotContain(app);
    assertThat(graph.getNodes()).containsKey(app.id());
  }

  // ── removeOrphanNodes — TOPIC (retry / dlt) ───────────────────────────────

  @Test
  void removeOrphanNodes_removesRetryTopicWithNoEdges() {
    var graph = new TopologyGraph();
    var retryTopic = TopologyNode.forTopic("orders.retry");
    graph.addNode(retryTopic);

    List<TopologyNode> removed = graph.removeOrphanNodes();

    assertThat(removed).containsExactly(retryTopic);
    assertThat(graph.getNodes()).doesNotContainKey(retryTopic.id());
  }

  @Test
  void removeOrphanNodes_removesDltTopicWithNoEdges() {
    var graph = new TopologyGraph();
    var dltTopic = TopologyNode.forTopic("orders.dlt");
    graph.addNode(dltTopic);

    List<TopologyNode> removed = graph.removeOrphanNodes();

    assertThat(removed).containsExactly(dltTopic);
    assertThat(graph.getNodes()).doesNotContainKey(dltTopic.id());
  }

  @Test
  void removeOrphanNodes_retryAndDltMatchIsCaseInsensitive() {
    var graph = new TopologyGraph();
    var upperRetry = TopologyNode.forTopic("orders.RETRY");
    var upperDlt = TopologyNode.forTopic("orders.DLT");
    graph.addNode(upperRetry);
    graph.addNode(upperDlt);

    List<TopologyNode> removed = graph.removeOrphanNodes();

    assertThat(removed).containsExactlyInAnyOrder(upperRetry, upperDlt);
  }

  @Test
  void removeOrphanNodes_keepsRetryTopicThatHasEdges() {
    var graph = new TopologyGraph();
    var retryTopic = TopologyNode.forTopic("orders.retry");
    var group = TopologyNode.forConsumerGroup("retry-consumer");
    graph.addNode(retryTopic);
    graph.addNode(group);
    graph.addEdge(TopologyEdge.of(retryTopic.id(), group.id(), EdgeType.CONSUMES, "test"));

    List<TopologyNode> removed = graph.removeOrphanNodes();

    assertThat(removed).doesNotContain(retryTopic);
    assertThat(graph.getNodes()).containsKey(retryTopic.id());
  }

  @Test
  void removeOrphanNodes_keepsRegularTopicWithNoEdges() {
    var graph = new TopologyGraph();
    var normalTopic = TopologyNode.forTopic("orders.created");
    graph.addNode(normalTopic);

    List<TopologyNode> removed = graph.removeOrphanNodes();

    assertThat(removed).isEmpty();
    assertThat(graph.getNodes()).containsKey(normalTopic.id());
  }

  @Test
  void downstream_followsConsumesAndProducesForwardAndDeploysInReverse() {
    var graph = new TopologyGraph();
    var topicA = TopologyNode.forTopic("topic-a");
    var groupB = TopologyNode.forConsumerGroup("group-b");
    var appC = TopologyNode.forApplication("app-c", Map.of());
    var topicD = TopologyNode.forTopic("topic-d");
    graph.addNode(topicA);
    graph.addNode(groupB);
    graph.addNode(appC);
    graph.addNode(topicD);
    graph.addEdge(TopologyEdge.of(topicA.id(), groupB.id(), EdgeType.CONSUMES, "a→b"));
    graph.addEdge(TopologyEdge.of(appC.id(), groupB.id(), EdgeType.DEPLOYS, "c→b"));
    graph.addEdge(TopologyEdge.of(appC.id(), topicD.id(), EdgeType.PRODUCES, "c→d"));

    assertThat(graph.downstream(topicA.id())).containsExactly(groupB.id());
    assertThat(graph.downstream(groupB.id())).containsExactly(appC.id());
    assertThat(graph.downstream(appC.id())).containsExactly(topicD.id());
    assertThat(graph.downstream(topicD.id())).isEmpty();
    // Structural adjacency is unchanged: DEPLOYS still leaves the application node.
    assertThat(graph.neighbors(appC.id())).containsExactly(groupB.id(), topicD.id());
    assertThat(graph.neighbors(groupB.id())).isEmpty();
  }

  @Test
  void downstream_isRebuiltWhenOrphanPruningDropsEdges() {
    var graph = new TopologyGraph();
    var groupB = TopologyNode.forConsumerGroup("group-b");
    var appC = TopologyNode.forApplication("app-c", Map.of());
    graph.addNode(groupB);
    graph.addNode(appC);
    graph.addEdge(TopologyEdge.of(appC.id(), groupB.id(), EdgeType.DEPLOYS, "c→b"));
    assertThat(graph.downstream(groupB.id())).containsExactly(appC.id());

    graph.removeOrphanNodes(); // group-b consumes nothing → pruned, then app-c isolated → pruned

    assertThat(graph.downstream(groupB.id())).isEmpty();
  }
}
