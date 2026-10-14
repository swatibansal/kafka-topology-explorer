package io.github.swatibansal.kafkatopology.kafka.traversal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.swatibansal.kafkatopology.model.EdgeType;
import io.github.swatibansal.kafkatopology.model.NodeType;
import io.github.swatibansal.kafkatopology.model.TopologyEdge;
import io.github.swatibansal.kafkatopology.model.TopologyGraph;
import io.github.swatibansal.kafkatopology.model.TopologyNode;
import io.github.swatibansal.kafkatopology.model.TraversalResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link GraphTraversalService}. Graphs are built manually — no Spring context or
 * Kafka broker required.
 */
class GraphTraversalServiceTest {

  private GraphTraversalService service;

  @BeforeEach
  void setUp() {
    service = new GraphTraversalService();
  }

  // ── helper ──────────────────────────────────────────────────────────────────

  private static TopologyGraph linearGraph(
      String topicName, String groupId, NodeType groupType) {
    var graph = new TopologyGraph();
    var topic = TopologyNode.forTopic(topicName);
    var group =
        groupType == NodeType.STREAMS_APP
            ? TopologyNode.forStreamsApp(groupId)
            : TopologyNode.forConsumerGroup(groupId);
    graph.addNode(topic);
    graph.addNode(group);
    graph.addEdge(
        TopologyEdge.of(topic.id(), group.id(), EdgeType.CONSUMES, topicName + " → " + groupId));
    return graph;
  }

  // ── single-hop ───────────────────────────────────────────────────────────────

  @Test
  void trace_singleConsumer_returnsSingleTwoNodePath() {
    TopologyGraph graph = linearGraph("orders.created", "order-service", NodeType.CONSUMER_GROUP);

    TraversalResult result = service.trace("orders.created", graph);

    assertThat(result.startTopic()).isEqualTo("orders.created");
    assertThat(result.pathCount()).isEqualTo(1);
    assertThat(result.paths().get(0).nodes())
        .extracting(TopologyNode::name)
        .containsExactly("orders.created", "order-service");
    assertThat(result.paths().get(0).cycleDetected()).isFalse();
    assertThat(result.paths().get(0).hopCount()).isEqualTo(1);
  }

  @Test
  void trace_multipleConsumers_returnsOnePathPerConsumer() {
    var graph = new TopologyGraph();
    var topic = TopologyNode.forTopic("orders.created");
    var groupA = TopologyNode.forConsumerGroup("service-a");
    var groupB = TopologyNode.forConsumerGroup("service-b");
    graph.addNode(topic);
    graph.addNode(groupA);
    graph.addNode(groupB);
    graph.addEdge(TopologyEdge.of(topic.id(), groupA.id(), EdgeType.CONSUMES, "a"));
    graph.addEdge(TopologyEdge.of(topic.id(), groupB.id(), EdgeType.CONSUMES, "b"));

    TraversalResult result = service.trace("orders.created", graph);

    assertThat(result.pathCount()).isEqualTo(2);
    assertThat(result.finalDestinations())
        .extracting(TopologyNode::name)
        .containsExactlyInAnyOrder("service-a", "service-b");
  }

  // ── multi-hop ────────────────────────────────────────────────────────────────

  @Test
  void trace_deepGraph_traversesAllHops() {
    // topicA → streamsB → topicC → groupD
    var graph = new TopologyGraph();
    var topicA = TopologyNode.forTopic("topic-a");
    var streamsB = TopologyNode.forStreamsApp("streams-b");
    var topicC = TopologyNode.forTopic("topic-c");
    var groupD = TopologyNode.forConsumerGroup("group-d");
    graph.addNode(topicA);
    graph.addNode(streamsB);
    graph.addNode(topicC);
    graph.addNode(groupD);
    graph.addEdge(TopologyEdge.of(topicA.id(), streamsB.id(), EdgeType.CONSUMES, "a→b"));
    graph.addEdge(TopologyEdge.of(streamsB.id(), topicC.id(), EdgeType.PRODUCES, "b→c"));
    graph.addEdge(TopologyEdge.of(topicC.id(), groupD.id(), EdgeType.CONSUMES, "c→d"));

    TraversalResult result = service.trace("topic-a", graph);

    assertThat(result.pathCount()).isEqualTo(1);
    assertThat(result.paths().get(0).nodes())
        .extracting(TopologyNode::name)
        .containsExactly("topic-a", "streams-b", "topic-c", "group-d");
    assertThat(result.paths().get(0).hopCount()).isEqualTo(3);
    assertThat(result.finalDestinations())
        .extracting(TopologyNode::name)
        .containsExactly("group-d");
  }

  @Test
  void trace_diamondGraph_enumeratesBothPaths() {
    // topicA → streamsB \
    //                    → topicD → groupE
    // topicA → streamsC /
    var graph = new TopologyGraph();
    var topicA = TopologyNode.forTopic("topic-a");
    var streamsB = TopologyNode.forStreamsApp("streams-b");
    var streamsC = TopologyNode.forStreamsApp("streams-c");
    var topicD = TopologyNode.forTopic("topic-d");
    var groupE = TopologyNode.forConsumerGroup("group-e");
    graph.addNode(topicA);
    graph.addNode(streamsB);
    graph.addNode(streamsC);
    graph.addNode(topicD);
    graph.addNode(groupE);
    graph.addEdge(TopologyEdge.of(topicA.id(), streamsB.id(), EdgeType.CONSUMES, "a→b"));
    graph.addEdge(TopologyEdge.of(topicA.id(), streamsC.id(), EdgeType.CONSUMES, "a→c"));
    graph.addEdge(TopologyEdge.of(streamsB.id(), topicD.id(), EdgeType.PRODUCES, "b→d"));
    graph.addEdge(TopologyEdge.of(streamsC.id(), topicD.id(), EdgeType.PRODUCES, "c→d"));
    graph.addEdge(TopologyEdge.of(topicD.id(), groupE.id(), EdgeType.CONSUMES, "d→e"));

    TraversalResult result = service.trace("topic-a", graph);

    assertThat(result.pathCount()).isEqualTo(2);
    // Both paths must end at group-e.
    assertThat(result.finalDestinations())
        .extracting(TopologyNode::name)
        .containsExactly("group-e");
    // One path goes through streams-b, the other through streams-c.
    assertThat(result.paths())
        .anySatisfy(
            path ->
                assertThat(path.nodes())
                    .extracting(TopologyNode::name)
                    .containsExactly("topic-a", "streams-b", "topic-d", "group-e"))
        .anySatisfy(
            path ->
                assertThat(path.nodes())
                    .extracting(TopologyNode::name)
                    .containsExactly("topic-a", "streams-c", "topic-d", "group-e"));
  }

  // ── application hops (DEPLOYS reversed, PRODUCES forward) ───────────────────

  @Test
  void trace_groupDeployedByApplication_continuesThroughApplicationToProducedTopic() {
    // topicA → groupB ⇐DEPLOYS= appC → topicD → groupE
    var graph = new TopologyGraph();
    var topicA = TopologyNode.forTopic("topic-a");
    var groupB = TopologyNode.forConsumerGroup("group-b");
    var appC = TopologyNode.forApplication("app-c", java.util.Map.of());
    var topicD = TopologyNode.forTopic("topic-d");
    var groupE = TopologyNode.forConsumerGroup("group-e");
    graph.addNode(topicA);
    graph.addNode(groupB);
    graph.addNode(appC);
    graph.addNode(topicD);
    graph.addNode(groupE);
    graph.addEdge(TopologyEdge.of(topicA.id(), groupB.id(), EdgeType.CONSUMES, "a→b"));
    graph.addEdge(TopologyEdge.of(appC.id(), groupB.id(), EdgeType.DEPLOYS, "c→b"));
    graph.addEdge(TopologyEdge.of(appC.id(), topicD.id(), EdgeType.PRODUCES, "c→d"));
    graph.addEdge(TopologyEdge.of(topicD.id(), groupE.id(), EdgeType.CONSUMES, "d→e"));

    TraversalResult result = service.trace("topic-a", graph);

    assertThat(result.pathCount()).isEqualTo(1);
    assertThat(result.paths().get(0).nodes())
        .extracting(TopologyNode::name)
        .containsExactly("topic-a", "group-b", "app-c", "topic-d", "group-e");
    assertThat(result.paths().get(0).hopCount()).isEqualTo(4);
    assertThat(result.finalDestinations())
        .extracting(TopologyNode::name)
        .containsExactly("group-e");
  }

  @Test
  void trace_applicationDeployingTwoGroups_doesNotCrossToSiblingGroup() {
    // topicA → groupB ⇐DEPLOYS= appC =DEPLOYS⇒ groupX   (appC produces nothing)
    var graph = new TopologyGraph();
    var topicA = TopologyNode.forTopic("topic-a");
    var groupB = TopologyNode.forConsumerGroup("group-b");
    var appC = TopologyNode.forApplication("app-c", java.util.Map.of());
    var groupX = TopologyNode.forConsumerGroup("group-x");
    graph.addNode(topicA);
    graph.addNode(groupB);
    graph.addNode(appC);
    graph.addNode(groupX);
    graph.addEdge(TopologyEdge.of(topicA.id(), groupB.id(), EdgeType.CONSUMES, "a→b"));
    graph.addEdge(TopologyEdge.of(appC.id(), groupB.id(), EdgeType.DEPLOYS, "c→b"));
    graph.addEdge(TopologyEdge.of(appC.id(), groupX.id(), EdgeType.DEPLOYS, "c→x"));

    TraversalResult result = service.trace("topic-a", graph);

    assertThat(result.pathCount()).isEqualTo(1);
    assertThat(result.paths().get(0).nodes())
        .extracting(TopologyNode::name)
        .containsExactly("topic-a", "group-b", "app-c");
    assertThat(result.paths().get(0).cycleDetected()).isFalse();
    assertThat(result.finalDestinations())
        .extracting(TopologyNode::name)
        .containsExactly("app-c");
  }

  @Test
  void trace_applicationProducesToTopicItsOwnGroupConsumes_terminatesWithCycleFlag() {
    // topicA → groupB ⇐DEPLOYS= appC → topicA
    var graph = new TopologyGraph();
    var topicA = TopologyNode.forTopic("topic-a");
    var groupB = TopologyNode.forConsumerGroup("group-b");
    var appC = TopologyNode.forApplication("app-c", java.util.Map.of());
    graph.addNode(topicA);
    graph.addNode(groupB);
    graph.addNode(appC);
    graph.addEdge(TopologyEdge.of(topicA.id(), groupB.id(), EdgeType.CONSUMES, "a→b"));
    graph.addEdge(TopologyEdge.of(appC.id(), groupB.id(), EdgeType.DEPLOYS, "c→b"));
    graph.addEdge(TopologyEdge.of(appC.id(), topicA.id(), EdgeType.PRODUCES, "c→a"));

    TraversalResult result = service.trace("topic-a", graph);

    assertThat(result.pathCount()).isEqualTo(1);
    assertThat(result.paths().get(0).nodes())
        .extracting(TopologyNode::name)
        .containsExactly("topic-a", "group-b", "app-c");
    assertThat(result.paths().get(0).cycleDetected()).isTrue();
  }

  // ── cycle handling ───────────────────────────────────────────────────────────

  @Test
  void trace_directCycle_terminatesWithCycleFlag() {
    // topicA → streamsB → topicA (cycle back to start)
    var graph = new TopologyGraph();
    var topicA = TopologyNode.forTopic("topic-a");
    var streamsB = TopologyNode.forStreamsApp("streams-b");
    graph.addNode(topicA);
    graph.addNode(streamsB);
    graph.addEdge(TopologyEdge.of(topicA.id(), streamsB.id(), EdgeType.CONSUMES, "a→b"));
    graph.addEdge(TopologyEdge.of(streamsB.id(), topicA.id(), EdgeType.PRODUCES, "b→a"));

    TraversalResult result = service.trace("topic-a", graph);

    assertThat(result.pathCount()).isEqualTo(1);
    assertThat(result.paths().get(0).cycleDetected()).isTrue();
    // Path terminates at streams-b (the node whose only outgoing edge is the back-edge).
    assertThat(result.paths().get(0).destination().name()).isEqualTo("streams-b");
  }

  @Test
  void trace_mixedCyclicAndNonCyclicBranches_onlyNonCyclicBranchesReachLeaf() {
    // topicA → streamsB → topicA (cycle)
    // topicA → groupC (non-cyclic leaf)
    var graph = new TopologyGraph();
    var topicA = TopologyNode.forTopic("topic-a");
    var streamsB = TopologyNode.forStreamsApp("streams-b");
    var groupC = TopologyNode.forConsumerGroup("group-c");
    graph.addNode(topicA);
    graph.addNode(streamsB);
    graph.addNode(groupC);
    graph.addEdge(TopologyEdge.of(topicA.id(), streamsB.id(), EdgeType.CONSUMES, "a→b"));
    graph.addEdge(TopologyEdge.of(streamsB.id(), topicA.id(), EdgeType.PRODUCES, "b→a"));
    graph.addEdge(TopologyEdge.of(topicA.id(), groupC.id(), EdgeType.CONSUMES, "a→c"));

    TraversalResult result = service.trace("topic-a", graph);

    // Exactly two paths: the cycle branch (terminates at streams-b) and the clean branch.
    assertThat(result.pathCount()).isEqualTo(2);
    assertThat(result.paths())
        .anySatisfy(p -> assertThat(p.cycleDetected()).isTrue())
        .anySatisfy(p -> assertThat(p.cycleDetected()).isFalse());
  }

  // ── edge cases ───────────────────────────────────────────────────────────────

  @Test
  void trace_unconsumedTopic_returnsSinglePathOfJustTheTopic() {
    var graph = new TopologyGraph();
    graph.addNode(TopologyNode.forTopic("orphan-topic"));

    TraversalResult result = service.trace("orphan-topic", graph);

    assertThat(result.pathCount()).isEqualTo(1);
    assertThat(result.paths().get(0).nodes())
        .extracting(TopologyNode::name)
        .containsExactly("orphan-topic");
    assertThat(result.paths().get(0).hopCount()).isEqualTo(0);
    assertThat(result.finalDestinations())
        .extracting(TopologyNode::name)
        .containsExactly("orphan-topic");
  }

  @Test
  void trace_unknownTopic_throwsIllegalArgumentException() {
    var graph = new TopologyGraph();

    assertThatThrownBy(() -> service.trace("does-not-exist", graph))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("does-not-exist");
  }
}
