package io.github.swatibansal.kafkatopology.kafka.discovery;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.swatibansal.kafkatopology.model.NodeType;
import io.github.swatibansal.kafkatopology.model.TopologyNode;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.TestPropertySource;

/**
 * Integration tests for {@link TopologyDiscoveryService} using an in-process Kafka broker.
 *
 * <p>The embedded broker is pre-seeded with topics and, where needed, synthetic consumer-group
 * offsets (via {@link AdminClient#alterConsumerGroupOffsets}) to exercise edge creation without
 * requiring a running consumer process.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@EmbeddedKafka(
    partitions = 1,
    topics = {
      "orders.created",
      "inventory.updated",
      "audit.events",
      "my-streams-app-order-store-changelog"
    })
@TestPropertySource(
    properties = {
      "kafka-agent.bootstrap-servers=${spring.embedded.kafka.brokers}",
      "kafka-agent.security.protocol=PLAINTEXT",
      "kafka-agent.discovery.topic-include-patterns="
    })
class TopologyDiscoveryServiceIT {

  @Autowired
  private TopologyDiscoveryService discoveryService;

  @Autowired
  private AdminClient adminClient;

  @Test
  void discover_createsTopicNodesForAllBrokerTopics() {
    var graph = discoveryService.discover();

    assertThat(graph.nodesByType(NodeType.TOPIC))
        .extracting(TopologyNode::name)
        .contains("orders.created", "inventory.updated");
  }

  @Test
  void discover_includesStreamsInternalTopicsAsTopicNodes() {
    // Kafka Streams changelog topics are regular Kafka topics (not marked internal).
    var graph = discoveryService.discover();

    assertThat(graph.nodesByType(NodeType.TOPIC))
        .extracting(TopologyNode::name)
        .contains("my-streams-app-order-store-changelog");
  }

  @Test
  void discover_topicWithoutCommittedOffsetsHasNoConsumesEdge() {
    // No test in this class commits offsets against "audit.events", so regardless of the order in
    // which the sibling tests run against the shared broker, this topic must stay edge-less.
    var graph = discoveryService.discover();

    String auditTopicId = TopologyNode.topicId("audit.events");
    assertThat(graph.getNodes()).containsKey(auditTopicId);
    assertThat(graph.getEdges())
        .noneMatch(e -> e.sourceId().equals(auditTopicId) || e.targetId().equals(auditTopicId));
  }

  @Test
  void discover_classifiesStreamsAppAndCreatesConsumesEdge() throws Exception {
    // Register a consumer group named "my-streams-app" by committing a synthetic offset.
    // This simulates the group being present in broker metadata without running an actual consumer.
    var partition = new TopicPartition("orders.created", 0);
    adminClient
        .alterConsumerGroupOffsets(
            "my-streams-app", Map.of(partition, new OffsetAndMetadata(0L)))
        .all()
        .get(10, TimeUnit.SECONDS);

    var graph = discoveryService.discover();

    // The group should be classified as STREAMS_APP (not CONSUMER_GROUP) because the topic
    // "my-streams-app-order-store-changelog" matches the heuristic pattern.
    assertThat(graph.nodesByType(NodeType.STREAMS_APP))
        .extracting(TopologyNode::name)
        .contains("my-streams-app");

    // A CONSUMES edge must exist from orders.created to the Streams app node.
    assertThat(graph.getEdges())
        .anyMatch(
            e ->
                e.sourceId().equals(TopologyNode.topicId("orders.created"))
                    && e.targetId().equals(TopologyNode.streamsId("my-streams-app")));
  }

  @Test
  void discover_topicMetadataPreservedAfterConsumesEdgeCreation() throws Exception {
    // Register a consumer group so addConsumesEdges runs for orders.created
    var partition = new TopicPartition("orders.created", 0);
    adminClient
        .alterConsumerGroupOffsets(
            "metadata-preservation-test", Map.of(partition, new OffsetAndMetadata(0L)))
        .all()
        .get(10, TimeUnit.SECONDS);

    var graph = discoveryService.discover();

    // Metadata must survive the addConsumesEdges guard — it must NOT be wiped
    TopologyNode ordersNode = graph.getNodes().get(TopologyNode.topicId("orders.created"));
    assertThat(ordersNode).isNotNull();
    assertThat(ordersNode.metadata())
        .as("partitions metadata must be preserved even after consumer-group edges are added")
        .containsKey("partitions");
  }

  @Test
  void discover_topicNodesContainPartitionAndMessageCountMetadata() {
    var graph = discoveryService.discover();

    TopologyNode ordersNode = graph.getNodes().get(TopologyNode.topicId("orders.created"));
    assertThat(ordersNode).isNotNull();
    assertThat(ordersNode.metadata()).containsEntry("partitions", "1");
    assertThat(ordersNode.metadata()).containsKey("messageCount");
  }

  @Test
  void discover_plainConsumerGroupCreatesConsumesEdge() throws Exception {
    var partition = new TopicPartition("inventory.updated", 0);
    adminClient
        .alterConsumerGroupOffsets(
            "inventory-service", Map.of(partition, new OffsetAndMetadata(0L)))
        .all()
        .get(10, TimeUnit.SECONDS);

    var graph = discoveryService.discover();

    assertThat(graph.nodesByType(NodeType.CONSUMER_GROUP))
        .extracting(TopologyNode::name)
        .contains("inventory-service");

    assertThat(graph.getEdges())
        .anyMatch(
            e ->
                e.sourceId().equals(TopologyNode.topicId("inventory.updated"))
                    && e.targetId().equals(TopologyNode.groupNodeId("inventory-service")));
  }
}
