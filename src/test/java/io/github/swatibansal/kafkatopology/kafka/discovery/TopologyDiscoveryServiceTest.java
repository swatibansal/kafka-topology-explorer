package io.github.swatibansal.kafkatopology.kafka.discovery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import io.github.swatibansal.kafkatopology.config.KafkaAgentProperties;
import io.github.swatibansal.kafkatopology.kafka.admin.KafkaAdminService;
import io.github.swatibansal.kafkatopology.model.TopologyGraph;
import io.github.swatibansal.kafkatopology.model.TopologyNode;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link TopologyDiscoveryService} with mocked collaborators. Broker-backed
 * behaviour is covered by {@code TopologyDiscoveryServiceIT}; this class pins the contract between
 * the discovery filters and Streams detection.
 */
class TopologyDiscoveryServiceTest {

  private static final String STREAMS_GROUP = "order-analytics";
  private static final String CHANGELOG = STREAMS_GROUP + "-order-totals-store-changelog";

  private final KafkaAdminService adminService = mock(KafkaAdminService.class);
  private final StreamsTopologyAnalyzer analyzer = mock(StreamsTopologyAnalyzer.class);

  private TopologyDiscoveryService serviceWithTopicFilter(List<String> includePatterns) {
    var props = new KafkaAgentProperties(
        "localhost:9092",
        new KafkaAgentProperties.Security("PLAINTEXT", "", "", "", ""),
        new KafkaAgentProperties.Admin(30000, 30000),
        new KafkaAgentProperties.Cache(60000),
        new KafkaAgentProperties.Discovery(includePatterns, true),
        new KafkaAgentProperties.Enrichment("prod", ""),
        new KafkaAgentProperties.GitHub(false, "", "", List.of()));
    return new TopologyDiscoveryService(adminService, analyzer, props);
  }

  @Test
  void discover_streamsDetectionSeesEveryBrokerTopicNotJustTheFilteredOnes() {
    // Discovery filters narrow the graph (here the numeric-name exclusion drops "12345").
    // Detection must still be handed the full broker listing, because a group's -changelog /
    // -repartition topics are what identify it as a Streams application even when a filter keeps
    // them out of the graph.
    Set<String> brokerTopics = Set.of("orders.order.created", CHANGELOG, "12345");
    given(adminService.listTopicNames()).willReturn(brokerTopics);
    given(adminService.describeTopics(any()))
        .willThrow(new IllegalStateException("not needed for this test"));
    given(adminService.listConsumerGroups())
        .willReturn(List.of(new ConsumerGroupListing(STREAMS_GROUP, false)));
    given(analyzer.detectStreamsApps(any(), any())).willReturn(Set.of());
    given(adminService.listConsumerGroupOffsets(STREAMS_GROUP)).willReturn(Map.of(
        new TopicPartition("orders.order.created", 0), new OffsetAndMetadata(0L)));

    TopologyGraph graph = serviceWithTopicFilter(List.of()).discover();

    verify(analyzer).detectStreamsApps(eq(Set.of(STREAMS_GROUP)), eq(brokerTopics));
    assertThat(graph.getNodes())
        .containsKey(TopologyNode.topicId("orders.order.created"))
        .doesNotContainKey(TopologyNode.topicId("12345"));
  }
}
