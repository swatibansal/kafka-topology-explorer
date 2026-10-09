package io.github.swatibansal.kafkatopology.kafka.admin;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.TopicDescription;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.TestPropertySource;

/**
 * Integration tests for {@link KafkaAdminService} using an in-process Kafka broker.
 *
 * <p>{@code @EmbeddedKafka} starts a real broker, sets {@code spring.embedded.kafka.brokers},
 * and {@code @TestPropertySource} wires that address into {@code kafka-agent.bootstrap-servers}
 * so the {@link org.apache.kafka.clients.admin.AdminClient} bean targets the embedded cluster.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@EmbeddedKafka(
    partitions = 1,
    topics = {"test.topic.alpha", "test.topic.beta"})
@TestPropertySource(
    properties = {
      "kafka-agent.bootstrap-servers=${spring.embedded.kafka.brokers}",
      "kafka-agent.security.protocol=PLAINTEXT",
      "kafka-agent.discovery.topic-include-patterns="
    })
class KafkaAdminServiceIT {

  @Autowired
  private KafkaAdminService kafkaAdminService;

  @Test
  void listTopicNames_returnsEmbeddedTopics() {
    Set<String> topics = kafkaAdminService.listTopicNames();

    assertThat(topics).contains("test.topic.alpha", "test.topic.beta");
  }

  @Test
  void describeTopics_returnsPartitionMetadata() {
    Map<String, TopicDescription> descriptions =
        kafkaAdminService.describeTopics(Set.of("test.topic.alpha"));

    assertThat(descriptions).containsKey("test.topic.alpha");
    assertThat(descriptions.get("test.topic.alpha").partitions()).hasSize(1);
  }

  @Test
  void listConsumerGroups_returnsEmptyWhenNoConsumers() {
    Collection<ConsumerGroupListing> groups = kafkaAdminService.listConsumerGroups();

    assertThat(groups).isEmpty();
  }

  @Test
  void listTopicMessageCounts_returnsZeroForEmptyTopic() {
    Map<String, TopicDescription> descriptions =
        kafkaAdminService.describeTopics(Set.of("test.topic.alpha"));

    Map<String, Long> counts = kafkaAdminService.listTopicMessageCounts(descriptions);

    assertThat(counts).containsKey("test.topic.alpha");
    assertThat(counts.get("test.topic.alpha")).isEqualTo(0L);
  }
}
