package io.github.swatibansal.kafkatopology.kafka.discovery;

import java.util.Set;
import java.util.stream.Collectors;
import org.springframework.stereotype.Component;

/**
 * Heuristically identifies Kafka Streams applications from broker metadata.
 *
 * <p>Kafka Streams automatically creates internal topics whose names follow these conventions:
 *
 * <ul>
 *   <li>{@code <application.id>-<store-name>-changelog} — state-store changelog topics
 *   <li>{@code <application.id>-KSTREAM-REPARTITION-<N>-repartition} — repartition topics
 *   <li>{@code <application.id>-KSTREAM-*} — other Streams-internal processing nodes
 *   <li>{@code <application.id>-KTABLE-*} — KTable internal nodes
 * </ul>
 *
 * <p>By cross-referencing known consumer group IDs against all topic names, we can flag which
 * groups are Streams applications rather than plain consumers.
 *
 * <p><strong>Limitations:</strong> this is a best-effort heuristic. Applications that customise
 * their internal topic names, or groups whose IDs happen to share a prefix with an unrelated
 * topic, may be mis-classified.
 */
@Component
public class StreamsTopologyAnalyzer {

  /**
   * Scans all topics for Kafka Streams internal-topic patterns and returns the IDs of consumer
   * groups that appear to be Streams applications.
   *
   * @param consumerGroupIds all consumer group IDs registered with the cluster
   * @param allTopics all topics visible in the cluster (including Streams internal ones)
   * @return set of group IDs that match the Streams heuristic; never {@code null}
   */
  public Set<String> detectStreamsApps(Set<String> consumerGroupIds, Set<String> allTopics) {
    return consumerGroupIds.stream()
        .filter(groupId -> isStreamsApp(groupId, allTopics))
        .collect(Collectors.toSet());
  }

  /**
   * Returns {@code true} if any topic in {@code allTopics} matches the Kafka Streams internal
   * naming convention for the given consumer group / application ID.
   *
   * @param groupId consumer group ID (candidate Streams {@code application.id})
   * @param allTopics all topics including Streams internal ones
   * @return whether the group looks like a Kafka Streams application
   */
  public boolean isStreamsApp(String groupId, Set<String> allTopics) {
    String prefix = groupId + "-";
    return allTopics.stream()
        .filter(topic -> topic.startsWith(prefix))
        .anyMatch(
            topic ->
                topic.endsWith("-changelog")
                    || topic.endsWith("-repartition"));
  }

  /**
   * Returns {@code true} if the given topic is a Kafka Streams internal topic belonging to the
   * specified application. These topics are excluded from user-facing CONSUMES edges because they
   * represent infrastructure plumbing rather than business message flow.
   *
   * @param topic topic name to check
   * @param appId Streams {@code application.id}
   * @return whether the topic is a Streams-internal topic for the given app
   */
  public boolean isStreamsInternalTopic(String topic, String appId) {
    if (!topic.startsWith(appId + "-")) {
      return false;
    }
    return topic.endsWith("-changelog")
        || topic.endsWith("-repartition")
        || topic.contains("-KSTREAM-")
        || topic.contains("-KTABLE-");
  }
}
