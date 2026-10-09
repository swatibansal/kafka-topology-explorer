package io.github.swatibansal.kafkatopology.model;

import java.util.Map;

/**
 * An immutable node in the Kafka topology graph.
 *
 * <p>Node IDs are namespaced by type to avoid collisions between a topic and a consumer group that
 * share the same name (e.g. both named {@code "orders"}):
 *
 * <ul>
 *   <li>Topics: {@code topic:<name>}
 *   <li>Consumer groups: {@code group:<id>}
 *   <li>Streams apps: {@code streams:<app-id>}
 *   <li>Applications discovered from configuration: {@code app:<name>}
 * </ul>
 *
 * @param id unique identifier (namespaced by type)
 * @param type node classification
 * @param name human-readable name (topic name or group / app ID)
 * @param metadata additional broker-provided metadata (e.g. partition count, consumer state)
 */
public record TopologyNode(
    String id,
    NodeType type,
    String name,
    Map<String, String> metadata) {

  /** Creates a {@link NodeType#TOPIC} node for the given topic name with no metadata. */
  public static TopologyNode forTopic(String topicName) {
    return forTopic(topicName, Map.of());
  }

  /**
   * Creates a {@link NodeType#TOPIC} node with broker-supplied metadata.
   *
   * @param topicName Kafka topic name
   * @param metadata key-value pairs such as {@code partitions}, {@code replicationFactor},
   *     {@code messageCount}
   */
  public static TopologyNode forTopic(String topicName, Map<String, String> metadata) {
    return new TopologyNode(topicId(topicName), NodeType.TOPIC, topicName, Map.copyOf(metadata));
  }

  /** Creates a {@link NodeType#CONSUMER_GROUP} node for the given group ID. */
  public static TopologyNode forConsumerGroup(String groupId) {
    return new TopologyNode(groupNodeId(groupId), NodeType.CONSUMER_GROUP, groupId, Map.of());
  }

  /** Creates a {@link NodeType#STREAMS_APP} node for the given Streams application ID. */
  public static TopologyNode forStreamsApp(String appId) {
    return new TopologyNode(streamsId(appId), NodeType.STREAMS_APP, appId, Map.of());
  }

  /**
   * Creates a {@link NodeType#APPLICATION} node for the given application name.
   *
   * @param appName application name (the directory holding its {@code application.yml})
   * @param metadata optional metadata (candidateGroupIds, unmatched candidates, etc.)
   */
  public static TopologyNode forApplication(String appName, Map<String, String> metadata) {
    return new TopologyNode(applicationId(appName), NodeType.APPLICATION, appName, metadata);
  }

  /**
   * Returns the canonical node ID for a topic name.
   *
   * @param topicName topic name
   * @return namespaced node ID
   */
  public static String topicId(String topicName) {
    return "topic:" + topicName;
  }

  /**
   * Returns the canonical node ID for a consumer group.
   *
   * @param groupId consumer group ID
   * @return namespaced node ID
   */
  public static String groupNodeId(String groupId) {
    return "group:" + groupId;
  }

  /**
   * Returns the canonical node ID for a Kafka Streams application.
   *
   * @param appId Streams application.id
   * @return namespaced node ID
   */
  public static String streamsId(String appId) {
    return "streams:" + appId;
  }

  /**
   * Returns the canonical node ID for an application discovered from configuration.
   *
   * @param appName application name
   * @return namespaced node ID
   */
  public static String applicationId(String appName) {
    return "app:" + appName;
  }
}
