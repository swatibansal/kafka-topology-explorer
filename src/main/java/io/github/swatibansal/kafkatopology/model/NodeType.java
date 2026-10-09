package io.github.swatibansal.kafkatopology.model;

/** Classifies a node in the Kafka topology graph. */
public enum NodeType {

  /** A Kafka topic. */
  TOPIC,

  /** A plain Kafka consumer group (not a Kafka Streams application). */
  CONSUMER_GROUP,

  /**
   * A Kafka Streams application, identified heuristically by its {@code application.id} matching
   * the prefix of one or more changelog / repartition internal topics.
   */
  STREAMS_APP,

  /**
   * A deployed microservice discovered from its Spring Boot configuration. APPLICATION nodes
   * are linked to the {@link #CONSUMER_GROUP} or {@link #STREAMS_APP} nodes they own via
   * {@link EdgeType#DEPLOYS} edges.
   */
  APPLICATION
}
