package io.github.swatibansal.kafkatopology.model;

/** Describes the directional relationship between two nodes in the topology graph. */
public enum EdgeType {

  /** A consumer group or Streams application reads from a topic. */
  CONSUMES,

  /** A producer or Streams application writes to a topic. */
  PRODUCES,

  /**
   * An {@link NodeType#APPLICATION} deploys a microservice that owns the target
   * {@link NodeType#CONSUMER_GROUP} or {@link NodeType#STREAMS_APP} node.
   */
  DEPLOYS
}
