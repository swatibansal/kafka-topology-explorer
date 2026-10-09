package io.github.swatibansal.kafkatopology.model;

/**
 * An immutable directed edge in the Kafka topology graph.
 *
 * @param sourceId ID of the source node (see {@link TopologyNode#id()})
 * @param targetId ID of the target node
 * @param type relationship type between source and target
 * @param label human-readable description of the relationship
 */
public record TopologyEdge(
    String sourceId,
    String targetId,
    EdgeType type,
    String label) {

  /**
   * Convenience factory method.
   *
   * @param sourceId source node ID
   * @param targetId target node ID
   * @param type relationship type
   * @param label edge label
   * @return new {@code TopologyEdge}
   */
  public static TopologyEdge of(
      String sourceId, String targetId, EdgeType type, String label) {
    return new TopologyEdge(sourceId, targetId, type, label);
  }
}
