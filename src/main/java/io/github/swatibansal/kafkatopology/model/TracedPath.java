package io.github.swatibansal.kafkatopology.model;

import java.util.List;

/**
 * An immutable, ordered sequence of {@link TopologyNode}s representing one path through the
 * topology graph from a starting topic to a final destination.
 *
 * @param nodes ordered list of nodes from the starting topic to the destination; never empty
 * @param destination the last node in the path — the final consumer group, Streams app or
 *     application, or the start topic itself if nothing consumes it
 * @param cycleDetected {@code true} when the path was terminated because all downstream edges from
 *     the terminal node led back to nodes already on the path, indicating a topology cycle
 */
public record TracedPath(
    List<TopologyNode> nodes,
    TopologyNode destination,
    boolean cycleDetected) {

  /**
   * Returns the number of hops in this path (number of nodes minus one). A single-node path
   * (unconsumed topic) has zero hops.
   *
   * @return hop count
   */
  public int hopCount() {
    return nodes.size() - 1;
  }
}
