package io.github.swatibansal.kafkatopology.model;

/**
 * Why a node was pruned from the topology graph by {@link TopologyGraph#removeOrphanNodes()}.
 *
 * <p>Each constant corresponds to one pruning rule. The {@link #description()} is a short,
 * human-readable explanation suitable for an API response or a log line.
 */
public enum OrphanReason {

  /**
   * A {@link NodeType#CONSUMER_GROUP} or {@link NodeType#STREAMS_APP} that is the target of no
   * {@link EdgeType#CONSUMES} edge: the broker knows the group, but it has no committed offsets
   * on any topic in the graph (never committed, or only on filtered-out topics).
   */
  UNCONSUMED_GROUP(
      "Consumer group has no committed offsets on any topic in the graph"),

  /**
   * An {@link NodeType#APPLICATION} with no edges at all: none of the group IDs or topic names
   * extracted from its configuration matched a live node, so it could not be linked to Kafka.
   * The unmatched candidates are kept in the node's metadata.
   */
  UNLINKED_APPLICATION(
      "Application configuration names no consumer group or topic that exists on the broker"),

  /**
   * A {@link NodeType#TOPIC} whose name contains {@code retry} or {@code dlt} and that has no
   * edges: a retry or dead-letter topic that nothing consumes or produces into.
   */
  UNUSED_RETRY_OR_DLT_TOPIC(
      "Retry or dead-letter topic with no consumer group and no known producer");

  private final String description;

  OrphanReason(String description) {
    this.description = description;
  }

  /**
   * Returns a human-readable explanation of this rule.
   *
   * @return one-sentence description; never blank
   */
  public String description() {
    return description;
  }
}
