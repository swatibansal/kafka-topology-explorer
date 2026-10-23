package io.github.swatibansal.kafkatopology.model;

import java.util.Objects;

/**
 * A node pruned from the topology graph, together with the rule that removed it.
 *
 * @param node the node as it was before pruning, including its metadata (for an
 *     {@link NodeType#APPLICATION} this holds the unmatched {@code candidateGroupIds} and
 *     {@code candidateTopicNames})
 * @param reason the pruning rule that matched
 * @param description human-readable explanation of {@code reason}
 */
public record OrphanNode(TopologyNode node, OrphanReason reason, String description) {

  /**
   * Validates that no component is {@code null}.
   */
  public OrphanNode {
    Objects.requireNonNull(node, "node");
    Objects.requireNonNull(reason, "reason");
    Objects.requireNonNull(description, "description");
  }

  /**
   * Creates an orphan entry whose description is taken from the reason.
   *
   * @param node pruned node
   * @param reason pruning rule that matched
   * @return new entry; never {@code null}
   */
  public static OrphanNode of(TopologyNode node, OrphanReason reason) {
    return new OrphanNode(node, reason, reason.description());
  }
}
