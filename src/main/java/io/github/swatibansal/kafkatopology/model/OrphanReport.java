package io.github.swatibansal.kafkatopology.model;

import java.time.Instant;
import java.util.List;

/**
 * The nodes pruned as orphans during one topology refresh.
 *
 * <p>Orphans are discovered on the broker or in service configuration but have no place in the
 * published graph: a consumer group that consumes nothing, a service whose configuration matches
 * no live Kafka node, or a retry / dead-letter topic nothing touches. They are usually the most
 * actionable output of a refresh (stale groups, decommissioned services, forgotten dead-letter
 * topics), so they are kept alongside the graph and exposed via {@code GET /api/topology/orphans}.
 *
 * @param refreshedAt when the refresh that produced this report completed; {@code null} before
 *     the first successful refresh
 * @param count number of orphans (equals {@code orphans.size()})
 * @param orphans pruned nodes in removal order, each with the rule that removed it
 */
public record OrphanReport(Instant refreshedAt, int count, List<OrphanNode> orphans) {

  /**
   * Defensively copies the orphan list.
   */
  public OrphanReport {
    orphans = orphans == null ? List.of() : List.copyOf(orphans);
  }

  /**
   * Creates a report for the given orphans, stamped with {@code refreshedAt}.
   *
   * @param refreshedAt completion time of the refresh
   * @param orphans pruned nodes
   * @return report with {@code count} derived from the list
   */
  public static OrphanReport of(Instant refreshedAt, List<OrphanNode> orphans) {
    List<OrphanNode> copy = orphans == null ? List.of() : List.copyOf(orphans);
    return new OrphanReport(refreshedAt, copy.size(), copy);
  }

  /**
   * Returns the report used before any refresh has completed.
   *
   * @return empty report with a {@code null} timestamp
   */
  public static OrphanReport empty() {
    return new OrphanReport(null, 0, List.of());
  }
}
