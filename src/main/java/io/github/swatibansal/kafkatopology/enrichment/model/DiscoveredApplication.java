package io.github.swatibansal.kafkatopology.enrichment.model;

import java.util.List;

/**
 * A microservice discovered from configuration, together with the Kafka consumer group IDs and
 * producer topic names extracted from that configuration.
 *
 * <p>{@code candidateGroupIds} holds every value that looks like a consumer group ID. The
 * {@link io.github.swatibansal.kafkatopology.enrichment.ApplicationEnrichmentService} decides
 * which of these match live Kafka nodes and creates
 * {@link io.github.swatibansal.kafkatopology.model.EdgeType#DEPLOYS} edges accordingly.
 *
 * <p>{@code candidateTopicNames} holds every value that looks like a Kafka topic name. Names that
 * match live {@link io.github.swatibansal.kafkatopology.model.NodeType#TOPIC} nodes and are not
 * consumed by the application's own groups become
 * {@link io.github.swatibansal.kafkatopology.model.EdgeType#PRODUCES} edges.
 *
 * <p>Both candidate lists are stored as immutable copies; a {@code null} list or namespace is
 * normalised to empty.
 *
 * @param name                application name (repository or directory name)
 * @param namespace           grouping label such as a GitHub organisation; empty when unknown
 * @param candidateGroupIds   consumer group ID values extracted from configuration (immutable)
 * @param candidateTopicNames topic name values extracted from configuration (immutable)
 */
public record DiscoveredApplication(
    String name,
    String namespace,
    List<String> candidateGroupIds,
    List<String> candidateTopicNames) {

  /** Normalises {@code null} inputs and defensively copies the candidate lists. */
  public DiscoveredApplication {
    namespace = namespace == null ? "" : namespace;
    candidateGroupIds = candidateGroupIds == null ? List.of() : List.copyOf(candidateGroupIds);
    candidateTopicNames =
        candidateTopicNames == null ? List.of() : List.copyOf(candidateTopicNames);
  }

  /**
   * Creates an application with no extracted candidates.
   *
   * @param name      application name
   * @param namespace grouping label; may be empty
   * @return application with empty candidate lists
   */
  public static DiscoveredApplication empty(String name, String namespace) {
    return new DiscoveredApplication(name, namespace, List.of(), List.of());
  }
}
