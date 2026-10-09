package io.github.swatibansal.kafkatopology.enrichment.model;

import java.util.List;

/**
 * Summary of a completed application enrichment pass.
 *
 * <p>Returned by
 * {@link io.github.swatibansal.kafkatopology.enrichment.ApplicationEnrichmentService#enrich} and
 * stored in {@link io.github.swatibansal.kafkatopology.service.TopologyCacheService} for the
 * {@code GET /api/topology/applications} endpoint.
 *
 * @param applicationsFound  total number of applications returned by the source
 * @param applicationsLinked number of applications that produced at least one DEPLOYS or PRODUCES
 *                           edge
 * @param applications       full list of discovered applications, stored as an immutable copy;
 *                           {@code null} is normalised to empty
 */
public record EnrichmentResult(
    int applicationsFound,
    int applicationsLinked,
    List<DiscoveredApplication> applications) {

  /** Defensively copies the application list, treating {@code null} as empty. */
  public EnrichmentResult {
    applications = applications == null ? List.of() : List.copyOf(applications);
  }

  /**
   * Returns an empty result representing a skipped or failed enrichment pass.
   *
   * @return result with zero counts and no applications
   */
  public static EnrichmentResult empty() {
    return new EnrichmentResult(0, 0, List.of());
  }
}
