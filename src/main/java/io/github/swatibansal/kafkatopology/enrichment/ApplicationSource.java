package io.github.swatibansal.kafkatopology.enrichment;

import io.github.swatibansal.kafkatopology.enrichment.model.DiscoveredApplication;
import java.util.List;

/**
 * Supplies the list of applications (microservices) whose configuration should be overlaid onto
 * the Kafka topology graph.
 *
 * <p>Implementations read configuration from somewhere outside the broker (a GitHub organisation,
 * a local directory of {@code application.yml} files) and never throw: failures are logged and an
 * empty list is returned.
 */
@FunctionalInterface
public interface ApplicationSource {

  /**
   * Fetches every application this source knows about.
   *
   * @return applications with their candidate consumer-group IDs and topic names; never
   *     {@code null}
   */
  List<DiscoveredApplication> fetchApplications();
}
