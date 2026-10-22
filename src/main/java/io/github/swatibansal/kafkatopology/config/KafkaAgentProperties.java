package io.github.swatibansal.kafkatopology.config;

import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Root configuration properties for the Kafka Topology Explorer.
 *
 * <p>All values are externalised via environment variables with safe defaults for local
 * development. Example {@code application.yml} structure:
 *
 * <pre>{@code
 * kafka-agent:
 *   bootstrap-servers: localhost:19092
 *   security:
 *     protocol: PLAINTEXT
 *   admin:
 *     request-timeout-ms: 30000
 *   cache:
 *     refresh-interval-ms: 86400000
 *   enrichment:
 *     active-profile: prod
 *     local-directory: /demo/apps
 * }</pre>
 *
 * @param bootstrapServers comma-separated list of Kafka broker addresses
 * @param security         authentication and transport security settings
 * @param admin            AdminClient timeout settings
 * @param cache            topology cache settings
 * @param discovery        topic discovery filter settings
 * @param enrichment       application enrichment settings shared by all sources
 * @param github           GitHub application source settings
 */
@ConfigurationProperties(prefix = "kafka-agent")
public record KafkaAgentProperties(
    String bootstrapServers,
    Security security,
    Admin admin,
    Cache cache,
    Discovery discovery,
    Enrichment enrichment,
    GitHub github) {

  /**
   * Kafka security / authentication settings.
   *
   * @param protocol       security protocol (e.g. {@code PLAINTEXT}, {@code SSL}, {@code SASL_SSL})
   * @param saslMechanism  SASL mechanism (e.g. {@code PLAIN}, {@code SCRAM-SHA-512}); blank for
   *                       non-SASL protocols
   * @param saslJaasConfig full JAAS config string; takes precedence over user name and password
   * @param saslUserName   SASL user name used to build a JAAS string when no explicit config is
   *                       given; supply via environment variable only
   * @param saslPassword   SASL password used together with {@code saslUserName}; supply via
   *                       environment variable only
   */
  public record Security(
      String protocol,
      String saslMechanism,
      String saslJaasConfig,
      String saslUserName,
      String saslPassword) {}

  /**
   * AdminClient timeout configuration.
   *
   * @param requestTimeoutMs    per-request timeout in milliseconds
   * @param defaultApiTimeoutMs overall API timeout in milliseconds
   */
  public record Admin(
      int requestTimeoutMs,
      int defaultApiTimeoutMs) {}

  /**
   * Topology cache settings.
   *
   * @param refreshIntervalMs delay between successive background cache refreshes in milliseconds
   */
  public record Cache(long refreshIntervalMs) {}

  /**
   * Topic discovery filter settings.
   *
   * <p>Filters are applied after the raw topic list is fetched from the broker and before any nodes
   * are added to the graph. Consumer groups are then matched only against the surviving topic set,
   * so groups whose subscribed topics are all filtered out will produce no edges.
   *
   * @param topicIncludePatterns list of case-insensitive substrings; a topic is included when its
   *     name contains <em>any</em> of these strings. An empty list disables pattern filtering.
   * @param excludeNumericTopics when {@code true} (default), topics whose names consist entirely of
   *     digits are excluded — these are typically auto-generated internal IDs.
   */
  public record Discovery(List<String> topicIncludePatterns, boolean excludeNumericTopics) {}

  /**
   * Application enrichment settings shared by every application source.
   *
   * @param activeProfile  Spring profile whose values are extracted from each
   *                       {@code application.yml} (e.g. {@code "prod"})
   * @param localDirectory directory containing one sub-directory per application, each holding an
   *                       {@code application.yml}; blank disables the local source
   */
  public record Enrichment(String activeProfile, String localDirectory) {}

  /**
   * GitHub application source settings.
   *
   * <p>When {@code enabled} is {@code true}, {@code application.yml} is fetched from each
   * configured repository via the GitHub Contents API. Takes precedence over the local directory
   * source.
   *
   * @param enabled whether the GitHub source is active
   * @param org     GitHub organisation or user name (e.g. {@code "acme-shop"})
   * @param token   GitHub token with read access to repository contents; provide via the
   *                {@code GITHUB_TOKEN} environment variable
   * @param repos   list of repository names to scan (without the org prefix)
   */
  public record GitHub(
      boolean enabled,
      String org,
      String token,
      List<String> repos) {}
}
