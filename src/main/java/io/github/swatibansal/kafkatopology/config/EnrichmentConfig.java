package io.github.swatibansal.kafkatopology.config;

import io.github.swatibansal.kafkatopology.enrichment.AppYmlApplicationParser;
import io.github.swatibansal.kafkatopology.enrichment.ApplicationEnrichmentService;
import io.github.swatibansal.kafkatopology.enrichment.ApplicationSource;
import io.github.swatibansal.kafkatopology.enrichment.LocalAppYmlSource;
import io.github.swatibansal.kafkatopology.enrichment.SpringAppYmlGroupIdExtractor;
import io.github.swatibansal.kafkatopology.github.GitHubAppYmlClient;
import java.nio.file.Path;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Wires the application enrichment pipeline.
 *
 * <p>Source selection order:
 * <ol>
 *   <li>{@code kafka-agent.github.enabled=true} → {@link GitHubAppYmlClient}
 *   <li>{@code kafka-agent.enrichment.local-directory} set → {@link LocalAppYmlSource}
 *   <li>otherwise → a no-op source; {@link ApplicationEnrichmentService} returns an empty result
 *       without touching the graph
 * </ol>
 */
@Configuration
public class EnrichmentConfig {

  private static final Logger log = LoggerFactory.getLogger(EnrichmentConfig.class);
  private static final String DEFAULT_PROFILE = "prod";

  /**
   * Creates the {@link SpringAppYmlGroupIdExtractor} shared by all application sources.
   *
   * @return extractor instance
   */
  @Bean
  public SpringAppYmlGroupIdExtractor springAppYmlGroupIdExtractor() {
    return new SpringAppYmlGroupIdExtractor();
  }

  /**
   * Creates the parser shared by all application sources, bound to the active profile.
   *
   * @param props     application properties
   * @param extractor Spring application.yml extractor
   * @return parser instance
   */
  @Bean
  public AppYmlApplicationParser appYmlApplicationParser(
      KafkaAgentProperties props, SpringAppYmlGroupIdExtractor extractor) {
    return new AppYmlApplicationParser(extractor, activeProfile(props));
  }

  /**
   * Creates the {@link ApplicationSource} for the configured mode.
   *
   * @param props  application properties
   * @param parser shared application.yml parser
   * @return source instance; never {@code null}
   */
  @Bean
  public ApplicationSource applicationSource(
      KafkaAgentProperties props, AppYmlApplicationParser parser) {
    var github = props.github();
    String localDir = localDirectory(props);
    if (github != null && github.enabled()) {
      log.info("GitHub application.yml enrichment enabled — org: {}, repos: {}, profile: {}",
          github.org(), github.repos() == null ? 0 : github.repos().size(),
          parser.activeProfile());
      if (!localDir.isBlank()) {
        log.info("Ignoring local application directory '{}' because the GitHub source "
            + "takes precedence", localDir);
      }
      return new GitHubAppYmlClient(github, parser);
    }
    if (!localDir.isBlank()) {
      log.info("Local application.yml enrichment enabled — directory: {}, profile: {}",
          localDir, parser.activeProfile());
      return new LocalAppYmlSource(Path.of(localDir), parser);
    }
    log.info("Application enrichment is disabled "
        + "(set ENRICHMENT_LOCAL_DIR or GITHUB_ENRICHMENT_ENABLED=true to enable)");
    return List::of;
  }

  /**
   * Creates the {@link ApplicationEnrichmentService} with the resolved source and enabled flag.
   *
   * @param source application source bean
   * @param props  application properties
   * @return enrichment service
   */
  @Bean
  public ApplicationEnrichmentService applicationEnrichmentService(
      ApplicationSource source, KafkaAgentProperties props) {
    return new ApplicationEnrichmentService(source, enrichmentEnabled(props));
  }

  /** Single definition of "enrichment is on", shared by source selection and the service. */
  private static boolean enrichmentEnabled(KafkaAgentProperties props) {
    var github = props.github();
    return (github != null && github.enabled()) || !localDirectory(props).isBlank();
  }

  private static String activeProfile(KafkaAgentProperties props) {
    var enrichment = props.enrichment();
    if (enrichment == null || enrichment.activeProfile() == null
        || enrichment.activeProfile().isBlank()) {
      return DEFAULT_PROFILE;
    }
    return enrichment.activeProfile();
  }

  private static String localDirectory(KafkaAgentProperties props) {
    var enrichment = props.enrichment();
    if (enrichment == null || enrichment.localDirectory() == null) {
      return "";
    }
    return enrichment.localDirectory().trim();
  }
}
