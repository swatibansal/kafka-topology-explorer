package io.github.swatibansal.kafkatopology.config;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.swatibansal.kafkatopology.config.KafkaAgentProperties.Enrichment;
import io.github.swatibansal.kafkatopology.config.KafkaAgentProperties.GitHub;
import io.github.swatibansal.kafkatopology.enrichment.AppYmlApplicationParser;
import io.github.swatibansal.kafkatopology.enrichment.ApplicationSource;
import io.github.swatibansal.kafkatopology.enrichment.LocalAppYmlSource;
import io.github.swatibansal.kafkatopology.enrichment.SpringAppYmlGroupIdExtractor;
import io.github.swatibansal.kafkatopology.github.GitHubAppYmlClient;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Unit tests for the source-selection logic in {@link EnrichmentConfig}. */
class EnrichmentConfigTest {

  private final EnrichmentConfig config = new EnrichmentConfig();
  private final SpringAppYmlGroupIdExtractor extractor = new SpringAppYmlGroupIdExtractor();
  private final AppYmlApplicationParser parser = new AppYmlApplicationParser(extractor, "prod");

  @Test
  void applicationSource_githubEnabled_returnsGitHubClient() {
    var props = props(
        new Enrichment("prod", "/demo/apps"),
        new GitHub(true, "acme-shop", "token", List.of("order-service")));

    ApplicationSource source = config.applicationSource(props, parser);

    assertThat(source).isInstanceOf(GitHubAppYmlClient.class);
  }

  @Test
  void applicationSource_localDirectorySet_returnsLocalSource() {
    var props = props(new Enrichment("prod", "/demo/apps"), new GitHub(false, "", "", List.of()));

    ApplicationSource source = config.applicationSource(props, parser);

    assertThat(source).isInstanceOf(LocalAppYmlSource.class);
  }

  @Test
  void applicationSource_nothingConfigured_returnsNoOpSource() {
    var props = props(new Enrichment("prod", "  "), new GitHub(false, "", "", List.of()));

    ApplicationSource source = config.applicationSource(props, parser);

    assertThat(source).isNotInstanceOfAny(GitHubAppYmlClient.class, LocalAppYmlSource.class);
    assertThat(source.fetchApplications()).isEmpty();
  }

  @Test
  void appYmlApplicationParser_nullEnrichment_fallsBackToProd() {
    var props = props(null, null);

    AppYmlApplicationParser created = config.appYmlApplicationParser(props, extractor);

    assertThat(created.activeProfile()).isEqualTo("prod");
  }

  private static KafkaAgentProperties props(Enrichment enrichment, GitHub github) {
    return new KafkaAgentProperties("localhost:9092", null, null, null, null, enrichment, github);
  }
}
