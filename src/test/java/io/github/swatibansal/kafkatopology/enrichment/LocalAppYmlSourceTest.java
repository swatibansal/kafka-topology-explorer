package io.github.swatibansal.kafkatopology.enrichment;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.swatibansal.kafkatopology.enrichment.model.DiscoveredApplication;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Unit tests for {@link LocalAppYmlSource} using a temporary directory. */
class LocalAppYmlSourceTest {

  @TempDir
  Path root;

  private LocalAppYmlSource source;

  @BeforeEach
  void setUp() {
    var parser = new AppYmlApplicationParser(new SpringAppYmlGroupIdExtractor(), "prod");
    source = new LocalAppYmlSource(root, parser);
  }

  @Test
  void fetchApplications_rootMissing_returnsEmpty() {
    var missing = new LocalAppYmlSource(root.resolve("does-not-exist"),
        new AppYmlApplicationParser(new SpringAppYmlGroupIdExtractor(), "prod"));

    assertThat(missing.fetchApplications()).isEmpty();
  }

  @Test
  void fetchApplications_emptyRoot_returnsEmpty() {
    assertThat(source.fetchApplications()).isEmpty();
  }

  @Test
  void fetchApplications_flatLayout_readsApplicationYml() throws IOException {
    writeFile("shipping-service/application.yml", """
        spring:
          kafka:
            consumer:
              group-id: shipping-service-cg
        app:
          kafka:
            topics:
              created: shipping.shipment.created
        """);

    List<DiscoveredApplication> apps = source.fetchApplications();

    assertThat(apps).hasSize(1);
    assertThat(apps.getFirst().name()).isEqualTo("shipping-service");
    assertThat(apps.getFirst().namespace()).isEmpty();
    assertThat(apps.getFirst().candidateGroupIds()).containsExactly("shipping-service-cg");
    assertThat(apps.getFirst().candidateTopicNames())
        .containsExactly("shipping.shipment.created");
  }

  @Test
  void fetchApplications_mavenLayout_readsNestedApplicationYml() throws IOException {
    writeFile("search-indexer/src/main/resources/application.yml", """
        spring:
          kafka:
            consumer:
              group-id: search-indexer-cg
        """);

    List<DiscoveredApplication> apps = source.fetchApplications();

    assertThat(apps).hasSize(1);
    assertThat(apps.getFirst().candidateGroupIds()).containsExactly("search-indexer-cg");
  }

  @Test
  void fetchApplications_profileFile_isMergedWithBase() throws IOException {
    writeFile("payment-service/application.yml", """
        app:
          kafka:
            topics:
              captured: payments.payment.captured
        """);
    writeFile("payment-service/application-prod.yml", """
        spring:
          kafka:
            consumer:
              group-id: payment-service-cg
        """);

    List<DiscoveredApplication> apps = source.fetchApplications();

    assertThat(apps).hasSize(1);
    assertThat(apps.getFirst().candidateGroupIds()).containsExactly("payment-service-cg");
    assertThat(apps.getFirst().candidateTopicNames())
        .containsExactly("payments.payment.captured");
  }

  @Test
  void fetchApplications_directoryWithoutYml_yieldsEmptyCandidates() throws IOException {
    Files.createDirectories(root.resolve("gateway"));
    writeFile("gateway/README.md", "# gateway\n");

    List<DiscoveredApplication> apps = source.fetchApplications();

    assertThat(apps).hasSize(1);
    assertThat(apps.getFirst().name()).isEqualTo("gateway");
    assertThat(apps.getFirst().candidateGroupIds()).isEmpty();
    assertThat(apps.getFirst().candidateTopicNames()).isEmpty();
  }

  @Test
  void fetchApplications_ignoresPlainFilesInRoot_andSortsByName() throws IOException {
    writeFile("README.md", "# demo apps\n");
    writeFile("order-service/application.yml",
        "app:\n  kafka:\n    topics:\n      a: orders.order.created\n");
    writeFile("catalog-service/application.yml",
        "app:\n  kafka:\n    topics:\n      a: catalog.product.updated\n");

    List<DiscoveredApplication> apps = source.fetchApplications();

    assertThat(apps).extracting(DiscoveredApplication::name)
        .containsExactly("catalog-service", "order-service");
  }

  @Test
  void fetchApplications_hiddenDirectory_isIgnored() throws IOException {
    writeFile(".git/HEAD", "ref: refs/heads/main\n");
    writeFile("order-service/application.yml",
        "app:\n  kafka:\n    topics:\n      a: orders.order.created\n");

    List<DiscoveredApplication> apps = source.fetchApplications();

    assertThat(apps).extracting(DiscoveredApplication::name).containsExactly("order-service");
  }

  private void writeFile(String relative, String content) throws IOException {
    Path file = root.resolve(relative);
    Files.createDirectories(file.getParent());
    Files.writeString(file, content);
  }
}
