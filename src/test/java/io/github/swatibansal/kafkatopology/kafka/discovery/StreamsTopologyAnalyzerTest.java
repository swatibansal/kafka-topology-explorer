package io.github.swatibansal.kafkatopology.kafka.discovery;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link StreamsTopologyAnalyzer}. No Spring context required. */
class StreamsTopologyAnalyzerTest {

  private StreamsTopologyAnalyzer analyzer;

  @BeforeEach
  void setUp() {
    analyzer = new StreamsTopologyAnalyzer();
  }

  @Test
  void detectStreamsApps_identifiesAppByChangelogPattern() {
    var groups = Set.of("order-processor", "plain-consumer");
    var topics =
        Set.of(
            "orders.created",
            "order-processor-order-store-changelog",
            "order-processor-KSTREAM-REPARTITION-0000000001-repartition");

    assertThat(analyzer.detectStreamsApps(groups, topics))
        .containsExactly("order-processor")
        .doesNotContain("plain-consumer");
  }

  @Test
  void detectStreamsApps_identifiesMultipleStreamsApps() {
    var groups = Set.of("app-alpha", "app-beta", "plain-consumer");
    var topics =
        Set.of(
            "app-alpha-store-changelog",
            "app-beta-table-changelog",
            "some-user-topic");

    assertThat(analyzer.detectStreamsApps(groups, topics))
        .containsExactlyInAnyOrder("app-alpha", "app-beta")
        .doesNotContain("plain-consumer");
  }

  @Test
  void detectStreamsApps_returnsEmptyWhenNoStreamsApps() {
    var groups = Set.of("consumer-a", "consumer-b");
    var topics = Set.of("topic-x", "topic-y");

    assertThat(analyzer.detectStreamsApps(groups, topics)).isEmpty();
  }

  @Test
  void detectStreamsApps_returnsEmptyForEmptyInputs() {
    assertThat(analyzer.detectStreamsApps(Set.of(), Set.of())).isEmpty();
  }

  @Test
  void isStreamsInternalTopic_recognisesChangelog() {
    assertThat(analyzer.isStreamsInternalTopic("my-app-store-changelog", "my-app")).isTrue();
  }

  @Test
  void isStreamsInternalTopic_recognisesRepartition() {
    assertThat(
            analyzer.isStreamsInternalTopic(
                "my-app-KSTREAM-REPARTITION-0000000001-repartition", "my-app"))
        .isTrue();
  }

  @Test
  void isStreamsInternalTopic_recognisesKstreamInfix() {
    assertThat(
            analyzer.isStreamsInternalTopic("my-app-KSTREAM-JOINTHIS-0000000002-repartition",
                "my-app"))
        .isTrue();
  }

  @Test
  void isStreamsInternalTopic_returnsFalseForUserTopic() {
    assertThat(analyzer.isStreamsInternalTopic("orders.created", "my-app")).isFalse();
  }

  @Test
  void isStreamsInternalTopic_returnsFalseWhenPrefixBelongsToDifferentApp() {
    assertThat(
            analyzer.isStreamsInternalTopic("other-app-store-changelog", "my-app"))
        .isFalse();
  }

  @Test
  void isStreamsApp_returnsFalseWhenNoPrefixMatch() {
    var topics = Set.of("unrelated-topic-changelog");
    assertThat(analyzer.isStreamsApp("my-app", topics)).isFalse();
  }
}
