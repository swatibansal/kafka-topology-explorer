package io.github.swatibansal.kafkatopology.enrichment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.swatibansal.kafkatopology.enrichment.model.DiscoveredApplication;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link AppYmlApplicationParser}. No Spring context required. */
class AppYmlApplicationParserTest {

  private static final String BASE_YML = """
      spring:
        kafka:
          consumer:
            group-id: payment-service-cg
      app:
        kafka:
          topics:
            captured: payments.payment.captured
      """;

  private static final String PROFILE_YML = """
      spring:
        kafka:
          consumer:
            group-id: payment-service-cg
      app:
        kafka:
          topics:
            failed: payments.payment.failed
      """;

  private static final String DEFAULT_DOC = """
      spring:
        kafka:
          consumer:
            group-id: default-cg
      """;

  private static final String PROD_DOC = """
      ---
      spring:
        config:
          activate:
            on-profile: [prod]
        kafka:
          consumer:
            group-id: prod-cg
      """;

  private static final String QA_DOC = """
      ---
      spring:
        config:
          activate:
            on-profile: [qa]
        kafka:
          consumer:
            group-id: qa-cg
      """;

  private AppYmlApplicationParser parser;

  @BeforeEach
  void setUp() {
    parser = new AppYmlApplicationParser(new SpringAppYmlGroupIdExtractor(), "prod");
  }

  @Test
  void activeProfile_isExposed() {
    assertThat(parser.activeProfile()).isEqualTo("prod");
  }

  @Test
  void constructor_blankProfile_throws() {
    assertThatThrownBy(() -> new AppYmlApplicationParser(new SpringAppYmlGroupIdExtractor(), " "))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("activeProfile must not be blank");
  }

  @Test
  void parse_multiDocumentBase_usesActiveProfileDocument() {
    DiscoveredApplication app =
        parser.parse("payment-service", "", DEFAULT_DOC + PROD_DOC + QA_DOC, null);

    assertThat(app.candidateGroupIds()).containsExactly("prod-cg");
  }

  @Test
  void parse_multiDocumentBase_fallsBackToDefaultDocWhenProfileAbsent() {
    DiscoveredApplication app = parser.parse("payment-service", "", DEFAULT_DOC + QA_DOC, null);

    assertThat(app.candidateGroupIds()).containsExactly("default-cg");
  }

  @Test
  void parse_bothNull_returnsEmptyCandidates() {
    DiscoveredApplication app = parser.parse("payment-service", "acme", null, null);

    assertThat(app.name()).isEqualTo("payment-service");
    assertThat(app.namespace()).isEqualTo("acme");
    assertThat(app.candidateGroupIds()).isEmpty();
    assertThat(app.candidateTopicNames()).isEmpty();
  }

  @Test
  void parse_baseOnly_extractsGroupsPlusTopics() {
    DiscoveredApplication app = parser.parse("payment-service", "", BASE_YML, null);

    assertThat(app.candidateGroupIds()).containsExactly("payment-service-cg");
    assertThat(app.candidateTopicNames()).containsExactly("payments.payment.captured");
  }

  @Test
  void parse_profileOnly_extractsFromProfileFile() {
    DiscoveredApplication app = parser.parse("payment-service", "", null, PROFILE_YML);

    assertThat(app.candidateGroupIds()).containsExactly("payment-service-cg");
    assertThat(app.candidateTopicNames()).containsExactly("payments.payment.failed");
  }

  @Test
  void parse_baseAndProfile_mergesAndDeduplicates() {
    DiscoveredApplication app = parser.parse("payment-service", "", BASE_YML, PROFILE_YML);

    assertThat(app.candidateGroupIds()).containsExactly("payment-service-cg");
    assertThat(app.candidateTopicNames())
        .containsExactly("payments.payment.captured", "payments.payment.failed");
  }
}
