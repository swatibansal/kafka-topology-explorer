package io.github.swatibansal.kafkatopology.enrichment;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link SpringAppYmlGroupIdExtractor}. No Spring context required.
 *
 * <p>Test coverage focuses on the gap scenarios identified during topology analysis:
 * <ul>
 *   <li>Spring Boot multi-document YAML with profile-scoped group IDs
 *   <li>Spring {@code ${KEY:defaultValue}} expression resolution (the main gap fix)
 *   <li>Placeholder filtering ({@code CHANGE_ME}, bare {@code ${KEY}})
 *   <li>Micronaut-style {@code kafka.consumers.{name}.group-id} keys
 *   <li>Profile fallback and selection priority
 * </ul>
 */
class SpringAppYmlGroupIdExtractorTest {

  private SpringAppYmlGroupIdExtractor extractor;

  @BeforeEach
  void setUp() {
    extractor = new SpringAppYmlGroupIdExtractor();
  }

  // ── Spring Boot profile document selection ───────────────────────────────────

  @Test
  void profileDoc_literalGroupId_isExtracted() {
    // Canonical Spring Boot pattern: placeholder in default doc, real value in profile doc
    String yaml = """
        external:
          kafka:
            order-created-group-id: "CHANGE_ME"
        ---
        spring:
          config:
            activate:
              on-profile: [qa]
        external:
          kafka:
            order-created-group-id: "qa-order-created-cg-v1"
        """;

    assertThat(extractor.extract(yaml, "qa"))
        .containsExactly("qa-order-created-cg-v1");
  }

  @Test
  void profileDoc_qaDocPreferredOverDefaultDoc_whenBothHaveGroupId() {
    // Profile doc value wins over default doc value
    String yaml = """
        external:
          kafka:
            orders-group-id: "default-group"
        ---
        spring:
          config:
            activate:
              on-profile: [qa]
        external:
          kafka:
            orders-group-id: "qa-orders-group"
        """;

    assertThat(extractor.extract(yaml, "qa"))
        .containsExactly("qa-orders-group");
  }

  @Test
  void profileDoc_nonMatchingProfileDocIgnored_defaultDocUsedAsFallback() {
    // Only a [prod] doc exists; no [qa] doc → fall back to default doc
    String yaml = """
        external:
          kafka:
            orders-group-id: "default-orders-group"
        ---
        spring:
          config:
            activate:
              on-profile: [prod]
        external:
          kafka:
            orders-group-id: "prod-orders-group"
        """;

    assertThat(extractor.extract(yaml, "qa"))
        .containsExactly("default-orders-group");
  }

  @Test
  void profileDoc_onlyMatchingProfileDocPresent_noDefaultDoc() {
    // No default doc; only a [qa] profile doc
    String yaml = """
        ---
        spring:
          config:
            activate:
              on-profile: [qa]
        external:
          kafka:
            payment-group-id: "qa-payments-cg-v1"
        """;

    assertThat(extractor.extract(yaml, "qa"))
        .containsExactly("qa-payments-cg-v1");
  }

  @Test
  void profileDoc_noDocMatchesActiveProfile_returnsEmpty() {
    // No default doc and no matching [qa] doc → nothing to extract
    String yaml = """
        spring:
          config:
            activate:
              on-profile: [prod]
        external:
          kafka:
            group-id: "prod-group"
        ---
        spring:
          config:
            activate:
              on-profile: [staging]
        external:
          kafka:
            group-id: "staging-group"
        """;

    assertThat(extractor.extract(yaml, "qa")).isEmpty();
  }

  @Test
  void profileDoc_multipleGroupIds_allExtracted() {
    // Service subscribing to several topics — each has its own consumer group
    String yaml = """
        ---
        spring:
          config:
            activate:
              on-profile: [qa]
        external:
          kafka:
            orders-order-group-id: "qa-order-cg-v1"
            orders-status-group-id: "qa-status-cg-v1"
            orders-payment-group-id: "qa-payment-cg-v1"
        """;

    assertThat(extractor.extract(yaml, "qa"))
        .containsExactlyInAnyOrder(
            "qa-order-cg-v1",
            "qa-status-cg-v1",
            "qa-payment-cg-v1");
  }

  @Test
  void profileDoc_onProfileAsString_isHandled() {
    // on-profile can be a scalar string, not just a list
    String yaml = """
        ---
        spring:
          config:
            activate:
              on-profile: qa
        external:
          kafka:
            shipping-group-id: "qa-shipping-cg-v1"
        """;

    assertThat(extractor.extract(yaml, "qa"))
        .containsExactly("qa-shipping-cg-v1");
  }

  // ── Spring expression resolution ${KEY:defaultValue} ────────────────────────
  // This is the primary gap fix: group IDs embedded as expression defaults were
  // previously discarded entirely.

  @Test
  void springExpression_withDefault_defaultValueIsExtracted() {
    // Group ID stored as default in a Spring expression — the most common gap pattern
    String yaml = """
        ---
        spring:
          config:
            activate:
              on-profile: [qa]
        external:
          kafka:
            orders-order-created-group-id: "${KAFKA_ORDER_CREATED_GROUP_ID:qa-order-created-cg-v1}"
        """;

    assertThat(extractor.extract(yaml, "qa"))
        .containsExactly("qa-order-created-cg-v1");
  }

  @Test
  void springExpression_withDefault_inDefaultDoc_defaultValueIsExtracted() {
    // Expression default in default doc (no profile doc present)
    String yaml = """
        external:
          kafka:
            payment-group-id: "${PAYMENT_GROUP_ID:qa-payments-cg-v2}"
        """;

    assertThat(extractor.extract(yaml, "qa"))
        .containsExactly("qa-payments-cg-v2");
  }

  @Test
  void springExpression_bareWithoutDefault_isSkipped() {
    // ${KEY} with no colon — no default available at parse time
    String yaml = """
        ---
        spring:
          config:
            activate:
              on-profile: [qa]
        external:
          kafka:
            orders-group-id: "${KAFKA_ORDERS_GROUP_ID}"
        """;

    assertThat(extractor.extract(yaml, "qa")).isEmpty();
  }

  @Test
  void springExpression_withBlankDefault_isSkipped() {
    // ${KEY:} — empty default resolves to blank → treated as placeholder
    String yaml = """
        ---
        spring:
          config:
            activate:
              on-profile: [qa]
        external:
          kafka:
            orders-group-id: "${KAFKA_ORDERS_GROUP_ID:}"
        """;

    assertThat(extractor.extract(yaml, "qa")).isEmpty();
  }

  @Test
  void springExpression_withChangeMeDefault_isSkipped() {
    // ${KEY:CHANGE_ME} — CHANGE_ME default is still a placeholder
    String yaml = """
        ---
        spring:
          config:
            activate:
              on-profile: [qa]
        external:
          kafka:
            orders-group-id: "${KAFKA_ORDERS_GROUP_ID:CHANGE_ME}"
        """;

    assertThat(extractor.extract(yaml, "qa")).isEmpty();
  }

  @Test
  void springExpression_multipleWithDefaults_allExtracted() {
    // Several group IDs expressed as Spring expressions — all defaults extracted
    String yaml = """
        ---
        spring:
          config:
            activate:
              on-profile: [qa]
        external:
          kafka:
            customer-signup-group-id: "${CUSTOMER_SIGNUP_GROUP:qa-customer-signup-cg-v1}"
            customer-profile-group-id: "${CUSTOMER_PROFILE_GROUP:qa-customer-profile-cg-v2}"
        """;

    assertThat(extractor.extract(yaml, "qa"))
        .containsExactlyInAnyOrder("qa-customer-signup-cg-v1", "qa-customer-profile-cg-v2");
  }

  // ── Placeholder filtering ─────────────────────────────────────────────────────

  @Test
  void placeholder_changeMeValue_isSkipped() {
    String yaml = """
        external:
          kafka:
            orders-group-id: "CHANGE_ME"
        """;

    assertThat(extractor.extract(yaml, "qa")).isEmpty();
  }

  @Test
  void placeholder_changeMeInDefaultDoc_profileDocValueUsed() {
    // CHANGE_ME in default doc is irrelevant — profile doc value wins
    String yaml = """
        external:
          kafka:
            orders-group-id: "CHANGE_ME"
        ---
        spring:
          config:
            activate:
              on-profile: [qa]
        external:
          kafka:
            orders-group-id: "qa-orders-cg-v1"
        """;

    assertThat(extractor.extract(yaml, "qa"))
        .containsExactly("qa-orders-cg-v1");
  }

  @Test
  void noKafkaGroupKeys_returnsEmpty() {
    // YAML has no keys matching "kafka" + "group"
    String yaml = """
        server:
          port: 8080
        spring:
          datasource:
            url: jdbc:postgresql://localhost/db
        external:
          kafka:
            bootstrap-servers: "localhost:9092"
        """;

    assertThat(extractor.extract(yaml, "qa")).isEmpty();
  }

  @Test
  void duplicateGroupIds_areDeduplicated() {
    // Same group ID appears under two different keys (e.g. inherited default + profile)
    String yaml = """
        ---
        spring:
          config:
            activate:
              on-profile: [qa]
        external:
          kafka:
            primary-group-id: "qa-shared-group"
            secondary-group-id: "qa-shared-group"
        """;

    assertThat(extractor.extract(yaml, "qa"))
        .containsExactly("qa-shared-group");
  }

  // ── Micronaut-style YAML (kafka.consumers.{name}.group-id) ───────────────────
  // Micronaut services don't use spring.config.activate.on-profile; they have a single
  // flat document. The kafka.consumers.*.group-id key pattern matches isKafkaGroupKey().

  @Test
  void micronaut_kafkaConsumersGroupId_isExtracted() {
    String yaml = """
        micronaut:
          application:
            name: my-consumer-service
        kafka:
          bootstrap:
            servers: "localhost:9092"
          consumers:
            default:
              group-id: "qa-my-service-group"
        """;

    assertThat(extractor.extract(yaml, "qa"))
        .containsExactly("qa-my-service-group");
  }

  @Test
  void micronaut_multipleConsumers_allGroupIdsExtracted() {
    String yaml = """
        kafka:
          consumers:
            order-consumer:
              group-id: "qa-order-consumer-group"
            payment-consumer:
              group-id: "qa-payment-consumer-group"
        """;

    assertThat(extractor.extract(yaml, "qa"))
        .containsExactlyInAnyOrder("qa-order-consumer-group", "qa-payment-consumer-group");
  }

  @Test
  void micronaut_consumerWithClientIdOnly_returnsEmpty() {
    // Micronaut services sometimes use client-id instead of group-id
    String yaml = """
        micronaut:
          application:
            name: shipping-event-consumer
        kafka:
          consumers:
            shipping-event:
              client-id: "shipping-event-client"
        """;

    assertThat(extractor.extract(yaml, "qa")).isEmpty();
  }

  // ── resolveValue helper ──────────────────────────────────────────────────────

  @Test
  void resolveValue_literalString_returnedAsIs() {
    assertThat(extractor.resolveValue("qa-my-group")).isEqualTo("qa-my-group");
  }

  @Test
  void resolveValue_expressionWithDefault_returnsDefault() {
    assertThat(extractor.resolveValue("${GROUP_ID:qa-my-group}")).isEqualTo("qa-my-group");
  }

  @Test
  void resolveValue_expressionWithoutDefault_returnsNull() {
    assertThat(extractor.resolveValue("${GROUP_ID}")).isNull();
  }

  @Test
  void resolveValue_expressionWithBlankDefault_returnsBlank() {
    assertThat(extractor.resolveValue("${GROUP_ID:}")).isEqualTo("");
  }

  @Test
  void resolveValue_null_returnsNull() {
    assertThat(extractor.resolveValue(null)).isNull();
  }

  @Test
  void resolveValue_whitespaceAroundDefault_isTrimmed() {
    assertThat(extractor.resolveValue("${GROUP_ID:  qa-my-group  }")).isEqualTo("qa-my-group");
  }

  // ── Edge cases ───────────────────────────────────────────────────────────────

  @Test
  void nullInput_returnsEmpty() {
    assertThat(extractor.extract(null, "qa")).isEmpty();
  }

  @Test
  void blankInput_returnsEmpty() {
    assertThat(extractor.extract("   ", "qa")).isEmpty();
  }

  @Test
  void malformedYaml_returnsEmptyWithoutException() {
    assertThat(extractor.extract("{ not valid yaml: [", "qa")).isEmpty();
  }

  @Test
  void emptyYamlDocument_returnsEmpty() {
    // Single document that is just "---"
    assertThat(extractor.extract("---", "qa")).isEmpty();
  }

  @Test
  void yamlWithOnlyNullValues_returnsEmpty() {
    String yaml = """
        external:
          kafka:
            orders-group-id:
        """;

    assertThat(extractor.extract(yaml, "qa")).isEmpty();
  }

  // ── Regression: literal group-id in the profile document ─────────────────────
  // A placeholder in the default doc is replaced by "qa-status-group-v1" under profile qa.

  @Test
  void gapScenario_ordersStatusConsumer_groupIdExtracted() {
    String yaml = """
        kafka:
          bootstrap-servers: "localhost:9092"
        external:
          kafka:
            orders-status-group-id: "CHANGE_ME"
        ---
        spring:
          config:
            activate:
              on-profile: [qa]
        external:
          kafka:
            orders-status-group-id: "qa-status-group-v1"
        """;

    assertThat(extractor.extract(yaml, "qa"))
        .containsExactly("qa-status-group-v1");
  }

  // ── Regression: group id as a Spring expression default ──────────────────────
  // A service using ${ENV_VAR:qa-default-group} in its profile doc yields the default.

  @Test
  void gapScenario_expressionDefaultGroupId_nowExtracted() {
    String yaml = """
        external:
          kafka:
            order-audit-group-id: "CHANGE_ME"
        ---
        spring:
          config:
            activate:
              on-profile: [qa]
        external:
          kafka:
            order-audit-group-id: "${ORDER_AUDIT_GROUP:qa-order-audit-cg-v6}"
        """;

    assertThat(extractor.extract(yaml, "qa"))
        .containsExactly("qa-order-audit-cg-v6");
  }

  // ── Hierarchical group-id-base pattern ───────────────────────────────────────
  // group-id-base defined in the default doc; group-id in the profile doc references it via
  // "prefix-${some.nested.group-id-base}", a cross-document property reference with no
  // colon default.

  @Test
  void hierarchicalBase_groupIdResolvedFromDefaultDoc() {
    // Two consumers that share the base-key convention
    String yaml = """
        shop:
          kafka:
            consumer:
              reindex:
                group-id-base: "catalog-reindex-group-v1"
              delta:
                group-id-base: "catalog-delta-group-v1"
        ---
        spring:
          config:
            activate:
              on-profile: [qa]
        shop:
          kafka:
            consumer:
              reindex:
                group-id: "qa-${shop.kafka.consumer.reindex.group-id-base}"
              delta:
                group-id: "qa-${shop.kafka.consumer.delta.group-id-base}"
        """;

    assertThat(extractor.extract(yaml, "qa"))
        .containsExactlyInAnyOrder(
            "qa-catalog-reindex-group-v1",
            "qa-catalog-delta-group-v1");
  }

  @Test
  void hierarchicalBase_profileNotPresent_baseValuesFromDefaultDoc() {
    // No qa profile doc — fall back to default doc which only has group-id-base keys.
    // group-id-base values are themselves valid group IDs and should be returned.
    String yaml = """
        shop:
          kafka:
            consumer:
              my-consumer:
                group-id-base: "catalog-my-consumer-group-v1"
        ---
        spring:
          config:
            activate:
              on-profile: [prod]
        shop:
          kafka:
            consumer:
              my-consumer:
                group-id: "prod-${shop.kafka.consumer.my-consumer.group-id-base}"
        """;

    // Falls back to default doc; group-id-base value is a valid (non-placeholder) group ID
    assertThat(extractor.extract(yaml, "qa"))
        .containsExactly("catalog-my-consumer-group-v1");
  }

  @Test
  void hierarchicalBase_mixedLiteralAndReference_bothExtracted() {
    // One group-id is a literal, another uses a cross-doc reference
    String yaml = """
        shop:
          kafka:
            consumer:
              reindex:
                group-id-base: "catalog-reindex-group-v2"
        ---
        spring:
          config:
            activate:
              on-profile: [qa]
        shop:
          kafka:
            consumer:
              reindex:
                group-id: "qa-${shop.kafka.consumer.reindex.group-id-base}"
        external:
          kafka:
            other-group-id: "qa-other-cg-v1"
        """;

    assertThat(extractor.extract(yaml, "qa"))
        .containsExactlyInAnyOrder(
            "qa-catalog-reindex-group-v2",
            "qa-other-cg-v1");
  }

  // ── resolveWithContext ───────────────────────────────────────────────────────

  @Test
  void resolveWithContext_literalString_returnedAsIs() {
    assertThat(extractor.resolveWithContext("qa-my-group", Map.of()))
        .isEqualTo("qa-my-group");
  }

  @Test
  void resolveWithContext_bareRefInContext_resolved() {
    assertThat(extractor.resolveWithContext(
            "${some.key}", Map.of("some.key", "resolved-value")))
        .isEqualTo("resolved-value");
  }

  @Test
  void resolveWithContext_mixedPrefixAndRef_resolved() {
    assertThat(extractor.resolveWithContext(
            "qa-${shop.kafka.consumer.catalog.group-id-base}",
            Map.of("shop.kafka.consumer.catalog.group-id-base", "catalog-group-v1")))
        .isEqualTo("qa-catalog-group-v1");
  }

  @Test
  void resolveWithContext_refNotInContextFallsBackToDefault() {
    assertThat(extractor.resolveWithContext("${MISSING_KEY:fallback-value}", Map.of()))
        .isEqualTo("fallback-value");
  }

  @Test
  void resolveWithContext_refNotInContextNoDefault_returnsNull() {
    assertThat(extractor.resolveWithContext("${MISSING_KEY}", Map.of())).isNull();
  }

  @Test
  void resolveWithContext_contextPrecedesDefault() {
    // Context value takes priority over the expression's colon default
    assertThat(extractor.resolveWithContext(
            "${KEY:literal-default}",
            Map.of("KEY", "context-value")))
        .isEqualTo("context-value");
  }

  // ── Multi-listener consumer with several group ids ───────────────────────────

  @Test
  void multiListenerConsumer_allGroupIdsExtracted() {
    String yaml = """
        external:
          kafka:
            orders-order-group-id: "CHANGE_ME"
            orders-status-group-id: "CHANGE_ME"
            orders-customer-status-group-id: "CHANGE_ME"
            orders-payment-group-id: "CHANGE_ME"
            orders-fulfillment-group-id: "CHANGE_ME"
        ---
        spring:
          config:
            activate:
              on-profile: [qa]
        external:
          kafka:
            orders-order-group-id: "qa-order-cg-v1"
            orders-status-group-id: "qa-status-cg-v1"
            orders-customer-status-group-id: "qa-customer-status-cg-v1"
            orders-payment-group-id: "qa-payment-cg-v1"
            orders-fulfillment-group-id: "qa-fulfillment-cg-v1"
        """;

    assertThat(extractor.extract(yaml, "qa"))
        .containsExactlyInAnyOrder(
            "qa-order-cg-v1",
            "qa-status-cg-v1",
            "qa-customer-status-cg-v1",
            "qa-payment-cg-v1",
            "qa-fulfillment-cg-v1");
  }

  // ── extractTopics: Kafka producer topic name extraction ──────────────────────

  @Test
  void extractTopics_literalTopicName_isExtracted() {
    String yaml = """
        external:
          kafka:
            order-output-topic: "qa-orders-v1"
        """;

    assertThat(extractor.extractTopics(yaml, "qa"))
        .containsExactly("qa-orders-v1");
  }

  @Test
  void extractTopics_profileDocPreferredOverDefaultDoc() {
    String yaml = """
        external:
          kafka:
            order-output-topic: "CHANGE_ME"
        ---
        spring:
          config:
            activate:
              on-profile: [qa]
        external:
          kafka:
            order-output-topic: "qa-orders-v1"
        """;

    assertThat(extractor.extractTopics(yaml, "qa"))
        .containsExactly("qa-orders-v1");
  }

  @Test
  void extractTopics_springExpressionWithDefault_defaultValueExtracted() {
    String yaml = """
        ---
        spring:
          config:
            activate:
              on-profile: [qa]
        external:
          kafka:
            report-topic: "${REPORT_TOPIC:qa-reports-v1}"
        """;

    assertThat(extractor.extractTopics(yaml, "qa"))
        .containsExactly("qa-reports-v1");
  }

  @Test
  void extractTopics_multipleTopics_allExtracted() {
    String yaml = """
        ---
        spring:
          config:
            activate:
              on-profile: [qa]
        external:
          kafka:
            order-output-topic: "qa-orders-v1"
            payment-output-topic: "qa-payments-v1"
        """;

    assertThat(extractor.extractTopics(yaml, "qa"))
        .containsExactlyInAnyOrder("qa-orders-v1", "qa-payments-v1");
  }

  @Test
  void extractTopics_changeMePlaceholder_isSkipped() {
    String yaml = """
        external:
          kafka:
            order-output-topic: "CHANGE_ME"
        """;

    assertThat(extractor.extractTopics(yaml, "qa")).isEmpty();
  }

  @Test
  void extractTopics_noKafkaTopicKeys_returnsEmpty() {
    String yaml = """
        external:
          kafka:
            order-group-id: "qa-orders-cg-v1"
        """;

    assertThat(extractor.extractTopics(yaml, "qa")).isEmpty();
  }

  @Test
  void extractTopics_nullInput_returnsEmpty() {
    assertThat(extractor.extractTopics(null, "qa")).isEmpty();
  }

  @Test
  void extractTopics_groupKeyDoesNotMatchTopicKey() {
    // group keys should NOT be returned by extractTopics
    String yaml = """
        external:
          kafka:
            orders-group-id: "qa-orders-cg-v1"
            orders-topic: "qa-orders-events-v1"
        """;

    assertThat(extractor.extractTopics(yaml, "qa"))
        .containsExactly("qa-orders-events-v1");
    assertThat(extractor.extract(yaml, "qa"))
        .containsExactly("qa-orders-cg-v1");
  }

  // ── Kafka Streams application-id (equals the consumer group id) ──────────────

  @Test
  void streamsApplicationId_isExtractedAsGroupId() {
    String yaml = """
        spring:
          kafka:
            bootstrap-servers: "localhost:9092"
            streams:
              application-id: "${STREAMS_APPLICATION_ID:order-analytics}"
        """;

    assertThat(extractor.extract(yaml, "prod")).containsExactly("order-analytics");
    assertThat(extractor.extractTopics(yaml, "prod")).isEmpty();
  }

  @Test
  void streamsApplicationId_otherKafkaApplicationKeys_notTreatedAsGroup() {
    // spring.application.name has no "kafka"; spring.kafka.client-id has no "application.id"
    String yaml = """
        spring:
          application:
            name: order-analytics
          kafka:
            client-id: "order-analytics-client"
        """;

    assertThat(extractor.extract(yaml, "prod")).isEmpty();
  }

  @Test
  void streamsApplicationId_camelCaseKey_isExtracted() {
    String yaml = """
        spring:
          kafka:
            streams:
              applicationId: order-analytics
        """;

    assertThat(extractor.extract(yaml, "prod")).containsExactly("order-analytics");
  }
}
