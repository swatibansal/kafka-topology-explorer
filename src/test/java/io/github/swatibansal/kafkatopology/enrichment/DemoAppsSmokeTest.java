package io.github.swatibansal.kafkatopology.enrichment;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.swatibansal.kafkatopology.enrichment.model.DiscoveredApplication;
import java.nio.file.Path;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/**
 * Guards the demo data under {@code demo/apps}: every service must yield the group ID and
 * produced topics the Acme Shop topology expects, for the {@code prod} profile.
 */
class DemoAppsSmokeTest {

  private static final Path DEMO_APPS =
      Path.of(System.getProperty("basedir", "."), "demo", "apps");

  @Test
  void demoApps_extractExpectedGroupIdsAndTopics() {
    var source = new LocalAppYmlSource(
        DEMO_APPS, new AppYmlApplicationParser(new SpringAppYmlGroupIdExtractor(), "prod"));

    Map<String, DiscoveredApplication> apps = source.fetchApplications().stream()
        .collect(Collectors.toMap(DiscoveredApplication::name, Function.identity()));

    assertThat(apps).hasSize(9);
    assertThat(apps.get("order-service").candidateGroupIds()).isEmpty();
    assertThat(apps.get("order-service").candidateTopicNames()).containsExactly(
        "orders.order.created", "orders.order.updated", "orders.order.cancelled");
    assertThat(apps.get("payment-service").candidateGroupIds())
        .containsExactly("payment-service-cg");
    assertThat(apps.get("payment-service").candidateTopicNames()).containsExactly(
        "payments.payment.requested", "payments.payment.captured", "payments.payment.failed");
    assertThat(apps.get("inventory-service").candidateGroupIds())
        .containsExactly("inventory-service-cg");
    assertThat(apps.get("inventory-service").candidateTopicNames())
        .containsExactly("inventory.stock.reserved", "inventory.stock.released");
    assertThat(apps.get("shipping-service").candidateGroupIds())
        .containsExactly("shipping-service-cg");
    assertThat(apps.get("shipping-service").candidateTopicNames()).containsExactly(
        "shipping.shipment.created", "shipping.shipment.delivered",
        "notifications.email.requested");
    assertThat(apps.get("notification-service").candidateGroupIds())
        .containsExactly("notification-service-cg");
    assertThat(apps.get("notification-service").candidateTopicNames()).containsExactly(
        "orders.order.created", "payments.payment.failed", "shipping.shipment.delivered",
        "notifications.email.requested");
    assertThat(apps.get("search-indexer").candidateGroupIds())
        .containsExactly("search-indexer-cg");
    assertThat(apps.get("search-indexer").candidateTopicNames())
        .containsExactly("search.product.indexed");
    assertThat(apps.get("catalog-service").candidateGroupIds()).isEmpty();
    assertThat(apps.get("catalog-service").candidateTopicNames())
        .containsExactly("catalog.product.updated");
    assertThat(apps.get("fraud-detection-service").candidateGroupIds())
        .containsExactly("fraud-detection-cg");
    assertThat(apps.get("fraud-detection-service").candidateTopicNames())
        .containsExactly("payments.payment.failed");
    assertThat(apps.get("order-analytics").candidateGroupIds())
        .containsExactly("order-analytics");
    assertThat(apps.get("order-analytics").candidateTopicNames())
        .containsExactly("analytics.order.totals");
  }
}
