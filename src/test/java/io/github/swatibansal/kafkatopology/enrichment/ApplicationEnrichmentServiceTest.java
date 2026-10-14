package io.github.swatibansal.kafkatopology.enrichment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import io.github.swatibansal.kafkatopology.enrichment.model.DiscoveredApplication;
import io.github.swatibansal.kafkatopology.enrichment.model.EnrichmentResult;
import io.github.swatibansal.kafkatopology.model.EdgeType;
import io.github.swatibansal.kafkatopology.model.NodeType;
import io.github.swatibansal.kafkatopology.model.TopologyEdge;
import io.github.swatibansal.kafkatopology.model.TopologyGraph;
import io.github.swatibansal.kafkatopology.model.TopologyNode;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/** Unit tests for {@link ApplicationEnrichmentService}. No Spring context required. */
@ExtendWith(MockitoExtension.class)
class ApplicationEnrichmentServiceTest {

  @Mock
  private ApplicationSource source;

  @Test
  void enrich_whenDisabled_returnsEmptyResultAndNeverCallsSource() {
    var service = new ApplicationEnrichmentService(source, false);
    var graph = new TopologyGraph();

    EnrichmentResult result = service.enrich(graph);

    assertThat(result.applicationsFound()).isEqualTo(0);
    assertThat(result.applicationsLinked()).isEqualTo(0);
    assertThat(graph.nodeCount()).isEqualTo(0);
    verify(source, never()).fetchApplications();
  }

  @Test
  void enrich_candidateMatchesConsumerGroup_addsApplicationNodeAndDeploysEdge() {
    var service = new ApplicationEnrichmentService(source, true);
    var graph = graphWithConsumerGroup("payment-service-cg");
    given(source.fetchApplications())
        .willReturn(List.of(app("payment-service", List.of("payment-service-cg"))));

    EnrichmentResult result = service.enrich(graph);

    assertThat(result.applicationsFound()).isEqualTo(1);
    assertThat(result.applicationsLinked()).isEqualTo(1);
    var appNode = graph.getNodes().get(TopologyNode.applicationId("payment-service"));
    assertThat(appNode).isNotNull();
    assertThat(appNode.type()).isEqualTo(NodeType.APPLICATION);
    assertThat(graph.getEdges()).hasSize(1);
    var edge = graph.getEdges().getFirst();
    assertThat(edge.type()).isEqualTo(EdgeType.DEPLOYS);
    assertThat(edge.sourceId()).isEqualTo(TopologyNode.applicationId("payment-service"));
    assertThat(edge.targetId()).isEqualTo(TopologyNode.groupNodeId("payment-service-cg"));
  }

  @Test
  void enrich_candidateMatchesStreamsApp_addsDeploysEdge() {
    var service = new ApplicationEnrichmentService(source, true);
    var graph = new TopologyGraph();
    graph.addNode(TopologyNode.forStreamsApp("order-analytics"));
    given(source.fetchApplications())
        .willReturn(List.of(app("order-analytics", List.of("order-analytics"))));

    service.enrich(graph);

    var edge = graph.getEdges().getFirst();
    assertThat(edge.type()).isEqualTo(EdgeType.DEPLOYS);
    assertThat(edge.targetId()).isEqualTo(TopologyNode.streamsId("order-analytics"));
  }

  @Test
  void enrich_unmatchedCandidate_applicationNodeAddedWithCandidateMetadata() {
    var service = new ApplicationEnrichmentService(source, true);
    var graph = new TopologyGraph();
    given(source.fetchApplications())
        .willReturn(List.of(app("legacy-service", List.of("unknown-group"))));

    service.enrich(graph);

    var appNode = graph.getNodes().get(TopologyNode.applicationId("legacy-service"));
    assertThat(appNode).isNotNull();
    assertThat(appNode.metadata()).containsEntry("candidateGroupIds", "unknown-group");
    assertThat(graph.getEdges()).isEmpty();
  }

  @Test
  void enrich_noCandidates_applicationNodeStillAdded() {
    var service = new ApplicationEnrichmentService(source, true);
    var graph = new TopologyGraph();
    given(source.fetchApplications()).willReturn(List.of(app("gateway", List.of())));

    EnrichmentResult result = service.enrich(graph);

    assertThat(result.applicationsFound()).isEqualTo(1);
    assertThat(result.applicationsLinked()).isEqualTo(0);
    assertThat(graph.getNodes()).containsKey(TopologyNode.applicationId("gateway"));
    assertThat(graph.getEdges()).isEmpty();
  }

  @Test
  void enrich_multipleApps_allProcessed() {
    var graph = new TopologyGraph();
    graph.addNode(TopologyNode.forConsumerGroup("shipping-service-cg"));
    graph.addNode(TopologyNode.forConsumerGroup("search-indexer-cg"));
    given(source.fetchApplications()).willReturn(List.of(
        app("shipping-service", List.of("shipping-service-cg")),
        app("search-indexer", List.of("search-indexer-cg")),
        app("reporting", List.of("group-unknown"))));
    var service = new ApplicationEnrichmentService(source, true);

    EnrichmentResult result = service.enrich(graph);

    assertThat(result.applicationsFound()).isEqualTo(3);
    assertThat(result.applicationsLinked()).isEqualTo(2);
    assertThat(graph.getEdges()).hasSize(2);
  }

  @Test
  void enrich_pureProducerApp_matchesTopic_addsProducesEdge() {
    var service = new ApplicationEnrichmentService(source, true);
    var graph = new TopologyGraph();
    graph.addNode(TopologyNode.forTopic("orders.order.created"));
    given(source.fetchApplications())
        .willReturn(List.of(producerApp("order-service", List.of("orders.order.created"))));

    EnrichmentResult result = service.enrich(graph);

    assertThat(result.applicationsLinked()).isEqualTo(1);
    assertThat(graph.getEdges()).hasSize(1);
    var edge = graph.getEdges().getFirst();
    assertThat(edge.type()).isEqualTo(EdgeType.PRODUCES);
    assertThat(edge.sourceId()).isEqualTo(TopologyNode.applicationId("order-service"));
    assertThat(edge.targetId()).isEqualTo(TopologyNode.topicId("orders.order.created"));
  }

  @Test
  void enrich_appWithGroupIds_topicConsumedByOwnGroup_noProducesEdge() {
    var graph = new TopologyGraph();
    graph.addNode(TopologyNode.forConsumerGroup("notification-service-cg"));
    graph.addNode(TopologyNode.forTopic("orders.order.created"));
    graph.addEdge(TopologyEdge.of(
        TopologyNode.topicId("orders.order.created"),
        TopologyNode.groupNodeId("notification-service-cg"),
        EdgeType.CONSUMES,
        "orders.order.created → notification-service-cg"));
    given(source.fetchApplications()).willReturn(List.of(
        new DiscoveredApplication("notification-service", "",
            List.of("notification-service-cg"), List.of("orders.order.created"))));
    var service = new ApplicationEnrichmentService(source, true);

    service.enrich(graph);

    var deploys = graph.getEdges().stream().filter(e -> e.type() == EdgeType.DEPLOYS).toList();
    var produces = graph.getEdges().stream().filter(e -> e.type() == EdgeType.PRODUCES).toList();
    assertThat(deploys).hasSize(1);
    assertThat(produces).isEmpty();
  }

  @Test
  void enrich_appWithGroupIds_andSeparateProducedTopic_addsDeploysAndProducesEdges() {
    var graph = new TopologyGraph();
    graph.addNode(TopologyNode.forConsumerGroup("payment-service-cg"));
    graph.addNode(TopologyNode.forTopic("orders.order.created"));
    graph.addNode(TopologyNode.forTopic("payments.payment.captured"));
    graph.addEdge(TopologyEdge.of(
        TopologyNode.topicId("orders.order.created"),
        TopologyNode.groupNodeId("payment-service-cg"),
        EdgeType.CONSUMES,
        "orders.order.created → payment-service-cg"));
    given(source.fetchApplications()).willReturn(List.of(
        new DiscoveredApplication("payment-service", "", List.of("payment-service-cg"),
            List.of("orders.order.created", "payments.payment.captured"))));
    var service = new ApplicationEnrichmentService(source, true);

    service.enrich(graph);

    var deploys = graph.getEdges().stream().filter(e -> e.type() == EdgeType.DEPLOYS).toList();
    var produces = graph.getEdges().stream().filter(e -> e.type() == EdgeType.PRODUCES).toList();
    assertThat(deploys).hasSize(1);
    assertThat(deploys.getFirst().targetId())
        .isEqualTo(TopologyNode.groupNodeId("payment-service-cg"));
    assertThat(produces).hasSize(1);
    assertThat(produces.getFirst().targetId())
        .isEqualTo(TopologyNode.topicId("payments.payment.captured"));
  }

  @Test
  void enrich_pureProducerApp_unmatchedTopics_storedInMetadata() {
    var service = new ApplicationEnrichmentService(source, true);
    var graph = new TopologyGraph();
    given(source.fetchApplications())
        .willReturn(List.of(producerApp("reporting", List.of("reports.daily.generated"))));

    service.enrich(graph);

    var appNode = graph.getNodes().get(TopologyNode.applicationId("reporting"));
    assertThat(appNode).isNotNull();
    assertThat(appNode.metadata())
        .containsEntry("candidateTopicNames", "reports.daily.generated");
    assertThat(graph.getEdges()).isEmpty();
  }

  @Test
  void enrich_namespacePresent_storedInMetadata() {
    var service = new ApplicationEnrichmentService(source, true);
    var graph = new TopologyGraph();
    given(source.fetchApplications()).willReturn(List.of(
        new DiscoveredApplication("catalog-service", "acme-shop", List.of(), List.of())));

    service.enrich(graph);

    var appNode = graph.getNodes().get(TopologyNode.applicationId("catalog-service"));
    assertThat(appNode.metadata()).containsEntry("namespace", "acme-shop");
  }

  @Test
  void enrich_sourceReturnsEmpty_graphUnchangedExceptCounters() {
    var service = new ApplicationEnrichmentService(source, true);
    var graph = graphWithConsumerGroup("fraud-detection-cg");
    given(source.fetchApplications()).willReturn(List.of());

    EnrichmentResult result = service.enrich(graph);

    assertThat(result.applicationsFound()).isEqualTo(0);
    assertThat(result.applicationsLinked()).isEqualTo(0);
    assertThat(graph.nodeCount()).isEqualTo(1);
    assertThat(graph.getEdges()).isEmpty();
  }

  @Test
  void enrich_sourceThrows_returnsEmptyResultAndLeavesGraphUnchanged() {
    var service = new ApplicationEnrichmentService(source, true);
    var graph = graphWithConsumerGroup("payment-service-cg");
    given(source.fetchApplications()).willThrow(new IllegalStateException("GitHub unreachable"));

    EnrichmentResult result = service.enrich(graph);

    assertThat(result.applicationsFound()).isEqualTo(0);
    assertThat(result.applicationsLinked()).isEqualTo(0);
    assertThat(result.applications()).isEmpty();
    assertThat(graph.nodeCount()).isEqualTo(1);
    assertThat(graph.getEdges()).isEmpty();
  }

  @Test
  void enrich_duplicateGroupCandidates_yieldSingleDeploysEdge() {
    var service = new ApplicationEnrichmentService(source, true);
    var graph = graphWithConsumerGroup("payment-service-cg");
    given(source.fetchApplications()).willReturn(List.of(
        app("payment-service", List.of("payment-service-cg", "payment-service-cg"))));

    EnrichmentResult result = service.enrich(graph);

    assertThat(result.applicationsLinked()).isEqualTo(1);
    assertThat(graph.getEdges()).hasSize(1);
    assertThat(graph.getEdges().getFirst().type()).isEqualTo(EdgeType.DEPLOYS);
    assertThat(graph.getEdges().getFirst().targetId())
        .isEqualTo(TopologyNode.groupNodeId("payment-service-cg"));
  }

  private static TopologyGraph graphWithConsumerGroup(String groupId) {
    var g = new TopologyGraph();
    g.addNode(TopologyNode.forConsumerGroup(groupId));
    return g;
  }

  private static DiscoveredApplication app(String name, List<String> candidates) {
    return new DiscoveredApplication(name, "", candidates, List.of());
  }

  private static DiscoveredApplication producerApp(String name, List<String> topicNames) {
    return new DiscoveredApplication(name, "", List.of(), topicNames);
  }
}
