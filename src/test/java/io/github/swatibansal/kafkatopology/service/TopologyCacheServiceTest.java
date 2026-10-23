package io.github.swatibansal.kafkatopology.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.github.swatibansal.kafkatopology.enrichment.ApplicationEnrichmentService;
import io.github.swatibansal.kafkatopology.enrichment.model.EnrichmentResult;
import io.github.swatibansal.kafkatopology.kafka.discovery.TopologyDiscoveryService;
import io.github.swatibansal.kafkatopology.model.EdgeType;
import io.github.swatibansal.kafkatopology.model.OrphanNode;
import io.github.swatibansal.kafkatopology.model.OrphanReason;
import io.github.swatibansal.kafkatopology.model.TopologyEdge;
import io.github.swatibansal.kafkatopology.model.TopologyGraph;
import io.github.swatibansal.kafkatopology.model.TopologyNode;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Unit tests for {@link TopologyCacheService}.
 *
 * <p>Spring context is not loaded — the service is constructed directly and {@link
 * jakarta.annotation.PostConstruct} / {@code @Scheduled} lifecycle methods are invoked manually.
 */
@ExtendWith(MockitoExtension.class)
class TopologyCacheServiceTest {

  @Mock
  private TopologyDiscoveryService discoveryService;

  @Mock
  private ApplicationEnrichmentService enrichmentService;

  private TopologyCacheService cacheService;

  @BeforeEach
  void setUp() {
    // Construct without triggering @PostConstruct (Spring not involved).
    // lenient: not every test calls refresh(), so this stub would otherwise be flagged as unused.
    lenient().when(enrichmentService.enrich(any())).thenReturn(EnrichmentResult.empty());
    cacheService = new TopologyCacheService(discoveryService, enrichmentService);
  }

  @Test
  void getTopology_beforeAnyRefresh_returnsEmptyGraph() {
    assertThat(cacheService.getTopology().nodeCount()).isEqualTo(0);
    assertThat(cacheService.getLastRefreshedAt()).isNull();
  }

  @Test
  void refresh_callsDiscoveryAndPopulatesCache() {
    var graph = oneTopicGraph("orders.created");
    given(discoveryService.discover()).willReturn(graph);

    TopologyGraph result = cacheService.refresh();

    assertThat(result.nodeCount()).isEqualTo(1);
    assertThat(cacheService.getTopology().nodeCount()).isEqualTo(1);
    verify(discoveryService, times(1)).discover();
  }

  @Test
  void refresh_updatesLastRefreshedAt() {
    given(discoveryService.discover()).willReturn(new TopologyGraph());

    cacheService.refresh();

    assertThat(cacheService.getLastRefreshedAt()).isNotNull();
  }

  @Test
  void getTopology_returnsLatestGraphAfterMultipleRefreshes() {
    var small = oneTopicGraph("topic-a");
    var large = twoTopicGraph("topic-a", "topic-b");
    given(discoveryService.discover()).willReturn(small, large);

    cacheService.refresh();
    cacheService.refresh();

    assertThat(cacheService.getTopology().nodeCount()).isEqualTo(2);
  }

  @Test
  void init_whenDiscoveryFails_doesNotThrow() throws InterruptedException {
    // init() runs refresh() on a background virtual thread; wait for discover() to be invoked
    // before asserting so the test does not race the thread.
    var discoverCalled = new CountDownLatch(1);
    given(discoveryService.discover()).willAnswer(invocation -> {
      discoverCalled.countDown();
      throw new IllegalStateException("broker unreachable");
    });

    // init() should swallow the exception and leave the cache empty.
    cacheService.init();

    assertThat(discoverCalled.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(cacheService.getTopology().nodeCount()).isEqualTo(0);
    assertThat(cacheService.getLastRefreshedAt()).isNull();
  }

  @Test
  void refresh_whenDiscoveryFails_propagatesException() {
    given(discoveryService.discover())
        .willThrow(new IllegalStateException("broker unreachable"));

    assertThatThrownBy(() -> cacheService.refresh())
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("broker unreachable");
  }

  @Test
  void refresh_enrichmentResultStoredAndRetrievable() {
    var enriched = new EnrichmentResult(2, 1, java.util.List.of());
    given(discoveryService.discover()).willReturn(new TopologyGraph());
    given(enrichmentService.enrich(any())).willReturn(enriched);

    cacheService.refresh();

    assertThat(cacheService.getLastEnrichmentResult().applicationsFound()).isEqualTo(2);
    assertThat(cacheService.getLastEnrichmentResult().applicationsLinked()).isEqualTo(1);
  }

  @Test
  void getLastOrphanReport_beforeAnyRefresh_isEmpty() {
    assertThat(cacheService.getLastOrphanReport().count()).isZero();
    assertThat(cacheService.getLastOrphanReport().orphans()).isEmpty();
    assertThat(cacheService.getLastOrphanReport().refreshedAt()).isNull();
  }

  @Test
  void refresh_recordsPrunedOrphansWithReasons() {
    var graph = new TopologyGraph();
    var topic = TopologyNode.forTopic("orders.created");
    var liveGroup = TopologyNode.forConsumerGroup("order-processor");
    var deadGroup = TopologyNode.forConsumerGroup("never-committed-group");
    var deadDlt = TopologyNode.forTopic("orders.created.DLT");
    graph.addNode(topic);
    graph.addNode(liveGroup);
    graph.addNode(deadGroup);
    graph.addNode(deadDlt);
    graph.addEdge(TopologyEdge.of(topic.id(), liveGroup.id(), EdgeType.CONSUMES, "test"));
    given(discoveryService.discover()).willReturn(graph);

    cacheService.refresh();

    var report = cacheService.getLastOrphanReport();
    assertThat(report.count()).isEqualTo(2);
    assertThat(report.refreshedAt()).isEqualTo(cacheService.getLastRefreshedAt());
    assertThat(report.orphans()).extracting(OrphanNode::node)
        .containsExactlyInAnyOrder(deadGroup, deadDlt);
    assertThat(report.orphans()).extracting(OrphanNode::reason)
        .containsExactlyInAnyOrder(
            OrphanReason.UNCONSUMED_GROUP, OrphanReason.UNUSED_RETRY_OR_DLT_TOPIC);
    // The published graph no longer holds the pruned nodes.
    assertThat(cacheService.getTopology().getNodes())
        .containsOnlyKeys(topic.id(), liveGroup.id());
  }

  @Test
  void refresh_withNoOrphans_replacesPreviousReport() {
    var withOrphan = new TopologyGraph();
    withOrphan.addNode(TopologyNode.forConsumerGroup("lonely-group"));
    given(discoveryService.discover()).willReturn(withOrphan, new TopologyGraph());

    cacheService.refresh();
    assertThat(cacheService.getLastOrphanReport().count()).isEqualTo(1);

    cacheService.refresh();
    assertThat(cacheService.getLastOrphanReport().count()).isZero();
    assertThat(cacheService.getLastOrphanReport().refreshedAt()).isNotNull();
  }

  // ── helpers ──────────────────────────────────────────────────────────────────

  private static TopologyGraph oneTopicGraph(String topic) {
    var g = new TopologyGraph();
    g.addNode(TopologyNode.forTopic(topic));
    return g;
  }

  private static TopologyGraph twoTopicGraph(String t1, String t2) {
    var g = new TopologyGraph();
    g.addNode(TopologyNode.forTopic(t1));
    g.addNode(TopologyNode.forTopic(t2));
    return g;
  }
}
