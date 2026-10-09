package io.github.swatibansal.kafkatopology.service;

import io.github.swatibansal.kafkatopology.enrichment.ApplicationEnrichmentService;
import io.github.swatibansal.kafkatopology.enrichment.model.EnrichmentResult;
import io.github.swatibansal.kafkatopology.kafka.discovery.TopologyDiscoveryService;
import io.github.swatibansal.kafkatopology.model.TopologyGraph;
import io.github.swatibansal.kafkatopology.model.TopologyNode;
import jakarta.annotation.PostConstruct;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

/**
 * In-memory cache for the Kafka topology graph with periodic background refresh.
 *
 * <h2>Lifecycle</h2>
 *
 * <ol>
 *   <li>{@link #init()} fires on application startup ({@link PostConstruct}) and performs the
 *       initial discovery. Failures are caught and logged — a missing broker does not prevent the
 *       service from starting; the cache starts empty and is retried on the next scheduled tick.
 *   <li>{@link #refresh()} is called by the Spring scheduler at the interval configured via
 *       {@code kafka-agent.cache.refresh-interval-ms} (default: 60 000 ms). It can also be
 *       invoked directly by the {@code GET /api/topology/refresh} endpoint.
 * </ol>
 *
 * <h2>Enrichment</h2>
 *
 * <p>After the Kafka topology is discovered, {@link ApplicationEnrichmentService#enrich} is called
 * to overlay application nodes and DEPLOYS / PRODUCES edges. Enrichment is purely additive and
 * best-effort — it never throws and never prevents the Kafka topology from being published.
 *
 * <h2>Thread safety</h2>
 *
 * <p>The cached graph and enrichment result are stored in {@link AtomicReference} /
 * {@code volatile} fields. Readers always see a consistent snapshot; a slow refresh never blocks
 * callers.
 */
@Service
public class TopologyCacheService {

  private static final Logger log = LoggerFactory.getLogger(TopologyCacheService.class);
  private static final Logger orphanLog = LoggerFactory.getLogger("kafka-agent.orphans");

  private final TopologyDiscoveryService discoveryService;
  private final ApplicationEnrichmentService enrichmentService;
  private final AtomicReference<TopologyGraph> cachedGraph =
      new AtomicReference<>(new TopologyGraph());
  private volatile Instant lastRefreshedAt;
  private volatile EnrichmentResult lastEnrichmentResult = EnrichmentResult.empty();

  /**
   * Constructs a {@code TopologyCacheService}.
   *
   * @param discoveryService  service that queries the Kafka broker
   * @param enrichmentService service that overlays application data
   */
  public TopologyCacheService(
      TopologyDiscoveryService discoveryService,
      ApplicationEnrichmentService enrichmentService) {
    this.discoveryService = discoveryService;
    this.enrichmentService = enrichmentService;
  }

  /**
   * Kicks off the initial topology load in a background virtual thread so that the web server
   * starts immediately and health probes succeed from the first check.
   *
   * <p>The cache starts empty; the UI handles this gracefully. Any failure is logged as a warning
   * — the scheduler will retry on the next tick.
   */
  @PostConstruct
  public void init() {
    Thread.ofVirtual().name("topology-init").start(() -> {
      try {
        refresh();
      } catch (Exception e) {
        log.warn(
            "Initial topology load failed — cache is empty until the next scheduled refresh: {}",
            e.getMessage());
      }
    });
  }

  /**
   * Queries the Kafka broker for a fresh topology snapshot, enriches it with application data,
   * and atomically replaces the cached graph. Called automatically by the Spring task scheduler
   * and on demand via the REST API.
   *
   * @return the newly discovered and enriched {@link TopologyGraph}
   * @throws IllegalStateException if broker communication fails
   */
  @Scheduled(
      fixedDelayString = "${kafka-agent.cache.refresh-interval-ms:60000}",
      initialDelayString = "${kafka-agent.cache.refresh-interval-ms:60000}")
  public TopologyGraph refresh() {
    log.info("Refreshing Kafka topology cache");
    TopologyGraph graph = discoveryService.discover();
    lastEnrichmentResult = enrichmentService.enrich(graph);

    List<TopologyNode> orphans = graph.removeOrphanNodes();
    if (!orphans.isEmpty()) {
      log.info("Removed {} orphan node(s) from topology graph", orphans.size());
      orphans.forEach(n ->
          orphanLog.info("ORPHAN_NODE type={} id={} name={}", n.type(), n.id(), n.name()));
    }

    cachedGraph.set(graph);
    lastRefreshedAt = Instant.now();
    log.info(
        "Topology cache refreshed — nodes: {}, edges: {}", graph.nodeCount(), graph.edgeCount());
    return graph;
  }

  /**
   * Returns the most recently cached topology graph. Never {@code null}; returns an empty graph
   * if no successful discovery has occurred yet.
   *
   * @return current cached {@link TopologyGraph}
   */
  public TopologyGraph getTopology() {
    return cachedGraph.get();
  }

  /**
   * Returns the timestamp of the last successful cache refresh, or {@code null} if no refresh has
   * succeeded yet.
   *
   * @return last-refreshed instant, or {@code null}
   */
  public Instant getLastRefreshedAt() {
    return lastRefreshedAt;
  }

  /**
   * Returns the result of the most recent application enrichment pass.
   *
   * @return enrichment result; never {@code null}
   */
  public EnrichmentResult getLastEnrichmentResult() {
    return lastEnrichmentResult;
  }
}
