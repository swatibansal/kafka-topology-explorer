package io.github.swatibansal.kafkatopology.controller;

import io.github.swatibansal.kafkatopology.enrichment.model.EnrichmentResult;
import io.github.swatibansal.kafkatopology.kafka.traversal.GraphTraversalService;
import io.github.swatibansal.kafkatopology.model.ExportFormat;
import io.github.swatibansal.kafkatopology.model.NodeType;
import io.github.swatibansal.kafkatopology.model.TopologyEdge;
import io.github.swatibansal.kafkatopology.model.TopologyGraph;
import io.github.swatibansal.kafkatopology.model.TopologyNode;
import io.github.swatibansal.kafkatopology.model.TraversalResult;
import io.github.swatibansal.kafkatopology.service.GraphOutputService;
import io.github.swatibansal.kafkatopology.service.TopologyCacheService;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST API for querying the Kafka topology graph.
 *
 * <h2>Endpoints</h2>
 *
 * <ul>
 *   <li>{@code GET /api/topology} — full topology graph as JSON (served from cache)
 *   <li>{@code GET /api/topology/search?q=<query>} — subgraph of nodes whose name contains the
 *       query (case-insensitive), plus their immediate neighbours
 *   <li>{@code GET /api/topology/trace?topic=<name>} — all paths from a topic to leaf nodes
 *   <li>{@code GET /api/topology/export?format=<fmt>} — graph in Mermaid, DOT, or JSON format
 *   <li>{@code GET /api/topology/refresh} — forces an immediate cache refresh
 *   <li>{@code GET /api/topology/applications} — application enrichment result from the last
 *       refresh
 * </ul>
 */
@RestController
@RequestMapping("/api/topology")
public class TopologyController {

  private static final Logger log = LoggerFactory.getLogger(TopologyController.class);

  private final TopologyCacheService cacheService;
  private final GraphTraversalService traversalService;
  private final GraphOutputService outputService;

  /**
   * Constructs a {@code TopologyController}.
   *
   * @param cacheService service holding the cached topology graph
   * @param traversalService service that traces paths through the graph
   * @param outputService service that serialises the graph to different formats
   */
  public TopologyController(
      TopologyCacheService cacheService,
      GraphTraversalService traversalService,
      GraphOutputService outputService) {
    this.cacheService = cacheService;
    this.traversalService = traversalService;
    this.outputService = outputService;
  }

  /**
   * Returns the full cached Kafka topology graph as JSON. To force a re-discovery first, call
   * {@code GET /api/topology/refresh}.
   *
   * @return {@code 200 OK} with the topology graph
   */
  @GetMapping
  public ResponseEntity<TopologyGraph> getTopology() {
    return ResponseEntity.ok(cacheService.getTopology());
  }

  /**
   * Searches the cached topology for nodes whose name contains {@code q} (case-insensitive).
   * Returns a subgraph containing the matching nodes, their direct neighbours, and all edges
   * connecting those nodes. Useful for narrowing the view to a specific application or topic.
   *
   * @param q case-insensitive substring to match against node names; must not be blank
   * @return {@code 200 OK} with the matching subgraph (may contain zero nodes if nothing matched),
   *     or {@code 400} if {@code q} is blank
   */
  @GetMapping("/search")
  public ResponseEntity<TopologyGraph> search(@RequestParam String q) {
    if (q.isBlank()) {
      return ResponseEntity.badRequest().build();
    }
    log.debug("Topology search requested for query '{}'", q);
    return ResponseEntity.ok(
        buildSearchSubgraph(cacheService.getTopology(), q.trim().toLowerCase(Locale.ROOT)));
  }

  /**
   * Builds a subgraph containing nodes whose name matches {@code queryLower}, plus their
   * immediate incoming and outgoing neighbours for context.
   *
   * @param full the full cached topology
   * @param queryLower lower-cased search query
   * @return filtered subgraph; never {@code null}
   */
  private TopologyGraph buildSearchSubgraph(TopologyGraph full, String queryLower) {
    Set<String> matchedIds = new HashSet<>();
    for (TopologyNode node : full.getNodes().values()) {
      if (node.name().toLowerCase(Locale.ROOT).contains(queryLower)) {
        matchedIds.add(node.id());
      }
    }

    Set<String> included = new HashSet<>(matchedIds);
    for (String id : matchedIds) {
      included.addAll(full.neighbors(id));
    }
    for (TopologyEdge edge : full.getEdges()) {
      if (matchedIds.contains(edge.targetId())) {
        included.add(edge.sourceId());
      }
    }

    TopologyGraph result = new TopologyGraph();
    for (String id : included) {
      TopologyNode node = full.getNodes().get(id);
      if (node != null) {
        result.addNode(node);
      }
    }
    for (TopologyEdge edge : full.getEdges()) {
      if (included.contains(edge.sourceId()) && included.contains(edge.targetId())) {
        result.addEdge(edge);
      }
    }
    return result;
  }

  /**
   * Traces all paths from the given topic to every reachable final destination in the cached
   * topology graph. Supports both exact and partial (contains) matching:
   *
   * <ul>
   *   <li>Exact match — traced immediately.
   *   <li>Exactly one partial match — topic resolved and traced transparently.
   *   <li>Multiple partial matches — returns {@code 200} with {@code {"ambiguous":true,
   *       "candidates":[...]}} so the caller can select the intended topic.
   *   <li>No match — {@code 404}.
   * </ul>
   *
   * @param topic Kafka topic name or partial substring to match against topic names
   * @return {@code 200 OK} with a {@link TraversalResult} or an ambiguous-candidates map,
   *     or {@code 404} if no matching topic is found
   */
  @GetMapping("/trace")
  public ResponseEntity<Object> trace(@RequestParam String topic) {
    log.debug("Trace requested for topic '{}'", topic);
    TopologyGraph graph = cacheService.getTopology();

    // Fast path: exact match
    try {
      return ResponseEntity.ok(traversalService.trace(topic, graph));
    } catch (IllegalArgumentException e) {
      log.debug("Exact topic '{}' not found; trying partial match", topic);
    }

    // Partial match: collect TOPIC nodes whose name contains the query
    String lowerTopic = topic.toLowerCase(Locale.ROOT);
    List<TopologyNode> matches = graph.getNodes().values().stream()
        .filter(n -> n.type() == NodeType.TOPIC)
        .filter(n -> n.name().toLowerCase(Locale.ROOT).contains(lowerTopic))
        .sorted(Comparator.comparing(TopologyNode::name))
        .toList();

    if (matches.isEmpty()) {
      log.debug("Topic '{}' not found in topology (exact or partial)", topic);
      return ResponseEntity.notFound().build();
    }

    if (matches.size() == 1) {
      try {
        return ResponseEntity.ok(traversalService.trace(matches.get(0).name(), graph));
      } catch (IllegalArgumentException e) {
        return ResponseEntity.notFound().build();
      }
    }

    // Multiple matches — return candidate list for UI disambiguation
    List<String> candidates = matches.stream().map(TopologyNode::name).toList();
    log.debug("Ambiguous topic query '{}' — {} candidates: {}", topic, candidates.size(),
        candidates);
    return ResponseEntity.ok(Map.of("ambiguous", true, "candidates", candidates));
  }

  /**
   * Exports the cached topology graph in the requested format.
   *
   * <ul>
   *   <li>{@code mermaid} (default) — Mermaid flowchart, {@code text/plain}
   *   <li>{@code dot} — Graphviz DOT diagram, {@code text/plain}
   *   <li>{@code json} — JSON equivalent of {@code GET /api/topology}, {@code application/json}
   * </ul>
   *
   * @param format export format (case-insensitive); defaults to {@code mermaid}
   * @return {@code 200 OK} with the serialised graph, or {@code 400} for an unrecognised format
   */
  @GetMapping("/export")
  public ResponseEntity<String> export(
      @RequestParam(defaultValue = "mermaid") String format) {
    ExportFormat exportFormat;
    try {
      exportFormat = ExportFormat.parse(format);
    } catch (IllegalArgumentException e) {
      return ResponseEntity.badRequest().body(e.getMessage());
    }

    TopologyGraph graph = cacheService.getTopology();
    String body = outputService.export(graph, exportFormat);
    MediaType contentType =
        exportFormat == ExportFormat.JSON ? MediaType.APPLICATION_JSON : MediaType.TEXT_PLAIN;
    return ResponseEntity.ok().contentType(contentType).body(body);
  }

  /**
   * Forces an immediate topology re-discovery, updates the cache, and returns the refreshed
   * graph.
   *
   * @return {@code 200 OK} with the freshly discovered topology graph
   */
  @GetMapping("/refresh")
  public ResponseEntity<TopologyGraph> refresh() {
    log.info("Manual topology refresh requested");
    return ResponseEntity.ok(cacheService.refresh());
  }

  /**
   * Returns the application enrichment result from the most recent topology refresh. Shows which
   * applications were discovered from configuration and how many were linked to Kafka nodes via
   * {@code DEPLOYS} or {@code PRODUCES} edges.
   *
   * <p>Returns an empty result when enrichment is disabled.
   *
   * @return {@code 200 OK} with the last {@link EnrichmentResult}
   */
  @GetMapping("/applications")
  public ResponseEntity<EnrichmentResult> getApplications() {
    return ResponseEntity.ok(cacheService.getLastEnrichmentResult());
  }
}
