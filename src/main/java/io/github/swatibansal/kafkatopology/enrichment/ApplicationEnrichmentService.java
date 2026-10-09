package io.github.swatibansal.kafkatopology.enrichment;

import io.github.swatibansal.kafkatopology.enrichment.model.DiscoveredApplication;
import io.github.swatibansal.kafkatopology.enrichment.model.EnrichmentResult;
import io.github.swatibansal.kafkatopology.model.EdgeType;
import io.github.swatibansal.kafkatopology.model.NodeType;
import io.github.swatibansal.kafkatopology.model.TopologyEdge;
import io.github.swatibansal.kafkatopology.model.TopologyGraph;
import io.github.swatibansal.kafkatopology.model.TopologyNode;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Enriches a {@link TopologyGraph} with {@link NodeType#APPLICATION} nodes,
 * {@link EdgeType#DEPLOYS} edges, and {@link EdgeType#PRODUCES} edges sourced from application
 * configuration supplied by an {@link ApplicationSource}.
 *
 * <h2>Enrichment is purely additive</h2>
 *
 * <p>This service never modifies existing Kafka nodes or edges. It only appends APPLICATION nodes
 * and DEPLOYS/PRODUCES edges. If enrichment is disabled or the source returns no data, the graph
 * is returned unchanged.
 *
 * <h2>Linking strategy</h2>
 *
 * <ol>
 *   <li><strong>DEPLOYS edges (consumer apps)</strong>: if a candidate group ID matches a live
 *       {@link NodeType#CONSUMER_GROUP} or {@link NodeType#STREAMS_APP} node already in the
 *       topology, a {@link EdgeType#DEPLOYS} edge is added from the APPLICATION node to that node.
 *   <li><strong>PRODUCES edges</strong>: if a candidate topic name matches a live
 *       {@link NodeType#TOPIC} node <em>and</em> that topic is not already consumed by one of
 *       this application's own consumer groups (via a {@link EdgeType#CONSUMES} edge in the
 *       broker topology), a {@link EdgeType#PRODUCES} edge is added. This handles both pure
 *       producers and services that consume from some topics while producing to others.
 *   <li><strong>Unmatched candidates</strong>: candidate IDs that could not be matched are stored
 *       in the APPLICATION node's metadata under {@code "candidateGroupIds"} or
 *       {@code "candidateTopicNames"} for manual inspection.
 * </ol>
 *
 * <p>Registered exclusively as a {@link org.springframework.context.annotation.Bean} via
 * {@link io.github.swatibansal.kafkatopology.config.EnrichmentConfig}; not component-scanned.
 */
public class ApplicationEnrichmentService {

  private static final Logger log = LoggerFactory.getLogger(ApplicationEnrichmentService.class);

  private final ApplicationSource source;
  private final boolean enabled;

  /**
   * Constructs an {@code ApplicationEnrichmentService}.
   *
   * @param source  supplier of application configuration data
   * @param enabled whether enrichment is active (derived from properties at wiring time)
   */
  public ApplicationEnrichmentService(ApplicationSource source, boolean enabled) {
    this.source = source;
    this.enabled = enabled;
  }

  /**
   * Enriches the provided graph in-place with APPLICATION nodes, DEPLOYS edges, and PRODUCES
   * edges.
   *
   * <p>This method never throws. Any failure while reading configuration is logged and treated as
   * an empty application list, so the Kafka topology is always published.
   *
   * @param graph the Kafka topology graph to enrich; mutated in-place
   * @return summary of the enrichment pass
   */
  public EnrichmentResult enrich(TopologyGraph graph) {
    if (!enabled) {
      log.debug("Application enrichment is disabled; skipping");
      return EnrichmentResult.empty();
    }

    log.info("Starting application topology enrichment");

    Set<String> knownKafkaNodeIds = buildKafkaNodeIdSet(graph);
    Set<String> knownTopicNodeIds = buildTopicNodeIdSet(graph);
    List<DiscoveredApplication> apps = fetchApplicationsSafely();
    log.info("Application source returned {} application(s)", apps.size());

    int linked = 0;
    for (DiscoveredApplication app : apps) {
      if (processApplication(graph, app, knownKafkaNodeIds, knownTopicNodeIds)) {
        linked++;
      }
    }

    log.info("Application enrichment complete — apps: {}, linked to Kafka: {}",
        apps.size(), linked);
    return new EnrichmentResult(apps.size(), linked, apps);
  }

  /**
   * Calls the source, converting any failure or {@code null} return into an empty list so that
   * enrichment can never prevent the Kafka topology from being published.
   */
  private List<DiscoveredApplication> fetchApplicationsSafely() {
    try {
      List<DiscoveredApplication> apps = source.fetchApplications();
      return apps == null ? List.of() : apps;
    } catch (RuntimeException e) {
      log.warn("Application source failed; continuing without enrichment: {}", e.getMessage());
      return List.of();
    }
  }

  /**
   * Adds the APPLICATION node and any DEPLOYS or PRODUCES edges for a single application.
   *
   * <p>A candidate topic name produces a {@link EdgeType#PRODUCES} edge when two conditions hold:
   * <ol>
   *   <li>The topic exists as a {@link NodeType#TOPIC} node in the current graph.</li>
   *   <li>The topic is <em>not</em> already the source of a {@link EdgeType#CONSUMES} edge
   *       targeting one of this application's own consumer groups. Topics consumed via a known
   *       group represent inbound data, not outbound production.</li>
   * </ol>
   *
   * @return {@code true} if at least one DEPLOYS or PRODUCES edge was created
   */
  private boolean processApplication(
      TopologyGraph graph,
      DiscoveredApplication app,
      Set<String> knownKafkaNodeIds,
      Set<String> knownTopicNodeIds) {

    List<String> unmatched = new ArrayList<>();
    // LinkedHashSet: duplicate candidates must not yield duplicate edges; insertion order kept.
    Set<String> matched = new LinkedHashSet<>();

    for (String candidate : app.candidateGroupIds()) {
      String groupId = TopologyNode.groupNodeId(candidate);
      String streamsId = TopologyNode.streamsId(candidate);
      if (knownKafkaNodeIds.contains(groupId)) {
        matched.add(groupId);
      } else if (knownKafkaNodeIds.contains(streamsId)) {
        matched.add(streamsId);
      } else {
        unmatched.add(candidate);
      }
    }

    // Topics already consumed by this app's own consumer groups — identified by following
    // existing CONSUMES edges that target any of the matched group node IDs.
    Set<String> consumedByApp = graph.getEdges().stream()
        .filter(e -> e.type() == EdgeType.CONSUMES && matched.contains(e.targetId()))
        .map(TopologyEdge::sourceId)
        .collect(Collectors.toSet());

    List<String> unmatchedTopics = new ArrayList<>();
    Set<String> matchedTopics = new LinkedHashSet<>();

    for (String topicName : app.candidateTopicNames()) {
      String topicId = TopologyNode.topicId(topicName);
      if (!knownTopicNodeIds.contains(topicId)) {
        unmatchedTopics.add(topicName);
      } else if (!consumedByApp.contains(topicId)) {
        matchedTopics.add(topicId);
      }
      // else: topic is already consumed by this app's own group — skip to avoid a spurious edge
    }

    Map<String, String> metadata = buildMetadata(app, unmatched, unmatchedTopics);
    TopologyNode appNode = TopologyNode.forApplication(app.name(), metadata);
    graph.addNode(appNode);

    for (String targetId : matched) {
      graph.addEdge(TopologyEdge.of(
          appNode.id(), targetId, EdgeType.DEPLOYS,
          app.name() + " → " + targetId));
    }
    for (String targetId : matchedTopics) {
      graph.addEdge(TopologyEdge.of(
          appNode.id(), targetId, EdgeType.PRODUCES,
          app.name() + " → " + targetId));
    }

    if (!matched.isEmpty() || !matchedTopics.isEmpty()) {
      log.debug("App '{}' linked to {} Kafka node(s): {}, {} topic(s): {}",
          app.name(), matched.size(), matched, matchedTopics.size(), matchedTopics);
    } else {
      log.debug("App '{}' added with no confirmed Kafka links (candidates: {})",
          app.name(), app.candidateGroupIds());
    }
    return !matched.isEmpty() || !matchedTopics.isEmpty();
  }

  /** Collects IDs of all CONSUMER_GROUP and STREAMS_APP nodes in the graph. */
  private Set<String> buildKafkaNodeIdSet(TopologyGraph graph) {
    return graph.getNodes().values().stream()
        .filter(n -> n.type() == NodeType.CONSUMER_GROUP || n.type() == NodeType.STREAMS_APP)
        .map(TopologyNode::id)
        .collect(Collectors.toSet());
  }

  /** Collects IDs of all TOPIC nodes in the graph. */
  private Set<String> buildTopicNodeIdSet(TopologyGraph graph) {
    return graph.getNodes().values().stream()
        .filter(n -> n.type() == NodeType.TOPIC)
        .map(TopologyNode::id)
        .collect(Collectors.toSet());
  }

  private Map<String, String> buildMetadata(
      DiscoveredApplication app, List<String> unmatched, List<String> unmatchedTopics) {
    Map<String, String> meta = new LinkedHashMap<>();
    if (!app.namespace().isBlank()) {
      meta.put("namespace", app.namespace());
    }
    if (!unmatched.isEmpty()) {
      meta.put("candidateGroupIds", String.join(",", unmatched));
    }
    if (!unmatchedTopics.isEmpty()) {
      meta.put("candidateTopicNames", String.join(",", unmatchedTopics));
    }
    return Map.copyOf(meta);
  }
}
