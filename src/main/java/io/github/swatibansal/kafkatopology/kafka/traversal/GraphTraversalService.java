package io.github.swatibansal.kafkatopology.kafka.traversal;

import io.github.swatibansal.kafkatopology.model.TopologyGraph;
import io.github.swatibansal.kafkatopology.model.TopologyNode;
import io.github.swatibansal.kafkatopology.model.TracedPath;
import io.github.swatibansal.kafkatopology.model.TraversalResult;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Traverses a {@link TopologyGraph} using depth-first search (DFS) to enumerate all paths from a
 * starting topic to every reachable leaf (final destination) node.
 *
 * <h2>Direction of travel</h2>
 *
 * <p>The search follows the <em>data flow</em> exposed by {@link TopologyGraph#downstream}: from a
 * topic to the consumer groups that read it, from a consumer group to the application that runs
 * it (the {@code DEPLOYS} edge, taken in reverse), and from an application to the topics it
 * produces. A chain such as {@code orders.created → order-service-cg → order-service →
 * orders.validated → …} is therefore reported as one multi-hop path. Where no application is
 * linked to a group, the group remains the leaf.
 *
 * <h2>Algorithm</h2>
 *
 * <p>DFS with per-path cycle detection:
 *
 * <ul>
 *   <li>A <em>path set</em> tracks the node IDs on the current DFS stack. Any edge pointing to a
 *       node already in this set is a back-edge (cycle) and is skipped.
 *   <li>A node is recorded as a <em>leaf</em> when it has no downstream edges (true leaf) or when
 *       all its downstream edges point back into the current path (cycle terminus).
 *   <li>The algorithm backtracks fully, so all paths through a diamond-shaped sub-graph (multiple
 *       routes converging on the same node) are enumerated independently.
 * </ul>
 *
 * <h2>Complexity</h2>
 *
 * <p>Worst-case O(V + E) per path; total work is O(P × (V + E)) where P is the number of paths.
 * Kafka topologies are shallow (typically 2–4 hops), so this is negligible in practice.
 */
@Service
public class GraphTraversalService {

  private static final Logger log = LoggerFactory.getLogger(GraphTraversalService.class);

  /**
   * Traces all paths from the given topic to every reachable leaf in the topology graph.
   *
   * @param topicName name of the starting topic (not the node ID)
   * @param graph the topology graph to traverse
   * @return {@link TraversalResult} containing all paths and deduplicated final destinations
   * @throws IllegalArgumentException if {@code topicName} does not correspond to a node in the
   *     graph
   */
  public TraversalResult trace(String topicName, TopologyGraph graph) {
    String startId = TopologyNode.topicId(topicName);
    TopologyNode startNode = graph.getNodes().get(startId);
    if (startNode == null) {
      throw new IllegalArgumentException(
          "Topic '" + topicName + "' not found in the topology graph");
    }

    log.debug("Tracing paths from topic '{}'", topicName);

    List<TracedPath> paths = new ArrayList<>();
    List<TopologyNode> currentPath = new ArrayList<>();
    currentPath.add(startNode);

    Set<String> pathSet = new HashSet<>();
    pathSet.add(startId);

    dfs(startId, currentPath, pathSet, graph, paths);

    List<TopologyNode> finalDestinations =
        paths.stream().map(TracedPath::destination).distinct().toList();

    log.debug(
        "Found {} path(s) and {} final destination(s) from topic '{}'",
        paths.size(),
        finalDestinations.size(),
        topicName);

    return new TraversalResult(topicName, paths.size(), paths, finalDestinations);
  }

  /**
   * Recursive DFS step. Explores all downstream edges from {@code nodeId}, skipping back-edges
   * (cycle detection via {@code pathSet}). Records the current path when a leaf is reached.
   *
   * @param nodeId current node being visited
   * @param currentPath mutable list of nodes on the current DFS stack
   * @param pathSet IDs of nodes on the current DFS stack, for O(1) cycle detection
   * @param graph the topology graph
   * @param results accumulator for completed paths
   */
  private void dfs(
      String nodeId,
      List<TopologyNode> currentPath,
      Set<String> pathSet,
      TopologyGraph graph,
      List<TracedPath> results) {
    List<String> neighbors = graph.downstream(nodeId);

    // True leaf: no downstream edges — record the path immediately.
    if (neighbors.isEmpty()) {
      TopologyNode destination = currentPath.get(currentPath.size() - 1);
      results.add(new TracedPath(List.copyOf(currentPath), destination, false));
      return;
    }

    // Explore non-cyclic neighbours, track whether we found at least one.
    boolean allCyclic = true;
    for (String neighborId : neighbors) {
      if (pathSet.contains(neighborId)) {
        continue; // back-edge — skip to prevent infinite recursion
      }

      TopologyNode neighborNode = graph.getNodes().get(neighborId);
      if (neighborNode == null) {
        log.warn("Edge points to unknown node '{}'; skipping", neighborId);
        continue;
      }

      allCyclic = false;
      pathSet.add(neighborId);
      currentPath.add(neighborNode);

      dfs(neighborId, currentPath, pathSet, graph, results);

      // Backtrack: restore state for the next sibling branch.
      currentPath.remove(currentPath.size() - 1);
      pathSet.remove(neighborId);
    }

    // Cycle terminus: every downstream edge was a back-edge — record path with flag set.
    if (allCyclic) {
      TopologyNode destination = currentPath.get(currentPath.size() - 1);
      results.add(new TracedPath(List.copyOf(currentPath), destination, true));
    }
  }
}
