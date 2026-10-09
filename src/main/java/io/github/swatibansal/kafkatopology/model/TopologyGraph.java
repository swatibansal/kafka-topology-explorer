package io.github.swatibansal.kafkatopology.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Mutable in-memory representation of the Kafka topology discovered from a broker.
 *
 * <p>Nodes are keyed by their {@link TopologyNode#id()}. Edges are stored as a flat list and as
 * an adjacency map (source node ID → list of target node IDs) for O(1) neighbour lookup during
 * graph traversal.
 *
 * <p><strong>Thread safety:</strong> this class is not thread-safe. Build on a single thread,
 * then share the read-only views returned by the getter methods.
 */
public class TopologyGraph {

  private final Map<String, TopologyNode> nodes = new LinkedHashMap<>();
  private final List<TopologyEdge> edges = new ArrayList<>();
  private final Map<String, List<String>> adjacency = new LinkedHashMap<>();

  /**
   * Adds a node. If a node with the same {@link TopologyNode#id()} already exists it is
   * replaced.
   *
   * @param node node to add
   */
  public void addNode(TopologyNode node) {
    nodes.put(node.id(), node);
  }

  /**
   * Adds a directed edge. The source and target nodes do not need to exist yet; callers are
   * responsible for adding them before serialising the graph.
   *
   * @param edge directed edge to add
   */
  public void addEdge(TopologyEdge edge) {
    edges.add(edge);
    adjacency
        .computeIfAbsent(edge.sourceId(), k -> new ArrayList<>())
        .add(edge.targetId());
  }

  /**
   * Returns an unmodifiable view of all nodes, keyed by node ID.
   *
   * @return node map; never {@code null}
   */
  public Map<String, TopologyNode> getNodes() {
    return Collections.unmodifiableMap(nodes);
  }

  /**
   * Returns an unmodifiable view of all edges.
   *
   * @return edge list; never {@code null}
   */
  public List<TopologyEdge> getEdges() {
    return Collections.unmodifiableList(edges);
  }

  /**
   * Returns the IDs of all nodes directly reachable from {@code nodeId} via outgoing edges.
   *
   * @param nodeId source node ID
   * @return list of target node IDs; empty list if the node has no outgoing edges or does not
   *     exist
   */
  public List<String> neighbors(String nodeId) {
    return Collections.unmodifiableList(
        adjacency.getOrDefault(nodeId, List.of()));
  }

  /**
   * Returns all nodes of the specified type.
   *
   * @param type node type to filter by
   * @return collection of matching nodes; never {@code null}
   */
  public Collection<TopologyNode> nodesByType(NodeType type) {
    return nodes.values().stream()
        .filter(n -> n.type() == type)
        .toList();
  }

  /**
   * Returns the total number of nodes in the graph.
   *
   * @return node count
   */
  public int nodeCount() {
    return nodes.size();
  }

  /**
   * Returns the total number of edges in the graph.
   *
   * @return edge count
   */
  public int edgeCount() {
    return edges.size();
  }

  /**
   * Identifies and removes orphan nodes from the graph. Three categories are pruned:
   *
   * <ol>
   *   <li>{@link NodeType#CONSUMER_GROUP} and {@link NodeType#STREAMS_APP} nodes that are not the
   *       target of any {@link EdgeType#CONSUMES} edge (consume from no topic in the graph).
   *   <li>{@link NodeType#APPLICATION} nodes that have neither incoming nor outgoing edges of any
   *       type (completely isolated, i.e. in-degree = 0 and out-degree = 0).
   *   <li>{@link NodeType#TOPIC} nodes whose name contains {@code "retry"} or {@code "dlt"}
   *       (case-insensitive) and that also have no edges of any type (in-degree = 0 and
   *       out-degree = 0). Active retry / DLT topics that are still connected are kept.
   * </ol>
   *
   * <p>Edges whose source or target node was pruned (or never existed) are removed as well and
   * the adjacency map is rebuilt. Because dropping those edges can isolate further nodes, for
   * example an {@link NodeType#APPLICATION} whose only {@link EdgeType#DEPLOYS} target was an
   * orphan consumer group, pruning repeats until no node matches any rule above. Afterwards every
   * edge returned by {@link #getEdges()} references two existing nodes.
   *
   * <p>Removed nodes are returned so callers can log or audit them.
   *
   * @return immutable list of the nodes removed, in removal order; empty when none
   */
  public List<TopologyNode> removeOrphanNodes() {
    List<TopologyNode> removed = new ArrayList<>();
    removeDanglingEdges();
    List<TopologyNode> orphans = findOrphans();
    while (!orphans.isEmpty()) {
      orphans.forEach(n -> nodes.remove(n.id()));
      removed.addAll(orphans);
      removeDanglingEdges();
      orphans = findOrphans();
    }
    return List.copyOf(removed);
  }

  /**
   * Returns the nodes that currently qualify as orphans under the rules documented on
   * {@link #removeOrphanNodes()}, without modifying the graph.
   *
   * @return matching nodes; empty when none
   */
  private List<TopologyNode> findOrphans() {
    Set<String> consumedGroupIds = new HashSet<>();
    Set<String> sourceIds = new HashSet<>();
    Set<String> targetIds = new HashSet<>();
    for (TopologyEdge edge : edges) {
      sourceIds.add(edge.sourceId());
      targetIds.add(edge.targetId());
      if (edge.type() == EdgeType.CONSUMES) {
        consumedGroupIds.add(edge.targetId());
      }
    }
    return nodes.values().stream()
        .filter(n -> isOrphan(n, consumedGroupIds, sourceIds, targetIds))
        .toList();
  }

  /** Drops every edge whose source or target node is absent and rebuilds the adjacency map. */
  private void removeDanglingEdges() {
    edges.removeIf(e -> !nodes.containsKey(e.sourceId()) || !nodes.containsKey(e.targetId()));
    adjacency.clear();
    for (TopologyEdge edge : edges) {
      adjacency
          .computeIfAbsent(edge.sourceId(), k -> new ArrayList<>())
          .add(edge.targetId());
    }
  }

  /**
   * Returns {@code true} if the given node qualifies as an orphan under the pruning rules applied
   * by {@link #removeOrphanNodes()}.
   *
   * @param n node to test
   * @param consumedGroupIds IDs of nodes that are targets of a {@link EdgeType#CONSUMES} edge
   * @param sourceIds IDs of nodes that are sources of any edge
   * @param targetIds IDs of nodes that are targets of any edge
   * @return {@code true} if the node should be removed
   */
  private static boolean isOrphan(
      TopologyNode n,
      Set<String> consumedGroupIds,
      Set<String> sourceIds,
      Set<String> targetIds) {
    return switch (n.type()) {
      case CONSUMER_GROUP, STREAMS_APP -> !consumedGroupIds.contains(n.id());
      case APPLICATION -> !sourceIds.contains(n.id()) && !targetIds.contains(n.id());
      case TOPIC -> {
        String nameLower = n.name().toLowerCase(Locale.ROOT);
        yield (nameLower.contains("retry") || nameLower.contains("dlt"))
            && !sourceIds.contains(n.id())
            && !targetIds.contains(n.id());
      }
    };
  }
}
