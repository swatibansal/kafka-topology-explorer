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
 * two adjacency maps for O(1) neighbour lookup:
 *
 * <ul>
 *   <li><em>structural</em> (source node ID → target node IDs), exposed by {@link #neighbors},
 *       which follows every edge in its stored direction;
 *   <li><em>data flow</em> (node ID → nodes the data moves to next), exposed by
 *       {@link #downstream}, which follows {@link EdgeType#CONSUMES} and
 *       {@link EdgeType#PRODUCES} forwards but {@link EdgeType#DEPLOYS} in reverse, so that a
 *       trace can step from a consumer group to the application that runs it and on to the
 *       topics that application produces.
 * </ul>
 *
 * <p><strong>Thread safety:</strong> this class is not thread-safe. Build on a single thread,
 * then share the read-only views returned by the getter methods.
 */
public class TopologyGraph {

  private final Map<String, TopologyNode> nodes = new LinkedHashMap<>();
  private final List<TopologyEdge> edges = new ArrayList<>();
  private final Map<String, List<String>> adjacency = new LinkedHashMap<>();
  private final Map<String, List<String>> flowAdjacency = new LinkedHashMap<>();

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
    indexEdge(edge);
  }

  /** Records {@code edge} in both adjacency maps. */
  private void indexEdge(TopologyEdge edge) {
    adjacency
        .computeIfAbsent(edge.sourceId(), k -> new ArrayList<>())
        .add(edge.targetId());
    // DEPLOYS is stored application → group, but data flows group → application.
    boolean reversed = edge.type() == EdgeType.DEPLOYS;
    String from = reversed ? edge.targetId() : edge.sourceId();
    String to = reversed ? edge.sourceId() : edge.targetId();
    flowAdjacency.computeIfAbsent(from, k -> new ArrayList<>()).add(to);
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
   * Returns the IDs of the nodes that data reaches next from {@code nodeId}: the consumer groups
   * of a topic ({@link EdgeType#CONSUMES}), the application that runs a consumer group
   * ({@link EdgeType#DEPLOYS}, followed in reverse) and the topics an application writes to
   * ({@link EdgeType#PRODUCES}). An application is never stepped forwards into the other groups
   * it deploys, because no message travels that way.
   *
   * @param nodeId node ID
   * @return list of next node IDs in data-flow order; empty if the node is a sink or does not
   *     exist
   */
  public List<String> downstream(String nodeId) {
    return Collections.unmodifiableList(
        flowAdjacency.getOrDefault(nodeId, List.of()));
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
   * <p>Removed nodes are returned with the rule that removed them so callers can log, audit or
   * expose them; see {@link OrphanReport}.
   *
   * @return immutable list of the nodes removed, in removal order; empty when none
   */
  public List<OrphanNode> removeOrphanNodes() {
    List<OrphanNode> removed = new ArrayList<>();
    removeDanglingEdges();
    List<OrphanNode> orphans = findOrphans();
    while (!orphans.isEmpty()) {
      orphans.forEach(o -> nodes.remove(o.node().id()));
      removed.addAll(orphans);
      removeDanglingEdges();
      orphans = findOrphans();
    }
    return List.copyOf(removed);
  }

  /**
   * Returns the nodes that currently qualify as orphans under the rules documented on
   * {@link #removeOrphanNodes()}, each paired with the matching rule, without modifying the
   * graph.
   *
   * @return matching nodes; empty when none
   */
  private List<OrphanNode> findOrphans() {
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
    List<OrphanNode> orphans = new ArrayList<>();
    for (TopologyNode n : nodes.values()) {
      OrphanReason reason = orphanReason(n, consumedGroupIds, sourceIds, targetIds);
      if (reason != null) {
        orphans.add(OrphanNode.of(n, reason));
      }
    }
    return orphans;
  }

  /** Drops every edge whose source or target node is absent and rebuilds both adjacency maps. */
  private void removeDanglingEdges() {
    edges.removeIf(e -> !nodes.containsKey(e.sourceId()) || !nodes.containsKey(e.targetId()));
    adjacency.clear();
    flowAdjacency.clear();
    edges.forEach(this::indexEdge);
  }

  /**
   * Returns the pruning rule the given node matches under {@link #removeOrphanNodes()}, or
   * {@code null} if the node should be kept.
   *
   * @param n node to test
   * @param consumedGroupIds IDs of nodes that are targets of a {@link EdgeType#CONSUMES} edge
   * @param sourceIds IDs of nodes that are sources of any edge
   * @param targetIds IDs of nodes that are targets of any edge
   * @return matching reason, or {@code null} to keep the node
   */
  private static OrphanReason orphanReason(
      TopologyNode n,
      Set<String> consumedGroupIds,
      Set<String> sourceIds,
      Set<String> targetIds) {
    boolean edgeless = !sourceIds.contains(n.id()) && !targetIds.contains(n.id());
    return switch (n.type()) {
      case CONSUMER_GROUP, STREAMS_APP ->
          consumedGroupIds.contains(n.id()) ? null : OrphanReason.UNCONSUMED_GROUP;
      case APPLICATION -> edgeless ? OrphanReason.UNLINKED_APPLICATION : null;
      case TOPIC -> {
        String nameLower = n.name().toLowerCase(Locale.ROOT);
        boolean retryOrDlt = nameLower.contains("retry") || nameLower.contains("dlt");
        yield retryOrDlt && edgeless ? OrphanReason.UNUSED_RETRY_OR_DLT_TOPIC : null;
      }
    };
  }
}
