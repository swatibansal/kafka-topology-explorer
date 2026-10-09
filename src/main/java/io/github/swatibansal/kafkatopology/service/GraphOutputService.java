package io.github.swatibansal.kafkatopology.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.swatibansal.kafkatopology.model.ExportFormat;
import io.github.swatibansal.kafkatopology.model.NodeType;
import io.github.swatibansal.kafkatopology.model.TopologyEdge;
import io.github.swatibansal.kafkatopology.model.TopologyGraph;
import io.github.swatibansal.kafkatopology.model.TopologyNode;
import org.springframework.stereotype.Service;

/**
 * Serialises a {@link TopologyGraph} to one of the supported {@link ExportFormat}s.
 *
 * <h2>Mermaid</h2>
 *
 * <p>Produces a {@code flowchart LR} diagram. Node shapes encode the node type:
 *
 * <ul>
 *   <li>TOPIC — rectangle {@code ["name [TOPIC]"]}
 *   <li>CONSUMER_GROUP — rounded rectangle {@code ("name [CONSUMER_GROUP]")}
 *   <li>STREAMS_APP — diamond/rhombus {@code {"name [STREAMS_APP]"}}
 *   <li>APPLICATION — stadium/pill {@code (["name [APPLICATION]"])}
 * </ul>
 *
 * <p>Node IDs are sanitised to {@code [A-Za-z0-9_]} to satisfy Mermaid's identifier constraints.
 *
 * <h2>DOT (Graphviz)</h2>
 *
 * <p>Produces a {@code digraph} with {@code rankdir=LR}. Each node type gets a distinctive shape
 * and fill colour for visual clarity.
 *
 * <h2>JSON</h2>
 *
 * <p>Delegates to the auto-configured Jackson {@link ObjectMapper}; the output is identical to
 * {@code GET /api/topology}.
 */
@Service
public class GraphOutputService {

  private final ObjectMapper objectMapper;

  /**
   * Constructs a {@code GraphOutputService}.
   *
   * @param objectMapper the application-scoped Jackson mapper
   */
  public GraphOutputService(ObjectMapper objectMapper) {
    this.objectMapper = objectMapper;
  }

  /**
   * Serialises the graph using the requested format.
   *
   * @param graph topology graph to serialise
   * @param format target output format
   * @return serialised string representation
   * @throws IllegalStateException if JSON serialisation fails (wraps
   *     {@link JsonProcessingException})
   */
  public String export(TopologyGraph graph, ExportFormat format) {
    return switch (format) {
      case MERMAID -> toMermaid(graph);
      case DOT -> toDot(graph);
      case JSON -> toJson(graph);
    };
  }

  // ── Mermaid ────────────────────────────────────────────────────────────────

  /**
   * Converts the graph to a Mermaid {@code flowchart LR} diagram string.
   *
   * @param graph topology graph
   * @return Mermaid diagram source
   */
  public String toMermaid(TopologyGraph graph) {
    var sb = new StringBuilder();
    sb.append("flowchart LR\n");

    for (TopologyNode node : graph.getNodes().values()) {
      sb.append("    ")
          .append(mermaidId(node.id()))
          .append(mermaidShape(node))
          .append("\n");
    }

    if (!graph.getEdges().isEmpty()) {
      sb.append("\n");
      for (TopologyEdge edge : graph.getEdges()) {
        sb.append("    ")
            .append(mermaidId(edge.sourceId()))
            .append(" -->|")
            .append(edge.type())
            .append("| ")
            .append(mermaidId(edge.targetId()))
            .append("\n");
      }
    }

    return sb.toString();
  }

  /** Returns the Mermaid node declaration including shape brackets and label. */
  private String mermaidShape(TopologyNode node) {
    String label = node.name() + " [" + node.type() + "]";
    return switch (node.type()) {
      case TOPIC -> "[\"" + label + "\"]";
      case CONSUMER_GROUP -> "(\"" + label + "\")";
      case STREAMS_APP -> "{\"" + label + "\"}";
      case APPLICATION -> "([\"" + label + "\"])";  // stadium / pill shape
    };
  }

  /**
   * Sanitises a node ID to a valid Mermaid identifier by replacing all non-alphanumeric
   * characters with underscores.
   */
  private String mermaidId(String nodeId) {
    return nodeId.replaceAll("[^A-Za-z0-9]", "_");
  }

  // ── DOT (Graphviz) ─────────────────────────────────────────────────────────

  /**
   * Converts the graph to a Graphviz DOT {@code digraph} string.
   *
   * @param graph topology graph
   * @return DOT source
   */
  public String toDot(TopologyGraph graph) {
    var sb = new StringBuilder();
    sb.append("digraph KafkaTopology {\n");
    sb.append("    rankdir=LR;\n");
    sb.append("    graph [label=\"Kafka Topology\", fontsize=14, fontname=\"Arial\"];\n");
    sb.append("    node  [fontname=\"Arial\", fontsize=11];\n");
    sb.append("    edge  [fontname=\"Arial\", fontsize=9];\n");

    appendDotNodes(sb, graph, NodeType.TOPIC, "Topics",
        "cylinder", "lightblue");
    appendDotNodes(sb, graph, NodeType.CONSUMER_GROUP, "Consumer Groups",
        "box", "lightyellow");
    appendDotNodes(sb, graph, NodeType.STREAMS_APP, "Kafka Streams Applications",
        "hexagon", "lightgreen");
    appendDotNodes(sb, graph, NodeType.APPLICATION, "Applications",
        "component", "lightsalmon");

    if (!graph.getEdges().isEmpty()) {
      sb.append("\n    // Edges\n");
      for (TopologyEdge edge : graph.getEdges()) {
        sb.append("    ")
            .append(dotId(edge.sourceId()))
            .append(" -> ")
            .append(dotId(edge.targetId()))
            .append(" [label=\"")
            .append(edge.type())
            .append("\"];\n");
      }
    }

    sb.append("}\n");
    return sb.toString();
  }

  private void appendDotNodes(
      StringBuilder sb,
      TopologyGraph graph,
      NodeType type,
      String sectionComment,
      String shape,
      String fillColor) {
    var nodesOfType = graph.nodesByType(type);
    if (nodesOfType.isEmpty()) {
      return;
    }
    sb.append("\n    // ").append(sectionComment).append("\n");
    for (TopologyNode node : nodesOfType) {
      sb.append("    ")
          .append(dotId(node.id()))
          .append(" [label=\"")
          .append(escapeDotLabel(node.name()))
          .append("\\n[")
          .append(node.type())
          .append("]\", shape=")
          .append(shape)
          .append(", style=filled, fillcolor=")
          .append(fillColor)
          .append("];\n");
    }
  }

  /** Wraps a node ID in double quotes, escaping any inner double-quote characters. */
  private String dotId(String nodeId) {
    return "\"" + nodeId.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
  }

  /** Escapes backslashes and double quotes in a DOT label string. */
  private String escapeDotLabel(String label) {
    return label.replace("\\", "\\\\").replace("\"", "\\\"");
  }

  // ── JSON ───────────────────────────────────────────────────────────────────

  /**
   * Serialises the graph to a pretty-printed JSON string using the application's
   * {@link ObjectMapper}.
   *
   * @param graph topology graph
   * @return JSON string
   * @throws IllegalStateException if serialisation fails
   */
  public String toJson(TopologyGraph graph) {
    try {
      return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(graph);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Failed to serialise topology graph to JSON", e);
    }
  }
}
