package io.github.swatibansal.kafkatopology.model;

/**
 * Supported export formats for the Kafka topology graph.
 *
 * <p>Used as a request parameter on the {@code GET /api/topology/export} endpoint.
 */
public enum ExportFormat {

  /** Standard JSON — same structure as {@code GET /api/topology}. */
  JSON,

  /**
   * <a href="https://mermaid.js.org/">Mermaid</a> {@code flowchart LR} diagram. Paste the output
   * directly into a Markdown code fence with the {@code mermaid} language tag to render it.
   */
  MERMAID,

  /**
   * <a href="https://graphviz.org/">Graphviz</a> DOT language. Render with
   * {@code dot -Tsvg topology.dot -o topology.svg} or any Graphviz-compatible tool.
   */
  DOT;

  /**
   * Parses a format string case-insensitively.
   *
   * @param value raw string (e.g. {@code "mermaid"}, {@code "DOT"})
   * @return matching enum constant
   * @throws IllegalArgumentException if the value does not match any constant
   */
  public static ExportFormat parse(String value) {
    return switch (value.toLowerCase(java.util.Locale.ROOT)) {
      case "json" -> JSON;
      case "mermaid" -> MERMAID;
      case "dot" -> DOT;
      default ->
          throw new IllegalArgumentException(
              "Unsupported format '" + value + "'. Supported values: json, mermaid, dot");
    };
  }
}
