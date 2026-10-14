package io.github.swatibansal.kafkatopology.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import io.github.swatibansal.kafkatopology.enrichment.model.EnrichmentResult;
import io.github.swatibansal.kafkatopology.kafka.traversal.GraphTraversalService;
import io.github.swatibansal.kafkatopology.model.TopologyGraph;
import io.github.swatibansal.kafkatopology.model.TopologyNode;
import io.github.swatibansal.kafkatopology.model.TracedPath;
import io.github.swatibansal.kafkatopology.model.TraversalResult;
import io.github.swatibansal.kafkatopology.service.GraphOutputService;
import io.github.swatibansal.kafkatopology.service.TopologyCacheService;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;

/**
 * Web-layer tests for {@link TopologyController}.
 *
 * <p>{@link TopologyCacheService}, {@link GraphTraversalService}, and {@link GraphOutputService}
 * are mocked so no Kafka broker or scheduled jobs are needed.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
@AutoConfigureMockMvc
@TestPropertySource(
    properties = {
      "kafka-agent.bootstrap-servers=localhost:9092",
      "kafka-agent.security.protocol=PLAINTEXT",
      "kafka-agent.admin.request-timeout-ms=5000",
      "kafka-agent.admin.default-api-timeout-ms=5000",
      "kafka-agent.cache.refresh-interval-ms=3600000"
    })
class TopologyControllerTest {

  @Autowired
  private MockMvc mockMvc;

  @MockBean
  private TopologyCacheService cacheService;

  @MockBean
  private GraphTraversalService traversalService;

  @MockBean
  private GraphOutputService outputService;

  // ── GET /api/topology ─────────────────────────────────────────────────────────

  @Test
  void getTopology_returnsGraphJson() throws Exception {
    var graph = new TopologyGraph();
    graph.addNode(TopologyNode.forTopic("orders.created"));
    given(cacheService.getTopology()).willReturn(graph);

    mockMvc
        .perform(get("/api/topology"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.nodes['topic:orders.created'].name").value("orders.created"))
        .andExpect(jsonPath("$.nodes['topic:orders.created'].type").value("TOPIC"));
  }

  // ── GET /api/topology/trace ───────────────────────────────────────────────────

  @Test
  void trace_knownTopic_returnsTraversalResult() throws Exception {
    var topicNode = TopologyNode.forTopic("orders.created");
    var groupNode = TopologyNode.forConsumerGroup("order-service");
    var path = new TracedPath(List.of(topicNode, groupNode), groupNode, false);
    var result =
        new TraversalResult("orders.created", 1, List.of(path), List.of(groupNode));

    given(cacheService.getTopology()).willReturn(new TopologyGraph());
    given(traversalService.trace(eq("orders.created"), any())).willReturn(result);

    mockMvc
        .perform(get("/api/topology/trace").param("topic", "orders.created"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.startTopic").value("orders.created"))
        .andExpect(jsonPath("$.pathCount").value(1))
        .andExpect(jsonPath("$.paths[0].cycleDetected").value(false))
        .andExpect(jsonPath("$.finalDestinations[0].name").value("order-service"));
  }

  @Test
  void trace_unknownTopic_returns404() throws Exception {
    given(cacheService.getTopology()).willReturn(new TopologyGraph());
    given(traversalService.trace(eq("unknown"), any()))
        .willThrow(new IllegalArgumentException("not found"));

    mockMvc
        .perform(get("/api/topology/trace").param("topic", "unknown"))
        .andExpect(status().isNotFound());
  }

  @Test
  void trace_missingTopicParam_returns400() throws Exception {
    mockMvc.perform(get("/api/topology/trace")).andExpect(status().isBadRequest());
  }

  @Test
  void trace_partialTopicName_singleMatch_tracesResolvedTopic() throws Exception {
    var topicNode = TopologyNode.forTopic("orders.created");
    var graph = new TopologyGraph();
    graph.addNode(topicNode);
    given(cacheService.getTopology()).willReturn(graph);
    // Partial query "orders" has no exact match
    given(traversalService.trace(eq("orders"), any()))
        .willThrow(new IllegalArgumentException("not found"));
    // Resolved exact name traces successfully
    var groupNode = TopologyNode.forConsumerGroup("order-service");
    var path = new TracedPath(List.of(topicNode, groupNode), groupNode, false);
    var result = new TraversalResult("orders.created", 1, List.of(path), List.of(groupNode));
    given(traversalService.trace(eq("orders.created"), any())).willReturn(result);

    mockMvc
        .perform(get("/api/topology/trace").param("topic", "orders"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.startTopic").value("orders.created"))
        .andExpect(jsonPath("$.pathCount").value(1));
  }

  @Test
  void trace_partialTopicName_multipleMatches_returnsAmbiguous() throws Exception {
    var topicA = TopologyNode.forTopic("orders.created");
    var topicB = TopologyNode.forTopic("orders.cancelled");

    var graph = new TopologyGraph();
    graph.addNode(topicA);
    graph.addNode(topicB);
    given(cacheService.getTopology()).willReturn(graph);
    given(traversalService.trace(eq("orders"), any()))
        .willThrow(new IllegalArgumentException("not found"));

    mockMvc
        .perform(get("/api/topology/trace").param("topic", "orders"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.ambiguous").value(true))
        .andExpect(jsonPath("$.candidates").isArray())
        .andExpect(jsonPath("$.candidates.length()").value(2));
  }

  // ── GET /api/topology/export ──────────────────────────────────────────────────

  @Test
  void export_mermaidFormat_returnsTextPlain() throws Exception {
    given(cacheService.getTopology()).willReturn(new TopologyGraph());
    given(outputService.export(any(), any())).willReturn("flowchart LR\n");

    mockMvc
        .perform(get("/api/topology/export").param("format", "mermaid"))
        .andExpect(status().isOk())
        .andExpect(content().contentTypeCompatibleWith(MediaType.TEXT_PLAIN))
        .andExpect(content().string("flowchart LR\n"));
  }

  @Test
  void export_dotFormat_returnsTextPlain() throws Exception {
    given(cacheService.getTopology()).willReturn(new TopologyGraph());
    given(outputService.export(any(), any())).willReturn("digraph KafkaTopology {\n}\n");

    mockMvc
        .perform(get("/api/topology/export").param("format", "dot"))
        .andExpect(status().isOk())
        .andExpect(content().contentTypeCompatibleWith(MediaType.TEXT_PLAIN));
  }

  @Test
  void export_jsonFormat_returnsApplicationJson() throws Exception {
    given(cacheService.getTopology()).willReturn(new TopologyGraph());
    given(outputService.export(any(), any())).willReturn("{\"nodes\":{},\"edges\":[]}");

    mockMvc
        .perform(get("/api/topology/export").param("format", "json"))
        .andExpect(status().isOk())
        .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON));
  }

  @Test
  void export_defaultFormat_isMermaid() throws Exception {
    given(cacheService.getTopology()).willReturn(new TopologyGraph());
    given(outputService.export(any(), any())).willReturn("flowchart LR\n");

    // No format param — should default to mermaid.
    mockMvc
        .perform(get("/api/topology/export"))
        .andExpect(status().isOk())
        .andExpect(content().contentTypeCompatibleWith(MediaType.TEXT_PLAIN));
  }

  @Test
  void export_unknownFormat_returns400() throws Exception {
    given(cacheService.getTopology()).willReturn(new TopologyGraph());

    mockMvc
        .perform(get("/api/topology/export").param("format", "xml"))
        .andExpect(status().isBadRequest());
  }

  // ── POST /api/topology/refresh ────────────────────────────────────────────────

  @Test
  void refresh_post_returnsRefreshedGraph() throws Exception {
    var refreshed = new TopologyGraph();
    refreshed.addNode(TopologyNode.forTopic("fresh-topic"));
    given(cacheService.refresh()).willReturn(refreshed);

    mockMvc
        .perform(post("/api/topology/refresh"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.nodes['topic:fresh-topic'].name").value("fresh-topic"));
  }

  @Test
  void refresh_get_isNotAllowed() throws Exception {
    // Refresh triggers a full re-discovery, so it must not be reachable by a plain link or a
    // browser prefetch.
    mockMvc.perform(get("/api/topology/refresh")).andExpect(status().isMethodNotAllowed());
  }

  // ── GET /api/topology/applications ───────────────────────────────

  @Test
  void getApplications_returnsEnrichmentResult() throws Exception {
    given(cacheService.getLastEnrichmentResult())
        .willReturn(new EnrichmentResult(3, 2, List.of()));

    mockMvc
        .perform(get("/api/topology/applications"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.applicationsFound").value(3))
        .andExpect(jsonPath("$.applicationsLinked").value(2));
  }

  @Test
  void getApplications_whenDisabled_returnsEmptyResult() throws Exception {
    given(cacheService.getLastEnrichmentResult()).willReturn(EnrichmentResult.empty());

    mockMvc
        .perform(get("/api/topology/applications"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.applicationsFound").value(0))
        .andExpect(jsonPath("$.applicationsLinked").value(0));
  }

  // ── GET /api/topology/search ──────────────────────────────────────────────────

  @Test
  void search_matchingQuery_returnsSubgraph() throws Exception {
    var graph = new TopologyGraph();
    graph.addNode(TopologyNode.forApplication("order-service", Map.of()));
    graph.addNode(TopologyNode.forConsumerGroup("order-processor"));
    given(cacheService.getTopology()).willReturn(graph);

    mockMvc
        .perform(get("/api/topology/search").param("q", "order"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.nodes['app:order-service'].name").value("order-service"))
        .andExpect(jsonPath("$.nodes['group:order-processor'].name").value("order-processor"));
  }

  @Test
  void search_caseInsensitive_matchesUpperCase() throws Exception {
    var graph = new TopologyGraph();
    graph.addNode(TopologyNode.forApplication("Order-Service", Map.of()));
    given(cacheService.getTopology()).willReturn(graph);

    mockMvc
        .perform(get("/api/topology/search").param("q", "order"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.nodes['app:Order-Service'].name").value("Order-Service"));
  }

  @Test
  void search_noMatches_returnsEmptyGraph() throws Exception {
    var graph = new TopologyGraph();
    graph.addNode(TopologyNode.forTopic("orders.created"));
    given(cacheService.getTopology()).willReturn(graph);

    mockMvc
        .perform(get("/api/topology/search").param("q", "xyz-nonexistent"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.nodes").isEmpty());
  }

  @Test
  void search_blankQuery_returns400() throws Exception {
    mockMvc
        .perform(get("/api/topology/search").param("q", "   "))
        .andExpect(status().isBadRequest());
  }

  @Test
  void search_missingQueryParam_returns400() throws Exception {
    mockMvc.perform(get("/api/topology/search")).andExpect(status().isBadRequest());
  }
}
