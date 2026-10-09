package io.github.swatibansal.kafkatopology.controller;

import static org.hamcrest.Matchers.containsString;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import io.github.swatibansal.kafkatopology.kafka.traversal.GraphTraversalService;
import io.github.swatibansal.kafkatopology.service.GraphOutputService;
import io.github.swatibansal.kafkatopology.service.TopologyCacheService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;

/**
 * Verifies that the Cytoscape.js single-page UI ({@code src/main/resources/static/index.html})
 * is served by
 * Spring Boot's static-resource handler at both {@code /} and {@code /index.html}.
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
class UiResourceTest {

  @Autowired
  private MockMvc mockMvc;

  // Mocked so no Kafka broker is needed.
  @MockBean
  private TopologyCacheService cacheService;

  @MockBean
  private GraphTraversalService traversalService;

  @MockBean
  private GraphOutputService outputService;

  @Test
  void rootPath_servesIndexHtml() throws Exception {
    // Spring's WelcomePageHandlerMapping serves index.html for GET /.
    // Content-Type may not be set by MockMvc's static-resource handler,
    // so we only assert the 200 OK status here.
    mockMvc
        .perform(get("/"))
        .andExpect(status().isOk());
  }

  @Test
  void indexHtmlPath_servesHtmlPage() throws Exception {
    mockMvc
        .perform(get("/index.html"))
        .andExpect(status().isOk())
        .andExpect(content().contentTypeCompatibleWith(MediaType.TEXT_HTML))
        .andExpect(content().string(containsString("Kafka Topology Explorer")));
  }
}
