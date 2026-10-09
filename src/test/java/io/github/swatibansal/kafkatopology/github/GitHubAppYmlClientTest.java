package io.github.swatibansal.kafkatopology.github;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.client.ExpectedCount.once;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.header;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withResourceNotFound;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withUnauthorizedRequest;

import io.github.swatibansal.kafkatopology.config.KafkaAgentProperties.GitHub;
import io.github.swatibansal.kafkatopology.enrichment.AppYmlApplicationParser;
import io.github.swatibansal.kafkatopology.enrichment.SpringAppYmlGroupIdExtractor;
import io.github.swatibansal.kafkatopology.enrichment.model.DiscoveredApplication;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestClient;

/**
 * Unit tests for {@link GitHubAppYmlClient} against a {@link MockRestServiceServer}; no network
 * access and no Spring context.
 */
class GitHubAppYmlClientTest {

  private static final String ORG = "acme-shop";
  private static final String TOKEN = "test-token";
  private static final String PROFILE = "prod";
  private static final String API = "https://api.github.com/repos/" + ORG + "/";
  private static final String RESOURCES = "/contents/src/main/resources/";

  private static final String ORDER_SERVICE_YML =
      """
      spring:
        kafka:
          consumer:
            group-id: order-service-group
      app:
        kafka:
          topic: orders.order.created
      """;

  private RestClient.Builder builder;
  private MockRestServiceServer server;
  private AppYmlApplicationParser parser;

  @BeforeEach
  void setUp() {
    builder = RestClient.builder();
    server = MockRestServiceServer.bindTo(builder).build();
    parser = new AppYmlApplicationParser(new SpringAppYmlGroupIdExtractor(), PROFILE);
  }

  private GitHubAppYmlClient client(String org, String token, List<String> repos) {
    return new GitHubAppYmlClient(new GitHub(true, org, token, repos), parser, builder);
  }

  private static String url(String repo, String file) {
    return API + repo + RESOURCES + file;
  }

  /** Mimics the GitHub Contents API: base64 with line breaks every 76 characters. */
  private static String contentsJson(String yaml) {
    String base64 = Base64.getMimeEncoder().encodeToString(yaml.getBytes(StandardCharsets.UTF_8));
    String escaped = base64.replace("\r\n", "\\n");
    return "{\"content\":\"" + escaped + "\",\"encoding\":\"base64\"}";
  }

  private void expectNotFound(String repo, String file) {
    server.expect(once(), requestTo(url(repo, file)))
        .andExpect(method(HttpMethod.GET))
        .andRespond(withResourceNotFound());
  }

  private void expectProfileFilesNotFound(String repo) {
    expectNotFound(repo, "application-" + PROFILE + ".yml");
    expectNotFound(repo, "application-" + PROFILE + ".yaml");
  }

  @Test
  void fetchApplications_applicationYmlFound_extractsGroupIdsAndTopicNames() {
    server.expect(once(), requestTo(url("order-service", "application.yml")))
        .andExpect(method(HttpMethod.GET))
        .andExpect(header("Authorization", "Bearer " + TOKEN))
        .andExpect(header("Accept", "application/vnd.github.v3+json"))
        .andRespond(withSuccess(contentsJson(ORDER_SERVICE_YML), MediaType.APPLICATION_JSON));
    expectProfileFilesNotFound("order-service");

    List<DiscoveredApplication> apps =
        client(ORG, TOKEN, List.of("order-service")).fetchApplications();

    server.verify();
    assertThat(apps).hasSize(1);
    DiscoveredApplication app = apps.get(0);
    assertThat(app.name()).isEqualTo("order-service");
    assertThat(app.namespace()).isEqualTo(ORG);
    assertThat(app.candidateGroupIds()).containsExactly("order-service-group");
    assertThat(app.candidateTopicNames()).containsExactly("orders.order.created");
  }

  @Test
  void fetchApplications_noYamlFilesInRepo_returnsApplicationWithEmptyCandidates() {
    expectNotFound("payment-service", "application.yml");
    expectNotFound("payment-service", "application.yaml");
    expectProfileFilesNotFound("payment-service");

    List<DiscoveredApplication> apps =
        client(ORG, TOKEN, List.of("payment-service")).fetchApplications();

    server.verify();
    assertThat(apps).hasSize(1);
    assertThat(apps.get(0).name()).isEqualTo("payment-service");
    assertThat(apps.get(0).candidateGroupIds()).isEmpty();
    assertThat(apps.get(0).candidateTopicNames()).isEmpty();
  }

  @Test
  void fetchApplications_unauthorizedOnFirstRepo_abortsWithEmptyList() {
    server.expect(once(), requestTo(url("order-service", "application.yml")))
        .andRespond(withUnauthorizedRequest());

    List<DiscoveredApplication> apps =
        client(ORG, TOKEN, List.of("order-service", "payment-service")).fetchApplications();

    // The second repo must never be requested; verify() fails on any unexpected call.
    server.verify();
    assertThat(apps).isEmpty();
  }

  @Test
  void fetchApplications_unauthorizedOnLaterRepo_returnsReposProcessedSoFar() {
    server.expect(once(), requestTo(url("order-service", "application.yml")))
        .andRespond(withSuccess(contentsJson(ORDER_SERVICE_YML), MediaType.APPLICATION_JSON));
    expectProfileFilesNotFound("order-service");
    server.expect(once(), requestTo(url("payment-service", "application.yml")))
        .andRespond(withUnauthorizedRequest());

    List<DiscoveredApplication> apps =
        client(ORG, TOKEN, List.of("order-service", "payment-service", "shipping-service"))
            .fetchApplications();

    server.verify();
    assertThat(apps).extracting(DiscoveredApplication::name).containsExactly("order-service");
  }

  @Test
  void fetchApplications_blankToken_returnsEmptyWithoutHttpCall() {
    List<DiscoveredApplication> apps =
        client(ORG, "  ", List.of("order-service")).fetchApplications();

    server.verify();
    assertThat(apps).isEmpty();
  }

  @Test
  void fetchApplications_blankOrg_returnsEmptyWithoutHttpCall() {
    List<DiscoveredApplication> apps =
        client("", TOKEN, List.of("order-service")).fetchApplications();

    server.verify();
    assertThat(apps).isEmpty();
  }
}
