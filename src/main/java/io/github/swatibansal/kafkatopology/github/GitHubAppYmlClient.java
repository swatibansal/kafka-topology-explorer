package io.github.swatibansal.kafkatopology.github;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.github.swatibansal.kafkatopology.config.KafkaAgentProperties.GitHub;
import io.github.swatibansal.kafkatopology.enrichment.AppYmlApplicationParser;
import io.github.swatibansal.kafkatopology.enrichment.ApplicationSource;
import io.github.swatibansal.kafkatopology.enrichment.model.DiscoveredApplication;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

/**
 * An {@link ApplicationSource} that fetches Spring {@code application.yml} files from each
 * configured GitHub repository via the Contents API and hands them to
 * {@link AppYmlApplicationParser}.
 *
 * <h2>How it works</h2>
 *
 * <ol>
 *   <li>For every repo in {@code kafka-agent.github.repos}, two file locations are tried:
 *       <ul>
 *         <li><strong>Spring Boot</strong> multi-document style:
 *             {@code src/main/resources/application.yml} (or {@code .yaml}).
 *         <li><strong>Profile-file</strong> style:
 *             {@code src/main/resources/application-{profile}.yml} (or {@code .yaml}).
 *       </ul>
 *   <li>All fetched content is base64-decoded and parsed for the configured active profile.
 *   <li>The extracted consumer group IDs and topic names become the candidates in the returned
 *       {@link DiscoveredApplication}; the enrichment service links them to live Kafka nodes.
 * </ol>
 *
 * <h2>Error handling</h2>
 *
 * <ul>
 *   <li>404 — file not found at the expected path; silently skipped.
 *   <li>401/403 — auth failure; logged at WARN; the entire repo scan is aborted.
 *   <li>Any other failure — logged at WARN; the repo is skipped and processing continues.
 *   <li>{@code fetchApplications()} never throws and always returns a (possibly empty) list.
 * </ul>
 *
 * <h2>Rate limiting note</h2>
 *
 * <p>Each refresh makes up to 4 API requests per configured repo. Size
 * {@code KAFKA_TOPOLOGY_REFRESH_MS} so that {@code repos × 4 × refreshes/hour} stays well below
 * GitHub's authenticated limit of 5 000 requests per hour.
 */
public class GitHubAppYmlClient implements ApplicationSource {

  private static final Logger log = LoggerFactory.getLogger(GitHubAppYmlClient.class);

  private static final String GITHUB_API_BASE = "https://api.github.com";
  private static final String APP_YML_PATH = "src/main/resources/application.yml";
  private static final String APP_YAML_PATH = "src/main/resources/application.yaml";

  private final GitHub config;
  private final AppYmlApplicationParser parser;
  private final RestClient restClient;

  /**
   * Constructs a {@code GitHubAppYmlClient} that talks to the public GitHub API.
   *
   * @param config GitHub enrichment configuration (org, token, repos)
   * @param parser parser that turns file contents into a {@link DiscoveredApplication}
   */
  public GitHubAppYmlClient(GitHub config, AppYmlApplicationParser parser) {
    this(config, parser, RestClient.builder());
  }

  /**
   * Constructs a {@code GitHubAppYmlClient} on top of a caller-supplied {@link RestClient.Builder}.
   * The GitHub base URL and default headers are applied to the builder before it is built, so a
   * test can bind a {@code MockRestServiceServer} to the builder first.
   *
   * @param config GitHub enrichment configuration (org, token, repos)
   * @param parser parser that turns file contents into a {@link DiscoveredApplication}
   * @param restClientBuilder builder used to create the HTTP client
   */
  public GitHubAppYmlClient(
      GitHub config, AppYmlApplicationParser parser, RestClient.Builder restClientBuilder) {
    this.config = config;
    this.parser = parser;
    this.restClient = restClientBuilder
        .baseUrl(GITHUB_API_BASE)
        .defaultHeader("Accept", "application/vnd.github.v3+json")
        .defaultHeader("Authorization", "Bearer " + config.token())
        .build();
  }

  /**
   * Fetches {@code application.yml} from each configured repository. Never throws.
   *
   * @return one {@link DiscoveredApplication} per scanned repo; never {@code null}
   */
  @Override
  public List<DiscoveredApplication> fetchApplications() {
    List<String> repos = config.repos();
    if (repos == null || repos.isEmpty()) {
      log.warn("GitHub enrichment enabled but no repos configured under kafka-agent.github.repos");
      return List.of();
    }
    log.info("GitHub application.yml enrichment: scanning {} repo(s) for profile '{}'",
        repos.size(), parser.activeProfile());

    if (config.token() == null || config.token().isBlank()) {
      log.warn("GitHub enrichment enabled but GITHUB_TOKEN is not set — "
          + "no APPLICATION nodes will be created. Set the GITHUB_TOKEN environment variable.");
      return List.of();
    }
    if (config.org() == null || config.org().isBlank()) {
      log.warn("GitHub enrichment enabled but GITHUB_ORG is not set — "
          + "no APPLICATION nodes will be created. Set the GITHUB_ORG environment variable.");
      return List.of();
    }

    List<DiscoveredApplication> results = new ArrayList<>();
    for (String repo : repos) {
      DiscoveredApplication app = processRepo(repo);
      if (app == null) {
        log.warn("Aborting GitHub scan after auth failure — "
            + "remaining repos skipped. Verify GITHUB_TOKEN is valid.");
        break;
      }
      results.add(app);
    }

    long withGroupIds = results.stream()
        .filter(a -> !a.candidateGroupIds().isEmpty())
        .count();
    log.info("GitHub enrichment complete — repos scanned: {}, with group IDs: {}",
        results.size(), withGroupIds);
    return List.copyOf(results);
  }

  /**
   * Fetches and parses YAML configuration for a single repo.
   *
   * @return the parsed application (possibly with empty candidates), or {@code null} if the repo
   *     scan should be aborted (auth error)
   */
  private DiscoveredApplication processRepo(String repo) {
    try {
      String baseContent = fetchFileContent(repo, APP_YML_PATH);
      if (baseContent == null) {
        baseContent = fetchFileContent(repo, APP_YAML_PATH);
      }
      String profileContent = fetchProfileFileQuietly(repo);
      if (baseContent == null && profileContent == null) {
        log.debug("No application yml found in repo '{}'", repo);
      }
      DiscoveredApplication app = parser.parse(repo, config.org(), baseContent, profileContent);
      log.debug("Repo '{}': extracted {} group ID(s): {}",
          repo, app.candidateGroupIds().size(), app.candidateGroupIds());
      return app;
    } catch (HttpClientErrorException e) {
      if (e.getStatusCode() == HttpStatus.UNAUTHORIZED
          || e.getStatusCode() == HttpStatus.FORBIDDEN) {
        log.warn("GitHub auth failure for repo '{}' ({}): check GITHUB_TOKEN",
            repo, e.getStatusCode());
        return null;
      }
      log.debug("HTTP error fetching repo '{}': {}", repo, e.getMessage());
      return DiscoveredApplication.empty(repo, config.org());
    } catch (RestClientException e) {
      log.warn("Failed to fetch application yml from repo '{}': {}", repo, e.getMessage());
      return DiscoveredApplication.empty(repo, config.org());
    } catch (Exception e) {
      log.warn("Unexpected error processing repo '{}': {}", repo, e.getMessage());
      return DiscoveredApplication.empty(repo, config.org());
    }
  }

  /**
   * Attempts to fetch {@code application-{profile}.yml} or {@code .yaml}. Never throws.
   *
   * @param repo repository name (without org prefix)
   * @return decoded file content, or {@code null} if absent or unreadable
   */
  private String fetchProfileFileQuietly(String repo) {
    String profile = parser.activeProfile();
    try {
      String content =
          fetchFileContent(repo, "src/main/resources/application-" + profile + ".yml");
      if (content == null) {
        content = fetchFileContent(repo, "src/main/resources/application-" + profile + ".yaml");
      }
      return content;
    } catch (Exception e) {
      log.debug("Could not fetch profile-specific yml for repo '{}': {}", repo, e.getMessage());
      return null;
    }
  }

  /**
   * Fetches and base64-decodes the content of a file from GitHub.
   *
   * @param repo     repository name (without org prefix)
   * @param filePath path within the repo
   * @return decoded file content, or {@code null} if the file is not found (HTTP 404)
   */
  private String fetchFileContent(String repo, String filePath) {
    String uri = String.format("/repos/%s/%s/contents/%s", config.org(), repo, filePath);
    try {
      GitHubContent response = restClient.get()
          .uri(uri)
          .retrieve()
          .body(GitHubContent.class);
      if (response == null || response.content() == null) {
        return null;
      }
      String cleanedContent = response.content().replaceAll("\\s", "");
      byte[] decoded = Base64.getDecoder().decode(cleanedContent);
      return new String(decoded, StandardCharsets.UTF_8);
    } catch (HttpClientErrorException e) {
      if (e.getStatusCode() == HttpStatus.NOT_FOUND) {
        return null;
      }
      throw e;
    }
  }

  /**
   * Subset of the GitHub Contents API response needed for file retrieval.
   *
   * @param content  base64-encoded file content (with embedded newlines from GitHub)
   * @param encoding encoding type; expected to be {@code "base64"}
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  private record GitHubContent(String content, String encoding) {}
}
