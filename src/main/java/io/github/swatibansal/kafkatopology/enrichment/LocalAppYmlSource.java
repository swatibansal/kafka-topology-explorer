package io.github.swatibansal.kafkatopology.enrichment;

import io.github.swatibansal.kafkatopology.enrichment.model.DiscoveredApplication;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link ApplicationSource} that reads Spring {@code application.yml} files from a local
 * directory instead of a remote Git host.
 *
 * <h2>Expected layout</h2>
 *
 * <pre>
 * &lt;root&gt;/
 *   order-service/
 *     application.yml                       (or src/main/resources/application.yml)
 *     application-prod.yml                  (optional profile file)
 *   payment-service/
 *     src/main/resources/application.yml
 * </pre>
 *
 * <p>Each immediate sub-directory of the root is one application and its name is the application
 * name. Plain files in the root are ignored. Applications are returned sorted by name so output is
 * deterministic. This source never throws; unreadable files yield empty candidates.
 */
public class LocalAppYmlSource implements ApplicationSource {

  private static final Logger log = LoggerFactory.getLogger(LocalAppYmlSource.class);

  private static final List<String> BASE_FILES = List.of(
      "src/main/resources/application.yml",
      "src/main/resources/application.yaml",
      "application.yml",
      "application.yaml");

  private final Path root;
  private final AppYmlApplicationParser parser;

  /**
   * Constructs a {@code LocalAppYmlSource}.
   *
   * @param root   directory containing one sub-directory per application
   * @param parser parser that turns file contents into a {@link DiscoveredApplication}
   */
  public LocalAppYmlSource(Path root, AppYmlApplicationParser parser) {
    this.root = root;
    this.parser = parser;
  }

  /**
   * Scans the root directory and parses every application sub-directory. Never throws.
   *
   * @return one {@link DiscoveredApplication} per sub-directory, sorted by name; never
   *     {@code null}
   */
  @Override
  public List<DiscoveredApplication> fetchApplications() {
    String rootProblem = rootProblem();
    if (rootProblem != null) {
      log.warn("Local application directory '{}' {}", root, rootProblem);
      return List.of();
    }
    try (Stream<Path> entries = Files.list(root)) {
      List<DiscoveredApplication> apps = entries
          .filter(Files::isDirectory)
          .filter(dir -> !isHidden(dir))
          .sorted()
          .map(this::readApplication)
          .toList();
      log.info("Local application.yml enrichment: parsed {} application(s) from '{}'",
          apps.size(), root);
      return apps;
    } catch (IOException | UncheckedIOException e) {
      log.warn("Failed to list local application directory '{}': {}", root, e.getMessage());
      return List.of();
    }
  }

  /** Returns a short description of why the root cannot be scanned, or null when it can. */
  private String rootProblem() {
    if (!Files.exists(root)) {
      return "does not exist";
    }
    if (!Files.isDirectory(root)) {
      return "is not a directory";
    }
    if (!Files.isReadable(root)) {
      return "is not readable";
    }
    return null;
  }

  private static boolean isHidden(Path dir) {
    Path fileName = dir.getFileName();
    return fileName != null && fileName.toString().startsWith(".");
  }

  private DiscoveredApplication readApplication(Path appDir) {
    String name = appDir.getFileName().toString();
    String base = firstExisting(appDir, BASE_FILES);
    String profile = firstExisting(appDir, profileFiles(parser.activeProfile()));
    DiscoveredApplication app = parser.parse(name, "", base, profile);
    log.debug("App '{}': {} group ID(s), {} topic(s)",
        name, app.candidateGroupIds().size(), app.candidateTopicNames().size());
    return app;
  }

  private static List<String> profileFiles(String profile) {
    return List.of(
        "src/main/resources/application-" + profile + ".yml",
        "src/main/resources/application-" + profile + ".yaml",
        "application-" + profile + ".yml",
        "application-" + profile + ".yaml");
  }

  /** Returns the content of the first existing regular file among the candidates, else null. */
  private String firstExisting(Path appDir, List<String> relativePaths) {
    for (String relative : relativePaths) {
      Path file = appDir.resolve(relative);
      if (Files.isRegularFile(file)) {
        try {
          return Files.readString(file);
        } catch (IOException e) {
          log.warn("Could not read '{}': {}", file, e.getMessage());
          return null;
        }
      }
    }
    return null;
  }
}
