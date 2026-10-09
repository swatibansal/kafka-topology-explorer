package io.github.swatibansal.kafkatopology.enrichment;

import io.github.swatibansal.kafkatopology.enrichment.model.DiscoveredApplication;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Turns the raw content of an application's Spring {@code application.yml} files into a
 * {@link DiscoveredApplication}.
 *
 * <p>Two inputs are accepted per application: the base file ({@code application.yml}, possibly
 * multi-document) and an optional profile-specific file ({@code application-<profile>.yml}).
 * Both are passed through {@link SpringAppYmlGroupIdExtractor} for the configured active profile
 * and the results are merged, base entries first, duplicates removed.
 *
 * <p>Shared by every {@link ApplicationSource} so the GitHub and local-directory sources behave
 * identically once they have read the file contents.
 */
public final class AppYmlApplicationParser {

  private final SpringAppYmlGroupIdExtractor extractor;
  private final String activeProfile;

  /**
   * Constructs a parser.
   *
   * @param extractor     YAML extractor for group IDs and topic names
   * @param activeProfile Spring profile whose values should be extracted (e.g. {@code "prod"})
   * @throws IllegalArgumentException if {@code activeProfile} is blank
   */
  public AppYmlApplicationParser(SpringAppYmlGroupIdExtractor extractor, String activeProfile) {
    this.extractor = Objects.requireNonNull(extractor, "extractor");
    this.activeProfile = Objects.requireNonNull(activeProfile, "activeProfile");
    if (activeProfile.isBlank()) {
      throw new IllegalArgumentException("activeProfile must not be blank");
    }
  }

  /**
   * Returns the active profile this parser extracts for.
   *
   * @return profile name; never blank
   */
  public String activeProfile() {
    return activeProfile;
  }

  /**
   * Parses the given file contents into a {@link DiscoveredApplication}.
   *
   * @param name           application name
   * @param namespace      grouping label; empty when unknown
   * @param baseContent    content of {@code application.yml}, or {@code null} if absent
   * @param profileContent content of {@code application-<profile>.yml}, or {@code null} if absent
   * @return application with merged, deduplicated candidates; never {@code null}
   */
  public DiscoveredApplication parse(
      String name, String namespace, String baseContent, String profileContent) {
    if (baseContent == null && profileContent == null) {
      return DiscoveredApplication.empty(name, namespace);
    }
    List<String> groupIds =
        baseContent != null ? extractor.extract(baseContent, activeProfile) : List.of();
    List<String> topics =
        baseContent != null ? extractor.extractTopics(baseContent, activeProfile) : List.of();
    if (profileContent != null) {
      groupIds = merge(groupIds, extractor.extract(profileContent, activeProfile));
      topics = merge(topics, extractor.extractTopics(profileContent, activeProfile));
    }
    return new DiscoveredApplication(name, namespace, groupIds, topics);
  }

  /**
   * Returns a deduplicated merge of {@code base} and {@code extra}, preserving insertion order.
   */
  private static List<String> merge(List<String> base, List<String> extra) {
    if (extra.isEmpty()) {
      return base;
    }
    Set<String> combined = new LinkedHashSet<>(base);
    combined.addAll(extra);
    return List.copyOf(combined);
  }
}
