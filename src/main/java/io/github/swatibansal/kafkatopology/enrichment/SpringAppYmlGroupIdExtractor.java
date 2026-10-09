package io.github.swatibansal.kafkatopology.enrichment;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import org.yaml.snakeyaml.error.YAMLException;

/**
 * Parses Spring Boot {@code application.yml} files and extracts Kafka consumer group IDs and
 * producer topic names for a given active profile.
 *
 * <h2>Multi-document profile resolution</h2>
 *
 * <p>Spring Boot's multi-document YAML format uses {@code ---} separators with
 * {@code spring.config.activate.on-profile} to declare profile-specific overrides. This extractor:
 *
 * <ol>
 *   <li>Parses every {@code ---}-separated document.
 *   <li>Finds the document matching {@code activeProfile} in
 *       {@code spring.config.activate.on-profile}.
 *   <li>Falls back to the default document (no profile activation) if no profile-specific block
 *       is present.
 *   <li>Recursively flattens the selected document's nested map into dotted keys.
 *   <li>Keeps values for keys that contain both {@code "kafka"} and {@code "group"}
 *       (case-insensitive, normalising {@code -} and {@code _} to {@code .}), plus Kafka Streams
 *       {@code application-id} keys, which double as consumer group ids.
 *   <li>Resolves Spring {@code ${KEY:defaultValue}} expressions and cross-document
 *       {@code ${some.nested.key}} property references. Resolution context is built by merging
 *       the default document's flat properties with the profile document's flat properties so
 *       that {@code group-id-base} values defined in the default doc are available when
 *       resolving the profile doc's {@code group-id} expressions.
 *   <li>Discards remaining placeholder values ({@code "CHANGE_ME"} or blank).
 * </ol>
 *
 * <p>This handles two common patterns found in Spring Boot consumers:
 *
 * <p><strong>Pattern 1 — literal group-id per profile:</strong>
 * <pre>{@code
 * external:
 *   kafka:
 *     order-created-group-id: "CHANGE_ME"   # default placeholder
 * ---
 * spring:
 *   config:
 *     activate:
 *       on-profile: [qa]
 * external:
 *   kafka:
 *     order-created-group-id: "qa-order-created-cg-v1"  # real value
 * }</pre>
 *
 * <p><strong>Pattern 2 — hierarchical group-id-base with cross-document reference:</strong>
 * <pre>{@code
 * shop:
 *   kafka:
 *     consumer:
 *       reindex:
 *         group-id-base: "catalog-reindex-group-v1"    # base in default doc
 * ---
 * spring:
 *   config:
 *     activate:
 *       on-profile: [qa]
 * shop:
 *   kafka:
 *     consumer:
 *       reindex:
 *         group-id: "qa-${shop.kafka.consumer.reindex.group-id-base}"  # cross-doc ref
 * }</pre>
 */
public class SpringAppYmlGroupIdExtractor {

  private static final Logger log = LoggerFactory.getLogger(SpringAppYmlGroupIdExtractor.class);

  /**
   * Extracts consumer group ID values from a Spring Boot {@code application.yml} file.
   *
   * @param yamlContent  raw YAML file content (may be multi-document)
   * @param activeProfile the Spring profile to prefer (e.g. {@code "qa"})
   * @return deduplicated list of consumer group ID values; never {@code null}
   */
  public List<String> extract(String yamlContent, String activeProfile) {
    return extractMatching(yamlContent, activeProfile, this::isKafkaGroupKey, "group ID");
  }

  /**
   * Extracts Kafka producer topic name values from a Spring Boot {@code application.yml} file.
   *
   * <p>Uses the same multi-document profile resolution as {@link #extract}, but filters keys
   * using {@link #isKafkaTopicKey} (keys containing both {@code "kafka"} and {@code "topic"}).
   *
   * @param yamlContent  raw YAML file content (may be multi-document)
   * @param activeProfile the Spring profile to prefer (e.g. {@code "qa"})
   * @return deduplicated list of topic name values; never {@code null}
   */
  public List<String> extractTopics(String yamlContent, String activeProfile) {
    return extractMatching(yamlContent, activeProfile, this::isKafkaTopicKey, "topic name");
  }

  /**
   * Shared extraction pipeline: parses the documents, selects the profile (or default) document,
   * flattens it, keeps the values whose keys satisfy {@code keyMatcher}, and resolves them against
   * the merged default-plus-profile context.
   *
   * @param yamlContent raw YAML file content (may be multi-document)
   * @param activeProfile the Spring profile to prefer
   * @param keyMatcher predicate selecting the flattened keys of interest
   * @param what human-readable noun for debug logging (e.g. {@code "group ID"})
   * @return deduplicated, resolved, non-placeholder values; never {@code null}
   */
  private List<String> extractMatching(
      String yamlContent, String activeProfile, Predicate<String> keyMatcher, String what) {
    if (yamlContent == null || yamlContent.isBlank()) {
      return List.of();
    }
    List<Map<String, Object>> documents = parseDocuments(yamlContent);
    if (documents.isEmpty()) {
      return List.of();
    }

    // Prefer the profile-specific document; fall back to default
    Map<String, Object> selected = findProfileDoc(documents, activeProfile);
    if (selected == null) {
      selected = findDefaultDoc(documents);
    }
    if (selected == null) {
      log.debug("No matching profile '{}' or default document found in YAML", activeProfile);
      return List.of();
    }

    Map<String, String> context = buildContext(documents, selected);
    Map<String, String> flat = new LinkedHashMap<>();
    flattenMap(selected, "", flat);

    Set<String> results = new LinkedHashSet<>();
    for (Map.Entry<String, String> entry : flat.entrySet()) {
      if (keyMatcher.test(entry.getKey())) {
        String resolved = resolveWithContext(entry.getValue(), context);
        if (resolved != null && !isPlaceholder(resolved)) {
          results.add(resolved);
        }
      }
    }

    if (!results.isEmpty()) {
      log.debug("Extracted {} {}(s) for profile '{}': {}", results.size(), what,
          activeProfile, results);
    }
    return List.copyOf(results);
  }

  // ── YAML parsing ─────────────────────────────────────────────────────────────

  @SuppressWarnings("unchecked")
  private List<Map<String, Object>> parseDocuments(String yamlContent) {
    List<Map<String, Object>> docs = new ArrayList<>();
    try {
      Yaml yaml = new Yaml(new SafeConstructor(new LoaderOptions()));
      for (Object doc : yaml.loadAll(yamlContent)) {
        if (doc instanceof Map<?, ?> map) {
          docs.add((Map<String, Object>) map);
        }
      }
    } catch (YAMLException | ClassCastException e) {
      log.warn("Failed to parse YAML content: {}", e.getMessage());
    }
    return docs;
  }

  // ── Profile document selection ────────────────────────────────────────────────

  /**
   * Returns the first document whose {@code spring.config.activate.on-profile} list contains the
   * given profile, or {@code null} if none match.
   */
  private Map<String, Object> findProfileDoc(
      List<Map<String, Object>> docs, String profile) {
    for (Map<String, Object> doc : docs) {
      if (isProfileDoc(doc, profile)) {
        return doc;
      }
    }
    return null;
  }

  /**
   * Returns the first document that has no {@code spring.config.activate.on-profile} activation,
   * skipping the typical "first default doc" check if it has a large {@code spring} block but no
   * profile activation. Returns {@code null} if all docs have profile activations.
   */
  private Map<String, Object> findDefaultDoc(List<Map<String, Object>> docs) {
    for (Map<String, Object> doc : docs) {
      if (!hasProfileActivation(doc)) {
        return doc;
      }
    }
    return null;
  }

  @SuppressWarnings("unchecked")
  private boolean isProfileDoc(Map<String, Object> doc, String profile) {
    try {
      Object spring = doc.get("spring");
      if (!(spring instanceof Map)) {
        return false;
      }
      Object config = ((Map<String, Object>) spring).get("config");
      if (!(config instanceof Map)) {
        return false;
      }
      Object activate = ((Map<String, Object>) config).get("activate");
      if (!(activate instanceof Map)) {
        return false;
      }
      Object onProfile = ((Map<String, Object>) activate).get("on-profile");
      if (onProfile instanceof List<?> list) {
        return list.stream().anyMatch(p -> profile.equalsIgnoreCase(String.valueOf(p)));
      }
      if (onProfile instanceof String s) {
        return profile.equalsIgnoreCase(s.trim());
      }
    } catch (ClassCastException e) {
      // malformed structure; not a profile doc
    }
    return false;
  }

  @SuppressWarnings("unchecked")
  private boolean hasProfileActivation(Map<String, Object> doc) {
    try {
      Object spring = doc.get("spring");
      if (!(spring instanceof Map)) {
        return false;
      }
      Object config = ((Map<String, Object>) spring).get("config");
      if (!(config instanceof Map)) {
        return false;
      }
      Object activate = ((Map<String, Object>) config).get("activate");
      if (!(activate instanceof Map)) {
        return false;
      }
      return ((Map<String, Object>) activate).containsKey("on-profile");
    } catch (ClassCastException e) {
      return false;
    }
  }

  // ── Map flattening ───────────────────────────────────────────────────────────

  @SuppressWarnings("unchecked")
  private void flattenMap(Map<String, Object> map, String prefix, Map<String, String> out) {
    for (Map.Entry<?, Object> entry : map.entrySet()) {
      // Use String.valueOf to safely handle non-String keys (e.g. integer keys in YAML)
      String rawKey = String.valueOf(entry.getKey());
      String key = prefix.isEmpty() ? rawKey : prefix + "." + rawKey;
      Object val = entry.getValue();
      if (val instanceof Map<?, ?> nested) {
        flattenMap((Map<String, Object>) nested, key, out);
      } else if (val instanceof String strVal && !strVal.isBlank()) {
        out.put(key, strVal);
      }
    }
  }

  // ── Key and value matching ───────────────────────────────────────────────────

  /**
   * Returns {@code true} if the dotted key looks like a Kafka consumer group key.
   *
   * <p>Two shapes qualify after normalising separators to {@code .}: keys containing both
   * {@code "kafka"} and {@code "group"} (e.g. {@code spring.kafka.consumer.group-id}), and Kafka
   * Streams application-id keys in kebab, snake, dotted or camelCase form (e.g.
   * {@code spring.kafka.streams.application-id} or {@code spring.kafka.streams.applicationId}),
   * because a Streams application id is also its consumer group id on the broker.
   */
  private boolean isKafkaGroupKey(String key) {
    String normalised = key.toLowerCase(Locale.ROOT).replace('-', '.').replace('_', '.');
    if (normalised.contains("kafka") && normalised.contains("group")) {
      return true;
    }
    String compact = normalised.replace(".", "");
    return normalised.contains("kafka")
        && normalised.contains("streams")
        && compact.endsWith("applicationid");
  }

  /**
   * Returns {@code true} if the dotted key looks like a Kafka producer topic key.
   * Checks that the normalised key (separators replaced with {@code .}) contains both
   * {@code "kafka"} and {@code "topic"}.
   */
  private boolean isKafkaTopicKey(String key) {
    String normalised = key.toLowerCase(Locale.ROOT).replace('-', '.').replace('_', '.');
    return normalised.contains("kafka") && normalised.contains("topic");
  }

  /**
   * Resolves a raw YAML string value against an empty context. Delegates to
   * {@link #resolveWithContext(String, Map)} with no context properties, which preserves
   * existing behaviour: {@code ${KEY:default}} yields {@code "default"}, bare {@code ${KEY}}
   * yields {@code null}.
   *
   * @param raw raw value from the YAML document (may be a Spring expression)
   * @return the resolved string value, or {@code null} if unresolvable
   */
  String resolveValue(String raw) {
    return resolveWithContext(raw, Map.of());
  }

  /**
   * Resolves a raw YAML string value by substituting all {@code ${KEY}} and
   * {@code ${KEY:defaultValue}} placeholders found anywhere in the string.
   *
   * <p>Resolution order for each placeholder:
   * <ol>
   *   <li>Look up {@code KEY} in {@code context} (exact match, then hyphen/underscore variants).
   *   <li>Fall back to the literal {@code defaultValue} after the colon, if present.
   *   <li>Return {@code null} if neither source is available.
   * </ol>
   *
   * <p>Supports mixed strings such as {@code "qa-${some.nested.group-id-base}"} where literal
   * text and property references are combined. Context values are treated as literals and are
   * not recursively resolved.
   *
   * @param raw raw value from the YAML document
   * @param context flat property map used to resolve cross-document references
   * @return the fully resolved string, or {@code null} if any placeholder cannot be resolved
   */
  String resolveWithContext(String raw, Map<String, String> context) {
    if (raw == null) {
      return null;
    }
    String trimmed = raw.trim();
    if (!trimmed.contains("${")) {
      return trimmed;
    }

    StringBuilder result = new StringBuilder();
    int i = 0;
    while (i < trimmed.length()) {
      int start = trimmed.indexOf("${", i);
      if (start < 0) {
        result.append(trimmed, i, trimmed.length());
        break;
      }
      result.append(trimmed, i, start);
      int end = trimmed.indexOf('}', start);
      if (end < 0) {
        return null; // malformed — no closing brace
      }
      String expr = trimmed.substring(start + 2, end);
      int colonIdx = expr.indexOf(':');
      String key = colonIdx >= 0 ? expr.substring(0, colonIdx) : expr;
      String defaultVal = colonIdx >= 0 ? expr.substring(colonIdx + 1).trim() : null;

      String value = lookupInContext(key, context);
      if (value != null) {
        result.append(value);
      } else if (defaultVal != null) {
        result.append(defaultVal);
      } else {
        return null; // bare ${KEY} with no default and not found in context
      }
      i = end + 1;
    }

    return result.toString().trim();
  }

  /**
   * Looks up a property key in the context map. If not found by exact match, retries with
   * hyphens and underscores normalised to match Spring's relaxed binding conventions.
   *
   * @param key property key extracted from a {@code ${...}} placeholder
   * @param context flat property map
   * @return the matching value, or {@code null} if absent
   */
  private String lookupInContext(String key, Map<String, String> context) {
    String value = context.get(key);
    if (value != null) {
      return value;
    }
    // Try with hyphens replaced by underscores and vice versa (Spring relaxed binding)
    value = context.get(key.replace('-', '_'));
    if (value != null) {
      return value;
    }
    return context.get(key.replace('_', '-'));
  }

  /**
   * Builds a resolution context by merging the flat properties from the default document (lower
   * priority) with those from the selected profile document (higher priority). This allows
   * profile-doc expressions to reference base values defined only in the default doc.
   *
   * @param documents all parsed YAML documents
   * @param selectedDoc the document selected for key extraction (profile or default)
   * @return merged flat property map; never {@code null}
   */
  private Map<String, String> buildContext(
      List<Map<String, Object>> documents, Map<String, Object> selectedDoc) {
    Map<String, String> context = new LinkedHashMap<>();
    Map<String, Object> defaultDoc = findDefaultDoc(documents);
    if (defaultDoc != null) {
      flattenMap(defaultDoc, "", context);
    }
    if (selectedDoc != null && selectedDoc != defaultDoc) {
      flattenMap(selectedDoc, "", context); // profile doc overrides defaults
    }
    return context;
  }

  /**
   * Returns {@code true} for values that are clearly unresolved placeholders and should be
   * skipped: blank strings or the literal {@code "CHANGE_ME"}.
   *
   * <p>Spring {@code ${KEY:defaultValue}} expressions are handled upstream by
   * {@link #resolveValue} — by the time this method is called the value has already been
   * resolved to its default (or {@code null} for bare {@code ${KEY}} expressions).
   */
  private boolean isPlaceholder(String value) {
    if (value == null || value.isBlank()) {
      return true;
    }
    return "CHANGE_ME".equalsIgnoreCase(value.trim());
  }
}
