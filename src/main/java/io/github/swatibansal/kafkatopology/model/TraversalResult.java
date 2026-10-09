package io.github.swatibansal.kafkatopology.model;

import java.util.List;

/**
 * The result of a topology traversal starting from a single Kafka topic.
 *
 * <p>Contains every distinct path from the starting topic to a leaf (final destination) node,
 * the deduplicated list of those destinations, and a pre-computed path count for convenience.
 *
 * @param startTopic name of the topic used as the traversal entry point
 * @param pathCount total number of paths discovered (equals {@code paths.size()})
 * @param paths all paths found; each path ends at a {@link NodeType#CONSUMER_GROUP},
 *     {@link NodeType#STREAMS_APP}, or the start topic itself if it has no consumers
 * @param finalDestinations deduplicated list of destination nodes across all paths
 */
public record TraversalResult(
    String startTopic,
    int pathCount,
    List<TracedPath> paths,
    List<TopologyNode> finalDestinations) {}
