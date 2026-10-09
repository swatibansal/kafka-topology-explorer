package io.github.swatibansal.kafkatopology;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Entry point for the Kafka Topology Explorer service.
 *
 * <p>Connects to a Kafka broker and builds a topology graph showing message flow from source
 * topics to all downstream consumer destinations. Exposes a REST API for querying the graph,
 * tracing paths, and exporting to Mermaid or Graphviz DOT format.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
@EnableScheduling
public class KafkaTopologyExplorerApplication {

  /** Application entry point. */
  public static void main(String[] args) {
    SpringApplication.run(KafkaTopologyExplorerApplication.class, args);
  }
}
