package io.github.swatibansal.kafkatopology;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

/**
 * Verifies that the Spring application context loads successfully without a live broker.
 *
 * <p>{@link org.apache.kafka.clients.admin.AdminClient} creation does not open a network
 * connection eagerly, so the context loads even when {@code localhost:9092} is unreachable. The
 * {@link io.github.swatibansal.kafkatopology.service.TopologyCacheService#init()} method attempts
 * the initial discovery, catches the connection failure, and leaves the cache empty — the context
 * load still succeeds.
 *
 * <p>The scheduler refresh interval is set to 1 hour to prevent background jobs from firing
 * during the test run.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@TestPropertySource(
    properties = {
      "kafka-agent.bootstrap-servers=localhost:9092",
      "kafka-agent.security.protocol=PLAINTEXT",
      "kafka-agent.admin.request-timeout-ms=5000",
      "kafka-agent.admin.default-api-timeout-ms=5000",
      "kafka-agent.cache.refresh-interval-ms=3600000"
    })
class KafkaTopologyExplorerApplicationTest {

  @Test
  void contextLoads() {
    // Passes when all beans are created and wired without errors.
  }
}
