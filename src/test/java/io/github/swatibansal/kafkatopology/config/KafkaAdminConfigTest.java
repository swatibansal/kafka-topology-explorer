package io.github.swatibansal.kafkatopology.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.swatibansal.kafkatopology.config.KafkaAgentProperties.Admin;
import io.github.swatibansal.kafkatopology.config.KafkaAgentProperties.Security;
import java.util.Map;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.junit.jupiter.api.Test;

/** Unit tests for the AdminClient configuration map built by {@link KafkaAdminConfig}. */
class KafkaAdminConfigTest {

  private static final String SCRAM_MODULE =
      "org.apache.kafka.common.security.scram.ScramLoginModule";
  private static final String PLAIN_MODULE =
      "org.apache.kafka.common.security.plain.PlainLoginModule";

  @Test
  void buildAdminConfig_explicitJaasWithCredentials_acceptedWithoutMechanism() {
    String jaas = PLAIN_MODULE + " required username=\"bob\" password=\"explicit\";";
    var props = props(new Security("SASL_SSL", "", jaas, "alice", "secret"));

    Map<String, Object> config = KafkaAdminConfig.buildAdminConfig(props);

    assertThat(config)
        .containsEntry(SaslConfigs.SASL_JAAS_CONFIG, jaas)
        .doesNotContainKey(SaslConfigs.SASL_MECHANISM);
  }

  @Test
  void buildAdminConfig_explicitJaasWithUserNameOnly_ignoresHalfSuppliedCredentials() {
    String jaas = PLAIN_MODULE + " required username=\"bob\" password=\"explicit\";";
    var props = props(new Security("SASL_SSL", "", jaas, "alice", ""));

    Map<String, Object> config = KafkaAdminConfig.buildAdminConfig(props);

    assertThat(config)
        .containsEntry(SaslConfigs.SASL_JAAS_CONFIG, jaas)
        .doesNotContainKey(SaslConfigs.SASL_MECHANISM);
  }

  @Test
  void buildAdminConfig_plainMechanism_selectsPlainLoginModule() {
    var props = props(new Security("SASL_SSL", "PLAIN", "", "alice", "secret"));

    Map<String, Object> config = KafkaAdminConfig.buildAdminConfig(props);

    assertThat(config).containsEntry(
        SaslConfigs.SASL_JAAS_CONFIG,
        PLAIN_MODULE + " required username=\"alice\" password=\"secret\";");
  }

  @Test
  void buildAdminConfig_userNameWithoutPassword_throws() {
    var props = props(new Security("SASL_SSL", "PLAIN", "", "alice", ""));

    assertThatThrownBy(() -> KafkaAdminConfig.buildAdminConfig(props))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("must be set together");
  }

  @Test
  void buildAdminConfig_scramCredentials_escapesQuotesAndBackslashesInJaas() {
    var props = props(new Security("SASL_PLAINTEXT", "SCRAM-SHA-512", "", "alice", "p\"a\\ss"));

    Map<String, Object> config = KafkaAdminConfig.buildAdminConfig(props);

    assertThat(config)
        .containsEntry(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT")
        .containsEntry(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-512")
        .containsEntry(
            SaslConfigs.SASL_JAAS_CONFIG,
            SCRAM_MODULE + " required username=\"alice\" password=\"p\\\"a\\\\ss\";");
  }

  @Test
  void buildAdminConfig_saslCredentialsWithoutMechanism_throws() {
    var props = props(new Security("SASL_SSL", "", "", "alice", "secret"));

    assertThatThrownBy(() -> KafkaAdminConfig.buildAdminConfig(props))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("KAFKA_SASL_MECHANISM must be set");
  }

  @Test
  void buildAdminConfig_plaintext_addsNoSaslEntries() {
    var props = props(new Security("PLAINTEXT", "", "", "", ""));

    Map<String, Object> config = KafkaAdminConfig.buildAdminConfig(props);

    assertThat(config)
        .containsEntry(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        .containsEntry(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT");
    assertThat(config.keySet()).noneMatch(key -> key.startsWith("sasl."));
  }

  private static KafkaAgentProperties props(Security security) {
    return new KafkaAgentProperties(
        "localhost:9092", security, new Admin(30000, 30000), null, null, null, null);
  }
}
