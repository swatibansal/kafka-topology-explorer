package io.github.swatibansal.kafkatopology.config;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configures and exposes a long-lived {@link AdminClient} bean for Kafka cluster introspection.
 *
 * <p>Spring closes the client gracefully on application shutdown via
 * {@code @Bean(destroyMethod = "close")}.
 */
@Configuration
public class KafkaAdminConfig {

  private static final String SASL_PROTOCOL_PREFIX = "SASL_";
  private static final String SCRAM_LOGIN_MODULE =
      "org.apache.kafka.common.security.scram.ScramLoginModule";
  private static final String PLAIN_LOGIN_MODULE =
      "org.apache.kafka.common.security.plain.PlainLoginModule";

  /**
   * Creates an {@link AdminClient} from the application properties.
   *
   * @param props application-level Kafka Topology Explorer properties
   * @return configured AdminClient instance
   */
  @Bean(destroyMethod = "close")
  public AdminClient adminClient(KafkaAgentProperties props) {
    return AdminClient.create(buildAdminConfig(props));
  }

  /**
   * Builds the AdminClient configuration map from the application properties. Exposed at package
   * level so the security wiring can be unit-tested without opening a broker connection.
   *
   * @param props application-level Kafka Topology Explorer properties
   * @return mutable configuration map suitable for {@link AdminClient#create(Map)}
   * @throws IllegalStateException if the SASL settings are inconsistent: only one of user name /
   *     password set without an explicit JAAS string, or credentials without a SASL mechanism
   */
  static Map<String, Object> buildAdminConfig(KafkaAgentProperties props) {
    var config = new HashMap<String, Object>();
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, props.bootstrapServers());
    config.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, props.admin().requestTimeoutMs());
    config.put(
        AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, props.admin().defaultApiTimeoutMs());
    applySecurityConfig(config, props.security());
    return config;
  }

  /**
   * Conditionally adds security entries to the AdminClient config map. Blank / null values are
   * ignored so that the default PLAINTEXT protocol requires no extra YAML keys.
   *
   * @param config mutable config map to populate
   * @param security security properties; may be {@code null}
   * @throws IllegalStateException if only one of user name / password is set under a SASL
   *     protocol without an explicit JAAS string, or if a JAAS string must be built from
   *     credentials without a SASL mechanism
   */
  private static void applySecurityConfig(
      Map<String, Object> config, KafkaAgentProperties.Security security) {
    if (security == null) {
      return;
    }
    if (isPresent(security.protocol())) {
      config.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, security.protocol());
    }
    boolean hasUserName = isPresent(security.saslUserName());
    boolean hasPassword = isPresent(security.saslPassword());
    boolean hasCredentials = hasUserName && hasPassword;
    boolean hasExplicitJaas = isPresent(security.saslJaasConfig());
    boolean buildsJaasFromCredentials = hasCredentials && !hasExplicitJaas;
    if (isSasl(security.protocol()) && !hasExplicitJaas && hasUserName != hasPassword) {
      throw new IllegalStateException(
          "KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD must be set together");
    }
    if (isSasl(security.protocol())
        && buildsJaasFromCredentials
        && !isPresent(security.saslMechanism())) {
      throw new IllegalStateException(
          "KAFKA_SASL_MECHANISM must be set (e.g. PLAIN or SCRAM-SHA-512) "
              + "when SASL credentials are supplied");
    }
    if (isPresent(security.saslMechanism())) {
      config.put(SaslConfigs.SASL_MECHANISM, security.saslMechanism());
    }
    if (hasExplicitJaas) {
      config.put(SaslConfigs.SASL_JAAS_CONFIG, security.saslJaasConfig());
    } else if (buildsJaasFromCredentials) {
      config.put(
          SaslConfigs.SASL_JAAS_CONFIG,
          buildJaasConfig(
              security.saslMechanism(), security.saslUserName(), security.saslPassword()));
    }
  }

  /**
   * Builds a JAAS config string from explicit username/password credentials. Both values are
   * escaped so that embedded quotes or backslashes cannot break out of the JAAS string.
   *
   * @param mechanism SASL mechanism (e.g. {@code SCRAM-SHA-512}, {@code PLAIN})
   * @param userName SASL username
   * @param password SASL password
   * @return well-formed JAAS config string suitable for {@code sasl.jaas.config}
   */
  private static String buildJaasConfig(String mechanism, String userName, String password) {
    String loginModule =
        (mechanism != null && mechanism.toUpperCase(Locale.ROOT).startsWith("SCRAM"))
            ? SCRAM_LOGIN_MODULE
            : PLAIN_LOGIN_MODULE;
    return loginModule
        + " required username=\"" + escapeJaas(userName)
        + "\" password=\"" + escapeJaas(password) + "\";";
  }

  /** Escapes backslashes and double quotes so the value is safe inside a JAAS quoted string. */
  private static String escapeJaas(String value) {
    return value.replace("\\", "\\\\").replace("\"", "\\\"");
  }

  private static boolean isSasl(String protocol) {
    return protocol != null
        && protocol.toUpperCase(Locale.ROOT).startsWith(SASL_PROTOCOL_PREFIX);
  }

  private static boolean isPresent(String value) {
    return value != null && !value.isBlank();
  }
}
