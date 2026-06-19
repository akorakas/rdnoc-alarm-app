package gr.ote.rdnoc.alarm.nsp;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class DefaultNspKafkaAdminClientFactory implements NspKafkaAdminClientFactory {

  private final String securityProtocol;
  private final String truststoreLocation;
  private final String truststorePassword;
  private final String truststoreType;
  private final String keystoreLocation;
  private final String keystorePassword;
  private final String keystoreType;
  private final String keyPassword;

  public DefaultNspKafkaAdminClientFactory(
      @Value("${spring.kafka.properties.security.protocol:}") String securityProtocol,
      @Value("${spring.kafka.properties.ssl.truststore.location:}") String truststoreLocation,
      @Value("${spring.kafka.properties.ssl.truststore.password:}") String truststorePassword,
      @Value("${spring.kafka.properties.ssl.truststore.type:}") String truststoreType,
      @Value("${spring.kafka.properties.ssl.keystore.location:}") String keystoreLocation,
      @Value("${spring.kafka.properties.ssl.keystore.password:}") String keystorePassword,
      @Value("${spring.kafka.properties.ssl.keystore.type:}") String keystoreType,
      @Value("${spring.kafka.properties.ssl.key.password:}") String keyPassword
  ) {
    this.securityProtocol = securityProtocol;
    this.truststoreLocation = truststoreLocation;
    this.truststorePassword = truststorePassword;
    this.truststoreType = truststoreType;
    this.keystoreLocation = keystoreLocation;
    this.keystorePassword = keystorePassword;
    this.keystoreType = keystoreType;
    this.keyPassword = keyPassword;
  }

  @Override
  public AdminClient create(String bootstrapServers) {
    Map<String, Object> props = new HashMap<>();

    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(AdminClientConfig.CLIENT_ID_CONFIG, "rdnoc-nsp-input-admin");

    putIfPresent(props, "security.protocol", securityProtocol);

    putIfPresent(props, "ssl.truststore.location", truststoreLocation);
    putIfPresent(props, "ssl.truststore.password", truststorePassword);
    putIfPresent(props, "ssl.truststore.type", truststoreType);

    putIfPresent(props, "ssl.keystore.location", keystoreLocation);
    putIfPresent(props, "ssl.keystore.password", keystorePassword);
    putIfPresent(props, "ssl.keystore.type", keystoreType);
    putIfPresent(props, "ssl.key.password", keyPassword);

    return AdminClient.create(props);
  }

  private void putIfPresent(Map<String, Object> props, String key, String value) {
    if (value != null && !value.isBlank()) {
      props.put(key, value);
    }
  }
}