package gr.ote.rdnoc.alarm.nsp;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.stereotype.Component;

@Component
public class DefaultNspKafkaAdminClientFactory implements NspKafkaAdminClientFactory {

  private final KafkaProperties kafkaProperties;

  public DefaultNspKafkaAdminClientFactory(KafkaProperties kafkaProperties) {
    this.kafkaProperties = kafkaProperties;
  }

  @Override
  public AdminClient create(String bootstrapServers) {
    if (bootstrapServers == null || bootstrapServers.isBlank()) {
      throw new IllegalArgumentException("bootstrapServers is blank");
    }

    /*
     * Important:
     * Build from spring.kafka.consumer.* because NSP input Kafka uses the
     * consumer-side SSL/truststore settings.
     *
     * Then override only bootstrap.servers because failover decides which
     * NSP Kafka site to use at runtime.
     */
    Map<String, Object> props = new HashMap<>(
        kafkaProperties.buildConsumerProperties()
    );

    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers.trim());
    props.put(AdminClientConfig.CLIENT_ID_CONFIG, "rdnoc-nsp-input-admin");

    /*
     * AdminClient does not need deserializers/group settings.
     * They are harmless, but removing them avoids confusing Kafka logs.
     */
    props.remove(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
    props.remove(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
    props.remove(ConsumerConfig.GROUP_ID_CONFIG);
    props.remove(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
    props.remove(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG);
    props.remove(ConsumerConfig.MAX_POLL_RECORDS_CONFIG);
    props.remove(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG);
    props.remove(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG);
    props.remove(ConsumerConfig.ISOLATION_LEVEL_CONFIG);

    return AdminClient.create(props);
  }
}