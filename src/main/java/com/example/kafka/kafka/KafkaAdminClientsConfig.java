// src/main/java/com/example/kafka/kafka/KafkaAdminClientsConfig.java
package com.example.kafka.kafka;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaAdminClientsConfig {

  /**
   * OUTPUT AdminClient (built from spring.kafka.producer.*)
   * Used for validating output topic partitions.
   */
  @Bean(name = "outputAdminClient")
  public AdminClient outputAdminClient(KafkaProperties kafkaProperties) {
    Map<String, Object> cfg = new HashMap<>();

    // Use producer settings because output topic is on producer cluster
    cfg.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
        kafkaProperties.getProducer().getBootstrapServers());

    // copy producer "properties" (security.protocol, sasl.*, ssl.* etc)
    cfg.putAll(kafkaProperties.getProducer().getProperties());

    // optional client id (helps logs)
    cfg.putIfAbsent(AdminClientConfig.CLIENT_ID_CONFIG, "kafka-app-output-admin");

    return AdminClient.create(cfg);
  }
}
