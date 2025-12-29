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
   * Used for validating output topic partitions and verifying output topics.
   */
  @Bean(name = "outputAdminClient", destroyMethod = "close")
  public AdminClient outputAdminClient(KafkaProperties kafkaProperties) {
    Map<String, Object> cfg = new HashMap<>(kafkaProperties.buildProducerProperties(null));
    cfg.putIfAbsent(AdminClientConfig.CLIENT_ID_CONFIG, "kafka-app-output-admin");
    return AdminClient.create(cfg);
  }
}
