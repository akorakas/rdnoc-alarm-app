// src/main/java/com/example/kafka/config/AppKafkaProperties.java
package gr.ote.rdnoc.alarm.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.kafka")
public class AppKafkaProperties {
  /**
   * Kafka topic to CONSUME from.
   */
  private String inputTopic;

  public String getInputTopic() { return inputTopic; }
  public void setInputTopic(String inputTopic) { this.inputTopic = inputTopic; }
}
