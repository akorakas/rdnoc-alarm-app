package gr.ote.rdnoc.alarm.kafka;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.stereotype.Component;

import gr.ote.rdnoc.alarm.service.Transformer;
import gr.ote.rdnoc.alarm.sink.SinkRouter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class DynamicKafkaConsumer {

  private final ConsumerFactory<String, String> defaultConsumerFactory;
  private final KafkaProperties kafkaProperties;
  private final Transformer transformer;
  private final SinkRouter sinks;

  private final int concurrency;
  private final AckMode ackMode;
  private final long pollTimeoutMs;
  private final boolean missingTopicsFatal;

  private volatile ConcurrentMessageListenerContainer<String, String> container;
  private volatile String currentTopic;
  private volatile String currentBootstrapServers;

  public DynamicKafkaConsumer(
      ConsumerFactory<String, String> consumerFactory,
      KafkaProperties kafkaProperties,
      @Qualifier("kafkaTransformer") Transformer transformer,
      SinkRouter sinks,

      @Value("${spring.kafka.listener.concurrency:1}") int concurrency,
      @Value("${spring.kafka.listener.ack-mode:BATCH}") String ackMode,
      @Value("${app.kafka.dynamic.poll-timeout-ms:3000}") long pollTimeoutMs,
      @Value("${app.kafka.dynamic.missing-topics-fatal:true}") boolean missingTopicsFatal
  ) {
    this.defaultConsumerFactory = consumerFactory;
    this.kafkaProperties = kafkaProperties;
    this.transformer = transformer;
    this.sinks = sinks;

    this.concurrency = Math.max(1, concurrency);
    this.ackMode = parseAckMode(ackMode);
    this.pollTimeoutMs = pollTimeoutMs;
    this.missingTopicsFatal = missingTopicsFatal;
  }

  /**
   * Backward-compatible start method.
   * Uses spring.kafka.consumer.bootstrap-servers.
   */
  public synchronized void start(String topic) {
    start(topic, null);
  }

  /**
   * Start consuming from a runtime topic and runtime bootstrap servers.
   *
   * This is required for NSP failover because:
   * - Site A creates topic A on Site A Kafka
   * - Site B creates topic B on Site B Kafka
   */
  public synchronized void start(String topic, String kafkaBootstrapServers) {
    if (topic == null || topic.isBlank()) {
      throw new IllegalArgumentException("topic is blank");
    }

    String effectiveBootstrapServers = normalizeBootstrapServers(kafkaBootstrapServers);

    if (
        isRunning()
            && Objects.equals(this.currentTopic, topic)
            && Objects.equals(this.currentBootstrapServers, effectiveBootstrapServers)
    ) {
      log.info("DynamicKafkaConsumer already running on topic={}, bootstrapServers={}",
          topic, effectiveBootstrapServers);
      return;
    }

    stop();

    log.info("DynamicKafkaConsumer starting on topic={}, bootstrapServers={}, concurrency={}, ackMode={}, pollTimeoutMs={}, missingTopicsFatal={}",
        topic, effectiveBootstrapServers, concurrency, ackMode, pollTimeoutMs, missingTopicsFatal);

    ContainerProperties props = new ContainerProperties(topic);
    props.setAckMode(ackMode);
    props.setPollTimeout(pollTimeoutMs);
    props.setMissingTopicsFatal(missingTopicsFatal);
    props.setMessageListener((MessageListener<String, String>) this::handleRecord);

    ConsumerFactory<String, String> runtimeConsumerFactory =
        createConsumerFactory(effectiveBootstrapServers);

    ConcurrentMessageListenerContainer<String, String> c =
        new ConcurrentMessageListenerContainer<>(runtimeConsumerFactory, props);

    c.setConcurrency(concurrency);
    c.setBeanName("dynamic-kafka-consumer-" + topic);

    this.container = c;
    this.currentTopic = topic;
    this.currentBootstrapServers = effectiveBootstrapServers;

    c.start();
  }

  public synchronized void stop() {
    if (container != null) {
      try {
        log.info("DynamicKafkaConsumer stopping. topic={}, bootstrapServers={}",
            currentTopic, currentBootstrapServers);
        container.stop();
      } catch (Exception e) {
        log.warn("DynamicKafkaConsumer stop failed", e);
      } finally {
        container = null;
        currentTopic = null;
        currentBootstrapServers = null;
      }
    }
  }

  public void pause() {
    var c = container;
    if (c != null && c.isRunning()) {
      log.info("DynamicKafkaConsumer pausing. topic={}, bootstrapServers={}",
          currentTopic, currentBootstrapServers);
      c.pause();
    }
  }

  public void resume() {
    var c = container;
    if (c != null && c.isRunning()) {
      log.info("DynamicKafkaConsumer resuming. topic={}, bootstrapServers={}",
          currentTopic, currentBootstrapServers);
      c.resume();
    }
  }

  public boolean isRunning() {
    var c = container;
    return c != null && c.isRunning();
  }

  public String currentTopic() {
    return currentTopic;
  }

  public String currentBootstrapServers() {
    return currentBootstrapServers;
  }

  private ConsumerFactory<String, String> createConsumerFactory(String bootstrapServers) {
    if (bootstrapServers == null || bootstrapServers.isBlank()) {
      return defaultConsumerFactory;
    }

    Map<String, Object> props = new HashMap<>(kafkaProperties.buildConsumerProperties(null));
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

    return new DefaultKafkaConsumerFactory<>(props);
  }

  private String normalizeBootstrapServers(String bootstrapServers) {
    if (bootstrapServers != null && !bootstrapServers.isBlank()) {
      return bootstrapServers.trim();
    }

    Object configured = kafkaProperties.buildConsumerProperties(null)
        .get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);

    return configured == null ? null : configured.toString();
  }

  private void handleRecord(ConsumerRecord<String, String> record) {
    Map<String, String> headers = new HashMap<>();
    headers.put("source", "KAFKA");
    headers.put("kafka-topic", record.topic());
    headers.put("kafka-partition", String.valueOf(record.partition()));
    headers.put("kafka-offset", String.valueOf(record.offset()));

    if (record.key() != null) {
      headers.put("kafka-key", record.key());
    }

    try {
      String outJson = transformer.transform(record.value());
      sinks.sendOutput(record.key(), outJson, headers);
    } catch (Exception e) {
      log.error("DynamicKafkaConsumer: failed to transform/send record. topic={}, offset={}",
          record.topic(), record.offset(), e);

      try {
        sinks.sendDlt(record.key(), record.value(), headers);
      } catch (Exception ex) {
        log.warn("DynamicKafkaConsumer: failed to send to DLT", ex);
      }

      try {
        sinks.sendError(record.key(), "{\"error\":\"transform/send failed\"}", headers);
      } catch (Exception ex) {
        log.warn("DynamicKafkaConsumer: failed to send to error sink", ex);
      }
    }
  }

  private static AckMode parseAckMode(String s) {
    if (s == null || s.isBlank()) {
      return AckMode.BATCH;
    }

    try {
      return AckMode.valueOf(s.trim().toUpperCase());
    } catch (Exception ignored) {
      return AckMode.BATCH;
    }
  }
}