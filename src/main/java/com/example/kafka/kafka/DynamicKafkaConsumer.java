// src/main/java/com/example/kafka/kafka/DynamicKafkaConsumer.java
package com.example.kafka.kafka;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.stereotype.Component;

import com.example.kafka.service.Transformer;
import com.example.kafka.sink.SinkRouter;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class DynamicKafkaConsumer {

  private final ConsumerFactory<String, String> consumerFactory;
  private final Transformer transformer;
  private final SinkRouter sinks;

  // ✅ read from YAML (since this is NOT created by @KafkaListener factory)
  private final int concurrency;
  private final AckMode ackMode;
  private final long pollTimeoutMs;
  private final boolean missingTopicsFatal;

  private volatile ConcurrentMessageListenerContainer<String, String> container;
  private volatile String currentTopic;

  public DynamicKafkaConsumer(
      ConsumerFactory<String, String> consumerFactory,
      @Qualifier("kafkaTransformer") Transformer transformer,
      SinkRouter sinks,

      @Value("${spring.kafka.listener.concurrency:1}") int concurrency,
      @Value("${spring.kafka.listener.ack-mode:BATCH}") String ackMode,
      @Value("${app.kafka.dynamic.poll-timeout-ms:3000}") long pollTimeoutMs,
      @Value("${app.kafka.dynamic.missing-topics-fatal:true}") boolean missingTopicsFatal
  ) {
    this.consumerFactory = consumerFactory;
    this.transformer = transformer;
    this.sinks = sinks;

    this.concurrency = Math.max(1, concurrency);
    this.ackMode = parseAckMode(ackMode);
    this.pollTimeoutMs = pollTimeoutMs;
    this.missingTopicsFatal = missingTopicsFatal;
  }

  /** Start consuming from the given topic (switches topic if already running). */
  public synchronized void start(String topic) {
    if (topic == null || topic.isBlank()) {
      throw new IllegalArgumentException("topic is blank");
    }

    // If already running on same topic, do nothing
    if (isRunning() && Objects.equals(this.currentTopic, topic)) {
      log.info("DynamicKafkaConsumer already running on topic={}", topic);
      return;
    }

    // If running on different topic, stop and recreate
    stop();

    log.info("DynamicKafkaConsumer starting on topic={}, concurrency={}, ackMode={}, pollTimeoutMs={}, missingTopicsFatal={}",
        topic, concurrency, ackMode, pollTimeoutMs, missingTopicsFatal);

    ContainerProperties props = new ContainerProperties(topic);
    props.setAckMode(ackMode);
    props.setPollTimeout(pollTimeoutMs);
    props.setMissingTopicsFatal(missingTopicsFatal);

    props.setMessageListener((MessageListener<String, String>) this::handleRecord);

    ConcurrentMessageListenerContainer<String, String> c =
        new ConcurrentMessageListenerContainer<>(consumerFactory, props);

    c.setConcurrency(concurrency);
    c.setBeanName("dynamic-kafka-consumer");

    this.container = c;
    this.currentTopic = topic;

    c.start();
  }

  /** Stop consuming (if running). */
  public synchronized void stop() {
    if (container != null) {
      try {
        log.info("DynamicKafkaConsumer stopping (topic={})", currentTopic);
        container.stop();
      } catch (Exception e) {
        log.warn("DynamicKafkaConsumer stop failed", e);
      } finally {
        container = null;
        currentTopic = null;
      }
    }
  }

  /** Pause consuming (if running). */
  public void pause() {
    var c = container;
    if (c != null && c.isRunning()) {
      // note: pause/resume affects delivery; polling may still occur internally
      log.info("DynamicKafkaConsumer pausing (topic={})", currentTopic);
      c.pause();
    }
  }

  /** Resume consuming (if running). */
  public void resume() {
    var c = container;
    if (c != null && c.isRunning()) {
      log.info("DynamicKafkaConsumer resuming (topic={})", currentTopic);
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

  // ─────────────────────────────────────────────────────────────────────────

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
      log.error("DynamicKafkaConsumer: failed to transform/send record (topic={}, offset={})",
          record.topic(), record.offset(), e);

      // best-effort DLT + error
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
    if (s == null || s.isBlank()) return AckMode.BATCH;
    try {
      return AckMode.valueOf(s.trim().toUpperCase());
    } catch (Exception ignored) {
      return AckMode.BATCH;
    }
  }
}
