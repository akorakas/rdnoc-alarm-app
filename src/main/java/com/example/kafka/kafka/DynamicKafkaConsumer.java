package com.example.kafka.kafka;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Qualifier;
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

  private volatile ConcurrentMessageListenerContainer<String, String> container;
  private volatile String currentTopic;

  public DynamicKafkaConsumer(
      ConsumerFactory<String, String> consumerFactory,
      @Qualifier("kafkaTransformer") Transformer transformer,
      SinkRouter sinks
  ) {
    this.consumerFactory = consumerFactory;
    this.transformer = transformer;
    this.sinks = sinks;
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

    log.info("DynamicKafkaConsumer starting on topic={}", topic);

    ContainerProperties props = new ContainerProperties(topic);
    props.setAckMode(AckMode.BATCH);          // matches your yaml intent
    props.setPollTimeout(3000);

    props.setMessageListener((MessageListener<String, String>) this::handleRecord);

    ConcurrentMessageListenerContainer<String, String> c =
        new ConcurrentMessageListenerContainer<>(consumerFactory, props);

    // optional: name it for logs/metrics
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

      // send raw to DLT + error message to error sink (best-effort)
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
}
