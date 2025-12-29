package com.example.kafka.kafka;

import java.nio.charset.StandardCharsets;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import gr.ote.atlas.events.enums.EMSId;
import gr.ote.atlas.events.models.UnifiedEvent;

@Component
public class KafkaProducers {

  private static final Logger log = LoggerFactory.getLogger(KafkaProducers.class);

  private final KafkaTemplate<String, Object> template;

  public KafkaProducers(KafkaTemplate<String, Object> template) {
    this.template = template;
  }

  // --------------------------------------------------------------------------
  // ✅ NEW: Always produce with:
  //    key       = sourceEms (e.g. "NSP_ATNOI")
  //    partition = EMSId.ordinal() (e.g. 30)
  // --------------------------------------------------------------------------

  /** Use when you already have UnifiedEvent (best). */
  public void sendByEms(String topic, UnifiedEvent ue) {
    String emsKey = (ue != null && ue.getSourceEms() != null) ? ue.getSourceEms().name() : null;
    sendByEms(topic, emsKey, ue);
  }

  /** Use when you have JSON/string/etc but you know sourceEms. */
  public void sendByEms(String topic, String sourceEms, Object value) {
    String key = normalizeKey(sourceEms);
    Integer partition = partitionFromKey(key);

    ProducerRecord<String, Object> rec = new ProducerRecord<>(topic, partition, key, value);
    template.send(rec);

    if (log.isDebugEnabled()) {
      log.debug("Produced to {} (partition={}, key={})", topic, partition, key);
    }
  }

  /** Kafka-flow: same as sendByEms, but also add source topic/partition/offset headers. */
  public void sendByEmsWithSourceMeta(String topic, UnifiedEvent ue, ConsumerRecord<String, ?> src) {
    String emsKey = (ue != null && ue.getSourceEms() != null) ? ue.getSourceEms().name() : null;
    sendByEmsWithSourceMeta(topic, emsKey, ue, src);
  }

  /** Kafka-flow: same as above but you pass sourceEms explicitly (works with JSON strings too). */
  public void sendByEmsWithSourceMeta(String topic, String sourceEms, Object value, ConsumerRecord<String, ?> src) {
    String key = normalizeKey(sourceEms);
    Integer partition = partitionFromKey(key);

    ProducerRecord<String, Object> rec =
        new ProducerRecord<>(topic, partition, src != null ? src.timestamp() : null, key, value, null);

    if (src != null) {
      addSourceMetaHeaders(rec, src);
    }

    template.send(rec);

    if (log.isDebugEnabled()) {
      if (src != null) {
        log.debug("Produced to {} (partition={}, key={}) (src {}-{}@{})",
            topic, partition, key, src.topic(), src.partition(), src.offset());
      } else {
        log.debug("Produced to {} (partition={}, key={})", topic, partition, key);
      }
    }
  }

  // --------------------------------------------------------------------------
  // ⚠️ OLD methods (kept for compatibility, but NOT what you want for this use-case)
  // --------------------------------------------------------------------------

  /** Simple send using configured serializers. */
  public void send(String topic, String key, Object value) {
    template.send(topic, key, value);
  }

  /**
   * OLD: This uses src.key() as output key -> that’s why your output key becomes subscriptionId.
   * Keep it only if you really want to preserve the input key.
   */
  public void sendWithSourceMeta(String topic, Object value, ConsumerRecord<String, ?> src) {
    ProducerRecord<String, Object> rec =
        new ProducerRecord<>(topic, null, src.timestamp(), src.key(), value, null);

    addSourceMetaHeaders(rec, src);
    template.send(rec);

    if (log.isDebugEnabled()) {
      log.debug("Produced to {} (src {}-{}@{})", topic, src.topic(), src.partition(), src.offset());
    }
  }

  /** Alternative overload if you don’t have the ConsumerRecord handy. */
  public void sendWithSourceMeta(String topic, String key, Object value,
                                 String srcTopic, Integer srcPartition, Long srcOffset) {
    ProducerRecord<String, Object> rec = new ProducerRecord<>(topic, key, value);

    if (srcTopic != null) {
      rec.headers().add("source-topic", srcTopic.getBytes(StandardCharsets.UTF_8));
    }
    if (srcPartition != null) {
      rec.headers().add("source-partition", srcPartition.toString().getBytes(StandardCharsets.UTF_8));
    }
    if (srcOffset != null) {
      rec.headers().add("source-offset", srcOffset.toString().getBytes(StandardCharsets.UTF_8));
    }

    template.send(rec);

    if (log.isDebugEnabled()) {
      log.debug("Produced to {} (src {}-{}@{})", topic, srcTopic, srcPartition, srcOffset);
    }
  }

  // --------------------------------------------------------------------------
  // helpers
  // --------------------------------------------------------------------------

  private static void addSourceMetaHeaders(ProducerRecord<String, Object> rec, ConsumerRecord<String, ?> src) {
    rec.headers().add("kafka-topic", src.topic().getBytes(StandardCharsets.UTF_8));
    if (src.key() != null) {
      rec.headers().add("kafka-key", src.key().getBytes(StandardCharsets.UTF_8));
    }
    rec.headers().add("kafka-partition", Integer.toString(src.partition()).getBytes(StandardCharsets.UTF_8));
    rec.headers().add("kafka-offset", Long.toString(src.offset()).getBytes(StandardCharsets.UTF_8));
    rec.headers().add("source", "KAFKA".getBytes(StandardCharsets.UTF_8));
  }

  private static String normalizeKey(String sourceEms) {
    if (sourceEms == null) return null;
    String k = sourceEms.trim();
    return k.isEmpty() ? null : k;
  }

  /** Partition = EMSId.ordinal(). If invalid/missing -> null (Kafka will choose). */
  private static Integer partitionFromKey(String key) {
    if (key == null) return null;
    try {
      return EMSId.valueOf(key).ordinal();
    } catch (Exception e) {
      return null;
    }
  }
}
