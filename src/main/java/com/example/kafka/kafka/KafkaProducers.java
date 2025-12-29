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

  /** Simple send using the configured serializers. (unchanged) */
  public void send(String topic, String key, Object value) {
    template.send(topic, key, value);
  }

  // --------------------------------------------------------------------------
  // NEW: Send UnifiedEvent with:
  //  a) key = sourceEms (EMSId name)
  //  b) partition = EMSId.ordinal()
  // --------------------------------------------------------------------------

  /** Send UnifiedEvent with key+partition derived from ue.getSourceEms(). */
  public void sendUnifiedEvent(String topic, UnifiedEvent ue) {
    String key = deriveKeyFromUnifiedEvent(ue);
    Integer partition = derivePartitionFromKey(key);

    ProducerRecord<String, Object> rec = new ProducerRecord<>(topic, partition, key, ue);
    template.send(rec);

    if (log.isDebugEnabled()) {
      log.debug("Produced UE to {} (partition={}, key={})", topic, partition, key);
    }
  }

  /** Send UnifiedEvent + keep source meta headers (ConsumerRecord). */
  public void sendUnifiedEventWithSourceMeta(String topic, UnifiedEvent ue, ConsumerRecord<String, ?> src) {
    String key = deriveKeyFromUnifiedEvent(ue);
    Integer partition = derivePartitionFromKey(key);

    ProducerRecord<String, Object> rec =
        new ProducerRecord<>(topic, partition, src.timestamp(), key, ue, null);

    addSourceMetaHeaders(rec, src);

    template.send(rec);
    if (log.isDebugEnabled()) {
      log.debug("Produced UE to {} (partition={}, key={}) (src {}-{}@{})",
          topic, partition, key, src.topic(), src.partition(), src.offset());
    }
  }

  // --------------------------------------------------------------------------
  // Keep your existing APIs, but add a new overload that ALSO forces partition
  // based on EMSId mapping (when caller passes sourceEms explicitly).
  // --------------------------------------------------------------------------

  /**
   * NEW overload: caller supplies sourceEms string (e.g. "NSP_ATNOI").
   * Key will be sourceEms, partition will be EMSId.ordinal().
   */
  public void sendWithEmsPartition(String topic, String sourceEms, Object value) {
    String key = sourceEms;
    Integer partition = derivePartitionFromKey(key);

    ProducerRecord<String, Object> rec = new ProducerRecord<>(topic, partition, key, value);
    template.send(rec);

    if (log.isDebugEnabled()) {
      log.debug("Produced to {} (partition={}, key={})", topic, partition, key);
    }
  }

  /** Same as above, but also attaches source meta headers. */
  public void sendWithEmsPartitionAndSourceMeta(String topic, String sourceEms, Object value,
                                                ConsumerRecord<String, ?> src) {
    String key = sourceEms;
    Integer partition = derivePartitionFromKey(key);

    ProducerRecord<String, Object> rec =
        new ProducerRecord<>(topic, partition, src.timestamp(), key, value, null);

    addSourceMetaHeaders(rec, src);

    template.send(rec);
    if (log.isDebugEnabled()) {
      log.debug("Produced to {} (partition={}, key={}) (src {}-{}@{})",
          topic, partition, key, src.topic(), src.partition(), src.offset());
    }
  }

  // --------------------------------------------------------------------------
  // Your existing methods (kept) - but they still use src.key() today.
  // If you want those ALSO to use EMS partitioning, switch src.key() to derive.
  // --------------------------------------------------------------------------

  /** Send with source topic/partition/offset as headers (using a ConsumerRecord). */
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
    rec.headers().add("source-topic", src.topic().getBytes(StandardCharsets.UTF_8));
    rec.headers().add("source-partition", Integer.toString(src.partition()).getBytes(StandardCharsets.UTF_8));
    rec.headers().add("source-offset", Long.toString(src.offset()).getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Key should be the EMSId name (e.g. "NSP_ATNOI").
   * Returns null if not available.
   */
  private static String deriveKeyFromUnifiedEvent(UnifiedEvent ue) {
    if (ue == null || ue.getSourceEms() == null) return null;
    // assuming getSourceEms() returns EMSId
    return ue.getSourceEms().name();
  }

  /**
   * Partition = EMSId.ordinal(). If key is null/blank or not a valid EMSId, returns null (no forced partition).
   */
  private static Integer derivePartitionFromKey(String key) {
    if (key == null || key.isBlank()) return null;
    try {
      return EMSId.valueOf(key).ordinal();
    } catch (Exception ignore) {
      return null;
    }
  }
}
