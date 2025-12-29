package com.example.kafka.sink;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import gr.ote.atlas.events.enums.EMSId;

public class EmsKafkaChannelSender implements ChannelSender {

  private final KafkaTemplate<String, Object> template;
  private final String topic;
  private final ObjectMapper om = new ObjectMapper();

  public EmsKafkaChannelSender(KafkaTemplate<String, Object> template, String topic) {
    this.template = template;
    this.topic = topic;
  }

  @Override
  public void send(String ignoredKey, String payload, Map<String, String> headers) {
    // Default fallback (in case payload parsing fails)
    String key = ignoredKey;
    Integer partition = null;

    // Extract sourceEms from the produced UnifiedEvent JSON
    try {
      if (payload != null && !payload.isBlank()) {
        JsonNode root = om.readTree(payload);
        String sourceEms = root.path("sourceEms").asText(null);

        if (sourceEms != null && !sourceEms.isBlank()) {
          key = sourceEms;

          // Partition = EMSId ordinal (HUAWEI_FAN=0 ... NSP_ATNOI=30)
          partition = EMSId.valueOf(sourceEms).ordinal();
        }
      }
    } catch (Exception e) {
      // keep fallback key/partition
    }

    // Force partition when we could compute it
    ProducerRecord<String, Object> rec =
        new ProducerRecord<>(topic, partition, null, key, payload, null);

    if (headers != null) {
      headers.forEach((k, v) -> {
        if (v != null) rec.headers().add(k, v.getBytes(StandardCharsets.UTF_8));
      });
    }

    template.send(rec);
  }
}
