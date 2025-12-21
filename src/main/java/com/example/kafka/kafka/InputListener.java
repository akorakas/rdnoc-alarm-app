// src/main/java/com/example/kafka/kafka/InputListener.java
package com.example.kafka.kafka;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;   // ✅ add
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.example.kafka.service.Transformer;
import com.example.kafka.sink.SinkRouter;

@Component
@ConditionalOnProperty(
    prefix = "app.kafka",
    name = "mode",
    havingValue = "static",
    matchIfMissing = false
)
public class InputListener {

  public static final String LISTENER_ID = "alarm-input-listener";

  private static final Logger log = LoggerFactory.getLogger(InputListener.class);

  private final SinkRouter sinks;
  private final Transformer transformer;

  public InputListener(SinkRouter sinks, @Qualifier("kafkaTransformer") Transformer transformer) {
    this.sinks = sinks;
    this.transformer = transformer;
  }

  @KafkaListener(
      id = LISTENER_ID,
      topics = "${app.kafka.input-topic:}",          // ✅ see Change 2
      groupId = "${spring.kafka.consumer.group-id}"
  )
  public void onMessage(ConsumerRecord<String, String> record) {
    String key = record.key();
    String value = record.value();

    if (log.isDebugEnabled()) {
      log.debug("Consumed {}-{}@{} key={}", record.topic(), record.partition(), record.offset(), key);
    }

    String outJson = transformer.transform(value);

    Map<String, String> headers = new HashMap<>();
    headers.put("source-topic", record.topic());
    headers.put("source-partition", String.valueOf(record.partition()));
    headers.put("source-offset", String.valueOf(record.offset()));

    sinks.sendOutput(key, outJson, headers);
  }
}
