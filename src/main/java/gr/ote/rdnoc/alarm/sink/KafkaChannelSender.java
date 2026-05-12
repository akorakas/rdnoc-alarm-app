package gr.ote.rdnoc.alarm.sink;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;

public class KafkaChannelSender implements ChannelSender {
  private final KafkaTemplate<String, Object> template;
  private final String topic;

  public KafkaChannelSender(KafkaTemplate<String, Object> template, String topic) {
    this.template = template;
    this.topic = topic;
  }

  @Override
  public void send(String key, String payload, Map<String, String> headers) {
    ProducerRecord<String, Object> rec = new ProducerRecord<>(topic, key, payload);
    if (headers != null) {
      headers.forEach((k,v) -> {
        if (v != null) rec.headers().add(k, v.getBytes(StandardCharsets.UTF_8));
      });
    }
    template.send(rec);
  }
}
