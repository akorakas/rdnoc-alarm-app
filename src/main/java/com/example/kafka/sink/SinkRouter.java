package com.example.kafka.sink;

import java.util.Map;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.example.kafka.service.config.SinksProperties;

@Component
public class SinkRouter {

  private final ChannelSender outputSender;
  private final ChannelSender dltSender;
  private final ChannelSender errorSender;

  public SinkRouter(KafkaTemplate<String, Object> template, SinksProperties props) {

    // OUTPUT: EMS-aware Kafka sender (forces key+partition from payload)
    if ("file".equalsIgnoreCase(props.getOutput().getType())) {
      this.outputSender = new FileChannelSender(props.getOutput().getFile());
    } else {
      this.outputSender = new EmsKafkaChannelSender(template, props.getOutput().getTopic());
    }

    // DLT/ERROR: keep as-is (no forced partitioning unless you want it)
    this.dltSender   = buildSender(template, props.getDlt());
    this.errorSender = buildSender(template, props.getError());
  }

  private static ChannelSender buildSender(KafkaTemplate<String, Object> template,
                                           SinksProperties.Channel ch) {
    if ("file".equalsIgnoreCase(ch.getType())) {
      return new FileChannelSender(ch.getFile());
    }
    return new KafkaChannelSender(template, ch.getTopic());
  }

  public void sendOutput(String key, String json, Map<String, String> headers) {
    outputSender.send(key, json, headers);
  }

  public void sendDlt(String key, String json, Map<String, String> headers) {
    dltSender.send(key, json, headers);
  }

  public void sendError(String key, String json, Map<String, String> headers) {
    errorSender.send(key, json, headers);
  }
}
