package com.example.kafka.sink;

import java.util.Map;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.example.kafka.service.config.SinksProperties;

@Component
public class SinkRouter {

  private final ChannelSender outputSender;
  private final ChannelSender dltSender;
  private final ChannelSender errorSender;

  public SinkRouter(KafkaTemplate<String, Object> template, SinksProperties props) {
    this.outputSender = buildSender(template, props.getOutput());
    this.dltSender    = buildSender(template, props.getDlt());
    this.errorSender  = buildSender(template, props.getError());
  }

  private static ChannelSender buildSender(KafkaTemplate<String, Object> template,
                                           SinksProperties.Channel ch) {
    if ("file".equalsIgnoreCase(ch.getType())) {
      return new FileChannelSender(ch.getFile());
    }
    // default: kafka
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
