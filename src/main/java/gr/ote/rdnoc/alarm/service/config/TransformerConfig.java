package gr.ote.rdnoc.alarm.service.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import gr.ote.rdnoc.alarm.correlate.RedisAlarmInstanceCorrelator;
import gr.ote.rdnoc.alarm.service.Transformer;

@Configuration
@EnableConfigurationProperties({ TransformProperties.class, CorrelationProperties.class })
public class TransformerConfig {

  @Bean("kafkaTransformer")
  public Transformer kafkaTransformer(TransformProperties props, RedisAlarmInstanceCorrelator correlator) {
    return new Transformer(props.getPlaceholder(), props.getKafka().getPipeline(), correlator);
  }

  @Bean("restTransformer")
  public Transformer restTransformer(TransformProperties props, RedisAlarmInstanceCorrelator correlator) {
    return new Transformer(props.getPlaceholder(), props.getRest().getPipeline(), correlator);
  }
}