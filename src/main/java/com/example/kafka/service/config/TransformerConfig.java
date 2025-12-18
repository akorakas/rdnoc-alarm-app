package com.example.kafka.service.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.example.kafka.service.Transformer;

@Configuration
@EnableConfigurationProperties(TransformProperties.class)
public class TransformerConfig {

  @Bean("kafkaTransformer")
  public Transformer kafkaTransformer(TransformProperties props) {
    return new Transformer(props.getPlaceholder(), props.getKafka().getPipeline());
  }

  @Bean("restTransformer")
  public Transformer restTransformer(TransformProperties props) {
    return new Transformer(props.getPlaceholder(), props.getRest().getPipeline());
  }
}
