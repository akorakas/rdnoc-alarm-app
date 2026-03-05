package com.example.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.kafka.annotation.EnableKafka;

import com.example.kafka.service.config.CorrelationProperties;

@SpringBootApplication
@ConfigurationPropertiesScan
@EnableKafka
@EnableConfigurationProperties(CorrelationProperties.class)
public class KafkaApplication {
  public static void main(String[] args) {
    SpringApplication.run(KafkaApplication.class, args);
  }
}