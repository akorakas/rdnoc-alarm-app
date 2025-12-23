package com.example.kafka.service.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.example.kafka.service.pipeline.steps.UnifiedEventMapper;
import com.example.kafka.service.pipeline.steps.UnifiedEventStep;
import com.fasterxml.jackson.databind.ObjectMapper;

@Configuration
public class UnifiedEventStepConfig {

  @Bean
  public UnifiedEventMapper unifiedEventMapper() {
    return new UnifiedEventMapper();
  }

  @Bean
  public UnifiedEventStep unifiedEventStep(UnifiedEventMapper mapper, ObjectMapper om) {
    return new UnifiedEventStep(mapper, om);
  }
}
