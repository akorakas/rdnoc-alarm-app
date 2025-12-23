package com.example.kafka.service.config;

import java.util.List;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.example.kafka.service.pipeline.steps.TemplateStep;

/**
 * Exposes a TemplateStep bean for SyncMarkerFactory (SYNC_START / SYNC_END).
 *
 * IMPORTANT:
 * In rdnoc-alarm-app we have 2 pipelines:
 *  - transform.kafka.pipeline
 *  - transform.rest.pipeline
 *
 * Sync markers should match the REST snapshot output format, so we use
 * the template step found in transform.rest.pipeline.
 */
@Configuration
@ConditionalOnProperty(name = "transform.templates.enabled", havingValue = "true", matchIfMissing = false)
public class TemplateStepConfig {

  @Bean
  public TemplateStep templateStep(TransformProperties props) {
    String template = findTemplateOrFail("rest", props.getRest() != null ? props.getRest().getPipeline() : null);

    // keep same target as YAML (you use "$")
    String target = "$";

    return new TemplateStep(template, target);
  }

  private String findTemplateOrFail(String name, List<TransformProperties.Step> pipeline) {
    if (pipeline == null || pipeline.isEmpty()) {
      throw new IllegalStateException("No transform." + name + ".pipeline configured; cannot build TemplateStep bean.");
    }

    for (TransformProperties.Step step : pipeline) {
      if (!"template".equals(step.getType())) continue;

      String tpl = step.getTemplate();
      if (tpl == null || tpl.isBlank()) {
        throw new IllegalStateException("transform." + name + ".pipeline has a template step with empty template.");
      }
      return tpl;
    }

    throw new IllegalStateException("No template step found in transform." + name + ".pipeline; cannot build TemplateStep bean.");
  }
}
