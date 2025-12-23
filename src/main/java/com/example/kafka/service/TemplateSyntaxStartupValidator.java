package com.example.kafka.service;

import java.util.List;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import com.example.kafka.service.config.TransformProperties;
import com.fasterxml.jackson.databind.ObjectMapper;

@Component
@ConditionalOnProperty(name = "transform.templates.enabled", havingValue = "true", matchIfMissing = false)
public class TemplateSyntaxStartupValidator implements ApplicationRunner {

  private static final ObjectMapper M = new ObjectMapper();

  private final TransformProperties props;
  private final boolean enabled;

  public TemplateSyntaxStartupValidator(
      TransformProperties props,
      @Value("${transform.validate-on-start:true}") boolean enabled   // ✅ align with YAML
  ) {
    this.props = props;
    this.enabled = enabled;
  }

  @Override
  public void run(ApplicationArguments args) {
    if (!enabled) return;

    validatePipeline("kafka", props.getKafka() != null ? props.getKafka().getPipeline() : null);
    validatePipeline("rest",  props.getRest()  != null ? props.getRest().getPipeline()  : null);
  }

  private void validatePipeline(String name, List<TransformProperties.Step> pipeline) {
    if (pipeline == null || pipeline.isEmpty()) return;

    int idx = 0;
    for (var step : pipeline) {
      idx++;
      if (!"template".equals(step.getType())) continue;

      String tpl = step.getTemplate();
      if (tpl == null || tpl.isBlank()) {
        throw new IllegalStateException("Startup validation failed: [" + name + "] template step #" + idx + " has empty template.");
      }

      String renderedForCheck = tpl.replaceAll("\\$\\{[^}]+\\}", "0");

      try {
        M.readTree(renderedForCheck);
      } catch (com.fasterxml.jackson.core.JsonProcessingException ex) {
        String preview = renderedForCheck.length() > 400
            ? renderedForCheck.substring(0, 400) + "...(truncated)"
            : renderedForCheck;
        throw new IllegalStateException(
            "Startup validation failed: [" + name + "] template step #" + idx + " is not valid JSON after placeholder substitution.\n"
          + "Preview:\n" + preview, ex);
      }
    }
  }
}
