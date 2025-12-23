package com.example.kafka.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.example.kafka.atlas.UnifiedEventMapper;
import com.example.kafka.service.config.TransformProperties;
import com.example.kafka.service.errors.BadInputException;
import com.example.kafka.service.errors.TransformFailureException;
import com.example.kafka.service.pipeline.TransformContext;
import com.example.kafka.service.pipeline.TransformStep;
import com.example.kafka.service.pipeline.steps.ExtractStep;
import com.example.kafka.service.pipeline.steps.FlattenStep;
import com.example.kafka.service.pipeline.steps.HashStep;
import com.example.kafka.service.pipeline.steps.NeNameEnrichmentStep;
import com.example.kafka.service.pipeline.steps.RegexExtractStep;
import com.example.kafka.service.pipeline.steps.TemplateStep;
import com.example.kafka.service.pipeline.steps.UnifiedEventStep;
import com.example.kafka.service.pipeline.steps.UpdateStep;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Transformer {

  private static final ObjectMapper M = new ObjectMapper();
  private static final UnifiedEventMapper unifiedEventMapper = new UnifiedEventMapper();

  private final List<TransformStep> steps;

  public Transformer(String placeholder, List<TransformProperties.Step> pipeline) {
    this.steps = buildSteps(placeholder, pipeline);
  }

  public String transform(String inputJson) {
    try {
      var root = M.readTree(inputJson);
      var ctx  = new TransformContext(root);

      for (var s : steps) {
        try {
          s.apply(ctx);
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
          throw new TransformFailureException("Step JSON processing failed", e);
        } catch (java.io.IOException e) {
          throw new TransformFailureException("Step IO failed", e);
        } catch (IllegalArgumentException | IllegalStateException e) {
          throw new TransformFailureException(e.getMessage(), e);
        } catch (Exception e) {
          throw new TransformFailureException("Step failed", e);
        }
      }

      return (ctx.rendered != null) ? ctx.rendered : inputJson;

    } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
      throw new BadInputException("Input is not valid JSON", e);
    }
  }

  private static List<TransformStep> buildSteps(String placeholder, List<TransformProperties.Step> pipeline) {
    var out = new ArrayList<TransformStep>();
    if (pipeline == null) return out;

    for (var s : pipeline) {
      switch (s.getType()) {
        case "extract" -> out.add(new ExtractStep(
            s.getMappings(),
            s.getFromVar(),
            Boolean.TRUE.equals(s.getFailOnMissing()),
            Boolean.TRUE.equals(s.getFailOnBadJson())
        ));
        case "update" -> out.add(new UpdateStep(
            s.getStripCr(),
            s.getCompute(),
            placeholder
        ));
        case "regexExtract" -> {
          final int grp = Objects.requireNonNullElse(s.getGroup(), 1);
          out.add(new RegexExtractStep(
              s.getSource(),
              s.getPattern(),
              grp,
              s.getTarget(),
              s.getFallback()
          ));
        }
        case "flatten"  -> out.add(new FlattenStep(s.getRoots(), s.getIncludeTop(), s.getTarget()));
        case "hash"     -> out.add(new HashStep(s.getAlgorithm(), s.getFields(), s.getTarget()));
        case "neNameEnrich" -> out.add(new NeNameEnrichmentStep());
        case "unifiedEvent" -> out.add(new UnifiedEventStep(unifiedEventMapper, M));
        case "template" -> out.add(new TemplateStep(s.getTemplate(), s.getTarget()));
        default -> throw new IllegalArgumentException("Unknown step type: " + s.getType());
      }
    }

    return out;
  }
}
