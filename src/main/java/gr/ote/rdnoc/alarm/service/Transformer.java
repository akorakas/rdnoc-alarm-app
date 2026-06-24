package gr.ote.rdnoc.alarm.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.databind.ObjectMapper;

import gr.ote.rdnoc.alarm.atlas.UnifiedEventMapper;
import gr.ote.rdnoc.alarm.correlate.RedisAlarmInstanceCorrelator;
import gr.ote.rdnoc.alarm.service.config.TransformProperties;
import gr.ote.rdnoc.alarm.service.errors.BadInputException;
import gr.ote.rdnoc.alarm.service.errors.TransformFailureException;
import gr.ote.rdnoc.alarm.service.pipeline.TransformContext;
import gr.ote.rdnoc.alarm.service.pipeline.TransformStep;
import gr.ote.rdnoc.alarm.service.pipeline.steps.ExtractStep;
import gr.ote.rdnoc.alarm.service.pipeline.steps.FlattenStep;
import gr.ote.rdnoc.alarm.service.pipeline.steps.HashStep;
import gr.ote.rdnoc.alarm.service.pipeline.steps.NeNameEnrichmentStep;
import gr.ote.rdnoc.alarm.service.pipeline.steps.RegexExtractStep;
import gr.ote.rdnoc.alarm.service.pipeline.steps.TemplateStep;
import gr.ote.rdnoc.alarm.service.pipeline.steps.UnifiedEventStep;
import gr.ote.rdnoc.alarm.service.pipeline.steps.UpdateStep;

public class Transformer {

  private static final ObjectMapper M = new ObjectMapper().findAndRegisterModules();

  private final List<TransformStep> steps;

  public Transformer(String placeholder,
                     List<TransformProperties.Step> pipeline,
                     RedisAlarmInstanceCorrelator tnmsCorrelator,
                     UnifiedEventMapper unifiedEventMapper) {

    this.steps = buildSteps(placeholder, pipeline, tnmsCorrelator, unifiedEventMapper);
  }

  public String transform(String inputJson) {
    try {
      var root = M.readTree(inputJson);
      var ctx = new TransformContext(root);

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

  private static List<TransformStep> buildSteps(String placeholder,
                                                List<TransformProperties.Step> pipeline,
                                                RedisAlarmInstanceCorrelator tnmsCorrelator,
                                                UnifiedEventMapper unifiedEventMapper) {

    var out = new ArrayList<TransformStep>();

    if (pipeline == null) {
      return out;
    }

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

        case "flatten" -> out.add(new FlattenStep(
            s.getRoots(),
            s.getIncludeTop(),
            s.getTarget()
        ));

        case "hash" -> out.add(new HashStep(
            s.getAlgorithm(),
            s.getFields(),
            s.getTarget()
        ));

        case "neNameEnrich" -> out.add(new NeNameEnrichmentStep());

        case "unifiedEvent" -> out.add(new UnifiedEventStep(
            unifiedEventMapper,
            M,
            tnmsCorrelator
        ));

        case "template" -> out.add(new TemplateStep(
            s.getTemplate(),
            s.getTarget()
        ));

        default -> throw new IllegalArgumentException("Unknown step type: " + s.getType());
      }
    }

    return out;
  }
}