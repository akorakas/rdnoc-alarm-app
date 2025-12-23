package com.example.kafka.service.pipeline.steps;

import com.example.kafka.atlas.UnifiedEventMapper;
import com.example.kafka.service.pipeline.TransformContext;
import com.example.kafka.service.pipeline.TransformStep;
import com.fasterxml.jackson.databind.ObjectMapper;

import gr.ote.atlas.events.models.UnifiedEvent;

public class UnifiedEventStep implements TransformStep {

  private static final ObjectMapper M = new ObjectMapper();

  private final UnifiedEventMapper mapper = new UnifiedEventMapper();

  @Override
  public void apply(TransformContext ctx) throws Exception {
    UnifiedEvent u = mapper.fromContext(ctx);

    // keep if you want other steps later
    ctx.put("unifiedEvent", u);

    // final output
    ctx.rendered = M.writeValueAsString(u);
  }
}
