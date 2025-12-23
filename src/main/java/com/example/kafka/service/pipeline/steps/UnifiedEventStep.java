package com.example.kafka.service.pipeline.steps;

import com.example.kafka.service.pipeline.TransformContext;
import com.example.kafka.service.pipeline.TransformStep;
import com.fasterxml.jackson.databind.ObjectMapper;

import gr.ote.atlas.events.models.UnifiedEvent;

public class UnifiedEventStep implements TransformStep {

  private final UnifiedEventMapper mapper;
  private final ObjectMapper om;

  public UnifiedEventStep(UnifiedEventMapper mapper, ObjectMapper om) {
    this.mapper = mapper;
    this.om = om;
  }

  @Override
  public void apply(TransformContext ctx) throws Exception {
    UnifiedEvent ue = mapper.fromContext(ctx);

    // keep it if you want downstream steps to reuse it
    ctx.put("unifiedEvent", ue);

    // IMPORTANT: this is what Transformer.transform() returns
    ctx.rendered = om.writeValueAsString(ue);
  }
}
