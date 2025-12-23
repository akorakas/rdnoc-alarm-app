package com.example.kafka.service.pipeline.steps;

import com.example.kafka.atlas.UnifiedEventMapper;
import com.example.kafka.service.pipeline.TransformContext;
import com.example.kafka.service.pipeline.TransformStep;
import com.fasterxml.jackson.databind.JsonNode;
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
    // Ensure sourceEvent is JsonNode (it may currently be a String or Map)
    Object se = ctx.get("sourceEvent");
    JsonNode seNode = toJsonNode(se, om);
    ctx.put("sourceEvent", seNode);

    // If you also use alarmNode inside the mapper, normalize it too (optional but safe)
    Object an = ctx.get("alarmNode");
    if (an != null) ctx.put("alarmNode", toJsonNode(an, om));

    UnifiedEvent u = mapper.fromContext(ctx);

    // Final output
    ctx.rendered = om.writeValueAsString(u);
  }

  private static JsonNode toJsonNode(Object v, ObjectMapper om) throws Exception {
    if (v == null) return om.createObjectNode();
    if (v instanceof JsonNode j) return j;
    if (v instanceof String s) return om.readTree(s);     // JSON string -> JsonNode
    return om.valueToTree(v);                             // Map/List/etc -> JsonNode
  }
}
