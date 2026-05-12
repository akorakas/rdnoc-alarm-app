package gr.ote.rdnoc.alarm.service.pipeline.steps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.ote.rdnoc.alarm.atlas.UnifiedEventMapper;
import gr.ote.rdnoc.alarm.correlate.RedisAlarmInstanceCorrelator;
import gr.ote.rdnoc.alarm.service.pipeline.TransformContext;
import gr.ote.rdnoc.alarm.service.pipeline.TransformStep;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import gr.ote.atlas.events.models.UnifiedEvent;

public class UnifiedEventStep implements TransformStep {

  private static final Logger log = LoggerFactory.getLogger(UnifiedEventStep.class);

  private final UnifiedEventMapper mapper;
  private final ObjectMapper om;
  private final RedisAlarmInstanceCorrelator correlator;

  public UnifiedEventStep(UnifiedEventMapper mapper, ObjectMapper om, RedisAlarmInstanceCorrelator correlator) {
    this.mapper = mapper;
    this.om = om;
    this.correlator = correlator;
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

    // ✅ Redis correlation BEFORE serialization
    if (correlator != null) {
      try {
        correlator.correlate(u, seNode);
      } catch (Exception e) {
        log.warn("Redis correlation failed; continuing without correlation", e);
      }
    }

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

