package com.example.kafka.service.sync;

import org.springframework.stereotype.Component;

import com.example.kafka.service.pipeline.TransformContext;
import com.example.kafka.service.pipeline.steps.TemplateStep;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * SyncMarkerFactory
 *
 * Uses the SAME TemplateStep as the normal pipeline to produce:
 *  - type = "SYNC_START"
 *  - type = "SYNC_END"
 *
 * Fixes:
 *  - Ensure enrichedData is valid JSON by always setting enrichedDataJson = "null"
 *  - Ensure all template-required vars exist (avoid blank substitutions that break JSON)
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SyncMarkerFactory {

  private final TemplateStep templateStep;
  private final ObjectMapper mapper = new ObjectMapper();

  public String buildSyncStart() {
    return buildWithType("SYNC_START");
  }

  public String buildSyncEnd() {
    return buildWithType("SYNC_END");
  }

  private String buildWithType(String type) {
    try {
      ObjectNode emptyRoot = mapper.createObjectNode();
      TransformContext ctx = new TransformContext(emptyRoot);

      // ----------------------------------------------------------------------
      // Vars used by your YAML template
      // ----------------------------------------------------------------------

      // Depending on which template is injected (kafka/rest), it may use emsDomain or emsDomainNormalized.
      // Set both to be safe.
      ctx.put("emsDomain", "UNKNOWN");
      ctx.put("emsDomainNormalized", "UNKNOWN");

      // Basic fields
      ctx.put("serialNo", "");
      ctx.put("faultId", "");
      ctx.put("neName", "");
      ctx.put("neId", "");
      ctx.put("affectedObjectName", "");

      // If your template uses ${alarmIdentifier} or ${objectFullName}
      ctx.put("objectFullName", "");

      // SYNC type & severity
      ctx.put("type", type);
      ctx.put("severity", "");

      // timestamp in ms (numeric)
      ctx.put("timestamp", System.currentTimeMillis());

      // sourceEvent must be a JSON object (not a string)
      ctx.put("sourceEvent", mapper.createObjectNode());

      // Metadata fields used in template
      ctx.put("fdn", "");
      ctx.put("objectId", "");
      ctx.put("probableCause", "");
      ctx.put("alarmType", "");
      ctx.put("impactSafe", 0);

      // Your template expects ${serviceAffectingSafe} to be a JSON boolean/null literal (no quotes).
      // So put Boolean (will render true/false) or the string "null" if you want null.
      ctx.put("serviceAffectingSafe", false);

      // alarmIdentifier: use the type for easy filtering downstream
      ctx.put("alarmIdentifier", type);

      // ✅ CRITICAL: your template uses: "enrichedData": ${enrichedDataJson}
      // If this is missing, the output becomes: "enrichedData":
      // so we MUST always define it as the literal null.
      ctx.put("enrichedDataJson", "null");

      // Run same TemplateStep
      templateStep.apply(ctx);

      if (ctx.rendered != null && !ctx.rendered.isBlank()) {
        return ctx.rendered;
      }

      // Fallback (should basically never happen now)
      log.warn("SyncMarkerFactory: ctx.rendered was null/blank for type={}", type);
      ObjectNode fallback = mapper.createObjectNode();
      fallback.put("sourceEms", "NSP_ATNOI");
      fallback.put("emsVendorID", "NSP");
      fallback.put("emsDomain", "UNKNOWN");
      fallback.put("serialNo", "");
      fallback.put("faultId", "");
      fallback.put("neName", "");
      fallback.put("neEquipment", " | ");
      fallback.put("type", type);
      fallback.put("severity", "");
      fallback.put("timestamp", System.currentTimeMillis());
      fallback.set("sourceEvent", mapper.createObjectNode());

      ObjectNode md = mapper.createObjectNode();
      md.put("neId", "");
      md.put("fdn", "");
      md.put("objectId", "");
      md.put("emsDomainRaw", "");
      md.put("probableCause", "");
      md.put("alarmType", "");
      md.put("impact", 0);
      md.put("serviceAffecting", false);
      md.put("objectFullName", "");
      fallback.set("metadata", md);

      fallback.putNull("enrichedData");
      fallback.put("alarmIdentifier", type);

      return mapper.writeValueAsString(fallback);

    } catch (Exception e) {
      throw new RuntimeException("Failed to build sync marker '" + type + "'", e);
    }
  }
}
