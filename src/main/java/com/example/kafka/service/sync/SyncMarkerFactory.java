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
 * Χρησιμοποιεί το ΙΔΙΟ TemplateStep με το κανονικό pipeline
 * για να φτιάξει δύο ειδικά μηνύματα:
 *
 *  - type = "SYNC_START"
 *  - type = "SYNC_END"
 *
 * Τα υπόλοιπα πεδία είναι κενά ("" ή null) εκτός από:
 *  - timestamp = System.currentTimeMillis()
 *  - sourceEvent = {} (κενό JSON object)
 *
 * Το TemplateStep γράφει το τελικό JSON στο ctx.rendered.
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

      // ---- Πεδία UnifiedEvent ----

      // Από το template: "sourceEms": "NSP_ATNOI", "emsVendorID": "NSP"
      // Αυτά είναι hard-coded στο template, άρα δεν χρειάζεται να τα βάλουμε εδώ.

      // emsDomainNormalized που χρησιμοποιείται στο template
      ctx.put("emsDomainNormalized", "UNKNOWN");

      // Βασικά πεδία – όλα κενά για SYNC markers
      ctx.put("serialNo", "");
      ctx.put("faultId", "");
      ctx.put("neName", "");
      ctx.put("neId", "");
      ctx.put("affectedObjectName", "");

      ctx.put("type", type);          // SYNC_START ή SYNC_END
      ctx.put("severity", ""); // ή "" αν προτιμάς

      // Unix timestamp now (ms)
      ctx.put("timestamp", System.currentTimeMillis());

      // Κενό sourceEvent = {}
      ObjectNode emptySourceEvent = mapper.createObjectNode();
      ctx.put("sourceEvent", emptySourceEvent);

      // ---- metadata fields που χρησιμοποιούνται στο template ----
      ctx.put("fdn", "");
      ctx.put("objectId", "");
      ctx.put("emsDomain", "");
      ctx.put("probableCause", "");
      ctx.put("alarmType", "");
      ctx.put("impactSafe", 0);
      ctx.put("serviceAffectingSafe", false);
      ctx.put("objectFullName", "");

      // alarmIdentifier για SYNC markers – βάζω το type για να ξεχωρίζει
      ctx.put("alarmIdentifier", type);

      // Τρέχουμε το ίδιο TemplateStep με το pipeline
      templateStep.apply(ctx);

      if (ctx.rendered != null && !ctx.rendered.isBlank()) {
        return ctx.rendered;
      }

      // Fallback ασφαλείας
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
      fallback.set("metadata", mapper.createObjectNode());
      fallback.putNull("enrichedData");
      fallback.put("alarmIdentifier", type);

      return mapper.writeValueAsString(fallback);

    } catch (Exception e) {
      throw new RuntimeException("Failed to build sync marker '" + type + "'", e);
    }
  }
}
