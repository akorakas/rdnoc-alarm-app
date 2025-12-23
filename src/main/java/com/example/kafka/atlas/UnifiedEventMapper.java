package com.example.kafka.atlas;

import java.time.Instant;

import com.example.kafka.service.pipeline.TransformContext;
import com.fasterxml.jackson.databind.JsonNode;

import gr.ote.atlas.events.emsspecificevents.NokiaAtnoiAlarm;
import gr.ote.atlas.events.enums.EMSDomain;
import gr.ote.atlas.events.enums.EMSId;
import gr.ote.atlas.events.enums.EMSVendorID;
import gr.ote.atlas.events.enums.EventType;
import gr.ote.atlas.events.enums.Severity;
import gr.ote.atlas.events.models.UnifiedEvent;

public class UnifiedEventMapper {

  public UnifiedEvent fromContext(TransformContext ctx) {

    // UnifiedEventStep guarantees this is JsonNode now
    JsonNode sourceEventNode = ctx.get("sourceEvent");

    // From YAML (may be null depending on flow)
    String eventTime = ctx.get("eventTime");      // subscription only (ISO-8601)
    Object tsObj     = ctx.get("timestamp");      // both flows (ms), but could be Long or String
    Long tsMs        = toLong(tsObj);

    String typeStr   = ctx.get("type");           // you compute this in YAML
    String sevStr    = ctx.get("severity");       // you compute/normalize this in YAML

    String emsDomainRaw = ctx.get("emsDomain");   // extracted from YAML
    String neId = ctx.get("neId");
    String neName = ctx.get("neName");
    String affectedObjectName = ctx.get("affectedObjectName");
    String faultId = ctx.get("faultId");
    String serialNo = ctx.get("serialNo");
    String alarmIdentifier = ctx.get("alarmIdentifier");
    String objectFullName = ctx.get("objectFullName");

    UnifiedEvent ue = new UnifiedEvent();

    ue.setSourceEms(EMSId.NSP_ATNOI);
    ue.setEmsVendorID(EMSVendorID.NSP);

    // Prefer ctx.emsDomain; fallback to sourceEvent.sourceType
    ue.setEmsDomain(mapDomain(firstNonBlank(emsDomainRaw, getText(sourceEventNode, "sourceType"))));

    // Type from ctx.type (CLEAR/CHANGE/FAULT/FAULT_SYNC/UNKNOWN)
    ue.setType(mapEventType(typeStr));

    // Severity from ctx.severity; fallback to sourceEvent.severity
    ue.setSeverity(mapSeverity(firstNonBlank(sevStr, getText(sourceEventNode, "severity"))));

    // Timestamp: prefer eventTime ISO, else timestamp ms, else now
    ue.setTimestamp(parseEventTime(eventTime, tsMs));

    // Core fields: prefer ctx vars (you already extracted them)
    ue.setSerialNo(firstNonBlank(serialNo, getText(sourceEventNode, "objectId")));
    ue.setFaultId(firstNonBlank(faultId, getText(sourceEventNode, "alarmName")));
    ue.setNeName(firstNonBlank(neName, getText(sourceEventNode, "neName")));

    // Keep same style you used in template: neId | affectedObjectName
    String neEquip = (neId != null ? neId : "") + " | " + (affectedObjectName != null ? affectedObjectName : "");
    ue.setNeEquipment(neEquip);

    ue.setAlarmIdentifier(firstNonBlank(alarmIdentifier, objectFullName, faultId));

    // Build NokiaAtnoiAlarm as sourceEvent (from JsonNode)
    NokiaAtnoiAlarm n = new NokiaAtnoiAlarm();
    n.setOriginalSeverity(getText(sourceEventNode, "originalSeverity"));
    n.setNeId(getText(sourceEventNode, "neId"));
    n.setNeName(getText(sourceEventNode, "neName"));
    n.setAlarmName(getText(sourceEventNode, "alarmName"));
    n.setAffectedObjectName(getText(sourceEventNode, "affectedObjectName"));
    n.setAffectedObject(getText(sourceEventNode, "affectedObject"));
    n.setAlarmType(getText(sourceEventNode, "alarmType"));
    n.setProbableCause(getText(sourceEventNode, "probableCause"));
    n.setFirstTimeDetected(getLong(sourceEventNode, "firstTimeDetected"));
    n.setLastTimeDetected(getLong(sourceEventNode, "lastTimeDetected"));
    n.setAdminState(getText(sourceEventNode, "adminState"));
    n.setSourceType(getText(sourceEventNode, "sourceType"));
    n.setObjectId(getText(sourceEventNode, "objectId"));
    n.setObjectFullName(getText(sourceEventNode, "objectFullName"));
    n.setAdditionalText(getText(sourceEventNode, "additionalText"));
    n.setSourceSystem(getText(sourceEventNode, "sourceSystem"));

    ue.setSourceEvent(n);

    return ue;
  }

  // ---------------- helpers ----------------

  private static Instant parseEventTime(String eventTime, Long tsMs) {
    if (eventTime != null && !eventTime.isBlank()) {
      try {
        return Instant.parse(eventTime);
      } catch (Exception ignore) {
        // fall through to tsMs/now
      }
    }
    if (tsMs != null && tsMs > 0) {
      return Instant.ofEpochMilli(tsMs);
    }
    return Instant.now();
  }

  private static EventType mapEventType(String type) {
    if (type == null) return EventType.FAULT;

    // If your enum includes CLEAR/CHANGE/FAULT/SYNC_* you can also do valueOf safely with try/catch
    return switch (type.toUpperCase()) {
      case "CLEAR" -> EventType.CLEAR;
      case "CHANGE" -> EventType.CHANGE;
      case "FAULT", "FAULT_SYNC" -> EventType.FAULT;
      default -> EventType.FAULT;
    };
  }

  private static String firstNonBlank(String... vals) {
    if (vals == null) return null;
    for (String v : vals) {
      if (v != null && !v.isBlank()) return v;
    }
    return null;
  }

  private static Long toLong(Object v) {
    if (v == null) return null;
    if (v instanceof Long l) return l;
    if (v instanceof Integer i) return i.longValue();
    if (v instanceof Number n) return n.longValue();
    if (v instanceof String s) {
      try { return Long.parseLong(s.trim()); } catch (Exception ignore) {}
    }
    return null;
  }

  private static String getText(JsonNode n, String field) {
    if (n == null) return null;
    JsonNode v = n.get(field);
    return (v == null || v.isNull()) ? null : v.asText();
  }

  private static Long getLong(JsonNode n, String field) {
    if (n == null) return null;
    JsonNode v = n.get(field);
    return (v == null || v.isNull()) ? null : v.asLong();
  }

  private static Severity mapSeverity(String sev) {
    if (sev == null) return Severity.UNKNOWN;
    return switch (sev.toLowerCase()) {
      case "critical" -> Severity.CRITICAL;
      case "major" -> Severity.MAJOR;
      case "minor" -> Severity.MINOR;
      case "warning" -> Severity.WARNING;
      case "cleared" -> Severity.CLEARED;
      case "indeterminate" -> Severity.INDETERMINATE;
      default -> Severity.UNKNOWN;
    };
  }

  private static EMSDomain mapDomain(String sourceType) {
    if (sourceType == null) return EMSDomain.UNKNOWN;
    return switch (sourceType.toLowerCase()) {
      case "mdm" -> EMSDomain.IP;
      default -> EMSDomain.UNKNOWN;
    };
  }
}
