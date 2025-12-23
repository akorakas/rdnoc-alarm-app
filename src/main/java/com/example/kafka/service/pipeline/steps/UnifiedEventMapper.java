package com.example.kafka.service.pipeline.steps;

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
    // Expect these vars to exist from your previous extract/update steps:
    // - eventTime (String) or timestamp (long)
    // - sourceEvent (JsonNode)  <-- you set this in update step
    // - metadata / enrichedData if you build them earlier (optional)

    JsonNode sourceEventNode = ctx.get("sourceEvent");   // should be JsonNode
    String eventTime = ctx.get("eventTime");             // e.g. "2025-12-23T11:52:16Z"

    UnifiedEvent ue = new UnifiedEvent();

    ue.setSourceEms(EMSId.NSP_ATNOI);
    ue.setEmsVendorID(EMSVendorID.NSP);

    // emsDomain: in your input sourceType="mdm" but your EMSDomain enum is FAN/RAN/TRANSPORT/IP/UNKNOWN
    // so map it:
    ue.setEmsDomain(mapDomain(getText(sourceEventNode, "sourceType")));

    ue.setType(EventType.FAULT); // or map from alarm-change/alarm-delete/create later
    ue.setSeverity(mapSeverity(getText(sourceEventNode, "severity")));

    ue.setTimestamp(parseEventTime(eventTime));

    // Fill fields from source event
    ue.setSerialNo(getText(sourceEventNode, "objectId"));
    ue.setFaultId(getText(sourceEventNode, "alarmName"));
    ue.setNeName(getText(sourceEventNode, "neName"));
    ue.setNeEquipment(getText(sourceEventNode, "neId") + " | " + getText(sourceEventNode, "neName"));
    ue.setAlarmIdentifier(getText(sourceEventNode, "objectFullName"));

    // Build NokiaAtnoiAlarm object as sourceEvent
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

    // metadata/enrichedData: if you already computed these earlier, set them here too.
    // ue.setMetadata(ctx.get("metadata"));
    // ue.setEnrichedData(ctx.get("enrichedData"));

    return ue;
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

  private static Instant parseEventTime(String eventTime) {
    if (eventTime == null || eventTime.isBlank()) return Instant.now();
    return Instant.parse(eventTime); // expects ISO-8601 like 2025-12-23T11:52:16Z
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
    // you can refine this mapping later
    return switch (sourceType.toLowerCase()) {
      case "mdm" -> EMSDomain.IP;      // your example "mdm" looks IP-ish in your enums
      default -> EMSDomain.UNKNOWN;
    };
  }
}
