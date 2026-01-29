package com.example.kafka.atlas;

import java.time.Instant;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import com.example.kafka.service.pipeline.TransformContext;
import com.fasterxml.jackson.databind.JsonNode;

import gr.ote.atlas.events.emsspecificevents.NokiaAtnoiAlarm;
import gr.ote.atlas.events.emsspecificevents.TelegrafGenericEvent;
import gr.ote.atlas.events.enums.EMSDomain;
import gr.ote.atlas.events.enums.EMSId;
import gr.ote.atlas.events.enums.EMSVendorID;
import gr.ote.atlas.events.enums.EventType;
import gr.ote.atlas.events.enums.Severity;
import gr.ote.atlas.events.models.EnrichedData;
import gr.ote.atlas.events.models.UnifiedEvent;

public class UnifiedEventMapper {

  public UnifiedEvent fromContext(TransformContext ctx) {

    JsonNode sourceEventNode = ctx.get("sourceEvent");

    // NSP subscription only (ISO). Telegraf/ExaGrid usually won't have it.
    String eventTime = clean(ctx.get("eventTime"));

    // can be ms/sec/decimal-sec depending on flow
    Object tsObj = ctx.get("timestamp");
    Long tsMs = toEpochMillis(tsObj);

    // YAML computed fields (used for NON-ExaGrid flows)
    String typeStr = clean(ctx.get("type"));       // EVENT/FAULT/CLEAR...
    String sevStr  = clean(ctx.get("severity"));   // optional

    String emsDomainRaw = clean(ctx.get("emsDomain"));
    String neId = clean(ctx.get("neId"));
    String neName = clean(ctx.get("neName"));
    String affectedObjectName = clean(ctx.get("affectedObjectName"));
    String faultId = clean(ctx.get("faultId"));
    String serialNo = clean(ctx.get("serialNo"));
    String alarmIdentifier = clean(ctx.get("alarmIdentifier"));
    String objectFullName = clean(ctx.get("objectFullName"));

    // allow YAML override (EXAGRID)
    String sourceEmsRaw = clean(ctx.get("sourceEms"));     // "EXAGRID" or null
    String vendorRaw    = clean(ctx.get("emsVendorID"));   // "UNKNOWN" or null

    UnifiedEvent ue = new UnifiedEvent();

    EMSId sourceEms = parseEnumOrDefault(EMSId.class, sourceEmsRaw, EMSId.NSP_ATNOI);
    EMSVendorID vendor = parseEnumOrDefault(EMSVendorID.class, vendorRaw, EMSVendorID.NSP);

    ue.setSourceEms(sourceEms);
    ue.setEmsVendorID(vendor);

    // ------------------------------------------------------------------
    // ExaGrid / Telegraf mapping
    // ------------------------------------------------------------------
    if (sourceEms == EMSId.EXAGRID && looksLikeTelegraf(sourceEventNode)) {

      JsonNode fields = sourceEventNode.get("fields");

      String egId       = getText(fields, "egEventParamsId");
      String egName     = getText(fields, "egEventParamsName");
      String egDevName  = getText(fields, "egEventParamsDeviceName");
      String egSev      = getText(fields, "egEventParamsSeverity");       // "Error", "Audit", "Info", ...
      String egCreateMs = getText(fields, "egEventParamsCreateTimeRaw");  // millis as string

      // required UE fields per your spec
      ue.setEmsDomain(EMSDomain.UNKNOWN);
      ue.setSerialNo(egId);
      ue.setFaultId(egName);
      ue.setNeName(egDevName);
      ue.setNeEquipment("");            // explicitly blank
      ue.setAlarmIdentifier(null);      // explicitly null for now

      // IMPORTANT:
      // - Keep YAML in your pipeline for other systems
      // - For ExaGrid, do NOT trust YAML "type" because you want:
      //     if egEventParamsSeverity == "Error" => FAULT
      //     else                                => EVENT
      //
      // This is robust against hidden unicode whitespace (NBSP, etc.)
      String exaTypeStr = classifyExaGridType(egSev);

      // Optional sanity print (keep while debugging; remove later)
      System.out.println(
        "EXAGRID sanity: egSev=[" + egSev + "] cp=[" + dumpCodepoints(egSev) + "] " +
        "norm=[" + normalizeExaSeverity(egSev) + "] => exaType=[" + exaTypeStr + "] " +
        "ctx.type=[" + typeStr + "] ctx.sev=[" + sevStr + "]"
      );

      ue.setType(mapEventType(exaTypeStr));

      // You currently want severity always UNKNOWN for ExaGrid output
      // (If later you decide to map Error->MAJOR etc, change here)
      ue.setSeverity(Severity.UNKNOWN);

      // timestamp: prefer ExaGrid raw ms, else ctx.timestamp, else eventTime
      Long egTsMs = toEpochMillis(egCreateMs);
      ue.setTimestamp(parseEventTime(firstNonNull(egTsMs, tsMs), eventTime));

      // keep original raw telegraf payload as sourceEvent
      TelegrafGenericEvent t = new TelegrafGenericEvent();
      t.setFields(asStringMap(sourceEventNode.get("fields")));
      t.setTags(asStringMap(sourceEventNode.get("tags")));
      // telegraf "timestamp" is usually seconds
      t.setTimestamp(toEpochSecondsLong(sourceEventNode.get("timestamp")));
      ue.setSourceEvent(t);

      return ue;
    }

    // ------------------------------------------------------------------
    // NSP (and any other non-ExaGrid system) mapping
    // - This keeps relying on YAML-computed "type"/"severity" if present.
    // ------------------------------------------------------------------
    ue.setEmsDomain(mapDomain(firstNonBlank(emsDomainRaw, getText(sourceEventNode, "sourceType"))));
    ue.setType(mapEventType(typeStr));
    ue.setSeverity(mapSeverity(firstNonBlank(sevStr, getText(sourceEventNode, "severity"))));
    ue.setTimestamp(parseEventTime(tsMs, eventTime));

    ue.setSerialNo(firstNonBlank(serialNo, getText(sourceEventNode, "objectId")));
    ue.setFaultId(firstNonBlank(faultId, getText(sourceEventNode, "alarmName")));
    ue.setNeName(firstNonBlank(neName, getText(sourceEventNode, "neName")));

    String neEquip = (affectedObjectName != null ? affectedObjectName : "");
    ue.setNeEquipment(neEquip);

    ue.setAlarmIdentifier(firstNonBlank(alarmIdentifier, objectFullName, faultId, serialNo));

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

    // delete fallbacks
    if (n.getObjectId() == null) n.setObjectId(serialNo);
    if (n.getObjectFullName() == null) n.setObjectFullName(objectFullName);
    if (n.getAlarmName() == null) n.setAlarmName(faultId);
    if (n.getNeId() == null) n.setNeId(neId);
    if (n.getNeName() == null) n.setNeName(neName);

    ue.setSourceEvent(n);

    // enrichment
    Object edObj = ctx.get("enrichedData");
    if (edObj instanceof EnrichedData ed) {
      ue.setEnrichedData(ed);
    }

    // metadata
    Object mdObj = ctx.get("metadata");
    if (mdObj instanceof Map<?, ?> m) {
      @SuppressWarnings("unchecked")
      Map<String, Object> md = (Map<String, Object>) m;
      ue.setMetadata(md);
    }

    return ue;
  }

  // ---------------- ExaGrid helpers ----------------

  /**
   * Exact requirement:
   * - if fields.egEventParamsSeverity == "Error" -> FAULT
   * - else -> EVENT
   *
   * Robust against hidden whitespace (NBSP, etc).
   */
  private static String classifyExaGridType(String egSeverityRaw) {
    String s = normalizeExaSeverity(egSeverityRaw);
    // handle "error", "error " (NBSP), "error\r", etc.
    if (s.startsWith("error")) return "FAULT";
    return "EVENT";
  }

  /**
   * Strong normalization because Java trim() doesn't remove NBSP and some unicode spaces.
   */
  private static String normalizeExaSeverity(String v) {
    if (v == null) return "";
    String s = v;

    // Remove quotes
    s = s.replace("\"", "").replace("'", "");

    // Normalize common “invisible” whitespace to normal spaces
    s = s.replace('\u00A0', ' '); // NBSP
    s = s.replace('\u2007', ' '); // figure space
    s = s.replace('\u202F', ' '); // narrow NBSP

    // Remove control chars (includes \r \n \t)
    s = s.replaceAll("[\\p{Cntrl}]", "");

    // Collapse whitespace and trim
    s = s.trim().replaceAll("\\s+", " ");

    return s.toLowerCase(Locale.ROOT);
  }

  private static String dumpCodepoints(String v) {
    if (v == null) return "null";
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < v.length(); i++) {
      sb.append(String.format("U+%04X", (int) v.charAt(i)));
      if (i < v.length() - 1) sb.append(" ");
    }
    return sb.toString();
  }

  private static String clean(Object v) {
    if (v == null) return null;
    String s = String.valueOf(v).trim();
    if ((s.startsWith("'") && s.endsWith("'")) || (s.startsWith("\"") && s.endsWith("\""))) {
      s = s.substring(1, s.length() - 1).trim();
    }
    return s.isEmpty() ? null : s;
  }

  // ---------------- existing helpers ----------------

  private static Instant parseEventTime(Long tsMs, String eventTime) {
    if (tsMs != null && tsMs > 0) return Instant.ofEpochMilli(tsMs);
    if (eventTime != null && !eventTime.isBlank()) {
      try { return Instant.parse(eventTime); } catch (Exception ignore) {}
    }
    return Instant.now();
  }

  private static EventType mapEventType(String type) {
    if (type == null) return EventType.FAULT;

    String t = type.trim();

    if ((t.startsWith("'") && t.endsWith("'")) || (t.startsWith("\"") && t.endsWith("\""))) {
      t = t.substring(1, t.length() - 1).trim();
    }

    return switch (t.toUpperCase()) {
      case "EVENT" -> EventType.EVENT;
      case "CLEAR" -> EventType.CLEAR;
      case "CHANGE" -> EventType.CHANGE;
      case "FAULT" -> EventType.FAULT;
      case "FAULT_SYNC" -> EventType.FAULT_SYNC;
      case "SYNC_START" -> EventType.SYNC_START;
      case "SYNC_END" -> EventType.SYNC_END;
      default -> EventType.UNKNOWN;
    };
  }

  private static Severity mapSeverity(String sev) {
    if (sev == null) return Severity.UNKNOWN;
    String s = sev.trim().toLowerCase(Locale.ROOT);
    return switch (s) {
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
    return switch (sourceType.toLowerCase(Locale.ROOT)) {
      case "mdm" -> EMSDomain.TRANSPORT;
      default -> EMSDomain.UNKNOWN;
    };
  }

  private static String firstNonBlank(String... vals) {
    if (vals == null) return null;
    for (String v : vals) if (v != null && !v.isBlank()) return v;
    return null;
  }

  private static Long firstNonNull(Long a, Long b) {
    return a != null ? a : b;
  }

  private static boolean looksLikeTelegraf(JsonNode n) {
    return n != null && n.has("fields") && n.has("tags");
  }

  private static Map<String, String> asStringMap(JsonNode obj) {
    if (obj == null || !obj.isObject()) return null;
    Map<String, String> out = new HashMap<>();
    obj.fields().forEachRemaining(e -> out.put(
      e.getKey(),
      e.getValue().isNull() ? null : e.getValue().asText()
    ));
    return out;
  }

  private static Long toEpochSecondsLong(JsonNode v) {
    if (v == null || v.isNull()) return null;
    if (v.isNumber()) return v.longValue();
    if (v.isTextual()) {
      try { return Long.parseLong(v.asText().trim()); } catch (Exception ignore) {}
    }
    return null;
  }

  private static Long toEpochMillis(Object v) {
    if (v == null) return null;

    if (v instanceof Long l) return normalizeToMillis(l.doubleValue());
    if (v instanceof Integer i) return normalizeToMillis(i.doubleValue());
    if (v instanceof Number n) return normalizeToMillis(n.doubleValue());

    if (v instanceof String s) {
      String t = s.trim();
      if (t.isEmpty()) return null;
      try {
        double d = Double.parseDouble(t);
        return normalizeToMillis(d);
      } catch (Exception ignore) {}
    }
    return null;
  }

  private static Long normalizeToMillis(double d) {
    if (d <= 0) return null;
    // decimal seconds
    if (d > 1e9 && d < 1e12) return (long) Math.round(d * 1000.0);
    // millis already
    if (d >= 1e12) return (long) Math.round(d);
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

  private static <E extends Enum<E>> E parseEnumOrDefault(Class<E> enumClass, String raw, E fallback) {
    if (raw == null || raw.isBlank()) return fallback;
    try { return Enum.valueOf(enumClass, raw.trim()); }
    catch (Exception ignore) { return fallback; }
  }
}
