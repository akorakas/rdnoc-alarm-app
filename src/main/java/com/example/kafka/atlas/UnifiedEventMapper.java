package com.example.kafka.atlas;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private static final Logger log = LoggerFactory.getLogger(UnifiedEventMapper.class);

  // TNMS timestamp: "yyyy-MM-dd HH:mm:ss" (no timezone in string)
  private static final DateTimeFormatter TNMS_TS_FMT =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

  // Choose the zone TNMS uses for that string.
  // If TNMS timestamps are UTC, change to ZoneId.of("UTC"). "Europe/Athens"
  private static final ZoneId TNMS_ZONE = ZoneId.of("UTC");

  // MV36 raisingTime is SNMP DateAndTime hex (RFC2579 style)
  // If timezone is not provided in the payload, we assume this default zone.
  private static final ZoneId MV36_DEFAULT_ZONE = ZoneId.of("Europe/Athens");

  public UnifiedEvent fromContext(TransformContext ctx) {

    JsonNode sourceEventNode = ctx.get("sourceEvent");

    String eventTime = ctx.get("eventTime");   // NSP subscription only (ISO)
    Object tsObj     = ctx.get("timestamp");   // can be ms/sec/decimal-sec depending on flow
    Long tsMs        = toEpochMillis(tsObj);

    String typeStr   = clean(ctx.get("type"));       // YAML computed: EVENT/FAULT/CLEAR/ACKNOWLEDGE...
    String sevStr    = clean(ctx.get("severity"));   // YAML computed (optional)

    String emsDomainRaw = clean(ctx.get("emsDomain"));
    String neId = clean(ctx.get("neId"));
    String neName = clean(ctx.get("neName"));
    String affectedObjectName = clean(ctx.get("affectedObjectName"));
    String neEquipmentFromCtx = clean(ctx.get("neEquipment"));
    String faultId = clean(ctx.get("faultId"));
    String serialNo = clean(ctx.get("serialNo"));
    String alarmIdentifier = clean(ctx.get("alarmIdentifier"));
    String objectFullName = clean(ctx.get("objectFullName"));

    // allow YAML override (EXAGRID / INFINERA_TNMS / MV36_MOBILE etc.)
    String sourceEmsRaw = clean(ctx.get("sourceEms"));
    String vendorRaw    = clean(ctx.get("emsVendorID"));

    UnifiedEvent ue = new UnifiedEvent();

    EMSId sourceEms = parseEnumOrDefault(EMSId.class, sourceEmsRaw, EMSId.NSP_ATNOI);
    EMSVendorID vendor = parseEnumOrDefault(EMSVendorID.class, vendorRaw, EMSVendorID.NSP);

    ue.setSourceEms(sourceEms);
    ue.setEmsVendorID(vendor);

    // ------------------------------------------------------------------
    // TELEGRAF FLOWS
    //   - EXAGRID: special field extraction + type classification, severity always UNKNOWN
    //   - INFINERA_TNMS: trust YAML computed fields, but force sourceEvent to TelegrafGenericEvent
    //   - MV36_MOBILE: field extraction by prefix (dynamic keys), DateAndTime timestamp decode
    // ------------------------------------------------------------------
    boolean isTelegraf = looksLikeTelegraf(sourceEventNode);
    boolean isExaGrid  = sourceEms == EMSId.EXAGRID;
    boolean isTnms     = sourceEms == EMSId.INFINERA_TNMS;
    boolean isMv36     = sourceEms == EMSId.MV36_MOBILE;

    if (isTelegraf && (isExaGrid || isTnms || isMv36)) {

      // Always preserve the raw Telegraf payload as a TelegrafGenericEvent
      ue.setSourceEvent(toTelegrafGenericEvent(sourceEventNode));

      if (isExaGrid) {
        applyExaGridMapping(ue, sourceEventNode, tsMs, eventTime, typeStr, sevStr);
      } else if (isMv36) {
        applyMv36MobileMapping(ue, sourceEventNode, tsMs, eventTime);
      } else {
        // INFINERA_TNMS (and future Telegraf-based non-ExaGrid flows):
        // Use YAML-provided ctx fields for UnifiedEvent properties.
        ue.setEmsDomain(parseEnumOrDefault(EMSDomain.class, emsDomainRaw, EMSDomain.UNKNOWN));
        ue.setType(mapEventType(typeStr));
        ue.setSeverity(mapSeverity(sevStr));

        // Prefer TNMS alarm timestamp (string) over ctx timestamp
        JsonNode f = sourceEventNode != null ? sourceEventNode.get("fields") : null;
        Long tnmsMs = tnmsTimestampToMs(getText(f, "enmsAlTimeStamp"));
        ue.setTimestamp(parseEventTime(firstNonNull(tnmsMs, tsMs), eventTime));

        ue.setSerialNo(serialNo);
        ue.setFaultId(faultId);
        ue.setNeName(neName);

        // ✅ Fallback: build neName from Telegraf fields if YAML didn't populate it
        if (ue.getNeName() == null) {
          String a = getText(f, "enmsTrapNeIdName");
          String b = getText(f, "enmsNeName");
          String combined =
              (a != null && !a.isBlank() && b != null && !b.isBlank()) ? (a.trim() + "," + b.trim())
            : (a != null && !a.isBlank()) ? a.trim()
            : (b != null && !b.isBlank()) ? b.trim()
            : null;
          ue.setNeName(combined);
        }

        // TNMS uses ctx.neEquipment (from YAML), but keep compatibility with older flows
        ue.setNeEquipment(firstNonBlank(neEquipmentFromCtx, affectedObjectName, ""));

        ue.setAlarmIdentifier(alarmIdentifier);
      }

    } else {

      // ------------------------------------------------------------------
      // NSP mapping (existing behaviour preserved)
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
    }

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

  // ============================================================================================
  // EXAGRID mapping (preserves your existing behavior)
  // ============================================================================================

  private void applyExaGridMapping(UnifiedEvent ue,
                                  JsonNode sourceEventNode,
                                  Long tsMs,
                                  String eventTime,
                                  String typeStr,
                                  String sevStr) {

    JsonNode fields = sourceEventNode != null ? sourceEventNode.get("fields") : null;

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

    // Robust ExaGrid type decision (do NOT trust YAML for this)
    String exaType = classifyExaGridType(egSev);

    log.info("EXAGRID sanity: egSev=[{}] normalized=[{}] => exaType=[{}] ctx.type=[{}] ctx.sev=[{}]",
        egSev, normalize(egSev), exaType, typeStr, sevStr);

    ue.setType(mapEventType(exaType));

    // You want severity always UNKNOWN for ExaGrid output
    ue.setSeverity(Severity.UNKNOWN);

    // timestamp: prefer egEventParamsCreateTimeRaw, else ctx.timestamp, else eventTime
    Long egTsMs = toEpochMillis(egCreateMs);
    ue.setTimestamp(parseEventTime(firstNonNull(egTsMs, tsMs), eventTime));
  }

  private static String classifyExaGridType(String egSeverityRaw) {
    String s = normalize(egSeverityRaw);
    // Error => FAULT, Info/Audit/etc => EVENT
    if ("error".equals(s)) return "FAULT";
    return "EVENT";
  }

  // ============================================================================================
  // MV36_MOBILE mapping (NEW) - safe additive change, does not affect NSP/EXAGRID/TNMS behavior
  // ============================================================================================

  private void applyMv36MobileMapping(UnifiedEvent ue,
                                      JsonNode sourceEventNode,
                                      Long ctxTsMs,
                                      String eventTime) {

    JsonNode fields = sourceEventNode != null ? sourceEventNode.get("fields") : null;

    // Keys are dynamic: mv36AlarmStrNeUniqueName.0.<alarmId> etc.
    String neUniqueName = findFieldTextByPrefix(fields, "mv36AlarmStrNeUniqueName.");
    String shelf        = findFieldTextByPrefix(fields, "mv36AlarmStrShelf.");
    String card         = findFieldTextByPrefix(fields, "mv36AlarmStrCard.");
    String alarmStr     = findFieldTextByPrefix(fields, "mv36AlarmStr.");

    String raisingHex   = findFieldTextByPrefix(fields, "mv36AlarmRaisingTime.");
    Long mv36TsMs       = mv36DateAndTimeHexToEpochMs(raisingHex);

    Integer sevCode     = findFieldIntByPrefix(fields, "mv36AlarmSeverity.");
    String serial       = findFieldTextByPrefix(fields, "mv36AlarmId.");

    // Domain/vendor are already set at top; set domain if you want a fixed one here.
    ue.setEmsDomain(EMSDomain.UNKNOWN);

    // Required mappings
    ue.setNeName(clean(neUniqueName));
    ue.setSerialNo(clean(serial));
    ue.setFaultId(clean(alarmStr));

    // neEquipment = shelf/card
    String neEquipment = joinNonBlank("/", shelf, card);
    ue.setNeEquipment(neEquipment != null ? neEquipment : "");

    // alarmIdentifier = ne/shelf/card/alarmStr
    String identifier = joinNonBlank("/", neUniqueName, shelf, card, alarmStr);
    // Fallback if we couldn't build a full identifier
    ue.setAlarmIdentifier(firstNonBlank(identifier, alarmStr, serial));

    // Severity + Type rules from MV36 AlarmSeverity enum
    ue.setSeverity(mapMv36Severity(sevCode));
    ue.setType(mapMv36Type(sevCode));

    // Timestamp: prefer mv36 raising time; else ctx timestamp; else eventTime; else now
    ue.setTimestamp(parseEventTime(firstNonNull(mv36TsMs, ctxTsMs), eventTime));
  }

  private static Severity mapMv36Severity(Integer code) {
    if (code == null) return Severity.UNKNOWN;
    return switch (code) {
      case 1 -> Severity.CLEARED;
      case 2 -> Severity.INDETERMINATE;
      case 3 -> Severity.CRITICAL;
      case 4 -> Severity.MAJOR;
      case 5 -> Severity.MINOR;
      case 6 -> Severity.WARNING;
      default -> Severity.UNKNOWN; // 0 notApplicable and anything unexpected
    };
  }

  private static EventType mapMv36Type(Integer code) {
    if (code == null) return EventType.UNKNOWN;
    return switch (code) {
      case 1 -> EventType.CLEAR;         // cleared
      case 3, 4, 5, 6 -> EventType.FAULT; // critical/major/minor/warning
      default -> EventType.UNKNOWN;      // notApplicable(0), indeterminate(2), others
    };
  }

  private static String findFieldTextByPrefix(JsonNode fields, String prefix) {
    if (fields == null || !fields.isObject() || prefix == null) return null;
    var it = fields.fields();
    while (it.hasNext()) {
      var e = it.next();
      if (e.getKey().startsWith(prefix)) {
        return (e.getValue() == null || e.getValue().isNull()) ? null : e.getValue().asText();
      }
    }
    return null;
  }

  private static Integer findFieldIntByPrefix(JsonNode fields, String prefix) {
    String s = findFieldTextByPrefix(fields, prefix);
    if (s == null || s.isBlank()) return null;
    try { return Integer.parseInt(s.trim()); } catch (Exception ignore) { return null; }
  }

  /** Joins non-blank parts with separator. Skips "--" and trims whitespace. */
  private static String joinNonBlank(String sep, String... parts) {
    if (parts == null) return null;
    StringBuilder sb = new StringBuilder();
    for (String p : parts) {
      if (p == null) continue;
      String t = p.trim();
      if (t.isEmpty()) continue;
      if ("--".equals(t)) continue;
      if (sb.length() > 0) sb.append(sep);
      sb.append(t);
    }
    return sb.length() == 0 ? null : sb.toString();
  }

  /**
   * Decode SNMP DateAndTime hex (RFC2579 style) into epoch millis.
   *
   * Format (minimum 8 bytes):
   *   year(2), month, day, hour, minute, second, deciSecond
   * Optional timezone (3 bytes):
   *   direction('+'/'-'), hours, minutes
   *
   * MV36 special case: undefined raising time is 0000-01-01 00:00:00.0
   */
  private static Long mv36DateAndTimeHexToEpochMs(String hex) {
    if (hex == null || hex.isBlank()) return null;

    try {
      byte[] b = hexStringToBytes(hex.trim());
      if (b.length < 8) return null;

      int year  = ((b[0] & 0xFF) << 8) | (b[1] & 0xFF);
      int month = (b[2] & 0xFF);
      int day   = (b[3] & 0xFF);
      int hour  = (b[4] & 0xFF);
      int min   = (b[5] & 0xFF);
      int sec   = (b[6] & 0xFF);
      int deci  = (b[7] & 0xFF); // tenths

      // Undefined per MIB
      if (year == 0 && month == 1 && day == 1 && hour == 0 && min == 0 && sec == 0 && deci == 0) {
        return null;
      }

      ZoneId zone = MV36_DEFAULT_ZONE;

      // timezone bytes present?
      if (b.length >= 11) {
        char dir = (char) (b[8] & 0xFF); // '+' or '-'
        int tzH = (b[9] & 0xFF);
        int tzM = (b[10] & 0xFF);
        int totalMin = tzH * 60 + tzM;
        if (dir == '-') totalMin = -totalMin;
        zone = ZoneOffset.ofTotalSeconds(totalMin * 60);
      }

      // deci-seconds -> nanos
      int nanos = deci * 100_000_000;

      LocalDateTime ldt = LocalDateTime.of(year, month, day, hour, min, sec, nanos);
      return ldt.atZone(zone).toInstant().toEpochMilli();

    } catch (Exception ignore) {
      return null;
    }
  }

  private static byte[] hexStringToBytes(String s) {
    int len = s.length();
    if (len % 2 != 0) throw new IllegalArgumentException("hex length must be even");
    byte[] out = new byte[len / 2];
    for (int i = 0; i < len; i += 2) {
      out[i / 2] = (byte) Integer.parseInt(s.substring(i, i + 2), 16);
    }
    return out;
  }

  // ============================================================================================
  // Helpers (existing)
  // ============================================================================================

  private static TelegrafGenericEvent toTelegrafGenericEvent(JsonNode sourceEventNode) {
    TelegrafGenericEvent t = new TelegrafGenericEvent();
    if (sourceEventNode == null) return t;

    t.setFields(asStringMap(sourceEventNode.get("fields")));
    t.setTags(asStringMap(sourceEventNode.get("tags")));
    // telegraf "timestamp" is usually seconds
    t.setTimestamp(toEpochSecondsLong(sourceEventNode.get("timestamp")));
    return t;
  }

  private static String normalize(String v) {
    if (v == null) return "";
    return v.trim().replace("\"", "").replace("'", "").toLowerCase();
  }

  private static String clean(Object v) {
    if (v == null) return null;
    String s = String.valueOf(v).trim();
    if ((s.startsWith("'") && s.endsWith("'")) || (s.startsWith("\"") && s.endsWith("\""))) {
      s = s.substring(1, s.length() - 1).trim();
    }
    return s.isEmpty() ? null : s;
  }

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

      case "ACKNOWLEDGE" -> EventType.ACKNOWLEDGE;
      case "UNACKNOWLEDGE" -> EventType.UNACKNOWLEDGE;

      default -> EventType.UNKNOWN;
    };
  }

  private static Severity mapSeverity(String sev) {
    if (sev == null) return Severity.UNKNOWN;
    String s = sev.trim().toLowerCase();
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
    return switch (sourceType.toLowerCase()) {
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

  private static Long tnmsTimestampToMs(String tsStr) {
    if (tsStr == null || tsStr.isBlank()) return null;
    try {
      LocalDateTime ldt = LocalDateTime.parse(tsStr.trim(), TNMS_TS_FMT);
      return ldt.atZone(TNMS_ZONE).toInstant().toEpochMilli();
    } catch (Exception ignore) {
      return null;
    }
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