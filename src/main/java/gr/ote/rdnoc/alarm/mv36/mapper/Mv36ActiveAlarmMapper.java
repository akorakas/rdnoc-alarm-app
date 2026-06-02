package gr.ote.rdnoc.alarm.mv36.mapper;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.stereotype.Component;

import gr.ote.atlas.events.emsspecificevents.TelegrafGenericEvent;
import gr.ote.atlas.events.enums.EMSDomain;
import gr.ote.atlas.events.enums.EMSId;
import gr.ote.atlas.events.enums.EMSVendorID;
import gr.ote.atlas.events.enums.EventType;
import gr.ote.atlas.events.enums.Severity;
import gr.ote.atlas.events.models.UnifiedEvent;
import gr.ote.rdnoc.alarm.mv36.config.Mv36SnmpProperties;
import gr.ote.rdnoc.alarm.mv36.model.Mv36ActiveAlarm;
import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class Mv36ActiveAlarmMapper {

  private final Mv36SnmpProperties props;

  public UnifiedEvent toUnifiedEvent(Mv36ActiveAlarm alarm) {
    UnifiedEvent ue = new UnifiedEvent();

    EMSId sourceEms = parseEnumOrDefault(
        EMSId.class,
        props.getSnmp().getSourceEms(),
        EMSId.MV36_MOBILE
    );

    EMSVendorID vendor = parseEnumOrDefault(
        EMSVendorID.class,
        props.getSnmp().getEmsVendorId(),
        EMSVendorID.MV_36
    );

    EMSDomain domain = parseEnumOrDefault(
        EMSDomain.class,
        props.getSnmp().getEmsDomain(),
        EMSDomain.TRANSPORT
    );

    String neName = clean(alarm.getMv36AlarmStrNeUniqueName());
    String shelf = clean(alarm.getMv36AlarmStrShelf());
    String card = clean(alarm.getMv36AlarmStrCard());
    String port = clean(alarm.getMv36AlarmPortId());
    String faultId = clean(alarm.getMv36AlarmStr());

    String neEquipment = joinNonBlank("/", shelf, card, port);
    String identifier = joinNonBlank("/", neName, shelf, card, port, faultId);

    ue.setSourceEms(sourceEms);
    ue.setEmsVendorID(vendor);
    ue.setEmsDomain(domain);

    ue.setType(EventType.FAULT_SYNC);
    ue.setSeverity(mapSeverity(alarm.getMv36AlarmSeverity()));
    ue.setTimestamp(parseRaisingTime(alarm.getMv36AlarmRaisingTime()));

    ue.setSerialNo(clean(alarm.getMv36AlarmId()));
    ue.setFaultId(faultId);
    ue.setNeName(neName);
    ue.setNeEquipment(neEquipment != null ? neEquipment : "");
    ue.setAlarmIdentifier(firstNonBlank(identifier, faultId, alarm.getMv36AlarmId()));

    TelegrafGenericEvent sourceEvent = new TelegrafGenericEvent();
    sourceEvent.setFields(alarm.toFieldMap());
    sourceEvent.setTags(Map.of(
        "source", "MV36_SNMP_SYNC",
        "sourceEms", sourceEms.name()
    ));
    sourceEvent.setTimestamp(Instant.now().getEpochSecond());

    ue.setSourceEvent(sourceEvent);

    Map<String, Object> metadata = new LinkedHashMap<>();
    metadata.put("source", "MV36_SNMP_SYNC");
    metadata.put("sourceIndex", alarm.getSourceIndex());
    metadata.put("mv36AlarmId", alarm.getMv36AlarmId());
    metadata.put("mv36AlarmNeId", alarm.getMv36AlarmNeId());
    metadata.put("mv36AlarmEventType", alarm.getMv36AlarmEventType());
    metadata.put("mv36AlarmStrProbCause", alarm.getMv36AlarmStrProbCause());
    metadata.put("mv36AlarmStrEventType", alarm.getMv36AlarmStrEventType());

    ue.setMetadata(metadata);

    return ue;
  }

  private static Instant parseRaisingTime(String value) {
    if (value == null || value.isBlank()) {
      return Instant.now();
    }

    try {
      return OffsetDateTime.parse(value.trim()).toInstant();
    } catch (Exception ignored) {
      return Instant.now();
    }
  }

  private static Severity mapSeverity(String raw) {
    Integer code = parseInt(raw);

    if (code == null) {
      return Severity.UNKNOWN;
    }

    return switch (code) {
      case 1 -> Severity.CLEARED;
      case 2 -> Severity.INDETERMINATE;
      case 3 -> Severity.CRITICAL;
      case 4 -> Severity.MAJOR;
      case 5 -> Severity.MINOR;
      case 6 -> Severity.WARNING;
      default -> Severity.UNKNOWN;
    };
  }

  private static Integer parseInt(String raw) {
    if (raw == null || raw.isBlank()) {
      return null;
    }

    String s = raw.trim();

    // Handles both "3" and "critical(3)"
    int open = s.indexOf('(');
    int close = s.indexOf(')');
    if (open >= 0 && close > open) {
      s = s.substring(open + 1, close);
    }

    try {
      return Integer.parseInt(s.trim());
    } catch (Exception e) {
      return null;
    }
  }

  private static String clean(String v) {
    if (v == null) {
      return null;
    }

    String s = v.trim();

    if (s.isEmpty() || "--".equals(s)) {
      return null;
    }

    return s;
  }

  private static String joinNonBlank(String sep, String... parts) {
    StringBuilder sb = new StringBuilder();

    if (parts == null) {
      return null;
    }

    for (String p : parts) {
      String c = clean(p);
      if (c == null) {
        continue;
      }

      if (sb.length() > 0) {
        sb.append(sep);
      }

      sb.append(c);
    }

    return sb.length() == 0 ? null : sb.toString();
  }

  private static String firstNonBlank(String... values) {
    if (values == null) {
      return null;
    }

    for (String v : values) {
      if (v != null && !v.isBlank()) {
        return v;
      }
    }

    return null;
  }

  private static <E extends Enum<E>> E parseEnumOrDefault(Class<E> enumClass, String raw, E fallback) {
    if (raw == null || raw.isBlank()) {
      return fallback;
    }

    try {
      return Enum.valueOf(enumClass, raw.trim());
    } catch (Exception e) {
      return fallback;
    }
  }
}