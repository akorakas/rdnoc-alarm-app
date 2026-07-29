package gr.ote.rdnoc.alarm.mv36.mapper;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import org.springframework.stereotype.Component;

import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;
import gr.ote.atlas.events.emsspecificevents.TelegrafGenericEvent;
import gr.ote.atlas.events.enums.EMSDomain;
import gr.ote.atlas.events.enums.EMSId;
import gr.ote.atlas.events.enums.EMSVendorID;
import gr.ote.atlas.events.enums.EventType;
import gr.ote.atlas.events.enums.Severity;
import gr.ote.atlas.events.models.EnrichedData;
import gr.ote.atlas.events.models.UnifiedEvent;
import gr.ote.rdnoc.alarm.mv36.config.Mv36SnmpProperties;
import gr.ote.rdnoc.alarm.mv36.enrich.Mv36NeEnrichmentService;
import gr.ote.rdnoc.alarm.mv36.model.Mv36ActiveAlarm;
import gr.ote.rdnoc.alarm.mv36.model.Mv36NetworkElement;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class Mv36ActiveAlarmMapper {

  private static final ObjectMapper ENRICHED_DATA_MAPPER = JsonMapper.builder().build();

  private final Mv36SnmpProperties props;
  private final org.springframework.beans.factory.ObjectProvider<Mv36NeEnrichmentService> neEnrichmentServiceProvider;

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

    Mv36NeEnrichmentService neEnrichmentService = neEnrichmentServiceProvider.getIfAvailable();

    Optional<Mv36NetworkElement> neOpt = Optional.empty();

    if (neEnrichmentService != null) {
      neOpt = neEnrichmentService.findByAlarmNeId(clean(alarm.getMv36AlarmNeId()));

      if (neOpt.isEmpty()) {
        neOpt = neEnrichmentService.findByUniqueName(clean(alarm.getMv36AlarmStrNeUniqueName()));
      }

      log.info(
        "MV36 SYNC enrichment lookup: alarmId={}, alarmNeId={}, alarmUniqueName={}, cacheHit={}, mv36NeName={}",
        alarm.getMv36AlarmId(),
        alarm.getMv36AlarmNeId(),
        alarm.getMv36AlarmStrNeUniqueName(),
        neOpt.isPresent(),
        neOpt.map(Mv36NetworkElement::getMv36NeName).orElse(null)
      );
    } else {
      log.warn(
        "MV36 SYNC enrichment service unavailable: alarmId={}, alarmNeId={}, alarmUniqueName={}",
        alarm.getMv36AlarmId(),
        alarm.getMv36AlarmNeId(),
        alarm.getMv36AlarmStrNeUniqueName()
    );
    }
    
    Mv36NetworkElement ne = neOpt.orElse(null);

    String mv36NeName = ne != null ? clean(ne.getMv36NeName()) : null;
    String mv36NeTypeStr = ne != null ? clean(ne.getMv36NeTypeStr()) : null;
    String mv36NeUniqueName = ne != null ? clean(ne.getMv36NeUniqueName()) : null;
    String mv36NeId = ne != null ? clean(ne.getMv36NeId()) : null;

    String neName = firstNonBlank(
        mv36NeName,
        clean(alarm.getMv36AlarmStrNeUniqueName()),
        clean(alarm.getMv36AlarmNeId())
    );

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

    // MV36 location enrichment:
    // mv36NeName = 0057-61 MEGAOTE -> affectedLocation.name = 0057
    ue.setEnrichedData(buildMv36EnrichedData(mv36NeName));

    Map<String, String> sourceFields = alarm.toFieldMap();

    if (mv36NeId != null) sourceFields.put("mv36NeId", mv36NeId);
    if (mv36NeName != null) sourceFields.put("mv36NeName", mv36NeName);
    if (mv36NeUniqueName != null) sourceFields.put("mv36NeUniqueName", mv36NeUniqueName);
    if (mv36NeTypeStr != null) sourceFields.put("mv36NeTypeStr", mv36NeTypeStr);

    // In case enrichment service finds by fallback internally in future
    if (neEnrichmentService != null) {
      neEnrichmentService.enrichFields(sourceFields);
    }

    TelegrafGenericEvent sourceEvent = new TelegrafGenericEvent();
    sourceEvent.setFields(sourceFields);
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
    metadata.put("mv36NeId", mv36NeId);
    metadata.put("mv36NeName", mv36NeName);
    metadata.put("mv36NeUniqueName", mv36NeUniqueName);
    metadata.put("mv36NeTypeStr", mv36NeTypeStr);

    ue.setMetadata(metadata);

    return ue;
  }

  private static EnrichedData buildMv36EnrichedData(String neName) {
    String locationName = extractLocationNameFromMv36NeName(neName);

    if (locationName == null) {
      return null;
    }

    Map<String, Object> affectedLocation = new LinkedHashMap<>();
    affectedLocation.put("inventoryId", null);
    affectedLocation.put("name", locationName);
    affectedLocation.put("longitude", null);
    affectedLocation.put("latitude", null);

    Map<String, Object> root = new LinkedHashMap<>();
    root.put("affectedLocation", affectedLocation);
    root.put("transport", null);
    root.put("affectedSite", null);
    root.put("affectedController", null);
    root.put("affectedCell", null);
    root.put("affectedCellId", null);
    root.put("affectedTechnologies", null);
    root.put("probableCause", null);

    return ENRICHED_DATA_MAPPER.convertValue(root, EnrichedData.class);
  }

  private static String extractLocationNameFromMv36NeName(String neName) {
    String s = clean(neName);

    if (s == null) {
      return null;
    }

    int dash = s.indexOf('-');

    if (dash <= 0) {
      return null;
    }

    String location = s.substring(0, dash).trim();

    return location.isEmpty() ? null : location;
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