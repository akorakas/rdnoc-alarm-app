package gr.ote.rdnoc.alarm.service.sync;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import gr.ote.atlas.events.emsspecificevents.NokiaAtnoiAlarm;
import gr.ote.atlas.events.emsspecificevents.TelegrafGenericEvent;
import gr.ote.atlas.events.enums.EMSDomain;
import gr.ote.atlas.events.enums.EMSId;
import gr.ote.atlas.events.enums.EMSVendorID;
import gr.ote.atlas.events.enums.EventType;
import gr.ote.atlas.events.enums.Severity;
import gr.ote.atlas.events.models.UnifiedEvent;
import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class SyncMarkerFactory {

  private static final ObjectMapper M = new ObjectMapper().findAndRegisterModules();

  /**
   * Backward-compatible methods.
   * These preserve old NSP/ATNOI behavior.
   *
   * New code should prefer the source-aware methods below.
   */
  public String buildSyncStart() {
    return build(EventType.SYNC_START, EMSId.NSP_ATNOI, EMSVendorID.NSP, EMSDomain.UNKNOWN);
  }

  public String buildSyncEnd() {
    return build(EventType.SYNC_END, EMSId.NSP_ATNOI, EMSVendorID.NSP, EMSDomain.UNKNOWN);
  }

  /**
   * Source-aware marker methods.
   * These should be used by SyncCoordinator for all systems.
   */
  public String buildSyncStart(EMSId sourceEms, EMSVendorID vendor, EMSDomain domain) {
    return build(EventType.SYNC_START, sourceEms, vendor, domain);
  }

  public String buildSyncEnd(EMSId sourceEms, EMSVendorID vendor, EMSDomain domain) {
    return build(EventType.SYNC_END, sourceEms, vendor, domain);
  }

  private String build(EventType type, EMSId sourceEms, EMSVendorID vendor, EMSDomain domain) {
    if (sourceEms == null) {
      throw new IllegalArgumentException("sourceEms must not be null");
    }
    if (vendor == null) {
      throw new IllegalArgumentException("vendor must not be null");
    }
    if (domain == null) {
      throw new IllegalArgumentException("domain must not be null");
    }

    if (sourceEms == EMSId.NSP_ATNOI) {
      return buildNspMarker(type, sourceEms, vendor, domain);
    }

    if (sourceEms == EMSId.MV36_MOBILE) {
      return buildMv36Marker(type, sourceEms, vendor, domain);
    }

    return buildGenericMarker(type, sourceEms, vendor, domain);
  }

  private String buildNspMarker(
      EventType type,
      EMSId sourceEms,
      EMSVendorID vendor,
      EMSDomain domain
  ) {
    try {
      UnifiedEvent u = baseMarker(type, sourceEms, vendor, domain);

      NokiaAtnoiAlarm se = new NokiaAtnoiAlarm();
      se.setEventType(type);
      se.setObjectIdentifier(type.name());
      se.setAdditionalText("SYNC_MARKER");
      se.setAlarmName(type.name());
      se.setNeId("");
      se.setNeName("");

      u.setSourceEvent(se);

      u.setMetadata(Map.of(
          "source", "SYNC",
          "sourceEms", sourceEms.name(),
          "markerType", type.name()
      ));

      return M.writeValueAsString(u);

    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to build NSP sync marker " + type, e);
    }
  }

  private String buildMv36Marker(
      EventType type,
      EMSId sourceEms,
      EMSVendorID vendor,
      EMSDomain domain
  ) {
    try {
      UnifiedEvent u = baseMarker(type, sourceEms, vendor, domain);

      TelegrafGenericEvent se = new TelegrafGenericEvent();

      Map<String, String> fields = new LinkedHashMap<>();
      fields.put("sourceIndex", "");
      fields.put("mv36AlarmId", "");
      fields.put("mv36AlarmSeverity", "");
      fields.put("mv36AlarmNeId", "");
      fields.put("mv36AlarmEventType", "");
      fields.put("mv36AlarmRaisingTime", "");
      fields.put("mv36AlarmStrNeUniqueName", "");
      fields.put("mv36AlarmStrShelf", "");
      fields.put("mv36AlarmStrCard", "");
      fields.put("mv36AlarmPortId", "");
      fields.put("mv36AlarmStr", "");
      fields.put("mv36AlarmStrProbCause", "");
      fields.put("mv36AlarmStrEventType", "");

      Map<String, String> tags = new LinkedHashMap<>();
      tags.put("source", "MV36_SNMP_SYNC");
      tags.put("sourceEms", sourceEms.name());
      tags.put("markerType", type.name());

      se.setFields(fields);
      se.setTags(tags);
      se.setTimestamp(Instant.now().getEpochSecond());

      u.setSourceEvent(se);

      u.setMetadata(Map.of(
          "source", "MV36_SNMP_SYNC",
          "sourceEms", sourceEms.name(),
          "markerType", type.name()
      ));

      return M.writeValueAsString(u);

    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to build MV36 sync marker " + type, e);
    }
  }

  private String buildGenericMarker(
      EventType type,
      EMSId sourceEms,
      EMSVendorID vendor,
      EMSDomain domain
  ) {
    try {
      UnifiedEvent u = baseMarker(type, sourceEms, vendor, domain);

      TelegrafGenericEvent se = new TelegrafGenericEvent();

      Map<String, String> fields = new LinkedHashMap<>();
      fields.put("sourceIndex", "");
      fields.put("markerType", type.name());

      Map<String, String> tags = new LinkedHashMap<>();
      tags.put("source", "SYNC");
      tags.put("sourceEms", sourceEms.name());
      tags.put("markerType", type.name());

      se.setFields(fields);
      se.setTags(tags);
      se.setTimestamp(Instant.now().getEpochSecond());

      u.setSourceEvent(se);

      u.setMetadata(Map.of(
          "source", "SYNC",
          "sourceEms", sourceEms.name(),
          "markerType", type.name()
      ));

      return M.writeValueAsString(u);

    } catch (JsonProcessingException e) {
      throw new RuntimeException(
          "Failed to build generic sync marker " + type + " for " + sourceEms,
          e
      );
    }
  }

  private UnifiedEvent baseMarker(
      EventType type,
      EMSId sourceEms,
      EMSVendorID vendor,
      EMSDomain domain
  ) {
    UnifiedEvent u = new UnifiedEvent();

    u.setSourceEms(sourceEms);
    u.setEmsVendorID(vendor);
    u.setEmsDomain(domain);

    u.setType(type);
    u.setSeverity(Severity.UNKNOWN);
    u.setTimestamp(Instant.now());

    u.setSerialNo("");
    u.setFaultId(type.name());
    u.setNeName("");
    u.setNeEquipment("");
    u.setAlarmIdentifier(sourceEms.name() + "_" + type.name());

    return u;
  }
}