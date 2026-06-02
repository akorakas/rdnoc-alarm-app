package gr.ote.rdnoc.alarm.service.sync;

import java.time.Instant;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

import gr.ote.atlas.events.emsspecificevents.NokiaAtnoiAlarm;
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
   * Backward-compatible NSP marker methods.
   */
  public String buildSyncStart() {
    return buildSyncStart(EMSId.NSP_ATNOI, EMSVendorID.NSP, EMSDomain.UNKNOWN);
  }

  public String buildSyncEnd() {
    return buildSyncEnd(EMSId.NSP_ATNOI, EMSVendorID.NSP, EMSDomain.UNKNOWN);
  }

  /**
   * Source-aware marker methods.
   */
  public String buildSyncStart(EMSId sourceEms, EMSVendorID vendor, EMSDomain domain) {
    return build(EventType.SYNC_START, sourceEms, vendor, domain);
  }

  public String buildSyncEnd(EMSId sourceEms, EMSVendorID vendor, EMSDomain domain) {
    return build(EventType.SYNC_END, sourceEms, vendor, domain);
  }

  private String build(EventType type, EMSId sourceEms, EMSVendorID vendor, EMSDomain domain) {
    try {
      UnifiedEvent u = new UnifiedEvent();

      u.setSourceEms(sourceEms);
      u.setEmsVendorID(vendor);
      u.setEmsDomain(domain);

      u.setType(type);
      u.setSeverity(Severity.UNKNOWN);
      u.setTimestamp(Instant.now());

      u.setSerialNo("");
      u.setFaultId("");
      u.setNeName("");
      u.setNeEquipment("");
      u.setAlarmIdentifier(sourceEms.name() + "_" + type.name());

      NokiaAtnoiAlarm se = new NokiaAtnoiAlarm();
      se.setEventType(type);
      se.setObjectIdentifier(sourceEms.name() + "_" + type.name());
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

    } catch (Exception e) {
      throw new RuntimeException("Failed to build sync marker " + type + " for " + sourceEms, e);
    }
  }
}