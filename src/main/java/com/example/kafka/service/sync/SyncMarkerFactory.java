package com.example.kafka.service.sync;

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

  private final ObjectMapper om; // <-- inject Spring's mapper

  public String buildSyncStart() { return build(EventType.SYNC_START); }
  public String buildSyncEnd()   { return build(EventType.SYNC_END); }

  private String build(EventType type) {
    try {
      UnifiedEvent u = new UnifiedEvent();

      // Stable identity
      u.setSourceEms(EMSId.NSP_ATNOI);
      u.setEmsVendorID(EMSVendorID.NSP);
      u.setEmsDomain(EMSDomain.UNKNOWN);

      // Marker type + timestamp
      u.setType(type);
      u.setSeverity(Severity.UNKNOWN);
      u.setTimestamp(Instant.now());

      // Keep required strings non-null (safer for downstream)
      u.setSerialNo("");
      u.setFaultId("");
      u.setNeName("");
      u.setNeEquipment("");
      u.setAlarmIdentifier(type.name());

      // Put an object as sourceEvent (so JSON has "sourceEvent": {...})
      NokiaAtnoiAlarm se = new NokiaAtnoiAlarm();
      se.setEventType(type);
      se.setObjectIdentifier(type.name());
      se.setAdditionalText("SYNC_MARKER");
      se.setAlarmName(type.name());
      se.setNeId("");
      se.setNeName("");

      u.setSourceEvent(se);

      // Optional metadata (handy for filtering/debug)
      u.setMetadata(Map.of(
          "source", "SYNC",
          "markerType", type.name()
      ));

      return om.writeValueAsString(u);

    } catch (Exception e) {
      throw new RuntimeException("Failed to build sync marker " + type, e);
    }
  }
}
