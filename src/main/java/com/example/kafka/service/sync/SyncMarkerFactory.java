package com.example.kafka.service.sync;

import java.time.Instant;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

import gr.ote.atlas.events.emsspecificevents.SystemSpecificEvent;
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

  private final ObjectMapper mapper;

  public String buildSyncStart() {
    return build(EventType.SYNC_START);
  }

  public String buildSyncEnd() {
    return build(EventType.SYNC_END);
  }

  private String build(EventType type) {
    try {
      UnifiedEvent u = new UnifiedEvent();

      u.setSourceEms(EMSId.NSP_ATNOI);
      u.setEmsVendorID(EMSVendorID.NSP);
      u.setEmsDomain(EMSDomain.UNKNOWN);

      u.setSerialNo("SYNC");
      u.setFaultId(type.name());
      u.setNeName("SYNC");
      u.setNeEquipment("SYNC");

      u.setType(type);
      u.setSeverity(Severity.UNKNOWN);
      u.setTimestamp(Instant.now());

      // minimal system-specific event
      SystemSpecificEvent src = new SystemSpecificEvent();
      src.setEventType(type);
      src.setObjectIdentifier("SYNC");
      src.setMetadata(Map.of("marker", true));
      u.setSourceEvent(src);

      u.setMetadata(Map.of("source", "SYNC"));
      u.setEnrichedData(null);
      u.setAlarmIdentifier(type.name());

      return mapper.writeValueAsString(u);

    } catch (Exception e) {
      throw new RuntimeException("Failed to build sync marker " + type, e);
    }
  }
}
