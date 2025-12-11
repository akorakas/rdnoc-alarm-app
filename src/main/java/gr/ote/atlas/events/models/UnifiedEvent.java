package gr.ote.atlas.events.models;

import gr.ote.atlas.events.emsspecificevents.SystemSpecificEvent;
import gr.ote.atlas.events.enums.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.time.Instant;
import java.util.Map;

@Getter
@Setter
@NoArgsConstructor
public class UnifiedEvent implements Serializable {
    private EMSId sourceEms;
    private EMSVendorID emsVendorID;
    private EMSDomain emsDomain;
    private String serialNo;
    private String faultId;
    private String neName;
    private String neEquipment;
    private EventType type;
    private Severity severity;
    private Instant timestamp;
    private SystemSpecificEvent sourceEvent;
    private Map<String, Object> metadata;
    private EnrichedData enrichedData;
    private String alarmIdentifier;
}
