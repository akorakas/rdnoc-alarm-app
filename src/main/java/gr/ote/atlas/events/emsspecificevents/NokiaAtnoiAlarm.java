package gr.ote.atlas.events.emsspecificevents;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter @Setter @NoArgsConstructor @ToString(callSuper = true)
public class NokiaAtnoiAlarm extends SystemSpecificEvent {
    private String originalSeverity;
    private String neId;
    private String neName;
    private String alarmName;
    private String affectedObjectName;
    private String affectedObject;
    private String alarmType;
    private String probableCause;
    private Long firstTimeDetected;
    private Long lastTimeDetected;
    private String adminState;
    private String sourceType;
    private String objectId;
    private String objectFullName;
    private String additionalText;
    private String sourceSystem;
}
