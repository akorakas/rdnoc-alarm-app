package gr.ote.atlas.events.emsspecificevents;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@NoArgsConstructor
@ToString(callSuper = true)
public class TelesysAlarmNotification extends SystemSpecificEvent {
    private Integer severity;
    private Long alarmNumber;
    private String alarmTime;
    private String correlationSeqId;
    private String alarmStr;
    private Long isOpen;
    private String eventName;
    private String ele3;
    private String ele2;
    private String ele1;
    private String moduleId;
    private String corrKey;
    private String nodeId;
    private Long alarmIdx;
    private Long seqId;
}
