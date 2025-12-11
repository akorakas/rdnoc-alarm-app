package gr.ote.atlas.events.emsspecificevents;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * @author Mathioudakis Charalampos on 5/2/23
 */
@Getter
@Setter
@NoArgsConstructor
@ToString(callSuper = true)
public class AlcatelOmsAlarmEntry extends SystemSpecificEvent{
    private Long currentAlarmId;
    private String friendlyName;
    private String eventTime;
    private String alarmType;
    private String probableCause;
    private String perceivedSeverity;
    private String additionalText;
    private String specificProblems;
    private String acknowledgementStatus;
    private String reserveStatus;
    private String additionalInformation;
    private String neLocationName;
    private String managedObjectInstance;
    private String acknowledgementUserName;
    private String adminState;
}
