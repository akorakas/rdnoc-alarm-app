package gr.ote.atlas.events.emsspecificevents;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@NoArgsConstructor
@ToString(callSuper = true)
public class EriAlarmNotification extends SystemSpecificEvent {
    private String eriAlarmManagedObject;
    private String eriAlarmIndex;
    private Long eriAlarmMajorType;
    private Long eriAlarmMinorType;
    private String eriAlarmSpecificProblem;
    private Integer eriAlarmLastSequenceNo;
    private Integer eriAlarmEventType;
    private String eriAlarmEventTime;
    private String eriAlarmOriginalEventTime;
    private String eriAlarmProbableCause;
    private String eriAlarmNObjAdditionalText;
    private boolean eriAlarmNObjMoreAdditionalText;
    private boolean eriAlarmNObjResourceId;
    private String eriAlarmResourceId;
}