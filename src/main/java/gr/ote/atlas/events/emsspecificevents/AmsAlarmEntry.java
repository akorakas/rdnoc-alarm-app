package gr.ote.atlas.events.emsspecificevents;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter @Setter @NoArgsConstructor @ToString(callSuper = true)
public class AmsAlarmEntry extends SystemSpecificEvent{
    private String notificationId; //(.1)
    private Integer alarmCategory; //(.2)
    private Integer alarmSeverity; //(.3)
    private String objectObjectId; //(.4)
    private String raisedTimeStamp; //(.5)
    private String clearedTimeStamp; //(.6)
    private String lastModificationTimeStamp; //(.7)
    private String additionalInfo; //(.8)
    private String sourceFriendlyName; //(.9)
    private String acknowledged; //(.10)
    private String probableCause; //(.11)
    private String alarmDomain; //(.12)
    private String notes; //(.13)
    private Integer asamAlarmServAffType; //(.14)
    private String specificProblem; //(.15)
    private String repairActions; //(.16)
    private String objectType; //(.17)
    private String emsDomain; //(.23)
    private String neAlarmType; //(.26)
    private String neIpAddress; //(.30)
}
