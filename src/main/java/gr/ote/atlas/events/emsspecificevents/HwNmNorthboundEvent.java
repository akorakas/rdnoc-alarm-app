package gr.ote.atlas.events.emsspecificevents;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter @Setter @NoArgsConstructor @ToString(callSuper = true)
public class HwNmNorthboundEvent extends SystemSpecificEvent{
    private String hwNmNorthboundNEName; //(.1)
    private String hwNmNorthboundNEType; //(.2)
    private String hwNmNorthboundObjectInstance; //(.3)
    private String hwNmNorthboundEventType; //(.4)
    private String hwNmNorthboundEventTime; //(.5)
    private String hwNmNorthboundProbableCause; //(.6)
    private String hwNmNorthboundSeverity; //(.7)
    private String hwNmNorthboundEventDetail; //(.8)
    private String hwNmNorthboundAdditionalInfo; //(.9)
    private String hwNmNorthboundFaultFlag; //(.10)
    private String hwNmNorthboundFaultFunction; //(.11)
    private String hwNmNorthboundDeviceIP; //(.12)
    private Integer hwNmNorthboundSerialNo; //(.13)
    private String hwNmNorthboundProbableRepair; //(.14)
    private String hwNmNorthboundResourceIDs; //(.15)
    private String hwNmNorthboundEventName; //(.24)
    private Integer hwNmNorthboundReasonID; //(.25)
    private Integer hwNmNorthboundFaultID; //(.26)
    private String hwNmNorthboundDeviceType; //(.27)
    private String hwNmNorthboundTrailName; //(.28)
    private Integer hwNmNorthboundRootAlarm; //(.29)
    private Integer hwNmNorthboundGroupID; //(.30)
    private Integer hwNmNorthboundMaintainStatus; //(.31)
    private String hwNmNorthboundRootAlarmSerialNo; //(.32)
    private Integer hwNmNorthboundConfirmStatus; //(.33)
    private Integer hwNmNorthboundRestoreStatus; //(.34)
}
