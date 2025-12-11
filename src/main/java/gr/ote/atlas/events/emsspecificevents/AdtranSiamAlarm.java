package gr.ote.atlas.events.emsspecificevents;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter @Setter @NoArgsConstructor @ToString(callSuper = true)
public class AdtranSiamAlarm extends SystemSpecificEvent{
    private String alarmTime;
    private String alarmDate;
    private String neAddress;
    private String neModel;
    private String neName;
    private Integer alarmSeverity;
    private Integer alarmState;
    private Integer alarmType;
    private String alarmName;
    private String alarmDescription;
    private String alarmEquipment;
}
