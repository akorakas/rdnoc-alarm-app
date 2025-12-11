package gr.ote.atlas.events.emsspecificevents;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * From IMAP_NORTHBOUND_MIB-V2.mib
 * The iMAP system sends real-time alarms to the NMS using this trap.
 * If an alarm occurs to an NE or the iMAP, the iMAP sends the alarm trap to the NMS.
 * OID = 1.3.6.1.4.1.2011.2.15.2.4.3.3.0.1
 */
@Getter @Setter @NoArgsConstructor @ToString(callSuper = true)
public class IMAPNorthboundFaultAlarmNotificationV2 extends SystemSpecificEvent{
    private String alarmCSN; //1.3.6.1.4.1.2011.2.15.2.4.3.3.1
    private String alarmCategory; //1.3.6.1.4.1.2011.2.15.2.4.3.3.2
    private String alarmOccurTime; //1.3.6.1.4.1.2011.2.15.2.4.3.3.3
    private String alarmMOName; //1.3.6.1.4.1.2011.2.15.2.4.3.3.4
    private Integer alarmProductID; //1.3.6.1.4.1.2011.2.15.2.4.3.3.5
    private String alarmNEType; //1.3.6.1.4.1.2011.2.15.2.4.3.3.6
    private String alarmNEDevID; //1.3.6.1.4.1.2011.2.15.2.4.3.3.7
    private String alarmDevCsn; //1.3.6.1.4.1.2011.2.15.2.4.3.3.8
    private Integer alarmID; //1.3.6.1.4.1.2011.2.15.2.4.3.3.9
    private Integer alarmType; //1.3.6.1.4.1.2011.2.15.2.4.3.3.10
    private Integer alarmLevel; //1.3.6.1.4.1.2011.2.15.2.4.3.3.11
    private Integer alarmRestore; //1.3.6.1.4.1.2011.2.15.2.4.3.3.12
    private Integer alarmConfirm; //1.3.6.1.4.1.2011.2.15.2.4.3.3.13
    private String alarmAckTime; //1.3.6.1.4.1.2011.2.15.2.4.3.3.14
    private String alarmRestoreTime; //1.3.6.1.4.1.2011.2.15.2.4.3.3.15
    private String alarmOperator; //1.3.6.1.4.1.2011.2.15.2.4.3.3.16
    private String alarmExtendInfo; //1.3.6.1.4.1.2011.2.15.2.4.3.3.27
    private String alarmProbableCause; //1.3.6.1.4.1.2011.2.15.2.4.3.3.28
    private String alarmProposedRepairActions; //1.3.6.1.4.1.2011.2.15.2.4.3.3.29
    private String alarmSpecificProblems; //1.3.6.1.4.1.2011.2.15.2.4.3.3.30
    private String alarmClearOperator; //1.3.6.1.4.1.2011.2.15.2.4.3.3.46
    private String alarmObjectInstanceType; //1.3.6.1.4.1.2011.2.15.2.4.3.3.47
    private String alarmClearCategory; //1.3.6.1.4.1.2011.2.15.2.4.3.3.48
    private String alarmClearType; //1.3.6.1.4.1.2011.2.15.2.4.3.3.49
    private String alarmServiceAffectFlag; //1.3.6.1.4.1.2011.2.15.2.4.3.3.50
    private String alarmAdditionalInfo; //1.3.6.1.4.1.2011.2.15.2.4.3.3.51

    @JsonCreator
    public IMAPNorthboundFaultAlarmNotificationV2(
            @JsonProperty(value = "iMAPNorthboundAlarmCSN.0") String alarmCSN,
            @JsonProperty(value = "iMAPNorthboundAlarmCategory.0") String alarmCategory,
            @JsonProperty(value = "iMAPNorthboundAlarmOccurTime.0") String alarmOccurTime,
            @JsonProperty(value = "iMAPNorthboundAlarmMOName.0") String alarmMOName,
            @JsonProperty(value = "iMAPNorthboundAlarmProductID.0") Integer alarmProductID,
            @JsonProperty(value = "iMAPNorthboundAlarmNEType.0") String alarmNEType,
            @JsonProperty(value = "iMAPNorthboundAlarmNEDevID.0") String alarmNEDevID,
            @JsonProperty(value = "iMAPNorthboundAlarmDevCsn.0") String alarmDevCsn,
            @JsonProperty(value = "iMAPNorthboundAlarmID.0") Integer alarmID,
            @JsonProperty(value = "iMAPNorthboundAlarmType.0") Integer alarmType,
            @JsonProperty(value = "iMAPNorthboundAlarmLevel.0") Integer alarmLevel,
            @JsonProperty(value = "iMAPNorthboundAlarmRestore.0") Integer alarmRestore,
            @JsonProperty(value = "iMAPNorthboundAlarmConfirm.0") Integer alarmConfirm,
            @JsonProperty(value = "iMAPNorthboundAlarmAckTime.0") String alarmAckTime,
            @JsonProperty(value = "iMAPNorthboundAlarmRestoreTime.0") String alarmRestoreTime,
            @JsonProperty(value = "iMAPNorthboundAlarmOperator.0") String alarmOperator,
            @JsonProperty(value = "iMAPNorthboundAlarmExtendInfo.0") String alarmExtendInfo,
            @JsonProperty(value = "iMAPNorthboundAlarmProbablecause.0") String alarmProbableCause,
            @JsonProperty(value = "iMAPNorthboundAlarmProposedrepairactions.0") String alarmProposedRepairActions,
            @JsonProperty(value = "iMAPNorthboundAlarmSpecificproblems.0") String alarmSpecificProblems,
            @JsonProperty(value = "iMAPNorthboundAlarmClearOperator.0") String alarmClearOperator,
            @JsonProperty(value = "iMAPNorthboundAlarmObjectInstanceType.0") String alarmObjectInstanceType,
            @JsonProperty(value = "iMAPNorthboundAlarmClearCategory.0") String alarmClearCategory,
            @JsonProperty(value = "iMAPNorthboundAlarmClearType.0") String alarmClearType,
            @JsonProperty(value = "iMAPNorthboundAlarmServiceAffectFlag.0") String alarmServiceAffectFlag,
            @JsonProperty(value = "iMAPNorthboundAlarmAdditionalInfo.0") String alarmAdditionalInfo) {
        this.alarmCSN = alarmCSN;
        this.alarmCategory = alarmCategory;
        this.alarmOccurTime = alarmOccurTime;
        this.alarmMOName = alarmMOName;
        this.alarmProductID = alarmProductID;
        this.alarmNEType = alarmNEType;
        this.alarmNEDevID = alarmNEDevID;
        this.alarmDevCsn = alarmDevCsn;
        this.alarmID = alarmID;
        this.alarmType = alarmType;
        this.alarmLevel = alarmLevel;
        this.alarmRestore = alarmRestore;
        this.alarmConfirm = alarmConfirm;
        this.alarmAckTime = alarmAckTime;
        this.alarmRestoreTime = alarmRestoreTime;
        this.alarmOperator = alarmOperator;
        this.alarmExtendInfo = alarmExtendInfo;
        this.alarmProbableCause = alarmProbableCause;
        this.alarmProposedRepairActions = alarmProposedRepairActions;
        this.alarmSpecificProblems = alarmSpecificProblems;
        this.alarmClearOperator = alarmClearOperator;
        this.alarmObjectInstanceType = alarmObjectInstanceType;
        this.alarmClearCategory = alarmClearCategory;
        this.alarmClearType = alarmClearType;
        this.alarmServiceAffectFlag = alarmServiceAffectFlag;
        this.alarmAdditionalInfo = alarmAdditionalInfo;
    }
}
