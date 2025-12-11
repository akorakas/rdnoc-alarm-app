package gr.ote.atlas.events.emsspecificevents;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@NoArgsConstructor
@ToString(callSuper = true)
public class InfineraEmsAlarm extends SystemSpecificEvent{
    private String additionalText;
    private Integer assertedSeverity;
    private Integer category;
    private String circuitId;
    private String customDescription;
    private Integer emsCorrelationId;
    private String emsName;
    private Integer emsNotificationId;
    private String emsProbableCause;
    private String emsTime;
    private Integer neCorrelationId;
    private String neName;
    private String neNodeId;
    private Integer neNotificationId;
    private String neProbableCause;
    private String neTime;
    private String objectName;
    private String objectType;
    private Integer perceivedSeverity;
    private String probableCauseDescription;
    private String proposedRepairActions;
    private Integer serviceAffecting;
    private Integer sessionNotificationId;
    private String sourceLabel;
}