package gr.ote.atlas.events.emsspecificevents;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@NoArgsConstructor
@ToString(callSuper = true)
public class NetActAlarmNotification extends SystemSpecificEvent{
    //common fields
    private Long nbiSequenceId;
    private String nbiAlarmId;
    private Integer nbiAlarmType;
    private String nbiObjectInstance;
    private String nbiEventTime;
    private Integer nbiProbableCause;
    private String nbiSpecificProblem;
    private Integer nbiPerceivedSeverity;
    private String nbiAdditionalText;
    private String nbiOptionalInformation;

    //nbiAlarmNewNotification && nbiAlarmChangedNotification fields
    private String nbiAlarmTime;
    private String nbiProposedRepairAction;

    //nbiAlarmAckChangedNotification fields
    private Integer nbiAckState;
    private String nbiAckSystemId;
    private String nbiAckTime;
    private String nbiAckUser;

    //nbiAlarmCommentNotification fields
    private String nbiCommentText;
    private String nbiCommentTime;
    private String nbiCommentUser;

    //nbiAlarmClearedNotification fields
    private String nbiClearSystemId;
    private String nbiClearTime;
    private String nbiClearUser;


}
