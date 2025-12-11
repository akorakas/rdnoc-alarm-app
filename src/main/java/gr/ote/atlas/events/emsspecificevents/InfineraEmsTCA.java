package gr.ote.atlas.events.emsspecificevents;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@NoArgsConstructor
@ToString(callSuper = true)
public class InfineraEmsTCA extends SystemSpecificEvent{
    private Integer tcaEmsNotificationId;
    private Integer tcaNeNotificationId;
    private String tcaEmsTime;
    private String tcaNeTime;
    private String tcaEmsName;
    private String tcaNeName;
    private String tcaNeNodeId;
    private String tcaObjectType;
    private String tcaObjectName;
    private Integer tcaClearableState;
    private String tcaParameterName;
    private String tcaLocation;
    private String tcaThresholdType;
    private String tcaThresholdValue;
    private String tcaCurrentValue;
    private String tcaGranularity;
    private Integer tcaPerceivedSeverity;
    private Integer tcaAssertedSeverity;
    private Integer tcaNeCorrelationId;
    private String tcaProbableCauseDescription;
    private String tcaCustomDescription;
    private String tcaSourceLabel;
}