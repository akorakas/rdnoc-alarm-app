package gr.ote.atlas.events.emsspecificevents;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@NoArgsConstructor
@ToString(callSuper = true)
public class AumsSpecificEvent extends SystemSpecificEvent{
    private String siteId;
    private String siteName;
    private String eettCode;
    private String siteCriticality;
    private String techDepartment;
    private String telRegion;
    private String telDepartment;
    private String assetId;
    private String assetName;
    private String aumsEventType;
    private String aumsSeverity;
    private String aumsAlarmCode;
    private String aumsEventTime;
    private String aumsNotificationId;
    private String aumsCorrelatedNotification;
    private String aumsAdditionalText;
    private String aimmsReferenceId;
    private String messageType;
}
