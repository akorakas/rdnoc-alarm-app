package gr.ote.atlas.events.emsspecificevents;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import gr.ote.atlas.events.enums.EventType;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;
import java.util.Map;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
@JsonSubTypes({
        @JsonSubTypes.Type(value = HwNmNorthboundEvent.class, name = "HwNmNorthboundEvent"),
        @JsonSubTypes.Type(value = AmsAlarmEntry.class, name = "AmsAlarmEntry"),
        @JsonSubTypes.Type(value = IMAPNorthboundFaultAlarmNotificationV2.class, name = "iMAPNorthboundFaultAlarmReportNotificationType"),
        @JsonSubTypes.Type(value = AdtranSiamAlarm.class, name = "AdtranSiamAlarm"),
        @JsonSubTypes.Type(value = EriAlarmNotification.class, name = "EriAlarmNotification"),
        @JsonSubTypes.Type(value = AumsSpecificEvent.class, name = "AumsSpecificEvent"),
        @JsonSubTypes.Type(value = AlcatelOmsAlarmEntry.class, name = "AlcatelOmsAlarmEntry"),
        @JsonSubTypes.Type(value = NetActAlarmNotification.class, name = "NetActAlarmNotification"),
        @JsonSubTypes.Type(value = InfineraEmsAlarm.class, name = "InfineraEmsAlarm"),
        @JsonSubTypes.Type(value = InfineraEmsTCA.class, name = "InfineraEmsTCA"),
        @JsonSubTypes.Type(value = TelesysAlarmNotification.class, name = "TelesysAlarmNotification"),
        @JsonSubTypes.Type(value = TelegrafGenericEvent.class, name = "TelegrafGenericEvent"),
        @JsonSubTypes.Type(value = GenericSourceEvent.class, name = "GenericSourceEvent")
})
@Getter
@Setter
@NoArgsConstructor
@ToString
public class SystemSpecificEvent implements Serializable {
    private EventType eventType;
    private String objectIdentifier;
    private Map<String, Object> metadata;
}
