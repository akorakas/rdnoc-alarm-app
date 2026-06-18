package gr.ote.atlas.events.emsspecificevents;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@NoArgsConstructor
@ToString(callSuper = true)
public class NokiaNfmTAlarm extends SystemSpecificEvent {

    private String originalSeverity;
    private String previousSeverity;
    private String highestSeverity;
    private String severity;

    private String neId;
    private String neName;

    private String alarmName;
    private String specificProblem;
    private String affectedObjectName;
    private String affectedObject;
    private String affectedObjectType;
    private String alarmType;
    private String probableCause;

    private Long firstTimeDetected;
    private Long lastTimeDetected;
    private Long lastTimeCleared;
    private Long lastTimeAcknowledged;
    private Long lastTimeSeverityChanged;
    private Long lastTimeEscalated;
    private Long lastTimeDeEscalated;

    private String adminState;
    private String sourceType;
    private String sourceSystem;

    private String objectId;
    private String fdn;
    private String objectFullName;

    private String additionalText;
    private String userText;

    private Boolean acknowledged;
    private Boolean wasAcknowledged;
    private String acknowledgedBy;
    private String clearedBy;
    private String deletedBy;

    private Integer frequency;
    private Integer numberOfOccurrences;
    private Integer numberOfOccurrencesSinceClear;
    private Integer numberOfOccurrencesSinceAck;

    private Boolean serviceAffecting;
    private Boolean implicitlyCleared;
    private Boolean rootCause;

    private Integer impact;
    private Integer nodeTimeOffset;

    /*
     * For Kafka notifications only.
     * Examples: CREATE, CHANGE, DELETE.
     */
    private String notificationType;

    /*
     * For Kafka notifications only.
     * Example: 2026-06-17T14:03:36Z.
     */
    private String eventTime;
}