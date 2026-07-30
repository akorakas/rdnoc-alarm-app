package gr.ote.rdnoc.alarm.atlas;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

import gr.ote.atlas.events.emsspecificevents.NokiaAtnoiAlarm;
import gr.ote.atlas.events.emsspecificevents.NokiaNfmTAlarm;
import gr.ote.atlas.events.emsspecificevents.TelegrafGenericEvent;
import gr.ote.atlas.events.enums.EMSDomain;
import gr.ote.atlas.events.enums.EMSId;
import gr.ote.atlas.events.enums.EMSVendorID;
import gr.ote.atlas.events.enums.EventType;
import gr.ote.atlas.events.enums.Severity;
import gr.ote.atlas.events.models.EnrichedData;
import gr.ote.atlas.events.models.UnifiedEvent;
import gr.ote.rdnoc.alarm.mv36.enrich.Mv36NeEnrichmentService;
import gr.ote.rdnoc.alarm.mv36.model.Mv36NetworkElement;
import gr.ote.rdnoc.alarm.service.pipeline.TransformContext;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

@Component
public class UnifiedEventMapper {

  private static final Logger log = LoggerFactory.getLogger(UnifiedEventMapper.class);

  private static final ObjectMapper ENRICHED_DATA_MAPPER = JsonMapper.builder().build();

  private final Mv36NeEnrichmentService mv36NeEnrichmentService;

  // TNMS timestamp: "yyyy-MM-dd HH:mm:ss" without timezone.
  private static final DateTimeFormatter TNMS_TS_FMT =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

  // Choose the zone TNMS uses for that string.
  private static final ZoneId TNMS_ZONE = ZoneId.of("UTC");

  // MV36 raisingTime is SNMP DateAndTime hex, RFC2579 style.
  // If timezone is not provided in the payload, we assume this default zone.
  private static final ZoneId MV36_DEFAULT_ZONE = ZoneId.of("Europe/Athens");

  // 1350 OMS eventTime: "yyyyMMddHHmmss" without timezone.
  // Use Athens unless you confirm that 1350 OMS sends UTC.
  private static final ZoneId OMS1350_DEFAULT_ZONE = ZoneId.of("Europe/Athens");

  public UnifiedEventMapper(ObjectProvider<Mv36NeEnrichmentService> enrichmentProvider) {
    this.mv36NeEnrichmentService = enrichmentProvider.getIfAvailable();

    log.info("UnifiedEventMapper initialized. mv36NeEnrichmentServiceAvailable={}",
        this.mv36NeEnrichmentService != null);
  }

  public UnifiedEvent fromContext(TransformContext ctx) {

    JsonNode sourceEventNode = ctx.get("sourceEvent");

    /*
     * For NOKIA_NFM_T:
     * - sourceEventNode should be the extracted alarm body.
     * - alarmNode can also be present and is preferred if available.
     *
     * This supports both:
     * - Kafka notification flow, where alarmNode is nsp-fault:alarm-create/change/delete.
     * - REST snapshot flow, where sourceEvent is already the alarm row.
     */
    JsonNode alarmNode = ctx.get("alarmNode");
    JsonNode fieldNode =
        alarmNode != null && !alarmNode.isNull()
            ? alarmNode
            : sourceEventNode;

    String eventTime = ctx.get("eventTime");
    Object tsObj     = ctx.get("timestamp");
    Long tsMs        = toEpochMillis(tsObj);

    String typeStr   = clean(ctx.get("type"));
    String sevStr    = clean(ctx.get("severity"));

    String emsDomainRaw = clean(ctx.get("emsDomain"));
    String neId = clean(ctx.get("neId"));
    String neName = clean(ctx.get("neName"));
    String affectedObjectName = clean(ctx.get("affectedObjectName"));
    String neEquipmentFromCtx = clean(ctx.get("neEquipment"));
    String faultId = clean(ctx.get("faultId"));
    String serialNo = clean(ctx.get("serialNo"));
    String alarmIdentifier = clean(ctx.get("alarmIdentifier"));
    String objectFullName = clean(ctx.get("objectFullName"));

    String sourceEmsRaw = clean(ctx.get("sourceEms"));
    String vendorRaw    = clean(ctx.get("emsVendorID"));

    UnifiedEvent ue = new UnifiedEvent();

    EMSId sourceEms = parseEnumOrDefault(EMSId.class, sourceEmsRaw, EMSId.NSP_ATNOI);
    EMSVendorID vendor = parseEnumOrDefault(EMSVendorID.class, vendorRaw, EMSVendorID.NSP);

    ue.setSourceEms(sourceEms);
    ue.setEmsVendorID(vendor);

    boolean isTelegraf = looksLikeTelegraf(sourceEventNode);
    boolean isExaGrid  = sourceEms == EMSId.EXAGRID;
    boolean isTnms     = sourceEms == EMSId.INFINERA_TNMS;
    boolean isMv36 =
    sourceEms == EMSId.MV36_MOBILE
        || sourceEms == EMSId.MV36_FIXED_A
        || sourceEms == EMSId.MV36_FIXED_B
        || sourceEms == EMSId.MV36_FIXED_C;
    boolean isOms1350  =
        sourceEms == EMSId.NOKIA_1350_EML1
            || sourceEms == EMSId.NOKIA_1350_EML2
            || sourceEms == EMSId.NOKIA_1350_OTNE
            || sourceEms == EMSId.NOKIA_1350_PKT;
    boolean isNfmT     = sourceEms == EMSId.NOKIA_NFM_T;

    // ------------------------------------------------------------------
    // TELEGRAF FLOWS
    //   - EXAGRID
    //   - INFINERA_TNMS
    //   - MV36_MOBILE
    //   - NOKIA_1350_EML1 / EML2 / OTNE / PKT
    // ------------------------------------------------------------------
    if (isTelegraf && (isExaGrid || isTnms || isMv36 || isOms1350)) {

      ue.setSourceEvent(toTelegrafGenericEvent(sourceEventNode, isMv36));

      if (isExaGrid) {
        applyExaGridMapping(ue, sourceEventNode, tsMs, eventTime, typeStr, sevStr);

      } else if (isMv36) {
        applyMv36MobileMapping(ue, sourceEventNode, tsMs, eventTime, emsDomainRaw);

      } else if (isOms1350) {
        applyOms1350Mapping(ue, sourceEms, sourceEventNode, tsMs, eventTime, emsDomainRaw);

      } else {
        // INFINERA_TNMS and future Telegraf-based non-ExaGrid flows.
        ue.setEmsDomain(parseEnumOrDefault(EMSDomain.class, emsDomainRaw, EMSDomain.UNKNOWN));
        ue.setType(mapEventType(typeStr));
        ue.setSeverity(mapSeverity(sevStr));

        JsonNode f = sourceEventNode != null ? sourceEventNode.get("fields") : null;
        Long tnmsMs = tnmsTimestampToMs(getText(f, "enmsAlTimeStamp"));
        ue.setTimestamp(parseEventTime(firstNonNull(tnmsMs, tsMs), eventTime));

        ue.setSerialNo(serialNo);
        ue.setFaultId(faultId);
        ue.setNeName(neName);

        if (ue.getNeName() == null) {
          String a = getText(f, "enmsTrapNeIdName");
          String b = getText(f, "enmsNeName");
          String combined =
              (a != null && !a.isBlank() && b != null && !b.isBlank()) ? (a.trim() + "," + b.trim())
            : (a != null && !a.isBlank()) ? a.trim()
            : (b != null && !b.isBlank()) ? b.trim()
            : null;
          ue.setNeName(combined);
        }

        ue.setNeEquipment(firstNonBlank(neEquipmentFromCtx, affectedObjectName, ""));
        ue.setAlarmIdentifier(alarmIdentifier);
      }

    } else if (isNfmT) {

      // ------------------------------------------------------------------
      // NOKIA NFM-T / WSNOC mapping
      // ------------------------------------------------------------------
      applyNokiaNfmTMapping(
          ue,
          ctx,
          sourceEventNode,
          fieldNode,
          tsMs,
          eventTime,
          typeStr,
          sevStr,
          emsDomainRaw,
          neId,
          neName,
          affectedObjectName,
          neEquipmentFromCtx,
          faultId,
          serialNo,
          alarmIdentifier,
          objectFullName
      );

    } else {

      // ------------------------------------------------------------------
      // NSP / ATNOI mapping
      // Existing behavior preserved.
      // ------------------------------------------------------------------
      ue.setEmsDomain(mapDomain(firstNonBlank(emsDomainRaw, getText(sourceEventNode, "sourceType"))));
      ue.setType(mapEventType(typeStr));
      ue.setSeverity(mapSeverity(firstNonBlank(sevStr, readTextOrNewValue(sourceEventNode, "severity"))));
      ue.setTimestamp(parseEventTime(tsMs, eventTime));

      ue.setSerialNo(firstNonBlank(serialNo, getText(sourceEventNode, "objectId")));
      ue.setFaultId(firstNonBlank(faultId, getText(sourceEventNode, "alarmName")));
      ue.setNeName(firstNonBlank(neName, getText(sourceEventNode, "neName")));

      String neEquip = affectedObjectName != null ? affectedObjectName : "";
      ue.setNeEquipment(neEquip);

      ue.setAlarmIdentifier(firstNonBlank(alarmIdentifier, objectFullName, faultId, serialNo));

      NokiaAtnoiAlarm n = new NokiaAtnoiAlarm();
      n.setOriginalSeverity(getText(sourceEventNode, "originalSeverity"));
      n.setNeId(getText(sourceEventNode, "neId"));
      n.setNeName(getText(sourceEventNode, "neName"));
      n.setAlarmName(getText(sourceEventNode, "alarmName"));
      n.setAffectedObjectName(getText(sourceEventNode, "affectedObjectName"));
      n.setAffectedObject(getText(sourceEventNode, "affectedObject"));
      n.setAlarmType(getText(sourceEventNode, "alarmType"));
      n.setProbableCause(getText(sourceEventNode, "probableCause"));
      n.setFirstTimeDetected(getLong(sourceEventNode, "firstTimeDetected"));
      n.setLastTimeDetected(getLong(sourceEventNode, "lastTimeDetected"));
      n.setAdminState(getText(sourceEventNode, "adminState"));
      n.setSourceType(getText(sourceEventNode, "sourceType"));
      n.setObjectId(getText(sourceEventNode, "objectId"));
      n.setObjectFullName(getText(sourceEventNode, "objectFullName"));
      n.setAdditionalText(getText(sourceEventNode, "additionalText"));
      n.setSourceSystem(getText(sourceEventNode, "sourceSystem"));

      // Delete fallbacks.
      if (n.getObjectId() == null) n.setObjectId(serialNo);
      if (n.getObjectFullName() == null) n.setObjectFullName(objectFullName);
      if (n.getAlarmName() == null) n.setAlarmName(faultId);
      if (n.getNeId() == null) n.setNeId(neId);
      if (n.getNeName() == null) n.setNeName(neName);

      ue.setSourceEvent(n);
    }

    // enrichedData from context can override only if present.
    Object edObj = ctx.get("enrichedData");
    if (edObj instanceof EnrichedData ed) {
      ue.setEnrichedData(ed);
    }

    // metadata from context can override only if present.
    Object mdObj = ctx.get("metadata");
    if (mdObj instanceof Map<?, ?> m) {
      @SuppressWarnings("unchecked")
      Map<String, Object> md = (Map<String, Object>) m;
      ue.setMetadata(md);
    }

    return ue;
  }

  // ============================================================================================
  // NOKIA NFM-T / WSNOC mapping
  // ============================================================================================

  private static void applyNokiaNfmTMapping(
      UnifiedEvent ue,
      TransformContext ctx,
      JsonNode sourceEventNode,
      JsonNode fieldNode,
      Long tsMs,
      String eventTime,
      String typeStr,
      String sevStr,
      String emsDomainRaw,
      String neId,
      String neName,
      String affectedObjectName,
      String neEquipmentFromCtx,
      String faultId,
      String serialNo,
      String alarmIdentifier,
      String objectFullName
  ) {

    EventType eventType = mapEventType(typeStr);

    /*
     * For NFM-T, sourceType is usually "nfmt".
     * The old mapDomain() does not know "nfmt", so force TRANSPORT unless YAML explicitly provides another valid enum.
     */
    ue.setEmsDomain(parseEnumOrDefault(
        EMSDomain.class,
        emsDomainRaw,
        EMSDomain.TRANSPORT
    ));

    ue.setType(eventType);

    String effectiveSeverity = firstNonBlank(
        sevStr,
        readTextOrNewValue(fieldNode, "severity"),
        readTextOrNewValue(sourceEventNode, "severity")
    );

    ue.setSeverity(mapSeverity(effectiveSeverity));

    Long effectiveTimestampMs;

    if (eventType == EventType.CLEAR) {
      effectiveTimestampMs = firstNonNull(
          tsMs,
          readLongOrNewValue(fieldNode, "lastTimeCleared")
      );

      if (effectiveTimestampMs == null) {
        effectiveTimestampMs = firstNonNull(
            readLongOrNewValue(fieldNode, "lastTimeDetected"),
            readLongOrNewValue(fieldNode, "firstTimeDetected")
        );
      }

    } else {
      effectiveTimestampMs = firstNonNull(
          tsMs,
          readLongOrNewValue(fieldNode, "lastTimeDetected")
      );

      if (effectiveTimestampMs == null) {
        effectiveTimestampMs = firstNonNull(
            readLongOrNewValue(fieldNode, "firstTimeDetected"),
            readLongOrNewValue(fieldNode, "lastTimeCleared")
        );
      }
    }

    ue.setTimestamp(parseEventTime(effectiveTimestampMs, eventTime));

    String effectiveSerialNo = firstNonBlank(
        serialNo,
        getText(fieldNode, "objectId"),
        getText(fieldNode, "fdn")
    );

    String effectiveFaultId = firstNonBlank(
        faultId,
        getText(fieldNode, "alarmName"),
        getText(fieldNode, "probableCause"),
        getText(fieldNode, "specificProblem")
    );

    String effectiveNeName = firstNonBlank(
        neName,
        getText(fieldNode, "neName")
    );

    String effectiveNeEquipment = firstNonBlank(
        neEquipmentFromCtx,
        affectedObjectName,
        getText(fieldNode, "affectedObjectName"),
        ""
    );

    String effectiveObjectFullName = firstNonBlank(
        objectFullName,
        getText(fieldNode, "objectFullName")
    );

    String nfmtIdentifier = joinNonBlank("/", effectiveNeEquipment, effectiveFaultId);
    
    String effectiveAlarmIdentifier = firstNonBlank(
        nfmtIdentifier,
        alarmIdentifier,
        effectiveObjectFullName,
        getText(fieldNode, "affectedObject"),
        effectiveFaultId,
        effectiveSerialNo
    );

    ue.setSerialNo(effectiveSerialNo);
    ue.setFaultId(effectiveFaultId);
    ue.setNeName(effectiveNeName);
    ue.setNeEquipment(effectiveNeEquipment);
    ue.setAlarmIdentifier(effectiveAlarmIdentifier);

    NokiaNfmTAlarm n = new NokiaNfmTAlarm();

    n.setOriginalSeverity(readTextOrNewValue(fieldNode, "originalSeverity"));
    n.setPreviousSeverity(readTextOrNewValue(fieldNode, "previousSeverity"));
    n.setHighestSeverity(readTextOrNewValue(fieldNode, "highestSeverity"));
    n.setSeverity(readTextOrNewValue(fieldNode, "severity"));

    n.setNeId(getText(fieldNode, "neId"));
    n.setNeName(getText(fieldNode, "neName"));

    n.setAlarmName(getText(fieldNode, "alarmName"));
    n.setSpecificProblem(getText(fieldNode, "specificProblem"));
    n.setAffectedObjectName(getText(fieldNode, "affectedObjectName"));
    n.setAffectedObject(getText(fieldNode, "affectedObject"));
    n.setAffectedObjectType(getText(fieldNode, "affectedObjectType"));
    n.setAlarmType(getText(fieldNode, "alarmType"));
    n.setProbableCause(getText(fieldNode, "probableCause"));

    n.setFirstTimeDetected(readLongOrNewValue(fieldNode, "firstTimeDetected"));
    n.setLastTimeDetected(readLongOrNewValue(fieldNode, "lastTimeDetected"));
    n.setLastTimeCleared(readLongOrNewValue(fieldNode, "lastTimeCleared"));
    n.setLastTimeAcknowledged(readLongOrNewValue(fieldNode, "lastTimeAcknowledged"));
    n.setLastTimeSeverityChanged(readLongOrNewValue(fieldNode, "lastTimeSeverityChanged"));
    n.setLastTimeEscalated(readLongOrNewValue(fieldNode, "lastTimeEscalated"));
    n.setLastTimeDeEscalated(readLongOrNewValue(fieldNode, "lastTimeDeEscalated"));

    n.setAdminState(getText(fieldNode, "adminState"));
    n.setSourceType(getText(fieldNode, "sourceType"));
    n.setSourceSystem(getText(fieldNode, "sourceSystem"));

    n.setObjectId(getText(fieldNode, "objectId"));
    n.setFdn(getText(fieldNode, "fdn"));
    n.setObjectFullName(getText(fieldNode, "objectFullName"));

    n.setAdditionalText(getText(fieldNode, "additionalText"));
    n.setUserText(getText(fieldNode, "userText"));

    n.setAcknowledged(readBooleanOrNewValue(fieldNode, "acknowledged"));
    n.setWasAcknowledged(readBooleanOrNewValue(fieldNode, "wasAcknowledged"));
    n.setAcknowledgedBy(getText(fieldNode, "acknowledgedBy"));
    n.setClearedBy(getText(fieldNode, "clearedBy"));
    n.setDeletedBy(getText(fieldNode, "deletedBy"));

    n.setFrequency(readIntOrNewValue(fieldNode, "frequency"));
    n.setNumberOfOccurrences(readIntOrNewValue(fieldNode, "numberOfOccurrences"));
    n.setNumberOfOccurrencesSinceClear(readIntOrNewValue(fieldNode, "numberOfOccurrencesSinceClear"));
    n.setNumberOfOccurrencesSinceAck(readIntOrNewValue(fieldNode, "numberOfOccurrencesSinceAck"));

    n.setServiceAffecting(readBooleanOrNewValue(fieldNode, "serviceAffecting"));
    n.setImplicitlyCleared(readBooleanOrNewValue(fieldNode, "implicitlyCleared"));
    n.setRootCause(readBooleanOrNewValue(fieldNode, "rootCause"));

    n.setImpact(readIntOrNewValue(fieldNode, "impact"));
    n.setNodeTimeOffset(readIntOrNewValue(fieldNode, "nodeTimeOffset"));

    n.setNotificationType(clean(ctx.get("alarmEventKind")));
    n.setEventTime(eventTime);

    // Fallbacks for DELETE or sparse notifications.
    if (n.getObjectId() == null) n.setObjectId(effectiveSerialNo);
    if (n.getFdn() == null) n.setFdn(getText(fieldNode, "fdn"));
    if (n.getObjectFullName() == null) n.setObjectFullName(effectiveObjectFullName);
    if (n.getAlarmName() == null) n.setAlarmName(effectiveFaultId);
    if (n.getNeId() == null) n.setNeId(neId);
    if (n.getNeName() == null) n.setNeName(effectiveNeName);
    if (n.getAffectedObjectName() == null) n.setAffectedObjectName(effectiveNeEquipment);

    ue.setSourceEvent(n);
  }

  // ============================================================================================
  // EXAGRID mapping
  // ============================================================================================

  private void applyExaGridMapping(
      UnifiedEvent ue,
      JsonNode sourceEventNode,
      Long tsMs,
      String eventTime,
      String typeStr,
      String sevStr
  ) {

    JsonNode fields = sourceEventNode != null ? sourceEventNode.get("fields") : null;

    String egId       = getText(fields, "egEventParamsId");
    String egName     = getText(fields, "egEventParamsName");
    String egDevName  = getText(fields, "egEventParamsDeviceName");
    String egSev      = getText(fields, "egEventParamsSeverity");
    String egCreateMs = getText(fields, "egEventParamsCreateTimeRaw");

    ue.setEmsDomain(EMSDomain.UNKNOWN);
    ue.setSerialNo(egId);
    ue.setFaultId(egName);
    ue.setNeName(egDevName);
    ue.setNeEquipment("");
    ue.setAlarmIdentifier(null);

    String exaType = classifyExaGridType(egSev);

    log.info("EXAGRID sanity: egSev=[{}] normalized=[{}] => exaType=[{}] ctx.type=[{}] ctx.sev=[{}]",
        egSev, normalize(egSev), exaType, typeStr, sevStr);

    ue.setType(mapEventType(exaType));
    ue.setSeverity(Severity.UNKNOWN);

    Long egTsMs = toEpochMillis(egCreateMs);
    ue.setTimestamp(parseEventTime(firstNonNull(egTsMs, tsMs), eventTime));
  }

  private static String classifyExaGridType(String egSeverityRaw) {
    String s = normalize(egSeverityRaw);
    if ("error".equals(s)) return "FAULT";
    return "EVENT";
  }

  // ============================================================================================
  // MV36_MOBILE mapping
  // ============================================================================================

  private void applyMv36MobileMapping(
      UnifiedEvent ue,
      JsonNode sourceEventNode,
      Long ctxTsMs,
      String eventTime,
      String emsDomainRaw
  ) {

    JsonNode fields = sourceEventNode != null ? sourceEventNode.get("fields") : null;

    String neUniqueName = findFieldText(fields, "mv36AlarmStrNeUniqueName");
    String shelf        = findFieldText(fields, "mv36AlarmStrShelf");
    String card         = findFieldText(fields, "mv36AlarmStrCard");
    String portId       = findFieldText(fields, "mv36AlarmPortId");
    String alarmStr     = findFieldText(fields, "mv36AlarmStr");
    String alarmNeId    = findFieldText(fields, "mv36AlarmNeId");

    String mv36NeName = null;
    String mv36NeTypeStr = null;
    String mv36NeUniqueName = null;
    String mv36NeId = null;

    log.info(
        "MV36 Kafka enrichment lookup: alarmNeId={}, neUniqueName={}, enrichmentServiceAvailable={}",
        alarmNeId,
        neUniqueName,
        mv36NeEnrichmentService != null
    );

    if (mv36NeEnrichmentService != null) {
      var neOpt = mv36NeEnrichmentService.findByAlarmNeId(alarmNeId);

      if (neOpt.isEmpty() && neUniqueName != null && !neUniqueName.isBlank()) {
        neOpt = mv36NeEnrichmentService.findByUniqueName(neUniqueName);
      }

      if (neOpt.isPresent()) {
        Mv36NetworkElement ne = neOpt.get();

        mv36NeId = clean(ne.getMv36NeId());
        mv36NeName = clean(ne.getMv36NeName());
        mv36NeUniqueName = clean(ne.getMv36NeUniqueName());
        mv36NeTypeStr = clean(ne.getMv36NeTypeStr());

      } else {
        log.warn(
            "MV36 Kafka enrichment cache miss: alarmNeId={}, neUniqueName={}",
            alarmNeId,
            neUniqueName
        );
      }
    }

    String effectiveNeName = firstNonBlank(
        mv36NeName,
        neUniqueName,
        alarmNeId
    );

    String raisingHex = findFieldText(fields, "mv36AlarmRaisingTime");
    Long mv36TsMs = mv36DateAndTimeHexToEpochMs(raisingHex);

    Integer sevCode = findFieldInt(fields, "mv36AlarmSeverity");
    String serial = findFieldText(fields, "mv36AlarmId");

    ue.setEmsDomain(parseEnumOrDefault(EMSDomain.class, emsDomainRaw, EMSDomain.UNKNOWN));

    ue.setNeName(clean(effectiveNeName));
    ue.setSerialNo(clean(serial));
    ue.setFaultId(clean(alarmStr));

    String neEquipment = joinNonBlank("/", shelf, card, portId);
    ue.setNeEquipment(neEquipment != null ? neEquipment : "");

    String identifier = joinNonBlank("/", effectiveNeName, shelf, card, portId, alarmStr);
    ue.setAlarmIdentifier(firstNonBlank(identifier, alarmStr, serial));

    ue.setEnrichedData(buildMv36EnrichedData(effectiveNeName));

    ue.setSeverity(mapMv36Severity(sevCode));
    ue.setType(mapMv36Type(sevCode));

    if (ue.getSourceEvent() instanceof TelegrafGenericEvent t && t.getFields() != null) {
      Map<String, String> srcFields = t.getFields();

      if (mv36NeId != null) srcFields.put("mv36NeId", mv36NeId);
      if (mv36NeName != null) srcFields.put("mv36NeName", mv36NeName);
      if (mv36NeUniqueName != null) srcFields.put("mv36NeUniqueName", mv36NeUniqueName);
      if (mv36NeTypeStr != null) srcFields.put("mv36NeTypeStr", mv36NeTypeStr);
    }

    ue.setTimestamp(parseEventTime(firstNonNull(mv36TsMs, ctxTsMs), eventTime));
  }

  private static EnrichedData buildMv36EnrichedData(String neName) {
    String locationName = extractLocationNameFromMv36NeName(neName);

    if (locationName == null) {
      return null;
    }

    Map<String, Object> affectedLocation = new LinkedHashMap<>();
    affectedLocation.put("inventoryId", null);
    affectedLocation.put("name", locationName);
    affectedLocation.put("longitude", null);
    affectedLocation.put("latitude", null);

    Map<String, Object> root = new LinkedHashMap<>();
    root.put("affectedLocation", affectedLocation);
    root.put("transport", null);
    root.put("affectedSite", null);
    root.put("affectedController", null);
    root.put("affectedCell", null);
    root.put("affectedCellId", null);
    root.put("affectedTechnologies", null);
    root.put("probableCause", null);

    return ENRICHED_DATA_MAPPER.convertValue(root, EnrichedData.class);
  }

  private static String extractLocationNameFromMv36NeName(String neName) {
    String s = clean(neName);

    if (s == null) {
      return null;
    }

    int dash = s.indexOf('-');

    if (dash <= 0) {
      return null;
    }

    String location = s.substring(0, dash).trim();

    return location.isEmpty() ? null : location;
  }

  private static Severity mapMv36Severity(Integer code) {
    if (code == null) return Severity.UNKNOWN;
    return switch (code) {
      case 1 -> Severity.CLEARED;
      case 2 -> Severity.INDETERMINATE;
      case 3 -> Severity.CRITICAL;
      case 4 -> Severity.MAJOR;
      case 5 -> Severity.MINOR;
      case 6 -> Severity.WARNING;
      default -> Severity.UNKNOWN;
    };
  }

  private static EventType mapMv36Type(Integer code) {
    if (code == null) return EventType.UNKNOWN;
    return switch (code) {
      case 1 -> EventType.CLEAR;
      case 3, 4, 5, 6 -> EventType.FAULT;
      default -> EventType.UNKNOWN;
    };
  }

  // ============================================================================================
  // MV36 field lookup helpers
  // ============================================================================================

  private static String findFieldText(JsonNode fields, String baseName) {
    if (fields == null || !fields.isObject() || baseName == null) {
      return null;
    }

    JsonNode exact = fields.get(baseName);
    if (exact != null && !exact.isNull()) {
      return exact.asString();
    }

    return findFieldTextByPrefix(fields, baseName + ".");
  }

  private static Integer findFieldInt(JsonNode fields, String baseName) {
    String s = findFieldText(fields, baseName);
    if (s == null || s.isBlank()) {
      return null;
    }

    try {
      return Integer.parseInt(s.trim());
    } catch (NumberFormatException ignore) {
      return null;
    }
  }

  private static String findFieldTextByPrefix(JsonNode fields, String prefix) {
    if (fields == null || !fields.isObject() || prefix == null) {
      return null;
    }

    for (Map.Entry<String, JsonNode> e : fields.properties()) {
      if (e.getKey().startsWith(prefix)) {
        JsonNode value = e.getValue();
        return value == null || value.isNull() ? null : value.asString();
      }
    }

    return null;
  }

  private static String joinNonBlank(String sep, String... parts) {
    if (parts == null) return null;

    StringBuilder sb = new StringBuilder();

    for (String p : parts) {
      if (p == null) continue;

      String t = p.trim();
      if (t.isEmpty()) continue;
      if ("--".equals(t)) continue;

      if (sb.length() > 0) sb.append(sep);
      sb.append(t);
    }

    return sb.length() == 0 ? null : sb.toString();
  }

  private static Long mv36DateAndTimeHexToEpochMs(String hex) {
    if (hex == null || hex.isBlank()) return null;

    try {
      byte[] b = hexStringToBytes(hex.trim());
      if (b.length < 8) return null;

      int year  = ((b[0] & 0xFF) << 8) | (b[1] & 0xFF);
      int month = (b[2] & 0xFF);
      int day   = (b[3] & 0xFF);
      int hour  = (b[4] & 0xFF);
      int min   = (b[5] & 0xFF);
      int sec   = (b[6] & 0xFF);
      int deci  = (b[7] & 0xFF);

      if (year == 0 && month == 1 && day == 1 && hour == 0 && min == 0 && sec == 0 && deci == 0) {
        return null;
      }

      ZoneId zone = MV36_DEFAULT_ZONE;

      if (b.length >= 11) {
        char dir = (char) (b[8] & 0xFF);
        int tzH = (b[9] & 0xFF);
        int tzM = (b[10] & 0xFF);

        int totalMin = tzH * 60 + tzM;
        if (dir == '-') totalMin = -totalMin;

        zone = ZoneOffset.ofTotalSeconds(totalMin * 60);
      }

      int nanos = deci * 100_000_000;

      LocalDateTime ldt = LocalDateTime.of(year, month, day, hour, min, sec, nanos);
      return ldt.atZone(zone).toInstant().toEpochMilli();

    } catch (Exception ignore) {
      return null;
    }
  }

  private static byte[] hexStringToBytes(String s) {
    int len = s.length();

    if (len % 2 != 0) {
      throw new IllegalArgumentException("hex length must be even");
    }

    byte[] out = new byte[len / 2];

    for (int i = 0; i < len; i += 2) {
      out[i / 2] = (byte) Integer.parseInt(s.substring(i, i + 2), 16);
    }

    return out;
  }

  // ============================================================================================
  // NOKIA / ALCATEL 1350 OMS mapping
  // ============================================================================================

  private void applyOms1350Mapping(
      UnifiedEvent ue,
      EMSId sourceEms,
      JsonNode sourceEventNode,
      Long ctxTsMs,
      String eventTimeFromCtx,
      String emsDomainRaw
  ) {
    JsonNode fields = sourceEventNode != null ? sourceEventNode.get("fields") : null;
    JsonNode tags   = sourceEventNode != null ? sourceEventNode.get("tags") : null;

    String trapName = getText(tags, "name");
    String trapOid  = getText(tags, "oid");

    String currentAlarmId = getText(fields, "currentAlarmId");
    String eventTime      = getText(fields, "eventTime");
    String friendlyName   = getText(fields, "friendlyName");
    String perceivedSev   = getText(fields, "perceivedSeverity");
    String probableCause  = getText(fields, "probableCause");

    boolean isClear =
        "alarmHandoffTraps.0.2".equals(trapName)
            || ".1.3.6.1.4.1.637.65.1.1.2.0.2".equals(trapOid);

    ue.setSourceEvent(toTelegrafGenericEvent(sourceEventNode, false));

    ue.setEmsDomain(parseEnumOrDefault(
        EMSDomain.class,
        emsDomainRaw,
        EMSDomain.TRANSPORT
    ));

    ue.setType(isClear ? EventType.CLEAR : EventType.FAULT);
    ue.setSeverity(isClear ? Severity.CLEARED : mapSeverity(perceivedSev));

    Long omsTsMs = oms1350EventTimeToEpochMs(eventTime);

    ue.setTimestamp(parseEventTime(
        firstNonNull(omsTsMs, ctxTsMs),
        eventTimeFromCtx
    ));

    ue.setSerialNo(clean(currentAlarmId));
    ue.setFaultId(clean(probableCause));

    String neName = extractOms1350NeName(friendlyName);
    String neEquipment = extractOms1350NeEquipment(friendlyName);

    if (sourceEms == EMSId.NOKIA_1350_OTNE) {
      // OTNE special mapping:
      //   neName      <- friendlyName
      //   neEquipment <- null
      ue.setNeName(clean(friendlyName));
      ue.setNeEquipment(clean(friendlyName));
    } else if (sourceEms == EMSId.NOKIA_1350_EML1
        || sourceEms == EMSId.NOKIA_1350_EML2) {
      // EML1 / EML2:
      //   neName      <- before first "/" in friendlyName
      //   neEquipment <- after first "/" in friendlyName
      //
      // Fallbacks:
      //   if neName is null      -> friendlyName
      //   if neEquipment is null -> friendlyName
      ue.setNeName(firstNonBlank(neName, friendlyName, getText(tags, "agent_address")));
      ue.setNeEquipment(firstNonBlank(neEquipment, friendlyName));

    } else {
      // PKT and any future 1350 flow:
      // keep previous behavior
      ue.setNeName(firstNonBlank(neName, friendlyName, getText(tags, "agent_address")));
      ue.setNeEquipment(firstNonBlank(neEquipment, ""));
    }

    boolean useFriendlyNameProbableCauseIdentifier =
    sourceEms == EMSId.NOKIA_1350_EML1
        || sourceEms == EMSId.NOKIA_1350_EML2
        || sourceEms == EMSId.NOKIA_1350_OTNE;

    if (useFriendlyNameProbableCauseIdentifier) {
      String identifier = joinNonBlank("/", friendlyName, probableCause);
      ue.setAlarmIdentifier(firstNonBlank(identifier, currentAlarmId));
    } else {
      ue.setAlarmIdentifier(clean(currentAlarmId));
    }
  }

  private static String extractOms1350NeName(String friendlyName) {
    String s = clean(friendlyName);
    if (s == null) return null;

    int slash = s.indexOf('/');
    if (slash > 0) {
      return s.substring(0, slash).trim();
    }

    return s;
  }

  private static String extractOms1350NeEquipment(String friendlyName) {
    String s = clean(friendlyName);
    if (s == null) return "";

    int slash = s.indexOf('/');
    if (slash > 0 && slash + 1 < s.length()) {
      return s.substring(slash + 1).trim();
    }

    return "";
  }

  private static Long oms1350EventTimeToEpochMs(String eventTime) {
    if (eventTime == null || eventTime.isBlank()) {
      return null;
    }

    try {
      DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
      LocalDateTime ldt = LocalDateTime.parse(eventTime.trim(), fmt);

      return ldt.atZone(OMS1350_DEFAULT_ZONE).toInstant().toEpochMilli();

    } catch (Exception ignore) {
      return null;
    }
  }

  // ============================================================================================
  // Generic helpers
  // ============================================================================================

  private static TelegrafGenericEvent toTelegrafGenericEvent(JsonNode sourceEventNode, boolean trimFieldKeysFromDot) {
    TelegrafGenericEvent t = new TelegrafGenericEvent();

    if (sourceEventNode == null) {
      return t;
    }

    Map<String, String> fields = asStringMap(sourceEventNode.get("fields"));

    if (trimFieldKeysFromDot && fields != null && !fields.isEmpty()) {
      fields = trimKeysFromFirstDot(fields);
    }

    t.setFields(fields);
    t.setTags(asStringMap(sourceEventNode.get("tags")));
    t.setTimestamp(toEpochSecondsLong(sourceEventNode.get("timestamp")));

    return t;
  }

  private static Map<String, String> trimKeysFromFirstDot(Map<String, String> in) {
    Map<String, String> out = new LinkedHashMap<>();

    for (Map.Entry<String, String> e : in.entrySet()) {
      String k = e.getKey();

      if (k == null) {
        continue;
      }

      int dot = k.indexOf('.');
      String nk = (dot > 0) ? k.substring(0, dot) : k;

      out.putIfAbsent(nk, e.getValue());
    }

    return out;
  }

  private static String normalize(String v) {
    if (v == null) return "";
    return v.trim().replace("\"", "").replace("'", "").toLowerCase();
  }

  private static String clean(Object v) {
    if (v == null) return null;

    String s = String.valueOf(v).trim();

    if ((s.startsWith("'") && s.endsWith("'")) || (s.startsWith("\"") && s.endsWith("\""))) {
      s = s.substring(1, s.length() - 1).trim();
    }

    if (s.isEmpty() || "--".equals(s)) {
      return null;
    }

    return s;
  }

  private static Instant parseEventTime(Long tsMs, String eventTime) {
    if (tsMs != null && tsMs > 0) {
      return Instant.ofEpochMilli(tsMs);
    }

    if (eventTime != null && !eventTime.isBlank()) {
      try {
        return Instant.parse(eventTime);
      } catch (Exception ignore) {
        // fallback below
      }
    }

    return Instant.now();
  }

  private static EventType mapEventType(String type) {
    if (type == null) return EventType.FAULT;

    String t = type.trim();

    if ((t.startsWith("'") && t.endsWith("'")) || (t.startsWith("\"") && t.endsWith("\""))) {
      t = t.substring(1, t.length() - 1).trim();
    }

    return switch (t.toUpperCase()) {
      case "EVENT" -> EventType.EVENT;
      case "CLEAR" -> EventType.CLEAR;
      case "CHANGE" -> EventType.CHANGE;
      case "FAULT" -> EventType.FAULT;
      case "FAULT_SYNC" -> EventType.FAULT_SYNC;
      case "SYNC_START" -> EventType.SYNC_START;
      case "SYNC_END" -> EventType.SYNC_END;
      case "ACKNOWLEDGE" -> EventType.ACKNOWLEDGE;
      case "UNACKNOWLEDGE" -> EventType.UNACKNOWLEDGE;
      default -> EventType.UNKNOWN;
    };
  }

  private static Severity mapSeverity(String sev) {
    if (sev == null) return Severity.UNKNOWN;

    String s = sev.trim().toLowerCase();

    return switch (s) {
      case "critical" -> Severity.CRITICAL;
      case "major" -> Severity.MAJOR;
      case "minor" -> Severity.MINOR;
      case "warning" -> Severity.WARNING;
      case "cleared" -> Severity.CLEARED;
      case "indeterminate" -> Severity.INDETERMINATE;
      default -> Severity.UNKNOWN;
    };
  }

  private static EMSDomain mapDomain(String sourceType) {
    if (sourceType == null) return EMSDomain.UNKNOWN;

    return switch (sourceType.toLowerCase()) {
      case "mdm" -> EMSDomain.TRANSPORT;
      default -> EMSDomain.UNKNOWN;
    };
  }

  private static String firstNonBlank(String... vals) {
    if (vals == null) return null;

    for (String v : vals) {
      if (v != null && !v.isBlank()) {
        return v;
      }
    }

    return null;
  }

  private static Long firstNonNull(Long a, Long b) {
    return a != null ? a : b;
  }

  private static boolean looksLikeTelegraf(JsonNode n) {
    return n != null && n.has("fields") && n.has("tags");
  }

  private static Map<String, String> asStringMap(JsonNode obj) {
    if (obj == null || !obj.isObject()) {
      return null;
    }

    Map<String, String> out = new HashMap<>();

    obj.properties().forEach(e -> out.put(
        e.getKey(),
        e.getValue().isNull() ? null : e.getValue().asString()
    ));

    return out;
  }

  private static Long toEpochSecondsLong(JsonNode v) {
    if (v == null || v.isNull()) return null;

    if (v.isNumber()) {
      return v.longValue();
    }

    if (v.isString()) {
      try {
        return Long.parseLong(v.asString().trim());
      } catch (NumberFormatException ignore) {
        // ignore
      }
    }

    return null;
  }

  private static Long toEpochMillis(Object v) {
    if (v == null) return null;

    if (v instanceof Long l) return normalizeToMillis(l.doubleValue());
    if (v instanceof Integer i) return normalizeToMillis(i.doubleValue());
    if (v instanceof Number n) return normalizeToMillis(n.doubleValue());

    if (v instanceof String s) {
      String t = s.trim();

      if (t.isEmpty()) {
        return null;
      }

      try {
        return normalizeToMillis(Double.parseDouble(t));
      } catch (NumberFormatException ignore) {
        // ignore
      }
    }

    return null;
  }

  private static Long normalizeToMillis(double d) {
    if (d <= 0) return null;

    // decimal seconds
    if (d > 1e9 && d < 1e12) {
      return (long) Math.round(d * 1000.0);
    }

    // millis already
    if (d >= 1e12) {
      return (long) Math.round(d);
    }

    return null;
  }

  private static Long tnmsTimestampToMs(String tsStr) {
    if (tsStr == null || tsStr.isBlank()) return null;

    try {
      LocalDateTime ldt = LocalDateTime.parse(tsStr.trim(), TNMS_TS_FMT);
      return ldt.atZone(TNMS_ZONE).toInstant().toEpochMilli();
    } catch (Exception ignore) {
      return null;
    }
  }

  private static String getText(JsonNode n, String field) {
    if (n == null || field == null) {
      return null;
    }

    JsonNode v = n.get(field);

    if (v == null || v.isNull()) {
      return null;
    }

    if (v.isString() || v.isNumber() || v.isBoolean()) {
      return v.asString();
    }

    if (v.isObject()) {
      JsonNode newValue = v.get("new-value");

      if (newValue != null
          && !newValue.isNull()
          && (newValue.isString()
              || newValue.isNumber()
              || newValue.isBoolean())) {
        return newValue.asString();
      }

      JsonNode oldValue = v.get("old-value");

      if (oldValue != null
          && !oldValue.isNull()
          && (oldValue.isString()
              || oldValue.isNumber()
              || oldValue.isBoolean())) {
        return oldValue.asString();
      }
    }

    return null;
  }

  private static Long getLong(JsonNode n, String field) {
    if (n == null) return null;

    JsonNode v = n.get(field);

    return (v == null || v.isNull()) ? null : v.asLong();
  }

  private static String readTextOrNewValue(JsonNode node, String field) {
    if (node == null || field == null) {
      return null;
    }
  
    JsonNode v = node.get(field);
  
    if (v == null || v.isNull()) {
      return null;
    }
  
    if (v.isObject()) {
      JsonNode newValue = v.get("new-value");
    
      if (newValue != null
          && !newValue.isNull()
          && (newValue.isString()
              || newValue.isNumber()
              || newValue.isBoolean())) {
        return newValue.asString();
      }
    
      JsonNode oldValue = v.get("old-value");
    
      if (oldValue != null
          && !oldValue.isNull()
          && (oldValue.isString()
              || oldValue.isNumber()
              || oldValue.isBoolean())) {
        return oldValue.asString();
      }
    
      return null;
    }
  
    if (v.isString() || v.isNumber() || v.isBoolean()) {
      return v.asString();
    }
  
    return null;
  }

  private static Long readLongOrNewValue(JsonNode node, String field) {
    if (node == null || field == null) {
      return null;
    }

    JsonNode v = node.get(field);

    if (v == null || v.isNull()) {
      return null;
    }

    if (v.isObject()) {
      JsonNode newValue = v.get("new-value");
      if (newValue != null && !newValue.isNull()) {
        return jsonNodeToLong(newValue);
      }

      JsonNode oldValue = v.get("old-value");
      if (oldValue != null && !oldValue.isNull()) {
        return jsonNodeToLong(oldValue);
      }

      return null;
    }

    return jsonNodeToLong(v);
  }

  private static Integer readIntOrNewValue(JsonNode node, String field) {
    if (node == null || field == null) {
      return null;
    }

    JsonNode v = node.get(field);

    if (v == null || v.isNull()) {
      return null;
    }

    if (v.isObject()) {
      JsonNode newValue = v.get("new-value");
      if (newValue != null && !newValue.isNull()) {
        return jsonNodeToInt(newValue);
      }

      JsonNode oldValue = v.get("old-value");
      if (oldValue != null && !oldValue.isNull()) {
        return jsonNodeToInt(oldValue);
      }

      return null;
    }

    return jsonNodeToInt(v);
  }

  private static Boolean readBooleanOrNewValue(JsonNode node, String field) {
    if (node == null || field == null) {
      return null;
    }

    JsonNode v = node.get(field);

    if (v == null || v.isNull()) {
      return null;
    }

    if (v.isObject()) {
      JsonNode newValue = v.get("new-value");
      if (newValue != null && !newValue.isNull()) {
        return jsonNodeToBoolean(newValue);
      }

      JsonNode oldValue = v.get("old-value");
      if (oldValue != null && !oldValue.isNull()) {
        return jsonNodeToBoolean(oldValue);
      }

      return null;
    }

    return jsonNodeToBoolean(v);
  }

  private static Long jsonNodeToLong(JsonNode v) {
    if (v == null || v.isNull()) {
      return null;
    }

    if (v.isNumber()) {
      return v.asLong();
    }

    if (!v.isString()) {
      return null;
    }

    String value = v.asString();

    if (value == null || value.isBlank()) {
      return null;
    }

    try {
      return Long.valueOf(value.trim());
    } catch (NumberFormatException ignore) {
      return null;
    }
  }

  private static Integer jsonNodeToInt(JsonNode v) {
    if (v == null || v.isNull()) {
    return null;
  }
  
    if (v.isNumber()) {
      return v.asInt();
    }
  
    if (!v.isString()) {
      return null;
    }
  
    String value = v.asString();
  
    if (value == null || value.isBlank()) {
      return null;
    }
  
    try {
      return Integer.valueOf(value.trim());
    } catch (NumberFormatException ignore) {
      return null;
    }
  }

  private static Boolean jsonNodeToBoolean(JsonNode v) {
    if (v == null || v.isNull()) {
      return null;
    }

    if (v.isBoolean()) {
      return v.asBoolean();
    }

    if (v.isString()) {
      String s = v.asString();

      if (s == null || s.isBlank()) {
        return null;
      }

      String normalized = s.trim().toLowerCase();

      return switch (normalized) {
        case "true", "yes", "1" -> Boolean.TRUE;
        case "false", "no", "0" -> Boolean.FALSE;
        default -> null;
      };
    }

    if (v.isNumber()) {
      int i = v.asInt();
      if (i == 1) return Boolean.TRUE;
      if (i == 0) return Boolean.FALSE;
    }

    return null;
  }

  private static <E extends Enum<E>> E parseEnumOrDefault(Class<E> enumClass, String raw, E fallback) {
    if (raw == null || raw.isBlank()) return fallback;

    try {
      return Enum.valueOf(enumClass, raw.trim());
    } catch (Exception ignore) {
      return fallback;
    }
  }
}