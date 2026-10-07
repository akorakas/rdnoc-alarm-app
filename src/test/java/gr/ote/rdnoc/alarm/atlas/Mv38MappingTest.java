package gr.ote.rdnoc.alarm.atlas;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.support.StaticListableBeanFactory;

import gr.ote.rdnoc.alarm.mv36.enrich.Mv36NeEnrichmentService;
import gr.ote.rdnoc.alarm.service.Transformer;
import gr.ote.rdnoc.alarm.service.config.TransformProperties;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/** Runs the MV38 Fixed pipeline (as in application-mv38fixed.yml) on real collector samples. */
class Mv38MappingTest {

  private static final ObjectMapper M = new ObjectMapper();

  private static final String RAISE = """
      {"ts":"Thu Oct 01 14:03:45 EEST 2026","kind":"event",
       "alarmId":"KILKIS/3sl3/p07-THEB/7/sl22/p15 30001 Kilkis_OIKIAKO-TheB 1219932",
       "resourceName":"KILKIS/3sl3/p07-THEB/7/sl22/p15 30001 Kilkis_OIKIAKO-TheB 121993",
       "resource":"path","nmId":"","pathLinkId":"193835","signalType":"5 (SignalType_vc12)","lnType":"",
       "pathStruct":"0 (PathStruct_notStructurable)","resourceState":"16 (ResourceState_disableActivedActived)",
       "customerData":"","severity":"critical","raisingTime":"20261001140316-0200","ackTime":"","ackUser":"",
       "clearTime":"","clearUser":"","clearReason":"","clearAckTime":"","clearAckUser":"",
       "alarmState":"on","serviceState":"1 (ServiceState_inService)"}""";

  private static final String CLEAR = """
      {"ts":"Thu Oct 01 14:03:27 EEST 2026","kind":"event",
       "alarmId":"KILKIS/3sl3/p07-THEB/7/sl22/p15 30001 Kilkis_OIKIAKO-TheB 1219932",
       "resourceName":"KILKIS/3sl3/p07-THEB/7/sl22/p15 30001 Kilkis_OIKIAKO-TheB 121993",
       "resource":"path","nmId":"","pathLinkId":"193835","signalType":"5 (SignalType_vc12)","lnType":"",
       "pathStruct":"0 (PathStruct_notStructurable)","resourceState":"11 (ResourceState_enableActivedActived)",
       "customerData":"","severity":"critical","raisingTime":"20261001140204-0200","ackTime":"","ackUser":"",
       "clearTime":"20261001140318-0200","clearUser":"","clearReason":"2","clearAckTime":"","clearAckUser":"",
       "alarmState":"deleted","serviceState":"1 (ServiceState_inService)"}""";

  private static final String SNAPSHOT = """
      {"ts":"Thu Oct 01 14:02:44 EEST 2026","kind":"snapshot","alarmId":"test12345465","resourceName":"test1234546",
       "resource":"path","nmId":"","pathLinkId":"202794","signalType":"3 (SignalType_vc3)","lnType":"",
       "pathStruct":"0 (PathStruct_notStructurable)","resourceState":"6 (ResourceState_disabledActivated)",
       "customerData":"","severity":"critical","raisingTime":"20260721165042-0200","ackTime":"","ackUser":"",
       "clearTime":"","clearUser":"","clearReason":"","clearAckTime":"","clearAckUser":"",
       "alarmState":"on","serviceState":"1 (ServiceState_inService)"}""";

  private static final String MOBILE_SNAPSHOT = """
      {"ts":"Thu Oct 01 16:42:28 EEST 2026","kind":"snapshot","alarmId":"IP-0068-PATISSION-0073-NYMA.A_900_MPLS42",
       "resourceName":"IP-0068-PATISSION-0073-NYMA.A_900_MPLS","resource":"path","nmId":"","pathLinkId":"4625",
       "signalType":"8 (SignalType_vc4_ncv)","lnType":"","pathStruct":"0 (PathStruct_notStructurable)",
       "resourceState":"23 (ResourceState_disable)","customerData":"","severity":"critical",
       "raisingTime":"20230202150535-0200","ackTime":"","ackUser":"","clearTime":"","clearUser":"","clearReason":"",
       "clearAckTime":"","clearAckUser":"","alarmState":"on","serviceState":""}""";

  private static Transformer mv38Transformer() {
    return transformer("MV38_FIXED");
  }

  private static Transformer transformer(String sourceEms) {
    var extract = new TransformProperties.Step();
    extract.setType("extract");
    extract.setFailOnMissing(false);
    extract.setFailOnBadJson(true);
    extract.setMappings(Map.of("sourceEvent", "/"));

    var update = new TransformProperties.Step();
    update.setType("update");
    update.setCompute(List.of(
        assign("sourceEms", "'" + sourceEms + "'"),
        assign("emsVendorID", "'UNKNOWN'"),
        assign("emsDomain", "'TRANSPORT'")));

    var unified = new TransformProperties.Step();
    unified.setType("unifiedEvent");
    unified.setTarget("$");

    var mapper = new UnifiedEventMapper(
        new StaticListableBeanFactory().getBeanProvider(Mv36NeEnrichmentService.class));

    return new Transformer("x", List.of(extract, update, unified), null, mapper);
  }

  private static TransformProperties.ComputeAssignment assign(String set, String expr) {
    var c = new TransformProperties.ComputeAssignment();
    c.setSet(set);
    c.setExpr(expr);
    return c;
  }

  private static JsonNode run(String json) {
    return M.readTree(mv38Transformer().transform(json));
  }

  private static Instant ts(JsonNode ue) {
    // Transformer writes Instant as decimal seconds, e.g. 1790856196.000000000
    BigDecimal sec = new BigDecimal(ue.get("timestamp").asString());
    return Instant.ofEpochMilli(sec.movePointRight(3).longValue());
  }

  @Test
  void raiseIsMappedAsRequested() {
    JsonNode ue = run(RAISE);

    assertThat(ue.get("sourceEms").asString()).isEqualTo("MV38_FIXED");
    assertThat(ue.get("emsDomain").asString()).isEqualTo("TRANSPORT");
    assertThat(ue.get("type").asString()).isEqualTo("FAULT");
    assertThat(ue.get("severity").asString()).isEqualTo("CRITICAL");

    assertThat(ue.get("neName").asString())
        .isEqualTo("KILKIS/3sl3/p07-THEB/7/sl22/p15 30001 Kilkis_OIKIAKO-TheB 121993");
    assertThat(ue.get("neEquipment").asString()).isEqualTo("path/5 (SignalType_vc12)");
    assertThat(ue.get("faultId").asString()).isEqualTo("16 (ResourceState_disableActivedActived)");
    assertThat(ue.get("alarmIdentifier").asString()).isEqualTo(
        "KILKIS/3sl3/p07-THEB/7/sl22/p15 30001 Kilkis_OIKIAKO-TheB 121993"
            + "/path/5 (SignalType_vc12)/16 (ResourceState_disableActivedActived)");
    assertThat(ue.get("serialNo").asString())
        .isEqualTo("KILKIS/3sl3/p07-THEB/7/sl22/p15 30001 Kilkis_OIKIAKO-TheB 1219932");

    // 14:03:16 Athens (EEST, +03:00) = 11:03:16Z
    assertThat(ts(ue)).isEqualTo(Instant.parse("2026-10-01T11:03:16Z"));

    JsonNode se = ue.get("sourceEvent");
    assertThat(se.get("@type").asString()).isEqualTo("GenericSourceEvent");
    assertThat(se.get("pathLinkId").asString()).isEqualTo("193835");
    assertThat(se.get("kind").asString()).isEqualTo("event");
  }

  @Test
  void deletedIsAClearTimedAtClearTime() {
    JsonNode ue = run(CLEAR);

    assertThat(ue.get("type").asString()).isEqualTo("CLEAR");
    assertThat(ue.get("severity").asString()).isEqualTo("CLEARED");
    assertThat(ts(ue)).isEqualTo(Instant.parse("2026-10-01T11:03:18Z"));
    // Same serialNo as the raise -> the stable key for matching FAULT and CLEAR
    assertThat(ue.get("serialNo").asString())
        .isEqualTo("KILKIS/3sl3/p07-THEB/7/sl22/p15 30001 Kilkis_OIKIAKO-TheB 1219932");
    // resourceState differs from the raise, so faultId/alarmIdentifier differ too
    assertThat(ue.get("faultId").asString()).isEqualTo("11 (ResourceState_enableActivedActived)");
  }

  @Test
  void snapshotRecordIsAFaultSyncWithItsOriginalRaisingTime() {
    JsonNode ue = run(SNAPSHOT);

    assertThat(ue.get("type").asString()).isEqualTo("FAULT_SYNC");
    assertThat(ue.get("severity").asString()).isEqualTo("CRITICAL");
    assertThat(ue.get("neEquipment").asString()).isEqualTo("path/3 (SignalType_vc3)");
    // July = EEST (+03:00): 16:50:42 local = 13:50:42Z
    assertThat(ts(ue)).isEqualTo(Instant.parse("2026-07-21T13:50:42Z"));
  }

  @Test
  void mobileSnapshotUsesMv38MobileAndWinterTime() {
    JsonNode ue = M.readTree(transformer("MV38_MOBILE").transform(MOBILE_SNAPSHOT));

    assertThat(ue.get("sourceEms").asString()).isEqualTo("MV38_MOBILE");
    assertThat(ue.get("type").asString()).isEqualTo("FAULT_SYNC");
    assertThat(ue.get("neName").asString()).isEqualTo("IP-0068-PATISSION-0073-NYMA.A_900_MPLS");
    assertThat(ue.get("neEquipment").asString()).isEqualTo("path/8 (SignalType_vc4_ncv)");
    assertThat(ue.get("faultId").asString()).isEqualTo("23 (ResourceState_disable)");
    assertThat(ue.get("alarmIdentifier").asString()).isEqualTo(
        "IP-0068-PATISSION-0073-NYMA.A_900_MPLS/path/8 (SignalType_vc4_ncv)/23 (ResourceState_disable)");
    assertThat(ue.get("serialNo").asString()).isEqualTo("IP-0068-PATISSION-0073-NYMA.A_900_MPLS42");
    // February = EET (+02:00): 15:05:35 local = 13:05:35Z
    assertThat(ts(ue)).isEqualTo(Instant.parse("2023-02-02T13:05:35Z"));
  }

  @Test
  void snapshotStartAndEndRecordsBecomeSyncMarkers() {
    JsonNode start = M.readTree(transformer("MV38_MOBILE").transform(
        "{\"ts\":\"Thu Oct 01 16:42:27 EEST 2026\",\"kind\":\"snapshot_start\"}"));
    JsonNode end = M.readTree(transformer("MV38_MOBILE").transform(
        "{\"ts\":\"Thu Oct 01 16:42:29 EEST 2026\",\"kind\":\"snapshot_end\",\"count\":11}"));

    assertThat(start.get("type").asString()).isEqualTo("SYNC_START");
    assertThat(start.get("sourceEms").asString()).isEqualTo("MV38_MOBILE");
    assertThat(start.get("alarmIdentifier").asString()).isEqualTo("MV38_MOBILE_SYNC_START");
    assertThat(start.get("faultId").asString()).isEqualTo("SYNC_START");
    assertThat(start.get("metadata").get("source").asString()).isEqualTo("SYNC");

    assertThat(end.get("type").asString()).isEqualTo("SYNC_END");
    assertThat(end.get("alarmIdentifier").asString()).isEqualTo("MV38_MOBILE_SYNC_END");
    assertThat(end.get("sourceEvent").get("fields").get("count").asString()).isEqualTo("11");
  }
}
