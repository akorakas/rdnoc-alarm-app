package gr.ote.rdnoc.alarm.mv36;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.support.StaticListableBeanFactory;

import gr.ote.atlas.events.enums.EMSId;
import gr.ote.atlas.events.models.UnifiedEvent;
import gr.ote.rdnoc.alarm.atlas.UnifiedEventMapper;
import gr.ote.rdnoc.alarm.mv36.config.Mv36SnmpProperties;
import gr.ote.rdnoc.alarm.mv36.enrich.Mv36NeEnrichmentService;
import gr.ote.rdnoc.alarm.mv36.mapper.Mv36ActiveAlarmMapper;
import gr.ote.rdnoc.alarm.mv36.mapper.Mv36NeNames;
import gr.ote.rdnoc.alarm.mv36.model.Mv36ActiveAlarm;
import gr.ote.rdnoc.alarm.mv36.model.Mv36NetworkElement;
import gr.ote.rdnoc.alarm.service.Transformer;
import gr.ote.rdnoc.alarm.service.config.TransformProperties;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/** neName = mv36NeName + the ".N" suffix of mv36NeUniqueName, on both the trap and the sync path. */
class Mv36NeNameTest {

  private static final ObjectMapper M = new ObjectMapper();

  @Test
  void suffixRuleOnTheRequestedExamples() {
    assertThat(Mv36NeNames.withUniqueNameSuffix("D230-23_Kilkis_OMS1684", "KILKIS.3"))
        .isEqualTo("D230-23_Kilkis_OMS1684.3");
    assertThat(Mv36NeNames.withUniqueNameSuffix("ANTHILI_ESEETHA", "ANTHILI.1"))
        .isEqualTo("ANTHILI_ESEETHA.1");
    assertThat(Mv36NeNames.withUniqueNameSuffix("D246-03 Bodosakio", "BDSKIO.1"))
        .isEqualTo("D246-03 Bodosakio.1");
  }

  @Test
  void suffixRuleEdgeCases() {
    assertThat(Mv36NeNames.withUniqueNameSuffix("NE", "NODOT")).isEqualTo("NE");
    assertThat(Mv36NeNames.withUniqueNameSuffix("NE", "TRAILING.")).isEqualTo("NE");
    assertThat(Mv36NeNames.withUniqueNameSuffix("NE", null)).isEqualTo("NE");
    assertThat(Mv36NeNames.withUniqueNameSuffix("NE.3", "X.3")).isEqualTo("NE.3");   // no double suffix
    assertThat(Mv36NeNames.withUniqueNameSuffix("NE", "A.B.7")).isEqualTo("NE.7");   // last dot
    assertThat(Mv36NeNames.withUniqueNameSuffix(null, "X.3")).isNull();
  }

  private static Mv36NetworkElement halandri() {
    Mv36NetworkElement ne = new Mv36NetworkElement();
    ne.setMv36NeId("824");
    ne.setMv36NeName("0061-62 HALANDRI");
    ne.setMv36NeUniqueName("HALANDRI.62");
    ne.setMv36NeTypeStr("OMS1664");
    return ne;
  }

  private static StaticListableBeanFactory beansWith(Mv36NetworkElement ne) {
    Mv36NeEnrichmentService svc = mock(Mv36NeEnrichmentService.class);
    when(svc.findByAlarmNeId(any())).thenReturn(Optional.ofNullable(ne));
    when(svc.findByUniqueName(any())).thenReturn(Optional.ofNullable(ne));
    StaticListableBeanFactory bf = new StaticListableBeanFactory();
    bf.addBean("mv36NeEnrichmentService", svc);
    return bf;
  }

  @Test
  void syncPathFaultSync() {
    Mv36SnmpProperties props = new Mv36SnmpProperties();
    props.getSnmp().setSourceEms(EMSId.MV36_FIXED_B);
    Mv36ActiveAlarmMapper mapper = new Mv36ActiveAlarmMapper(
        props, beansWith(halandri()).getBeanProvider(Mv36NeEnrichmentService.class));

    Mv36ActiveAlarm a = new Mv36ActiveAlarm();
    a.setSourceIndex("0.1595405390");
    a.setMv36AlarmId("1595405390");
    a.setMv36AlarmSeverity("6");
    a.setMv36AlarmNeId("824");
    a.setMv36AlarmRaisingTime("2026-10-15T01:44:57+03:00");
    a.setMv36AlarmStrNeUniqueName("HALANDRI.62");
    a.setMv36AlarmStrShelf("Subrack 1 - Main");
    a.setMv36AlarmStrCard("LTU: 2M (32 port) LTU 1-1");
    a.setMv36AlarmPortId("1");
    a.setMv36AlarmStr("Trib Input AIS");

    UnifiedEvent ue = mapper.toUnifiedEvent(a);

    assertThat(ue.getNeName()).isEqualTo("0061-62 HALANDRI.62");
    // Unchanged: identifier and location use the plain NE name.
    assertThat(ue.getAlarmIdentifier())
        .isEqualTo("0061-62 HALANDRI/Subrack 1 - Main/LTU: 2M (32 port) LTU 1-1/1/Trib Input AIS");
    assertThat(ue.getEnrichedData().getAffectedLocation().getName()).isEqualTo("0061");
  }

  private static Transformer trapTransformer(StaticListableBeanFactory bf, String sourceEms) {
    var extract = new TransformProperties.Step();
    extract.setType("extract");
    extract.setFailOnMissing(false);
    extract.setFailOnBadJson(true);
    extract.setMappings(Map.of("sourceEvent", "/"));

    var update = new TransformProperties.Step();
    update.setType("update");
    var c1 = new TransformProperties.ComputeAssignment(); c1.setSet("sourceEms"); c1.setExpr("'" + sourceEms + "'");
    var c2 = new TransformProperties.ComputeAssignment(); c2.setSet("emsVendorID"); c2.setExpr("'MV_36'");
    var c3 = new TransformProperties.ComputeAssignment(); c3.setSet("emsDomain"); c3.setExpr("'TRANSPORT'");
    update.setCompute(List.of(c1, c2, c3));

    var unified = new TransformProperties.Step();
    unified.setType("unifiedEvent");
    unified.setTarget("$");

    return new Transformer("x", List.of(extract, update, unified), null,
        new UnifiedEventMapper(bf.getBeanProvider(Mv36NeEnrichmentService.class)));
  }

  private static final String TRAP = """
      {"name":"snmp_trap","timestamp":1792017897,
       "tags":{"source":"172.25.52.8","name":"mv36AlarmTrap"},
       "fields":{"mv36AlarmId":"1595405390","mv36AlarmSeverity":"6","mv36AlarmNeId":"824",
                 "mv36AlarmStrNeUniqueName":"HALANDRI.62","mv36AlarmStrShelf":"Subrack 1 - Main",
                 "mv36AlarmStrCard":"LTU: 2M (32 port) LTU 1-1","mv36AlarmPortId":"1",
                 "mv36AlarmStr":"Trib Input AIS"}}""";

  @Test
  void trapPathFaultAndClear() {
    Transformer t = trapTransformer(beansWith(halandri()), "MV36_MOBILE");

    JsonNode fault = M.readTree(t.transform(TRAP));
    assertThat(fault.get("type").asString()).isEqualTo("FAULT");
    assertThat(fault.get("neName").asString()).isEqualTo("0061-62 HALANDRI.62");
    assertThat(fault.get("alarmIdentifier").asString())
        .isEqualTo("0061-62 HALANDRI/Subrack 1 - Main/LTU: 2M (32 port) LTU 1-1/1/Trib Input AIS");

    JsonNode clear = M.readTree(t.transform(TRAP.replace("\"mv36AlarmSeverity\":\"6\"", "\"mv36AlarmSeverity\":\"1\"")));
    assertThat(clear.get("type").asString()).isEqualTo("CLEAR");
    assertThat(clear.get("neName").asString()).isEqualTo("0061-62 HALANDRI.62");
  }

  @Test
  void trapPathCacheMissKeepsTheUniqueNameFallback() {
    Transformer t = trapTransformer(beansWith(null), "MV36_FIXED_A");

    JsonNode ue = M.readTree(t.transform(TRAP));
    assertThat(ue.get("neName").asString()).isEqualTo("HALANDRI.62");
  }

  // Real MV36 Fixed B traps: same severity (3) on raise and clear; only mv36AlarmPhysicalState changes.
  private static final String FIXED_B_TRAP = """
      {"name":"mv36AlarmNotificationVerbose","timestamp":1792066583,
       "tags":{"mib":"MV36-PFM2-MIB","name":"mv36AlarmNotificationVerbose","source":"172.16.97.68"},
       "fields":{"mv36AlarmRaisingTime":"07ea0a0f0f1007002b0300","mv36AlarmSeverity":"3",
                 "mv36AlarmStrShelf":" --","mv36AlarmStr":"Switch Card A Fail","mv36AlarmPhysicalState":"1",
                 "mv36AlarmPortId":"-1","mv36AlarmStrNeUniqueName":"ARHS.35","mv36AlarmNeId":"20",
                 "mv36AlarmId":"394238680","mv36AlarmStrCard":"STM-16 G957 S16.1 TCM LEA"}}""";

  @Test
  void fixedPhysicalStateTwoOrFourIsAClear() {
    Transformer t = trapTransformer(beansWith(null), "MV36_FIXED_B");

    JsonNode raise = M.readTree(t.transform(FIXED_B_TRAP));
    assertThat(raise.get("type").asString()).isEqualTo("FAULT");
    assertThat(raise.get("severity").asString()).isEqualTo("CRITICAL");

    JsonNode clear = M.readTree(t.transform(FIXED_B_TRAP
        .replace("\"mv36AlarmPhysicalState\":\"1\"", "\"mv36AlarmPhysicalState\":\"2\"")
        .replace("07ea0a0f0f1007002b0300", "07ea0a0f0f1026002b0300")));
    assertThat(clear.get("type").asString()).isEqualTo("CLEAR");
    assertThat(clear.get("severity").asString()).isEqualTo("CLEARED");
    // Same key as the raise, so Atlas can close it
    assertThat(clear.get("serialNo").asString()).isEqualTo(raise.get("serialNo").asString());
    assertThat(clear.get("alarmIdentifier").asString()).isEqualTo(raise.get("alarmIdentifier").asString());
    // Timed at the clear trap's mv36AlarmRaisingTime (15:16:38 +03:00), 31s after the raise
    assertThat(clear.get("timestamp").asDouble() - raise.get("timestamp").asDouble()).isEqualTo(31.0);

    JsonNode raise3 = M.readTree(t.transform(FIXED_B_TRAP
        .replace("\"mv36AlarmPhysicalState\":\"1\"", "\"mv36AlarmPhysicalState\":\"3\"")));
    assertThat(raise3.get("type").asString()).isEqualTo("FAULT");
    JsonNode clear4 = M.readTree(t.transform(FIXED_B_TRAP
        .replace("\"mv36AlarmPhysicalState\":\"1\"", "\"mv36AlarmPhysicalState\":\"4\"")));
    assertThat(clear4.get("type").asString()).isEqualTo("CLEAR");
  }
}
