package gr.ote.rdnoc.alarm.nsp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.junit.jupiter.api.Test;

class NspSiteSelectorTest {

  private static NspFailoverProperties.SiteConfig site(String name, String host, String kafka) {
    var s = new NspFailoverProperties.SiteConfig();
    s.setName(name);
    s.setHost(host);
    s.setKafkaBootstrapServers(kafka);
    return s;
  }

  private static NspFailoverProperties legacy(String preferred, List<String> hosts, List<String> kafkas) {
    var p = new NspFailoverProperties();
    p.setEnabled(true);
    p.setPreferredHost(preferred);
    p.setHosts(hosts);
    p.setKafkaBootstrapServers(kafkas);
    return p;
  }

  @Test
  void legacyListsArePairedInWrittenOrderEvenWhenPreferredHostIsSecond() {
    // Regression: the old selector moved preferred-host to the front BEFORE pairing,
    // so hostA got kafkaB and hostB got kafkaA.
    var sel = new NspSiteSelector(
        legacy("hostB", List.of("hostA", "hostB"), List.of("kafkaA:9192", "kafkaB:9192")),
        "hostA", "fallback:9092");

    assertThat(sel.kafkaBootstrapServersFor("hostA")).isEqualTo("kafkaA:9192");
    assertThat(sel.kafkaBootstrapServersFor("hostB")).isEqualTo("kafkaB:9192");
    assertThat(sel.activeSite().host()).isEqualTo("hostB"); // preferred = initial choice only
    assertThat(sel.activeKafkaBootstrapServers()).isEqualTo("kafkaB:9192");
  }

  @Test
  void siteObjectsKeepHostAndKafkaTogether() {
    var p = new NspFailoverProperties();
    p.setEnabled(true);
    p.setSites(List.of(
        site("A", "172.17.45.132", "172.17.45.132:9192"),
        site("B", "172.17.42.132", "172.17.42.132:9192")));

    var sel = new NspSiteSelector(p, "172.17.42.132", "unused:9092");

    assertThat(sel.activeSite().name()).isEqualTo("A"); // no preferred-host -> first site
    sel.forceActiveHost("172.17.42.132");
    assertThat(sel.activeSite().name()).isEqualTo("B");
    assertThat(sel.activeKafkaBootstrapServers()).isEqualTo("172.17.42.132:9192");
  }

  @Test
  void probeOrderIsActiveSiteFirstThenTheOthers() {
    var sel = new NspSiteSelector(
        legacy(null, List.of("A", "B", "C"), List.of("ka", "kb", "kc")), null, (String) null);

    sel.forceActiveHost("B");

    assertThat(sel.sitesInProbeOrder()).extracting(NspSite::host).containsExactly("B", "A", "C");
    assertThat(sel.orderedHostsForAttempt()).containsExactly("B", "A", "C");
  }

  @Test
  void failoverDisabledOnlyAttemptsTheActiveHost() {
    var p = legacy(null, List.of("A", "B"), List.of("ka", "kb"));
    p.setEnabled(false);

    var sel = new NspSiteSelector(p, null, (String) null);

    assertThat(sel.orderedHostsForAttempt()).containsExactly("A");
  }

  @Test
  void singleHostFallsBackToConsumerBootstrapServers() {
    var sel = new NspSiteSelector(new NspFailoverProperties(), "nsp-host", "consumer-kafka:9192");

    assertThat(sel.activeSite().host()).isEqualTo("nsp-host");
    assertThat(sel.activeKafkaBootstrapServers()).isEqualTo("consumer-kafka:9192");
  }

  @Test
  void mismatchedLegacyListsFailAtStartup() {
    assertThatThrownBy(() -> new NspSiteSelector(
        legacy(null, List.of("A", "B"), List.of("ka")), null, (String) null))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("same number of entries");
  }

  @Test
  void siteWithoutKafkaFailsAtStartup() {
    var p = new NspFailoverProperties();
    p.setSites(List.of(site("A", "hostA", null)));

    assertThatThrownBy(() -> new NspSiteSelector(p, null, "fallback"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("no kafka-bootstrap-servers");
  }

  @Test
  void unknownHostIsIgnoredByForceActiveHost() {
    var sel = new NspSiteSelector(legacy(null, List.of("A", "B"), List.of("ka", "kb")), null, (String) null);

    sel.forceActiveHost("somewhere-else");

    assertThat(sel.activeSite().host()).isEqualTo("A");
  }
}
