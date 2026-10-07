package gr.ote.rdnoc.alarm.nsp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.common.KafkaFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import gr.ote.rdnoc.alarm.kafka.DynamicKafkaConsumer;
import gr.ote.rdnoc.alarm.sync.SyncCoordinator;

class NspSubscriptionManagerFailoverTest {

  static final String HOST_A = "172.17.45.132";
  static final String HOST_B = "172.17.42.132";
  static final String KAFKA_A = "172.17.45.132:9192";
  static final String KAFKA_B = "172.17.42.132:9192";

  NspClient nspClient;
  NspSiteSelector selector;
  InMemoryStateStore stateStore;
  DynamicKafkaConsumer consumer;
  SyncCoordinator sync;
  NspSiteProbe probe;
  NspSubscriptionManager manager;

  /** Hosts the probe currently considers active (REST + Kafka up). */
  final Set<String> activeHosts = new HashSet<>();

  @BeforeEach
  void setUp() throws Exception {
    var props = new NspFailoverProperties();
    props.setEnabled(true);
    props.setPreferredHost(HOST_A);
    props.setHosts(List.of(HOST_A, HOST_B));
    props.setKafkaBootstrapServers(List.of(KAFKA_A, KAFKA_B));
    props.setStartupWaitTimeoutMs(50);
    props.setStartupWaitSleepMs(10);
    props.getHealthCheck().setFailureThreshold(3);

    selector = new NspSiteSelector(props, HOST_B, (String) null);
    nspClient = mock(NspClient.class);
    stateStore = new InMemoryStateStore();
    consumer = mock(DynamicKafkaConsumer.class);
    sync = mock(SyncCoordinator.class);

    probe = mock(NspSiteProbe.class);
    when(probe.isActive(any())).thenAnswer(inv -> activeHosts.contains(((NspSite) inv.getArgument(0)).host()));

    // createSubscription() fails over internally; emulate "created on the currently active host".
    when(nspClient.createSubscription()).thenAnswer(inv -> {
      String h = selector.activeHost();
      return new NspClient.SubscriptionInfo("sub@" + h, "topic@" + h, h);
    });

    // Every topic exists.
    NspKafkaAdminClientFactory adminFactory = bootstrap -> {
      AdminClient admin = mock(AdminClient.class);
      DescribeTopicsResult r = mock(DescribeTopicsResult.class);
      when(r.allTopicNames()).thenReturn(KafkaFuture.completedFuture(Map.of()));
      when(admin.describeTopics(anyCollection())).thenReturn(r);
      return admin;
    };

    manager = new NspSubscriptionManager(
        nspClient, selector, stateStore, adminFactory, consumer, sync, probe, props,
        1_000, 10, 1_000, 10);
  }

  @Test
  void startupPicksTheActiveSiteForBothRestAndKafka() throws Exception {
    activeHosts.add(HOST_B); // A (preferred) is standby

    manager.startFlow("startup");

    assertThat(selector.activeSite().host()).isEqualTo(HOST_B);
    assertThat(stateStore.state.host()).isEqualTo(HOST_B);
    assertThat(stateStore.state.kafkaBootstrapServers()).isEqualTo(KAFKA_B);
    verify(consumer).start("topic@" + HOST_B, KAFKA_B);
  }

  @Test
  void startupRecreatesAStoredSubscriptionThatBelongsToTheStandbySite() throws Exception {
    stateStore.state = new NspSubscriptionState("sub@A", "topic@A", HOST_A, KAFKA_A);
    activeHosts.add(HOST_B);

    manager.startFlow("startup");

    verify(nspClient).deleteSubscription("sub@A", HOST_A); // best effort cleanup
    verify(nspClient).createSubscription();
    verify(consumer).start("topic@" + HOST_B, KAFKA_B);
    assertThat(stateStore.state.host()).isEqualTo(HOST_B);
  }

  @Test
  void startupReusesStoredSubscriptionOnTheActiveSiteAndRepairsItsKafka() throws Exception {
    // A state file written by the old buggy selector: host A paired with Kafka B.
    stateStore.state = new NspSubscriptionState("sub@A", "topic@A", HOST_A, KAFKA_B);
    activeHosts.add(HOST_A);

    manager.startFlow("startup");

    verify(nspClient, never()).createSubscription();
    verify(consumer).start("topic@A", KAFKA_A);
    assertThat(stateStore.state.kafkaBootstrapServers()).isEqualTo(KAFKA_A);
  }

  @Test
  void startupFailsWhenNoSiteIsActive() {
    assertThatThrownBy(() -> manager.startFlow("startup"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("No active NSP site");
  }

  @Test
  void healthyActiveSiteDoesNothing() throws Exception {
    activeHosts.add(HOST_A);
    manager.startFlow("startup");

    manager.checkActiveSite();
    manager.checkActiveSite();

    verify(nspClient, times(1)).createSubscription();
    verify(consumer, times(1)).start(anyString(), anyString());
  }

  @Test
  void failsOverOnlyAfterThresholdConsecutiveFailures() throws Exception {
    activeHosts.add(HOST_A);
    manager.startFlow("startup");
    verify(consumer).start("topic@" + HOST_A, KAFKA_A);

    // Site switchover: A becomes standby, B becomes active.
    activeHosts.clear();
    activeHosts.add(HOST_B);

    manager.checkActiveSite(); // 1/3
    manager.checkActiveSite(); // 2/3
    verify(nspClient, times(1)).createSubscription();

    manager.checkActiveSite(); // 3/3 -> failover

    verify(consumer).stop();
    verify(nspClient, times(2)).createSubscription();
    verify(consumer).start("topic@" + HOST_B, KAFKA_B);
    verify(sync, times(2)).runSync(anyString()); // startup + after failover
    assertThat(selector.activeSite().host()).isEqualTo(HOST_B);
    assertThat(stateStore.state.host()).isEqualTo(HOST_B);
  }

  @Test
  void switchoverAndBack_B_then_A_then_B() throws Exception {
    // Day 0: B (172.17.42.132) is active; A (preferred) is standby.
    activeHosts.add(HOST_B);
    manager.startFlow("startup");
    verify(consumer).start("topic@" + HOST_B, KAFKA_B);

    // Steady state: healthy checks do nothing.
    for (int i = 0; i < 10; i++) manager.checkActiveSite();
    verify(nspClient, times(1)).createSubscription();

    // Week 2: switchover to A.
    activeHosts.clear();
    activeHosts.add(HOST_A);
    manager.checkActiveSite();
    manager.checkActiveSite();
    manager.checkActiveSite(); // 3rd consecutive failure -> failover

    verify(nspClient).deleteSubscription("sub@" + HOST_B, HOST_B); // best effort on old site
    verify(consumer).start("topic@" + HOST_A, KAFKA_A);
    assertThat(stateStore.state.host()).isEqualTo(HOST_A);

    for (int i = 0; i < 10; i++) manager.checkActiveSite();
    verify(nspClient, times(2)).createSubscription();

    // Week 3: switchover back to B.
    activeHosts.clear();
    activeHosts.add(HOST_B);
    manager.checkActiveSite();
    manager.checkActiveSite();
    manager.checkActiveSite();

    verify(nspClient).deleteSubscription("sub@" + HOST_A, HOST_A);
    verify(consumer, times(2)).start("topic@" + HOST_B, KAFKA_B);
    verify(nspClient, times(3)).createSubscription();
    verify(sync, times(3)).runSync(anyString()); // startup + one per switchover
    assertThat(selector.activeSite().host()).isEqualTo(HOST_B);
    assertThat(stateStore.state.host()).isEqualTo(HOST_B);
    assertThat(stateStore.state.kafkaBootstrapServers()).isEqualTo(KAFKA_B);
  }

  @Test
  void failedRenewOnDeadSiteAlsoFailsOverWithoutWaitingForHealthChecks() throws Exception {
    activeHosts.add(HOST_B);
    manager.startFlow("startup");

    // Switchover happens; the 45-min renew fires before 3 health checks have failed.
    activeHosts.clear();
    activeHosts.add(HOST_A);
    org.mockito.Mockito.doThrow(new IllegalStateException("refused"))
        .when(nspClient).renewSubscription("sub@" + HOST_B, HOST_B);
    // createSubscription() fails over inside NspClient: B refuses, A answers -> active becomes A.
    when(nspClient.createSubscription()).thenAnswer(inv -> {
      selector.markSuccess(HOST_A);
      return new NspClient.SubscriptionInfo("sub@" + HOST_A, "topic@" + HOST_A, HOST_A);
    });

    manager.renewOrRecreate();

    verify(consumer).start("topic@" + HOST_A, KAFKA_A);
    assertThat(selector.activeSite().host()).isEqualTo(HOST_A);

    // Next health check: A is healthy and the subscription is on A -> nothing more to do.
    manager.checkActiveSite();
    verify(consumer, times(1)).start("topic@" + HOST_A, KAFKA_A);
  }

  @Test
  void transientFailureBelowThresholdResetsTheCounter() throws Exception {
    activeHosts.add(HOST_A);
    manager.startFlow("startup");

    activeHosts.clear();
    manager.checkActiveSite(); // 1/3
    manager.checkActiveSite(); // 2/3
    activeHosts.add(HOST_A);
    manager.checkActiveSite(); // healthy -> reset
    activeHosts.clear();
    manager.checkActiveSite(); // 1/3 again
    manager.checkActiveSite(); // 2/3

    verify(nspClient, times(1)).createSubscription();
  }

  @Test
  void noActiveSiteKeepsTheCurrentSubscription() throws Exception {
    activeHosts.add(HOST_A);
    manager.startFlow("startup");

    activeHosts.clear(); // both sites down
    for (int i = 0; i < 5; i++) manager.checkActiveSite();

    verify(consumer, never()).stop();
    verify(nspClient, times(1)).createSubscription();
    assertThat(stateStore.state.host()).isEqualTo(HOST_A);
  }

  @Test
  void subscriptionOnAnotherSiteThanTheActiveOneIsMoved() throws Exception {
    activeHosts.add(HOST_A);
    manager.startFlow("startup");

    // e.g. a REST call failed over and made B the active site while the consumer is still on A.
    activeHosts.add(HOST_B);
    selector.forceActiveHost(HOST_B);

    manager.checkActiveSite();

    verify(consumer).start("topic@" + HOST_B, KAFKA_B);
    assertThat(stateStore.state.host()).isEqualTo(HOST_B);
  }

  @Test
  void renewRecreatesWhenSubscriptionIsNotOnTheActiveSite() throws Exception {
    activeHosts.add(HOST_A);
    manager.startFlow("startup");
    activeHosts.add(HOST_B);
    selector.forceActiveHost(HOST_B);

    manager.renewOrRecreate();

    verify(nspClient, never()).renewSubscription(anyString(), anyString());
    verify(consumer).start("topic@" + HOST_B, KAFKA_B);
  }

  static class InMemoryStateStore implements SubscriptionStateStore {
    NspSubscriptionState state;

    @Override public Optional<NspSubscriptionState> load() { return Optional.ofNullable(state); }
    @Override public void save(NspSubscriptionState s) { state = s; }
    @Override public void clear() { state = null; }
  }
}
