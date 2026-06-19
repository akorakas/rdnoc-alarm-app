package gr.ote.rdnoc.alarm.nsp;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import gr.ote.rdnoc.alarm.kafka.DynamicKafkaConsumer;
import gr.ote.rdnoc.alarm.sync.SyncCoordinator;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class NspSubscriptionManager {

  private final NspClient nspClient;
  private final NspSiteSelector siteSelector;
  private final SubscriptionStateStore stateStore;
  private final NspKafkaAdminClientFactory adminClientFactory;
  private final DynamicKafkaConsumer consumer;
  private final SyncCoordinator sync;

  private final long topicWaitTimeoutMs;
  private final long topicWaitSleepMs;

  private final long lockWaitTimeoutMs;
  private final long lockWaitSleepMs;

  private final AtomicBoolean busy = new AtomicBoolean(false);
  private final AtomicBoolean startupCompleted = new AtomicBoolean(false);

  public NspSubscriptionManager(
      NspClient nspClient,
      NspSiteSelector siteSelector,
      SubscriptionStateStore stateStore,
      NspKafkaAdminClientFactory adminClientFactory,
      DynamicKafkaConsumer consumer,
      SyncCoordinator sync,
      @Value("${app.nsp.subscription.topic-wait-timeout-ms:180000}") long topicWaitTimeoutMs,
      @Value("${app.nsp.subscription.topic-wait-sleep-ms:2000}") long topicWaitSleepMs,
      @Value("${app.nsp.subscription.lock-wait-timeout-ms:60000}") long lockWaitTimeoutMs,
      @Value("${app.nsp.subscription.lock-wait-sleep-ms:250}") long lockWaitSleepMs
  ) {
    this.nspClient = nspClient;
    this.siteSelector = siteSelector;
    this.stateStore = stateStore;
    this.adminClientFactory = adminClientFactory;
    this.consumer = consumer;
    this.sync = sync;
    this.topicWaitTimeoutMs = topicWaitTimeoutMs;
    this.topicWaitSleepMs = topicWaitSleepMs;
    this.lockWaitTimeoutMs = lockWaitTimeoutMs;
    this.lockWaitSleepMs = lockWaitSleepMs;
  }

  public boolean isStartupCompleted() {
    return startupCompleted.get();
  }

  /**
   * Startup flow:
   *
   * 1. Load saved subscription state.
   * 2. If saved topic exists on the saved site's Kafka, reuse it.
   * 3. If missing, recreate.
   * 4. If unreachable and failover is enabled, recreate against currently available NSP site.
   * 5. Run snapshot sync.
   * 6. Start Kafka consumer using state.topicId + state.kafkaBootstrapServers.
   */
  public void startFlow(String reason) throws Exception {
    withLock("startFlow", () -> {
      Optional<NspSubscriptionState> existing = stateStore.load();

      if (existing.isPresent()) {
        NspSubscriptionState normalized = normalizeState(existing.get());

        TopicStatus status = checkTopic(normalized);

        if (status == TopicStatus.EXISTS) {
          log.info("Startup: reusing stored NSP subscription. subscriptionId={}, topicId={}, host={}, kafkaBootstrapServers={}",
              normalized.subscriptionId(), normalized.topicId(), normalized.host(), normalized.kafkaBootstrapServers());

          if (normalized.hasHost()) {
            siteSelector.forceActiveHost(normalized.host());
            nspClient.forceActiveHost(normalized.host());
          }

          stateStore.save(normalized);

          syncWithConsumerPaused(reason);
          startConsumerOn(normalized);

          startupCompleted.set(true);
          return null;
        }

        if (status == TopicStatus.MISSING) {
          log.warn("Startup: stored topic is missing. topicId={}, host={} -> recreate",
              normalized.topicId(), normalized.host());

          recreate("startup-topic-missing");
          startupCompleted.set(true);
          return null;
        }

        if (status == TopicStatus.UNREACHABLE_OR_UNKNOWN && siteSelector.isFailoverEnabled()) {
          log.warn("Startup: stored Kafka site is unreachable/unknown and failover is enabled. topicId={}, host={} -> recreate on available site",
              normalized.topicId(), normalized.host());

          recreate("startup-kafka-unreachable-failover");
          startupCompleted.set(true);
          return null;
        }

        log.warn("Startup: stored Kafka site is unreachable/unknown and failover is disabled. Will wait for topic.");
      }

      NspSubscriptionState state = ensureSubscription();

      waitTopicExists(state);

      syncWithConsumerPaused(reason);

      startConsumerOn(state);

      startupCompleted.set(true);
      return null;
    });
  }

  public void runPeriodicSync(String reason) {
    if (!startupCompleted.get()) {
      log.info("Periodic sync skipped because startup is not completed yet. reason={}", reason);
      return;
    }

    try {
      withLock("periodicSync", () -> {
        syncWithConsumerPaused(reason);
        return null;
      });
    } catch (Exception e) {
      log.error("Periodic sync failed", e);
    }
  }

  /**
   * Renewal flow:
   *
   * If renewal succeeds, keep same subscription/topic.
   * If renewal fails, recreate. createSubscription() should use NspClient failover logic,
   * so the recreated subscription can move from Site A to Site B.
   */
  public void renewOrRecreate() {
    try {
      withLock("renew", () -> {
        Optional<NspSubscriptionState> stateOpt = stateStore.load();

        if (stateOpt.isEmpty()) {
          log.warn("No subscription state on renew tick -> recreate");
          recreate("renew-no-state");
          return null;
        }

        NspSubscriptionState state = normalizeState(stateOpt.get());

        TopicStatus status = checkTopic(state);

        if (status == TopicStatus.MISSING) {
          log.warn("Topic missing: topicId={}, host={} -> recreate subscription",
              state.topicId(), state.host());

          recreate("topic-missing");
          return null;
        }

        if (status == TopicStatus.UNREACHABLE_OR_UNKNOWN) {
          log.warn("Kafka/topic status unknown for topicId={}, host={}. Will try REST renew; if REST fails, recreate/failover.",
              state.topicId(), state.host());
        }

        try {
          nspClient.renewSubscription(state.subscriptionId(), state.host());
          log.info("Renewed subscription. subscriptionId={}, host={}",
              state.subscriptionId(), state.host());
          return null;

        } catch (Exception renewException) {
          log.warn("Renew failed. subscriptionId={}, host={}. Will recreate/failover.",
              state.subscriptionId(), state.host(), renewException);

          recreate("renew-failed");
          return null;
        }
      });

    } catch (Exception e) {
      log.error("Renew/recreate flow failed", e);
    }
  }

  /**
   * Use this if another part of the app detects that active site changed.
   * It forces a clean subscription recreation on the currently selected active site.
   */
  public void failoverNow(String reason) {
    try {
      withLock("failoverNow", () -> {
        recreate("manual-or-detected-failover-" + reason);
        return null;
      });
    } catch (Exception e) {
      log.error("Failover failed. reason={}", reason, e);
    }
  }

  private NspSubscriptionState ensureSubscription() throws Exception {
    Optional<NspSubscriptionState> existing = stateStore.load();

    if (existing.isPresent()) {
      NspSubscriptionState state = normalizeState(existing.get());
      stateStore.save(state);
      return state;
    }

    var info = nspClient.createSubscription();

    String host = info.host();
    String kafkaBootstrapServers = siteSelector.kafkaBootstrapServersFor(host);

    var state = new NspSubscriptionState(
        info.subscriptionId(),
        info.topicId(),
        host,
        kafkaBootstrapServers
    );

    stateStore.save(state);

    log.info("Created NSP subscription. subscriptionId={}, topicId={}, host={}, kafkaBootstrapServers={}",
        state.subscriptionId(), state.topicId(), state.host(), state.kafkaBootstrapServers());

    return state;
  }

  private void recreate(String reason) throws Exception {
    log.warn("Recreating NSP subscription. reason={}", reason);

    try {
      consumer.stop();
    } catch (Exception e) {
      log.warn("Consumer stop failed during recreate. Continuing.", e);
    }

    Optional<NspSubscriptionState> oldStateOpt = stateStore.load();

    if (oldStateOpt.isPresent()) {
      NspSubscriptionState oldState = oldStateOpt.get();

      try {
        if (oldState.subscriptionId() != null && !oldState.subscriptionId().isBlank()) {
          nspClient.deleteSubscription(oldState.subscriptionId(), oldState.host());
          log.info("Deleted old NSP subscription. subscriptionId={}, host={}",
              oldState.subscriptionId(), oldState.host());
        }
      } catch (Exception e) {
        log.warn("Failed to delete old NSP subscription. Continuing with recreate. subscriptionId={}, host={}",
            oldState.subscriptionId(), oldState.host(), e);
      }
    }

    stateStore.clear();

    var info = nspClient.createSubscription();

    String host = info.host();
    String kafkaBootstrapServers = siteSelector.kafkaBootstrapServersFor(host);

    NspSubscriptionState newState = new NspSubscriptionState(
        info.subscriptionId(),
        info.topicId(),
        host,
        kafkaBootstrapServers
    );

    stateStore.save(newState);

    log.info("Created new NSP subscription. subscriptionId={}, topicId={}, host={}, kafkaBootstrapServers={}",
        newState.subscriptionId(), newState.topicId(), newState.host(), newState.kafkaBootstrapServers());

    waitTopicExists(newState);

    syncWithConsumerPaused("recreate-" + reason);

    startConsumerOn(newState);
  }

  private NspSubscriptionState normalizeState(NspSubscriptionState state) {
    if (state == null) {
      return null;
    }

    if (state.hasKafkaBootstrapServers()) {
      return state;
    }

    if (state.hasHost()) {
      String kafkaBootstrapServers = siteSelector.kafkaBootstrapServersFor(state.host());

      return new NspSubscriptionState(
          state.subscriptionId(),
          state.topicId(),
          state.host(),
          kafkaBootstrapServers
      );
    }

    throw new IllegalStateException(
        "Stored NSP subscription state does not contain host. Cannot determine Kafka bootstrap servers. "
            + "Delete the saved subscription state file and restart the application."
    );
  }

  private void syncWithConsumerPaused(String reason) {
    try {
      if (consumer.isRunning()) {
        consumer.pause();
      }
    } catch (Exception e) {
      log.warn("Failed to pause consumer before sync. Continuing.", e);
    }

    try {
      sync.runSync(reason);
    } finally {
      try {
        if (consumer.isRunning()) {
          consumer.resume();
        }
      } catch (Exception e) {
        log.error("Failed to resume consumer after sync", e);
      }
    }
  }

  private void startConsumerOn(NspSubscriptionState state) {
    String currentTopic = consumer.currentTopic();
    String currentBootstrapServers = consumer.currentBootstrapServers();

    if (
        consumer.isRunning()
            && state.topicId() != null
            && state.topicId().equals(currentTopic)
            && state.kafkaBootstrapServers() != null
            && state.kafkaBootstrapServers().equals(currentBootstrapServers)
    ) {
      log.info("Consumer already running on topic={}, kafkaBootstrapServers={}",
          state.topicId(), state.kafkaBootstrapServers());
      return;
    }

    log.info("Starting consumer on topic={}, kafkaBootstrapServers={}",
        state.topicId(), state.kafkaBootstrapServers());

    consumer.start(state.topicId(), state.kafkaBootstrapServers());
  }

  private void waitTopicExists(NspSubscriptionState state) throws Exception {
    long deadline = System.currentTimeMillis() + topicWaitTimeoutMs;

    while (System.currentTimeMillis() < deadline) {
      TopicStatus status = checkTopic(state);

      if (status == TopicStatus.EXISTS) {
        log.info("Topic exists. topicId={}, host={}, kafkaBootstrapServers={}",
            state.topicId(), state.host(), state.kafkaBootstrapServers());
        return;
      }

      TimeUnit.MILLISECONDS.sleep(topicWaitSleepMs);
    }

    throw new IllegalStateException(
        "Timed out waiting for topic to exist. topicId="
            + state.topicId()
            + ", host="
            + state.host()
            + ", kafkaBootstrapServers="
            + state.kafkaBootstrapServers()
    );
  }

  private enum TopicStatus {
    EXISTS,
    MISSING,
    UNREACHABLE_OR_UNKNOWN
  }

  private TopicStatus checkTopic(NspSubscriptionState state) {
    if (state == null) {
      return TopicStatus.UNREACHABLE_OR_UNKNOWN;
    }

    if (state.topicId() == null || state.topicId().isBlank()) {
      return TopicStatus.MISSING;
    }

    if (state.kafkaBootstrapServers() == null || state.kafkaBootstrapServers().isBlank()) {
      log.warn("Cannot check topic because kafkaBootstrapServers is empty. topicId={}, host={}",
          state.topicId(), state.host());
      return TopicStatus.UNREACHABLE_OR_UNKNOWN;
    }

    try (AdminClient admin = adminClientFactory.create(state.kafkaBootstrapServers())) {
      admin.describeTopics(List.of(state.topicId()))
          .allTopicNames()
          .get(5, TimeUnit.SECONDS);

      return TopicStatus.EXISTS;

    } catch (ExecutionException ee) {
      Throwable cause = ee.getCause();

      if (cause instanceof UnknownTopicOrPartitionException) {
        return TopicStatus.MISSING;
      }

      if (cause instanceof TopicAuthorizationException) {
        log.error("Not authorized to describe topic. topicId={}, kafkaBootstrapServers={}",
            state.topicId(), state.kafkaBootstrapServers(), cause);
        return TopicStatus.UNREACHABLE_OR_UNKNOWN;
      }

      log.warn("DescribeTopics failed. topicId={}, kafkaBootstrapServers={}, cause={}",
        state.topicId(),
        state.kafkaBootstrapServers(),
        cause == null ? "null" : cause.toString());

      return TopicStatus.UNREACHABLE_OR_UNKNOWN;

    } catch (TimeoutException te) {
      log.warn("DescribeTopics timed out. topicId={}, kafkaBootstrapServers={}",
          state.topicId(), state.kafkaBootstrapServers());

      return TopicStatus.UNREACHABLE_OR_UNKNOWN;

    } catch (Exception e) {
      log.warn("DescribeTopics failed. topicId={}, kafkaBootstrapServers={}",
          state.topicId(), state.kafkaBootstrapServers(), e);

      return TopicStatus.UNREACHABLE_OR_UNKNOWN;
    }
  }

  private <T> T withLock(String op, CheckedSupplier<T> fn) throws Exception {
    final long deadline = System.currentTimeMillis() + lockWaitTimeoutMs;

    while (!busy.compareAndSet(false, true)) {
      if (System.currentTimeMillis() >= deadline) {
        log.warn("Skip {}: manager is busy after waiting {}ms", op, lockWaitTimeoutMs);
        return null;
      }

      try {
        TimeUnit.MILLISECONDS.sleep(lockWaitSleepMs);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        log.warn("Interrupted while waiting for manager lock. op={}", op);
        return null;
      }
    }

    try {
      return fn.get();
    } finally {
      busy.set(false);
    }
  }

  @FunctionalInterface
  private interface CheckedSupplier<T> {
    T get() throws Exception;
  }
}