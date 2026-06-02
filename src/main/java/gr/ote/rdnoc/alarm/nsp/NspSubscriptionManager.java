// src/main/java/com/example/kafka/nsp/NspSubscriptionManager.java
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
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import gr.ote.rdnoc.alarm.kafka.DynamicKafkaConsumer;
import gr.ote.rdnoc.alarm.sync.SyncCoordinator;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class NspSubscriptionManager {

  private final NspClient nspClient;
  private final SubscriptionStateStore stateStore;

  /** ✅ INPUT AdminClient (built from spring.kafka.consumer.*). Do NOT close per call. */
  private final AdminClient inputAdmin;

  private final DynamicKafkaConsumer consumer;
  private final SyncCoordinator sync;

  /** ✅ Topic wait tuning */
  private final long topicWaitTimeoutMs;
  private final long topicWaitSleepMs;

  /** ✅ Lock wait tuning (prevents “skip & never start consumer”) */
  private final long lockWaitTimeoutMs;
  private final long lockWaitSleepMs;

  /**
   * ✅ Manager lock:
   * - prevents overlap between start/sync/renew
   * - but still allows sequential operations
   */
  private final AtomicBoolean busy = new AtomicBoolean(false);

  /**
   * ✅ Startup guard:
   * Prevents Scheduled sync from running BEFORE startup flow completes
   * (otherwise you get 2 snapshots on boot: scheduler + startup).
   */
  private final AtomicBoolean startupCompleted = new AtomicBoolean(false);

  public NspSubscriptionManager(
      NspClient nspClient,
      SubscriptionStateStore stateStore,
      @Qualifier("inputAdminClient") AdminClient inputAdmin,
      DynamicKafkaConsumer consumer,
      SyncCoordinator sync,
      @Value("${app.nsp.subscription.topic-wait-timeout-ms:180000}") long topicWaitTimeoutMs,
      @Value("${app.nsp.subscription.topic-wait-sleep-ms:2000}") long topicWaitSleepMs,
      @Value("${app.nsp.subscription.lock-wait-timeout-ms:60000}") long lockWaitTimeoutMs,
      @Value("${app.nsp.subscription.lock-wait-sleep-ms:250}") long lockWaitSleepMs
  ) {
    this.nspClient = nspClient;
    this.stateStore = stateStore;
    this.inputAdmin = inputAdmin;
    this.consumer = consumer;
    this.sync = sync;
    this.topicWaitTimeoutMs = topicWaitTimeoutMs;
    this.topicWaitSleepMs = topicWaitSleepMs;
    this.lockWaitTimeoutMs = lockWaitTimeoutMs;
    this.lockWaitSleepMs = lockWaitSleepMs;
  }

  /** Useful if you want to expose readiness status elsewhere */
  public boolean isStartupCompleted() {
    return startupCompleted.get();
  }

  /**
   * Startup entry:
   * - If state exists but topic is CONFIRMED missing -> recreate immediately.
   * - If Kafka is temporarily unreachable -> waitTopicExists() handles retry until timeout.
   *
   * IMPORTANT:
   * This method DOES A SNAPSHOT SYNC (sync.runSync()) and then starts consuming the NSP topic.
   */
  public void startFlow(String reason) throws Exception {
    withLock("startFlow", () -> {

      // If we have persisted state, validate it first.
      Optional<NspSubscriptionState> existing = stateStore.load();
      if (existing.isPresent()) {
        NspSubscriptionState st = existing.get();
        if (st.host() != null && !st.host().isBlank()) {
          nspClient.forceActiveHost(st.host());
        }
        TopicStatus status = checkTopic(st.topicId());

        if (status == TopicStatus.MISSING) {
          log.warn("Startup: stored topic missing {} -> recreate", st.topicId());
          recreate("startup-topic-missing");
          startupCompleted.set(true);
          return null;
        }
        // EXISTS -> proceed normally
        // UNREACHABLE_OR_UNKNOWN -> proceed to waitTopicExists() (do not recreate based on connectivity)
      }

      // Ensure subscription exists (create if missing)
      NspSubscriptionState state = ensureSubscription();

      // Wait for Kafka topic to exist (eventual consistency)
      waitTopicExists(state.topicId());

      // Run snapshot sync while consumer paused (if running)
      syncWithConsumerPaused(reason);

      // Start consumer on the subscription topic
      startConsumerOn(state.topicId());

      startupCompleted.set(true);
      return null;
    });
  }

  /**
   * Periodic sync entry called by SyncScheduler:
   * IMPORTANT:
   * - Skips until startup flow completes (prevents 2 snapshots on boot)
   * - Executes snapshot sync with consumer paused/resumed safely
   */
  public void runPeriodicSync(String reason) {
    // ✅ CRITICAL: prevents "scheduled sync" firing before startup flow finishes
    if (!startupCompleted.get()) {
      log.info("Periodic sync skipped (startup not completed yet). reason={}", reason);
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
   * Renew / recreate logic (if you have an external scheduler calling it).
   * Safe: will not recreate when Kafka is unreachable.
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

        NspSubscriptionState state = stateOpt.get();

        TopicStatus status = checkTopic(state.topicId());
        if (status == TopicStatus.MISSING) {
          log.warn("Topic missing: {} -> recreate subscription", state.topicId());
          recreate("topic-missing");
          return null;
        } else if (status == TopicStatus.UNREACHABLE_OR_UNKNOWN) {
          // Kafka unreachable/unknown: do NOT recreate (avoid spamming NSP).
          // Just fail this tick; next tick will retry.
          log.warn("Kafka/topic status unknown for {} (likely unreachable). Skipping renew this tick.",
              state.topicId());
          return null;
        }

        nspClient.renewSubscription(state.subscriptionId(), state.host());
        log.info("Renewed subscription {}", state.subscriptionId());
        return null;
      });
    } catch (Exception e) {
      log.error("Renew failed; attempting recreate", e);
      try {
        withLock("recreate-after-renew-failure", () -> {
          recreate("renew-failed");
          return null;
        });
      } catch (Exception ex) {
        log.error("Recreate after renew failure also failed", ex);
      }
    }
  }

  // ------------------ internals ------------------

  private NspSubscriptionState ensureSubscription() throws Exception {
    Optional<NspSubscriptionState> existing = stateStore.load();
    if (existing.isPresent()) {
      NspSubscriptionState st = existing.get();

      if (st.host() != null && !st.host().isBlank()) {
        nspClient.forceActiveHost(st.host());
      }

      return st;
    }

    var info = nspClient.createSubscription();
    var state = new NspSubscriptionState(info.subscriptionId(), info.topicId(), info.host());
    stateStore.save(state);

    log.info("Created subscriptionId={}, topicId={}, host={}",
        state.subscriptionId(), state.topicId(), state.host());

    return state;
  }

  private void recreate(String reason) throws Exception {
    log.warn("Recreating subscription. reason={}", reason);

    // stop consumer (if any)
    try {
      consumer.stop();
    } catch (Exception e) {
      log.warn("Consumer stop failed during recreate (continuing).", e);
    }

    // clear stored subscription info
    stateStore.clear();

    // create new subscription
    var state = ensureSubscription();

    // wait for topic existence
    waitTopicExists(state.topicId());

    // run sync and start consumer
    syncWithConsumerPaused("recreate-" + reason);
    startConsumerOn(state.topicId());
  }

  private void syncWithConsumerPaused(String reason) {
    try {
      if (consumer.isRunning()) consumer.pause();
    } catch (Exception e) {
      log.warn("Failed to pause consumer (continuing sync).", e);
    }

    try {
      sync.runSync(reason);
    } finally {
      try {
        if (consumer.isRunning()) consumer.resume();
      } catch (Exception e) {
        log.error("Failed to resume consumer after sync", e);
      }
    }
  }

  private void startConsumerOn(String topic) {
    String current = consumer.currentTopic();

    if (consumer.isRunning() && topic != null && topic.equals(current)) {
      log.info("Consumer already running on topic {}", topic);
      return;
    }

    log.info("Starting consumer on topic {}", topic);
    consumer.start(topic);
  }

  private void waitTopicExists(String topic) throws Exception {
    long deadline = System.currentTimeMillis() + topicWaitTimeoutMs;

    while (System.currentTimeMillis() < deadline) {
      TopicStatus s = checkTopic(topic);

      if (s == TopicStatus.EXISTS) {
        log.info("Topic exists: {}", topic);
        return;
      }

      // If missing, do not throw here; we keep waiting because creation can be eventually consistent.
      // If unreachable, also keep waiting (until timeout).
      TimeUnit.MILLISECONDS.sleep(topicWaitSleepMs);
    }

    throw new IllegalStateException("Timed out waiting for topic to exist: " + topic);
  }

  /** For legacy callers; prefer checkTopic() so you don't mask connectivity as "missing". */
  @SuppressWarnings("unused")
  private boolean topicExists(String topic) {
    return checkTopic(topic) == TopicStatus.EXISTS;
  }

  private enum TopicStatus { EXISTS, MISSING, UNREACHABLE_OR_UNKNOWN }

  /**
   * Distinguish real "missing topic" from "Kafka unreachable / metadata timeout".
   * IMPORTANT: Only treat MISSING as a signal to recreate subscriptions.
   */
  private TopicStatus checkTopic(String topic) {
    try {
      inputAdmin.describeTopics(List.of(topic))
          .allTopicNames()
          .get(5, TimeUnit.SECONDS);
      return TopicStatus.EXISTS;

    } catch (ExecutionException ee) {
      Throwable c = ee.getCause();

      if (c instanceof UnknownTopicOrPartitionException) return TopicStatus.MISSING;

      if (c instanceof TopicAuthorizationException) {
        log.error("Not authorized to describe topic {}", topic, c);
        return TopicStatus.UNREACHABLE_OR_UNKNOWN;
      }

      log.warn("DescribeTopics failed for {} (cause={})", topic, c.toString());
      return TopicStatus.UNREACHABLE_OR_UNKNOWN;

    } catch (TimeoutException te) {
      log.warn("DescribeTopics timed out for {}", topic);
      return TopicStatus.UNREACHABLE_OR_UNKNOWN;

    } catch (Exception e) {
      log.warn("DescribeTopics failed for {}", topic, e);
      return TopicStatus.UNREACHABLE_OR_UNKNOWN;
    }
  }

  /**
   * ✅ Blocking lock with timeout:
   * - prevents “Skip startFlow: busy” from killing startup consumption
   * - still prevents concurrent renew/sync/start from overlapping
   */
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
        log.warn("Interrupted while waiting for manager lock (op={})", op);
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
