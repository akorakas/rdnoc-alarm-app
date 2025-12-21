// src/main/java/com/example/kafka/nsp/NspSubscriptionManager.java
package com.example.kafka.nsp;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.example.kafka.kafka.DynamicKafkaConsumer;
import com.example.kafka.sync.SyncCoordinator;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class NspSubscriptionManager {

  private final NspClient nspClient;
  private final SubscriptionStateStore stateStore;

  /** ✅ Injected INPUT AdminClient (built from spring.kafka.consumer.*). Do NOT close per call. */
  private final AdminClient inputAdmin;

  private final DynamicKafkaConsumer consumer;
  private final SyncCoordinator sync;

  private final long topicWaitTimeoutMs;
  private final long topicWaitSleepMs;

  private final AtomicBoolean busy = new AtomicBoolean(false);

  public NspSubscriptionManager(
      NspClient nspClient,
      SubscriptionStateStore stateStore,
      @Qualifier("inputAdminClient") AdminClient inputAdmin,
      DynamicKafkaConsumer consumer,
      SyncCoordinator sync,
      @Value("${app.nsp.subscription.topic-wait-timeout-ms:180000}") long topicWaitTimeoutMs,
      @Value("${app.nsp.subscription.topic-wait-sleep-ms:2000}") long topicWaitSleepMs
  ) {
    this.nspClient = nspClient;
    this.stateStore = stateStore;
    this.inputAdmin = inputAdmin;
    this.consumer = consumer;
    this.sync = sync;
    this.topicWaitTimeoutMs = topicWaitTimeoutMs;
    this.topicWaitSleepMs = topicWaitSleepMs;
  }

  public void startFlow(String reason) throws Exception {
    withLock("startFlow", () -> {
      NspSubscriptionState state = ensureSubscription();
      waitTopicExists(state.topicId());

      syncWithConsumerPaused(reason);
      startConsumerOn(state.topicId());
      return null;
    });
  }

  public void runPeriodicSync(String reason) {
    try {
      withLock("periodicSync", () -> {
        syncWithConsumerPaused(reason);
        return null;
      });
    } catch (Exception e) {
      log.error("Periodic sync failed", e);
    }
  }

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

        if (!topicExists(state.topicId())) {
          log.warn("Topic missing: {} -> recreate subscription", state.topicId());
          recreate("topic-missing");
          return null;
        }

        nspClient.renewSubscription(state.subscriptionId());
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
    if (existing.isPresent()) return existing.get();

    var info = nspClient.createSubscription();
    var state = new NspSubscriptionState(info.subscriptionId(), info.topicId());
    stateStore.save(state);

    log.info("Created subscriptionId={}, topicId={}", state.subscriptionId(), state.topicId());
    return state;
  }

  private void recreate(String reason) throws Exception {
    log.warn("Recreating subscription. reason={}", reason);

    consumer.stop();
    stateStore.clear();

    var state = ensureSubscription();
    waitTopicExists(state.topicId());

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
    if (consumer.isRunning() && topic.equals(current)) {
      log.info("Consumer already running on topic {}", topic);
      return;
    }

    log.info("Starting consumer on topic {}", topic);
    consumer.start(topic);
  }

  private void waitTopicExists(String topic) throws Exception {
    long deadline = System.currentTimeMillis() + topicWaitTimeoutMs;
    while (System.currentTimeMillis() < deadline) {
      if (topicExists(topic)) {
        log.info("Topic exists: {}", topic);
        return;
      }
      TimeUnit.MILLISECONDS.sleep(topicWaitSleepMs);
    }
    throw new IllegalStateException("Timed out waiting for topic to exist: " + topic);
  }

  private boolean topicExists(String topic) {
    try {
      DescribeTopicsResult res = inputAdmin.describeTopics(java.util.List.of(topic));
      res.allTopicNames().get(5, TimeUnit.SECONDS);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  private <T> T withLock(String op, CheckedSupplier<T> fn) throws Exception {
    if (!busy.compareAndSet(false, true)) {
      log.warn("Skip {}: manager is busy (sync/renew/start already running)", op);
      return null;
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
