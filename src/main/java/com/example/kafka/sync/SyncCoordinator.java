package com.example.kafka.sync;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.stereotype.Component;

import com.example.kafka.kafka.InputListener;
import com.example.kafka.kafka.KafkaListenerController;
import com.example.kafka.nsp.NspRestPoller;
import com.example.kafka.service.sync.SyncMarkerFactory;
import com.example.kafka.sink.SinkRouter;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class SyncCoordinator {

  private final KafkaListenerController kafkaController;
  private final NspRestPoller restPoller;
  private final SyncMarkerFactory syncMarkerFactory;
  private final SinkRouter sinks;

  // Prevent overlapping syncs (startup + scheduler, or slow REST call)
  private final AtomicBoolean running = new AtomicBoolean(false);

  /**
   * Execute a full sync flow:
   *  1) pause kafka listener (if running)
   *  2) send SYNC_START
   *  3) fetch+publish snapshot via REST
   *  4) send SYNC_END
   *  5) resume/start kafka listener
   */
  public void runSync(String reason) {
    if (!running.compareAndSet(false, true)) {
      log.warn("Sync already running; skip. reason={}", reason);
      return;
    }

    Map<String, String> headers = new HashMap<>();
    headers.put("source", "SYNC");
    headers.put("reason", reason);

    final String listenerId = InputListener.LISTENER_ID;

    boolean kafkaWasRunning = false;

    try {
      kafkaWasRunning = kafkaController.isRunning(listenerId);

      // 1) Pause Kafka listener (only if it was running)
      if (kafkaWasRunning) {
        log.info("Sync: pausing kafka listener id={}", listenerId);
        kafkaController.pause(listenerId);
      } else {
        log.info("Sync: kafka listener not running; will sync before starting it. id={}", listenerId);
      }

      // 2) SYNC_START marker (best-effort)
      try {
        String syncStart = syncMarkerFactory.buildSyncStart();
        sinks.sendOutput(null, syncStart, headers);
        log.info("Sync: sent SYNC_START");
      } catch (Exception e) {
        log.error("Sync: failed to build/send SYNC_START", e);
      }

      // 3) Snapshot publish (may throw checked Exception)
      try {
        restPoller.fetchAndPublishActiveAlarmsOnce();
        log.info("Sync: snapshot published successfully");
      } catch (Exception e) {
        log.error("Sync: snapshot fetch/publish failed. reason={}", reason, e);
      }

      // 4) SYNC_END marker (best-effort)
      try {
        String syncEnd = syncMarkerFactory.buildSyncEnd();
        sinks.sendOutput(null, syncEnd, headers);
        log.info("Sync: sent SYNC_END");
      } catch (Exception e) {
        log.error("Sync: failed to build/send SYNC_END", e);
      }

    } finally {
      // 5) Resume OR start Kafka listener
      try {
        if (kafkaWasRunning) {
          log.info("Sync: resuming kafka listener id={}", listenerId);
          kafkaController.resume(listenerId);
        } else {
          log.info("Sync: starting kafka listener id={} (startup behavior)", listenerId);
          kafkaController.start(listenerId);
        }
      } catch (Exception e) {
        log.error("Sync: failed to resume/start kafka listener id={}", listenerId, e);
      } finally {
        running.set(false);
      }
    }
  }
}
