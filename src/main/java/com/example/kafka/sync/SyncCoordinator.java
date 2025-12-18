package com.example.kafka.sync;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

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

  private final ReentrantLock lock = new ReentrantLock();

  @Value("${app.sync.enabled:true}")
  private boolean syncEnabled;

  @Value("${app.sync.skip-if-running:true}")
  private boolean skipIfRunning;

  @Value("${app.inputs.kafka.enabled:true}")
  private boolean kafkaEnabled;

  @Value("${app.inputs.rest.enabled:true}")
  private boolean restEnabled;

  public void runSync(String reason) {
    if (!syncEnabled) {
      log.info("Sync disabled; skipping. reason={}", reason);
      return;
    }
    if (!restEnabled) {
      log.warn("REST input disabled; skipping sync. reason={}", reason);
      return;
    }

    boolean acquired = lock.tryLock();
    if (!acquired) {
      if (skipIfRunning) {
        log.warn("Sync already running; skipping. reason={}", reason);
        return;
      }
      lock.lock();
    }

    boolean kafkaWasRunning = false;

    try {
      kafkaWasRunning = kafkaEnabled && kafkaController.isRunning();

      if (kafkaWasRunning) {
        log.info("Pausing Kafka listener before sync. reason={}", reason);
        kafkaController.pause();
      } else {
        log.info("Kafka listener not running (or disabled); sync will proceed. reason={}", reason);
      }

      // SYNC_START marker
      sendMarker(syncMarkerFactory.buildSyncStart(), "SYNC_START");

      // REST snapshot alarms (transform & publish)
      restPoller.fetchAndPublishActiveAlarmsOnce();

      // SYNC_END marker
      sendMarker(syncMarkerFactory.buildSyncEnd(), "SYNC_END");

      log.info("Sync completed successfully. reason={}", reason);

    } catch (Exception e) {
      log.error("Sync failed. reason={}", reason, e);
      throw e;

    } finally {
      try {
        if (kafkaWasRunning) {
          log.info("Resuming Kafka listener after sync. reason={}", reason);
          kafkaController.resume();
        }
      } finally {
        lock.unlock();
      }
    }
  }

  private void sendMarker(String markerJson, String markerType) {
    Map<String, String> headers = new HashMap<>();
    headers.put("sync-marker", markerType);
    sinks.sendOutput(null, markerJson, headers);
  }
}
