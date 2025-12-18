// src/main/java/com/example/kafka/sync/SyncScheduler.java
package com.example.kafka.sync;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class SyncScheduler {

  private final SyncCoordinator coordinator;

  @Value("${app.sync.enabled:true}")
  private boolean syncEnabled;

  /**
   * Runs sync periodically (configurable via YAML).
   *
   * YAML:
   * app:
   *   sync:
   *     fixed-delay-ms: 60000
   *     initial-delay-ms: 0
   */
  @Scheduled(
      fixedDelayString = "${app.sync.fixed-delay-ms:60000}",
      initialDelayString = "${app.sync.initial-delay-ms:0}"
  )
  public void scheduledSync() {
    if (!syncEnabled) return;

    try {
      coordinator.runSync("scheduled");
    } catch (Exception e) {
      // Don't crash scheduler thread; just log
      log.error("Scheduled sync execution failed", e);
    }
  }
}
