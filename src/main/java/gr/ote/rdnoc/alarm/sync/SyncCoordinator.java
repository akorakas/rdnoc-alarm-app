package gr.ote.rdnoc.alarm.sync;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.stereotype.Component;

import gr.ote.rdnoc.alarm.nsp.NspRestPoller;
import gr.ote.rdnoc.alarm.service.sync.SyncMarkerFactory;
import gr.ote.rdnoc.alarm.sink.SinkRouter;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class SyncCoordinator {

  private final NspRestPoller restPoller;
  private final SyncMarkerFactory syncMarkerFactory;
  private final SinkRouter sinks;

  private final AtomicBoolean running = new AtomicBoolean(false);

  /**
   * Sync flow (NO kafka consumer pause/resume here):
   *  1) send SYNC_START (best-effort)
   *  2) fetch+publish snapshot via REST
   *  3) send SYNC_END (best-effort)
   *
   * Consumer pause/resume is handled by NspSubscriptionManager because topic is dynamic.
   */
  public void runSync(String reason) {
    if (!running.compareAndSet(false, true)) {
      log.warn("Sync already running; skip. reason={}", reason);
      return;
    }

    Map<String, String> headers = new HashMap<>();
    headers.put("source", "SYNC");
    headers.put("reason", reason);

    try {
      // 1) SYNC_START marker (best-effort)
      try {
        String syncStart = syncMarkerFactory.buildSyncStart();
        sinks.sendOutput(null, syncStart, headers);
        log.info("Sync: sent SYNC_START");
      } catch (Exception e) {
        log.error("Sync: failed to build/send SYNC_START", e);
      }

      // 2) Snapshot publish
      try {
        restPoller.fetchAndPublishActiveAlarmsOnce();
        log.info("Sync: snapshot published successfully");
      } catch (Exception e) {
        log.error("Sync: snapshot fetch/publish failed. reason={}", reason, e);
      }

      // 3) SYNC_END marker (best-effort)
      try {
        String syncEnd = syncMarkerFactory.buildSyncEnd();
        sinks.sendOutput(null, syncEnd, headers);
        log.info("Sync: sent SYNC_END");
      } catch (Exception e) {
        log.error("Sync: failed to build/send SYNC_END", e);
      }

    } finally {
      running.set(false);
    }
  }
}
