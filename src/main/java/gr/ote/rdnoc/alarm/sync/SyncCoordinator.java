package gr.ote.rdnoc.alarm.sync;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.stereotype.Component;

import gr.ote.rdnoc.alarm.nsp.NspRestPoller;
import gr.ote.rdnoc.alarm.service.sync.SyncMarkerFactory;
import gr.ote.rdnoc.alarm.service.sync.SyncMarkerProperties;
import gr.ote.rdnoc.alarm.sink.SinkRouter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class SyncCoordinator {

  private final NspRestPoller restPoller;
  private final SyncMarkerFactory syncMarkerFactory;
  private final SyncMarkerProperties syncMarkerProperties;
  private final SinkRouter sinks;

  private final AtomicBoolean running = new AtomicBoolean(false);

  /**
   * Sync flow:
   *  1) send SYNC_START
   *  2) fetch+publish snapshot via REST
   *  3) send SYNC_END
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
    headers.put("sourceEms", syncMarkerProperties.getSourceEms().name());

    boolean syncStarted = false;

    try {
      /*
       * 1) SYNC_START marker.
       *
       * This is no longer NSP hardcoded. It uses app.sync.marker.*.
       *
       * Important:
       * If SYNC_START cannot be sent, abort the sync. This avoids:
       *   - missing SYNC_START
       *   - FAULT_SYNC messages being sent
       *   - SYNC_END being sent alone
       */
      try {
        String syncStart = syncMarkerFactory.buildSyncStart(
            syncMarkerProperties.getSourceEms(),
            syncMarkerProperties.getEmsVendorId(),
            syncMarkerProperties.getEmsDomain()
        );

        sinks.sendOutput(null, syncStart, headers);
        syncStarted = true;

        log.info(
            "Sync: sent SYNC_START. sourceEms={}, vendor={}, domain={}, reason={}",
            syncMarkerProperties.getSourceEms(),
            syncMarkerProperties.getEmsVendorId(),
            syncMarkerProperties.getEmsDomain(),
            reason
        );

      } catch (Exception e) {
        log.error(
            "Sync: failed to build/send SYNC_START. Aborting sync. sourceEms={}, reason={}",
            syncMarkerProperties.getSourceEms(),
            reason,
            e
        );
        return;
      }

      /*
       * 2) Snapshot publish.
       */
      try {
        restPoller.fetchAndPublishActiveAlarmsOnce();
        log.info(
            "Sync: snapshot published successfully. sourceEms={}, reason={}",
            syncMarkerProperties.getSourceEms(),
            reason
        );

      } catch (Exception e) {
        log.error(
            "Sync: snapshot fetch/publish failed. sourceEms={}, reason={}",
            syncMarkerProperties.getSourceEms(),
            reason,
            e
        );
      }

      /*
       * 3) SYNC_END marker.
       *
       * Send SYNC_END only if SYNC_START was successfully sent.
       */
      if (syncStarted) {
        try {
          String syncEnd = syncMarkerFactory.buildSyncEnd(
              syncMarkerProperties.getSourceEms(),
              syncMarkerProperties.getEmsVendorId(),
              syncMarkerProperties.getEmsDomain()
          );

          sinks.sendOutput(null, syncEnd, headers);

          log.info(
              "Sync: sent SYNC_END. sourceEms={}, vendor={}, domain={}, reason={}",
              syncMarkerProperties.getSourceEms(),
              syncMarkerProperties.getEmsVendorId(),
              syncMarkerProperties.getEmsDomain(),
              reason
          );

        } catch (Exception e) {
          log.error(
              "Sync: failed to build/send SYNC_END. sourceEms={}, reason={}",
              syncMarkerProperties.getSourceEms(),
              reason,
              e
          );
        }
      }

    } finally {
      running.set(false);
    }
  }
}