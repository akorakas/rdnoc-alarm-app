package gr.ote.rdnoc.alarm.mv36.sync;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.stereotype.Component;

import gr.ote.atlas.events.enums.EMSDomain;
import gr.ote.atlas.events.enums.EMSId;
import gr.ote.atlas.events.enums.EMSVendorID;
import gr.ote.rdnoc.alarm.kafka.KafkaListenerController;
import gr.ote.rdnoc.alarm.mv36.config.Mv36SnmpProperties;
import gr.ote.rdnoc.alarm.service.sync.SyncMarkerFactory;
import gr.ote.rdnoc.alarm.sink.SinkRouter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class Mv36SyncCoordinator {

  private final Mv36SnmpProperties props;
  private final KafkaListenerController kafkaListenerController;
  private final SyncMarkerFactory syncMarkerFactory;
  private final Mv36SnmpPoller poller;
  private final SinkRouter sinks;

  private final AtomicBoolean running = new AtomicBoolean(false);

  public void runSync(String reason) {
    if (!running.compareAndSet(false, true)) {
      log.warn("MV36 sync already running; skip. reason={}", reason);
      return;
    }

    String listenerId = props.getSync().getKafkaListenerId();

    Map<String, String> headers = new HashMap<>();
    headers.put("source", "MV36-SNMP-SYNC");
    headers.put("sourceEms", "MV36_MOBILE");
    headers.put("reason", reason);

    boolean listenerWasRunning = false;

    try {
      listenerWasRunning = kafkaListenerController.isRunning(listenerId);

      if (listenerWasRunning) {
        log.info("MV36 sync: pausing Kafka listener {}", listenerId);
        kafkaListenerController.pause(listenerId);
      } else {
        log.info("MV36 sync: Kafka listener {} is not running; no pause needed", listenerId);
      }

      try {
        String syncStart = syncMarkerFactory.buildSyncStart(
            EMSId.MV36_MOBILE,
            EMSVendorID.MV_36,
            EMSDomain.TRANSPORT
        );

        sinks.sendOutput(null, syncStart, headers);
        log.info("MV36 sync: sent SYNC_START");
      } catch (Exception e) {
        log.error("MV36 sync: failed to build/send SYNC_START", e);
      }

      try {
        poller.fetchAndPublishActiveAlarmsOnce();
        log.info("MV36 sync: snapshot published successfully");
      } catch (Exception e) {
        log.error("MV36 sync: snapshot fetch/publish failed. reason={}", reason, e);
      }

      try {
        String syncEnd = syncMarkerFactory.buildSyncEnd(
            EMSId.MV36_MOBILE,
            EMSVendorID.MV_36,
            EMSDomain.TRANSPORT
        );

        sinks.sendOutput(null, syncEnd, headers);
        log.info("MV36 sync: sent SYNC_END");
      } catch (Exception e) {
        log.error("MV36 sync: failed to build/send SYNC_END", e);
      }

    } finally {
      try {
        if (listenerWasRunning) {
          log.info("MV36 sync: resuming Kafka listener {}", listenerId);
          kafkaListenerController.resume(listenerId);
        }
      } catch (Exception e) {
        log.error("MV36 sync: failed to resume Kafka listener {}", listenerId, e);
      }

      running.set(false);
    }
  }
}