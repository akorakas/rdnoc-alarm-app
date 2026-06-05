package gr.ote.rdnoc.alarm.mv36.sync;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import gr.ote.rdnoc.alarm.kafka.KafkaListenerController;
import gr.ote.rdnoc.alarm.mv36.config.Mv36SnmpProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "app.mv36.sync", name = "enabled", havingValue = "true")
public class Mv36StartupSyncRunner implements ApplicationRunner {

  private final Mv36SnmpProperties props;
  private final Mv36SyncCoordinator syncCoordinator;
  private final KafkaListenerController kafkaListenerController;

  @Override
  public void run(ApplicationArguments args) {
    if (!props.getSync().isRunOnStartup()) {
      log.info("MV36 startup sync skipped. app.mv36.sync.run-on-startup=false");
      return;
    }

    String listenerId = props.getSync().getKafkaListenerId();

    log.info("MV36 startup sync started. listenerId={}", listenerId);

    try {
      syncCoordinator.runSync("startup");
      log.info("MV36 startup sync completed");

    } catch (Exception e) {
      log.error("MV36 startup sync failed", e);

    } finally {
      if (props.getSync().isStartListenerAfterStartupSync()) {
        try {
          log.info("Starting Kafka listener {} after MV36 startup sync", listenerId);
          kafkaListenerController.start(listenerId);
        } catch (Exception e) {
          log.error("Failed to start Kafka listener {} after MV36 startup sync", listenerId, e);
        }
      } else {
        log.info(
            "Kafka listener not started after MV36 startup sync because app.mv36.sync.start-listener-after-startup-sync=false"
        );
      }
    }
  }
}