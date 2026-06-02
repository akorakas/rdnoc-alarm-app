package gr.ote.rdnoc.alarm.mv36.sync;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "app.mv36.sync", name = "enabled", havingValue = "true")
public class Mv36SyncScheduler {

  private final Mv36SyncCoordinator coordinator;

  @Scheduled(
      fixedDelayString = "${app.mv36.sync.fixed-delay-ms:7200000}",
      initialDelayString = "${app.mv36.sync.initial-delay-ms:120000}"
  )
  public void scheduledSync() {
    coordinator.runSync("scheduled");
  }
}