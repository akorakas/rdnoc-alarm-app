package gr.ote.rdnoc.alarm.sync;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import gr.ote.rdnoc.alarm.nsp.NspSubscriptionManager;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class SyncScheduler {

  private final NspSubscriptionManager manager;

  @Value("${app.sync.enabled:true}")
  private boolean syncEnabled;

  @Scheduled(
      fixedDelayString = "${app.sync.fixed-delay-ms:60000}",
      initialDelayString = "${app.sync.initial-delay-ms:60000}"
  )
  public void scheduledSync() {
    if (!syncEnabled) return;

    // IMPORTANT: go through manager so it pauses/resumes consumer and avoids overlap
    manager.runPeriodicSync("scheduled");
  }
}
