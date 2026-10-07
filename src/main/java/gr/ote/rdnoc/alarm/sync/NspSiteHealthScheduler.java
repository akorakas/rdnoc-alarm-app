package gr.ote.rdnoc.alarm.sync;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import gr.ote.rdnoc.alarm.nsp.NspSubscriptionManager;

import lombok.RequiredArgsConstructor;

/**
 * Periodically verifies the active NSP site (REST + Kafka) and fails over when it stops
 * being active. No-op unless app.rest.nsp.failover.enabled=true.
 */
@Component
@RequiredArgsConstructor
public class NspSiteHealthScheduler {

  private final NspSubscriptionManager manager;

  @Value("${app.rest.nsp.failover.health-check.enabled:true}")
  private boolean enabled;

  @Scheduled(
      fixedDelayString = "${app.rest.nsp.failover.health-check.interval-ms:60000}",
      initialDelayString = "${app.rest.nsp.failover.health-check.initial-delay-ms:60000}"
  )
  public void check() {
    if (!enabled) return;
    manager.checkActiveSite();
  }
}
