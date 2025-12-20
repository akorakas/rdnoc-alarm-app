package com.example.kafka.sync;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.example.kafka.nsp.NspSubscriptionManager;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class SubscriptionRenewScheduler {

  private final NspSubscriptionManager manager;

  @Value("${app.nsp.subscription.renew-enabled:true}")
  private boolean enabled;

  @Scheduled(
      fixedDelayString = "${app.nsp.subscription.renew-fixed-delay-ms:1200000}",
      initialDelayString = "${app.nsp.subscription.renew-initial-delay-ms:0}"
  )
  public void renew() {
    if (!enabled) return;

    // renewOrRecreate() already handles failures internally + recreate if needed
    manager.renewOrRecreate();
  }
}
