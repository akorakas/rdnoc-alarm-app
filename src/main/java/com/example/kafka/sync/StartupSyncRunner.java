package com.example.kafka.sync;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import com.example.kafka.nsp.NspSubscriptionManager;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class StartupSyncRunner implements ApplicationRunner {

  private final NspSubscriptionManager manager;

  @Value("${app.sync.run-on-startup:true}")
  private boolean runOnStartup;

  // whether subscription+kafka consumption is enabled (your “inputs.kafka.enabled”)
  @Value("${app.inputs.kafka.enabled:true}")
  private boolean kafkaEnabled;

  @Override
  public void run(ApplicationArguments args) throws Exception {
    if (!runOnStartup) {
      log.info("Startup flow skipped (app.sync.run-on-startup=false)");
      return;
    }

    if (!kafkaEnabled) {
      log.info("Kafka input disabled (app.inputs.kafka.enabled=false). Startup flow skipped.");
      return;
    }

    log.info("Startup: ensure subscription + run sync + start consumer on latest topic");
    manager.startFlow("startup");
  }
}
