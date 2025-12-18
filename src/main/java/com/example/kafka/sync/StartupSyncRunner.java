// src/main/java/com/example/kafka/sync/StartupSyncRunner.java
package com.example.kafka.sync;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import com.example.kafka.kafka.KafkaListenerController;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class StartupSyncRunner implements ApplicationRunner {

  private final SyncCoordinator coordinator;
  private final KafkaListenerController kafkaController;

  @Value("${app.sync.enabled:true}")
  private boolean syncEnabled;

  @Value("${app.sync.run-on-startup:true}")
  private boolean runOnStartup;

  @Value("${app.inputs.kafka.enabled:true}")
  private boolean kafkaEnabled;

  @Override
  public void run(ApplicationArguments args) {
    // 1) initial sync
    if (syncEnabled && runOnStartup) {
      log.info("Running startup sync...");
      coordinator.runSync("startup");
    } else {
      log.info("Startup sync skipped (enabled={}, runOnStartup={})", syncEnabled, runOnStartup);
    }

    // 2) start kafka streaming
    if (kafkaEnabled) {
      log.info("Starting Kafka listener after startup phase...");
      kafkaController.start();
    } else {
      log.info("Kafka input disabled; listener will not start.");
    }
  }
}
