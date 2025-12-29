// src/main/java/com/example/kafka/boot/ValidateOutputPartitionsOnStartup.java
package com.example.kafka.boot;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import com.example.kafka.service.config.SinksProperties;

import gr.ote.atlas.events.enums.EMSId;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class ValidateOutputPartitionsOnStartup implements ApplicationRunner {

  private final AdminClient outputAdmin;
  private final SinksProperties sinks;

  // ✅ default true to match your YAML intent (you can still disable in config)
  @Value("${app.kafka.output.validate-partitions-on-startup:true}")
  private boolean enabled;

  @Value("${app.kafka.output.validate-partitions-timeout-sec:10}")
  private long timeoutSec;

  public ValidateOutputPartitionsOnStartup(
      @Qualifier("outputAdminClient") AdminClient outputAdmin,
      SinksProperties sinks
  ) {
    this.outputAdmin = outputAdmin;
    this.sinks = sinks;
  }

  @Override
  public void run(ApplicationArguments args) throws Exception {

    if (!enabled) {
      log.info("Output partition validation disabled (app.kafka.output.validate-partitions-on-startup=false)");
      return;
    }

    // If output is file, nothing to validate
    if (sinks.getOutput() == null || "file".equalsIgnoreCase(sinks.getOutput().getType())) {
      log.info("Output sink is file -> skipping output partition validation.");
      return;
    }

    String topic = sinks.getOutput().getTopic();
    if (topic == null || topic.isBlank()) {
      throw new IllegalStateException("Output sink is kafka but app.sinks.output.topic is empty.");
    }

    int requiredPartitions = EMSId.values().length; // e.g. 31 (0..30)
    long timeoutMs = Duration.ofSeconds(timeoutSec).toMillis();

    log.info("Validating output topic partitions: topic='{}', requiredPartitions={}, timeout={}s",
        topic, requiredPartitions, timeoutSec);

    TopicDescription desc = describeTopic(topic, timeoutMs);
    int actualPartitions = desc.partitions().size();

    if (actualPartitions < requiredPartitions) {
      String msg =
          "FATAL: Output topic '" + topic + "' has " + actualPartitions + " partition(s) but the application requires at least "
              + requiredPartitions + " (EMSId ordinals 0.." + (requiredPartitions - 1) + "). "
              + "Fix: increase partitions for topic '" + topic + "' on the OUTPUT Kafka cluster.";
      log.error(msg);
      throw new IllegalStateException(msg);
    }

    log.info("OK: Output topic '{}' partition count = {} (required >= {}).",
        topic, actualPartitions, requiredPartitions);
  }

  private TopicDescription describeTopic(String topic, long timeoutMs) throws Exception {
    try {
      DescribeTopicsResult res = outputAdmin.describeTopics(java.util.List.of(topic));
      Map<String, TopicDescription> map =
          res.allTopicNames().get(timeoutMs, TimeUnit.MILLISECONDS);

      TopicDescription desc = map.get(topic);
      if (desc == null) {
        throw new UnknownTopicOrPartitionException("Topic not found: " + topic);
      }
      return desc;

    } catch (Exception e) {
      Throwable root = rootCause(e);

      if (root instanceof UnknownTopicOrPartitionException) {
        String msg =
            "FATAL: Output topic '" + topic + "' does not exist on OUTPUT Kafka cluster. "
                + "Create it with >= " + EMSId.values().length + " partitions and restart.";
        log.error(msg, e);
        throw new IllegalStateException(msg, e);
      }

      String msg =
          "FATAL: Could not describe output topic '" + topic + "' on OUTPUT Kafka cluster. "
              + "Cause: " + root.getClass().getSimpleName() + ": " + root.getMessage();
      log.error(msg, e);
      throw new IllegalStateException(msg, e);
    }
  }

  private static Throwable rootCause(Throwable t) {
    Throwable cur = t;
    while (cur.getCause() != null && cur.getCause() != cur) {
      cur = cur.getCause();
    }
    return cur;
  }
}
