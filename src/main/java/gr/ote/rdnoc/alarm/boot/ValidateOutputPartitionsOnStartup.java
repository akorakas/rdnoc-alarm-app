// src/main/java/com/example/kafka/boot/ValidateOutputPartitionsOnStartup.java
package gr.ote.rdnoc.alarm.boot;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import gr.ote.atlas.events.enums.EMSId;
import gr.ote.rdnoc.alarm.service.config.SinksProperties;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class ValidateOutputPartitionsOnStartup implements ApplicationRunner {

  private final AdminClient outputAdmin;
  private final SinksProperties sinks;

  // default true (matches your YAML intent)
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

    final String topic = sinks.getOutput().getTopic();
    if (topic == null || topic.isBlank()) {
      throw new IllegalStateException("Output sink is kafka but app.sinks.output.topic is empty.");
    }

    final int requiredPartitions = EMSId.values().length; // e.g. 31 (0..30)
    final long timeoutMs = Duration.ofSeconds(timeoutSec).toMillis();

    log.info("Validating output topic partitions: topic='{}', requiredPartitions={}, timeout={}s",
        topic, requiredPartitions, timeoutSec);

    TopicDescription desc = describeTopic(topic, timeoutMs);
    int actualPartitions = desc.partitions().size();

    if (actualPartitions < requiredPartitions) {
      String msg =
          "FATAL: Output topic '" + topic + "' has " + actualPartitions + " partition(s) but the application requires at least "
              + requiredPartitions + " partition(s) (EMSId ordinals 0.." + (requiredPartitions - 1) + "). "
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

      // Prefer the per-topic future (cleaner missing-topic behavior)
      TopicDescription desc =
          res.topicNameValues().get(topic).get(timeoutMs, TimeUnit.MILLISECONDS);

      if (desc == null) {
        throw new UnknownTopicOrPartitionException("Topic not found: " + topic);
      }
      return desc;

    } catch (Exception e) {
      Throwable root = rootCause(e);

      if (root instanceof UnknownTopicOrPartitionException) {
        String msg =
            "FATAL: Output topic '" + topic + "' does not exist on the OUTPUT Kafka cluster. "
                + "Create it with >= " + EMSId.values().length + " partitions and restart.";
        log.error(msg, e);
        throw new IllegalStateException(msg, e);
      }

      if (root instanceof TopicAuthorizationException) {
        String msg =
            "FATAL: Not authorized to describe output topic '" + topic + "' on OUTPUT Kafka cluster. "
                + "Fix ACLs for the producer principal (Describe/Read metadata on topic), then restart.";
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
