// src/main/java/com/example/kafka/boot/RequireTopics.java
package gr.ote.rdnoc.alarm.boot;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;

import gr.ote.rdnoc.alarm.service.config.SinksProperties;

@Configuration
public class RequireTopics {

  private static final Logger log = LoggerFactory.getLogger(RequireTopics.class);

  /** Admin client for the INPUT cluster (built from spring.kafka.consumer.*) */
  @Bean(name = "inputAdminClient", destroyMethod = "close")
  public AdminClient inputAdminClient(KafkaProperties props) {
    Map<String, Object> cfg = new HashMap<>(props.buildConsumerProperties());
    return AdminClient.create(cfg);
  }

  @Bean
  public ApplicationRunner verifyAllTopics(
      @Qualifier("inputAdminClient") AdminClient inputAdmin,
      // IMPORTANT: this now comes ONLY from KafkaAdminClientsConfig
      @Qualifier("outputAdminClient") AdminClient outputAdmin,

      KafkaListenerEndpointRegistry registry,

      @Value("${app.kafka.mode:dynamic}") String kafkaMode,
      @Value("${app.kafka.input-topic:}") String inputTopic,

      @Value("${app.kafka.verify-input-topic:false}") boolean verifyInputTopic,
      @Value("${app.kafka.start-listeners-after-verify:false}") boolean startListenersAfterVerify,

      SinksProperties sinksProps,
      @Value("${app.kafka.verify-timeout-sec:10}") int verifyTimeoutSec
  ) {
    return (ApplicationArguments args) -> {
      if (log.isDebugEnabled() && args != null) {
        log.debug("Startup args: {}", Arrays.toString(args.getSourceArgs()));
      }

      final String mode = trimOrNull(kafkaMode);
      final boolean isStatic = "static".equalsIgnoreCase(mode);
      final boolean isDynamic = !isStatic; // default dynamic

      // 1) Verify both clusters are reachable
      verifyClusterReachable(inputAdmin, verifyTimeoutSec, "INPUT");
      verifyClusterReachable(outputAdmin, verifyTimeoutSec, "OUTPUT");

      // 2) Input topic rules:
      //    - dynamic: topic comes from NSP subscription -> do NOT require/verify app.kafka.input-topic
      //    - static : app.kafka.input-topic MUST be set; optionally verify existence
      String input = trimOrNull(inputTopic);

      if (isDynamic) {
        log.info("[INPUT] app.kafka.mode={} -> skipping input-topic verification (topic created at runtime).", mode);
      } else {
        if (input == null || input.isBlank()) {
          throw new IllegalStateException("[INPUT] app.kafka.mode=static but app.kafka.input-topic is empty.");
        }

        if (verifyInputTopic) {
          verifyTopicsExist(inputAdmin, Set.of(input), verifyTimeoutSec, "INPUT");
        } else {
          log.info("[INPUT] Static mode: input-topic='{}' set, but verify-input-topic=false so topic existence won't be checked.", input);
        }
      }

      // 3) Verify sink topics (kafka-only) on OUTPUT cluster
      Set<String> requiredOutput = new LinkedHashSet<>();
      if (sinksProps.getOutput() != null && isKafka(sinksProps.getOutput().getType()))  requiredOutput.add(trimOrNull(sinksProps.getOutput().getTopic()));
      if (sinksProps.getDlt() != null && isKafka(sinksProps.getDlt().getType()))  requiredOutput.add(trimOrNull(sinksProps.getDlt().getTopic()));
      if (sinksProps.getError() != null && isKafka(sinksProps.getError().getType()))  requiredOutput.add(trimOrNull(sinksProps.getError().getTopic()));
      requiredOutput.removeIf(t -> t == null || t.isBlank());

      if (!requiredOutput.isEmpty()) {
        verifyTopicsExist(outputAdmin, requiredOutput, verifyTimeoutSec, "OUTPUT");
      } else {
        log.info("No OUTPUT topics to verify (all sinks are file-based or unset).");
      }

      // 4) Optional start listener containers
      if (startListenersAfterVerify && isStatic) {
        registry.start();
        log.info("Kafka listeners started after topic verification (static mode).");
      } else {
        log.info("Kafka listener registry not started (start-listeners-after-verify={}, mode={}).",
            startListenersAfterVerify, mode);
      }
    };
  }

  private static void verifyClusterReachable(AdminClient admin, int timeoutSec, String tag) {
    try {
      admin.describeCluster().nodes().get(timeoutSec, TimeUnit.SECONDS);
      log.info("[{}] Kafka cluster reachable.", tag);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while contacting " + tag + " Kafka cluster.", ie);
    } catch (ExecutionException | TimeoutException e) {
      throw new IllegalStateException("Cannot reach/authenticate to " + tag
          + " Kafka cluster. Check bootstrap.servers and security.*.", e);
    }
  }

  private static void verifyTopicsExist(AdminClient admin, Set<String> required, int timeoutSec, String tag) {
    List<String> topics = required.stream()
        .filter(Objects::nonNull)
        .filter(s -> !s.isBlank())
        .toList();

    if (topics.isEmpty()) {
      throw new IllegalStateException("No required topics configured for " + tag + " cluster.");
    }

    log.info("[{}] Verifying required topics: {}", tag, topics);

    final Set<String> existing;
    try {
      ListTopicsOptions options = new ListTopicsOptions().listInternal(false);
      existing = admin.listTopics(options).names().get(timeoutSec, TimeUnit.SECONDS);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while listing topics on " + tag + " cluster.", ie);
    } catch (ExecutionException | TimeoutException e) {
      throw new IllegalStateException("Failed to list topics via AdminClient on " + tag + " cluster.", e);
    }

    List<String> missing = topics.stream().filter(t -> !existing.contains(t)).toList();
    if (!missing.isEmpty()) {
      String msg = "[" + tag + "] Missing required topic(s): " + String.join(", ", missing);
      log.error(msg);
      throw new IllegalStateException(msg);
    }

    log.info("[{}] All required topics are present.", tag);
  }

  private static boolean isKafka(String type) {
    return type != null && "kafka".equalsIgnoreCase(type.trim());
  }

  private static String trimOrNull(String s) {
    return s == null ? null : s.trim();
  }
}
