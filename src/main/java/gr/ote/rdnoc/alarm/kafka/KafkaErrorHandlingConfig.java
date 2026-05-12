// src/main/java/com/example/kafka/kafka/KafkaErrorHandlingConfig.java
package gr.ote.rdnoc.alarm.kafka;

import java.util.HashMap;
import java.util.Map;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import gr.ote.rdnoc.alarm.service.errors.BadInputException;
import gr.ote.rdnoc.alarm.service.errors.TransformFailureException;
import gr.ote.rdnoc.alarm.sink.SinkRouter;

@Configuration
public class KafkaErrorHandlingConfig {

  /**
   * Routes failures to either the "error" sink (bad JSON) or the "dlt" sink
   * (pipeline/transform failure). The sink can be Kafka or file depending on YAML.
   */
  @Bean
  public DefaultErrorHandler errorHandler(SinkRouter sinks) {

    ConsumerRecordRecoverer recoverer = (rec, ex) -> {
      Throwable root = rootCause(ex);

      // Original key/value as strings (null-safe)
      String key = rec == null || rec.key() == null ? null : rec.key().toString();
      String payload = rec == null || rec.value() == null ? "null" : rec.value().toString();

      // Some provenance headers (also written by the file sink)
      Map<String, String> hdrs = new HashMap<>();
      if (rec != null) {
        hdrs.put("source-topic", rec.topic());
        hdrs.put("source-partition", String.valueOf(rec.partition()));
        hdrs.put("source-offset", String.valueOf(rec.offset()));
      }
      hdrs.put("error", root.getClass().getSimpleName());
      hdrs.put("errorMessage", root.getMessage());

      if (root instanceof BadInputException) {
        // Invalid JSON → errors sink
        sinks.sendError(key, payload, hdrs);
      } else {
        // Transform/pipeline failures → DLT sink
        sinks.sendDlt(key, payload, hdrs);
      }
    };

    // Publish once, immediately; then commit so we don't redeliver the same bad record
    DefaultErrorHandler eh = new DefaultErrorHandler(recoverer, new FixedBackOff(0L, 0));
    eh.setCommitRecovered(true);

    // Don't retry these (we know they won't succeed on retry)
    eh.addNotRetryableExceptions(
        BadInputException.class,
        TransformFailureException.class
        // optionally: org.apache.kafka.common.errors.SerializationException.class
    );

    return eh;
  }

  private static Throwable rootCause(Throwable t) {
    Throwable cur = t;
    while (cur.getCause() != null) cur = cur.getCause();
    return cur;
  }
}
