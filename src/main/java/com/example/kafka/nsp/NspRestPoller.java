// src/main/java/com/example/kafka/nsp/NspRestPoller.java
package com.example.kafka.nsp;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.example.kafka.service.Transformer;
import com.example.kafka.sink.SinkRouter;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class NspRestPoller {

  private final NspClient nspClient;
  private final Transformer transformer;
  private final SinkRouter sinks;

  @Value("${app.rest.nsp.host}")
  private String host;

  /**
   * Keep this flag ONLY for logging/feature toggle.
   * In the new design, paging is handled INSIDE NspClient (cursor-based),
   * so NspRestPoller does NOT do offset/limit loops anymore.
   */
  @Value("${app.rest.nsp.pagination.enabled:false}")
  private boolean paginationEnabled;

  public NspRestPoller(
      NspClient nspClient,
      @Qualifier("restTransformer") Transformer transformer,
      SinkRouter sinks
  ) {
    this.nspClient = nspClient;
    this.transformer = transformer;
    this.sinks = sinks;
  }

  @PostConstruct
  public void logProps() {
    log.info("NSP config from YAML: host={}", host);
    log.info("NSP pagination.enabled={} (NOTE: REST paging handled internally via cursor logic)", paginationEnabled);
  }

  /**
   * Fetch active alarms once via REST and publish to output sink.
   *
   * IMPORTANT:
   * NSP offset/limit pagination is ignored by this endpoint.
   * NspClient.fetchActiveAlarmEvents() implements cursor-based batching using:
   *   lastTimeDetected < <cursor>
   *
   * So this method simply fetches the list and publishes it.
   */
  public void fetchAndPublishActiveAlarmsOnce() throws Exception {

    Map<String, String> headers = new HashMap<>();
    headers.put("source", "NSP-REST");
    headers.put("source-host", host);

    int okTotal = 0;
    int failedTotal = 0;

    // ✅ This now returns ALL alarms across multiple REST calls (cursor strategy),
    // or single call if pagination is disabled in NspClient configuration.
    List<String> events = nspClient.fetchActiveAlarmEvents();

    log.info("NSP REST: fetched {} alarms (cursor-based={})",
        events.size(),
        paginationEnabled
    );

    for (String value : events) {
      try {
        String outJson = transformer.transform(value);
        sinks.sendOutput(null, outJson, headers);
        okTotal++;
      } catch (Exception e) {
        failedTotal++;
        log.error("NSP REST: failed to transform/send alarm event", e);
      }
    }

    log.info("NSP REST: snapshot publish finished. okTotal={}, failedTotal={}",
        okTotal, failedTotal);
  }
}
