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
        log.info("NSP config from YAML (via @Value): host={}", host);
    }

    public void fetchAndPublishActiveAlarmsOnce() throws Exception {
        List<String> events = nspClient.fetchActiveAlarmEvents();
        log.info("NSP REST: fetched {} alarms", events.size());

        Map<String, String> headers = new HashMap<>();
        headers.put("source", "NSP-REST");
        headers.put("source-host", host);

        int ok = 0;
        int failed = 0;

        for (String value : events) {
            try {
                String outJson = transformer.transform(value);
                sinks.sendOutput(null, outJson, headers);
                ok++;
            } catch (Exception e) {
                failed++;
                log.error("NSP REST: failed to transform/send single alarm event", e);
            }
        }

        log.info("NSP REST: snapshot publish finished. ok={}, failed={}", ok, failed);
    }
}
