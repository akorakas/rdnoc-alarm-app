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

    // ─────────────────────────────────────────────────────────────────────────
    // Pagination (inject same settings as NspClient so Poller can stream per page)
    // ─────────────────────────────────────────────────────────────────────────

    @Value("${app.rest.nsp.pagination.enabled:false}")
    private boolean paginationEnabled;

    @Value("${app.rest.nsp.pagination.limit:1000}")
    private int paginationLimit;

    @Value("${app.rest.nsp.pagination.max-pages:200}")
    private int paginationMaxPages;

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
        log.info("NSP pagination: enabled={}, limit={}, maxPages={}",
            paginationEnabled, paginationLimit, paginationMaxPages);
    }

    /**
     * Fetch active alarms once via REST and publish to output sink.
     *
     * If pagination is enabled:
     *  - fetch alarms page-by-page using offset/limit
     *  - publish each page immediately (no huge list in memory)
     *
     * If pagination is disabled:
     *  - fallback to old behavior (single call)
     */
    public void fetchAndPublishActiveAlarmsOnce() throws Exception {

        Map<String, String> headers = new HashMap<>();
        headers.put("source", "NSP-REST");
        headers.put("source-host", host);

        int okTotal = 0;
        int failedTotal = 0;

        if (!paginationEnabled) {
            // ✅ Old behavior: single call returns all alarms
            List<String> events = nspClient.fetchActiveAlarmEvents();
            log.info("NSP REST: fetched {} alarms (no pagination)", events.size());

            for (String value : events) {
                try {
                    String outJson = transformer.transform(value);
                    sinks.sendOutput(null, outJson, headers);
                    okTotal++;
                } catch (Exception e) {
                    failedTotal++;
                    log.error("NSP REST: failed to transform/send single alarm event", e);
                }
            }

            log.info("NSP REST: snapshot publish finished. ok={}, failed={}", okTotal, failedTotal);
            return;
        }

        // ✅ Paginated behavior: streaming publish per page
        final int limit = Math.max(1, paginationLimit);
        int offset = 0;

        int page = 0;

        while (true) {
            page++;

            if (page > paginationMaxPages) {
                log.warn("NSP REST pagination safety stop: reached max-pages={} at offset={}",
                    paginationMaxPages, offset);
                break;
            }

            List<String> eventsPage = nspClient.fetchActiveAlarmEventsPage(offset, limit);

            if (eventsPage.isEmpty()) {
                log.info("NSP REST pagination finished: empty page at offset={}, limit={}", offset, limit);
                break;
            }

            int okPage = 0;
            int failedPage = 0;

            for (String value : eventsPage) {
                try {
                    String outJson = transformer.transform(value);
                    sinks.sendOutput(null, outJson, headers);
                    okPage++;
                } catch (Exception e) {
                    failedPage++;
                    log.error("NSP REST: failed to transform/send alarm event (offset={}, limit={})",
                        offset, limit, e);
                }
            }

            okTotal += okPage;
            failedTotal += failedPage;

            log.info("NSP REST: page publish finished. offset={}, limit={}, count={}, ok={}, failed={}",
                offset, limit, eventsPage.size(), okPage, failedPage);

            // ✅ stop rule: last page detected
            if (eventsPage.size() < limit) {
                log.info("NSP REST pagination finished: last page detected (count < limit). offset={}", offset);
                break;
            }

            offset += limit;
        }

        log.info("NSP REST: snapshot publish finished (PAGINATED). okTotal={}, failedTotal={}",
            okTotal, failedTotal);
    }
}
