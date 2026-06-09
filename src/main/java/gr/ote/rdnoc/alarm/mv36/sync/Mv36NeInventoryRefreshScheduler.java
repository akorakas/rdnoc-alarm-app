package gr.ote.rdnoc.alarm.mv36.sync;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.annotation.Order;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import gr.ote.rdnoc.alarm.mv36.cache.Mv36NeCache;
import gr.ote.rdnoc.alarm.mv36.model.Mv36NetworkElement;
import gr.ote.rdnoc.alarm.mv36.snmp.Mv36NeInventoryClient;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@Order(50)
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "app.mv36.enrichment", name = "enabled", havingValue = "true")
public class Mv36NeInventoryRefreshScheduler implements ApplicationRunner {

  private final Mv36NeInventoryClient inventoryClient;
  private final Mv36NeCache cache;

  private final AtomicBoolean running = new AtomicBoolean(false);

  @Value("${app.mv36.enrichment.refresh-on-startup:true}")
  private boolean refreshOnStartup;

  @Override
  public void run(ApplicationArguments args) {
    if (!refreshOnStartup) {
      log.info("MV36 NE enrichment startup refresh disabled");
      return;
    }

    refreshOnce("startup");
  }

  @Scheduled(
      fixedDelayString = "${app.mv36.enrichment.fixed-delay-ms:7200000}",
      initialDelayString = "${app.mv36.enrichment.initial-delay-ms:7200000}"
  )
  public void scheduledRefresh() {
    refreshOnce("scheduled");
  }

  public int refreshOnce(String reason) {
    if (!running.compareAndSet(false, true)) {
      log.warn("MV36 NE inventory refresh already running. skip reason={}", reason);
      return cache.size();
    }

    try {
      log.info("MV36 NE inventory refresh started. reason={}", reason);

      List<Mv36NetworkElement> elements = inventoryClient.fetchNetworkElements();
      cache.replaceAll(elements);

      int size = cache.size();

      log.info("MV36 NE inventory refresh completed. reason={}, size={}",
          reason, size);

      return size;

    } catch (Exception e) {
      log.error("MV36 NE inventory refresh failed. reason={}", reason, e);
      return cache.size();

    } finally {
      running.set(false);
    }
  }

  public boolean isCacheEmpty() {
    return cache.isEmpty();
  }

  public int cacheSize() {
    return cache.size();
  }
}