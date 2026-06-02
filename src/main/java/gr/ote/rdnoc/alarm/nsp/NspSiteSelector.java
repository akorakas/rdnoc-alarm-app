package gr.ote.rdnoc.alarm.nsp;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class NspSiteSelector {

  @Value("${app.rest.nsp.host}")
  private String defaultHost;

  @Value("${app.rest.nsp.failover.enabled:false}")
  private boolean failoverEnabled;

  @Value("${app.rest.nsp.failover.preferred-host:}")
  private String preferredHost;

  @Value("${app.rest.nsp.failover.hosts:}")
  private List<String> configuredHosts;

  private final AtomicReference<String> activeHost = new AtomicReference<>();

  private List<String> hosts = List.of();

  @PostConstruct
  public void init() {
    Set<String> ordered = new LinkedHashSet<>();

    if (preferredHost != null && !preferredHost.isBlank()) {
      ordered.add(preferredHost.trim());
    }

    if (configuredHosts != null) {
      for (String h : configuredHosts) {
        if (h != null && !h.isBlank()) {
          ordered.add(h.trim());
        }
      }
    }

    if (defaultHost != null && !defaultHost.isBlank()) {
      ordered.add(defaultHost.trim());
    }

    this.hosts = new ArrayList<>(ordered);

    if (hosts.isEmpty()) {
      throw new IllegalStateException("No NSP host configured. Set app.rest.nsp.host or app.rest.nsp.failover.hosts");
    }

    this.activeHost.set(hosts.get(0));

    log.info("NSP site selector initialized. failoverEnabled={}, activeHost={}, hosts={}",
        failoverEnabled, activeHost.get(), hosts);
  }

  public boolean isFailoverEnabled() {
    return failoverEnabled;
  }

  public String activeHost() {
    return activeHost.get();
  }

  public void forceActiveHost(String host) {
    if (host == null || host.isBlank()) {
      return;
    }

    String normalized = host.trim();

    if (!hosts.contains(normalized)) {
      log.warn("Requested NSP active host {} is not in configured hosts {}; adding temporarily",
          normalized, hosts);
    }

    String previous = activeHost.getAndSet(normalized);

    if (!normalized.equals(previous)) {
      log.warn("NSP active host changed: {} -> {}", previous, normalized);
    }
  }

  public List<String> orderedHostsForAttempt() {
    if (!failoverEnabled) {
      return List.of(activeHost());
    }

    String current = activeHost();
    List<String> ordered = new ArrayList<>();

    if (current != null && !current.isBlank()) {
      ordered.add(current);
    }

    for (String h : hosts) {
      if (!ordered.contains(h)) {
        ordered.add(h);
      }
    }

    return ordered;
  }

  public void markSuccess(String host) {
    if (host == null || host.isBlank()) {
      return;
    }

    String previous = activeHost.getAndSet(host.trim());

    if (!host.trim().equals(previous)) {
      log.warn("NSP failover selected active host: {} -> {}", previous, host.trim());
    }
  }

  public void markFailure(String host, Exception e) {
    log.warn("NSP site failed: host={}, error={}", host, e.toString());
  }
}