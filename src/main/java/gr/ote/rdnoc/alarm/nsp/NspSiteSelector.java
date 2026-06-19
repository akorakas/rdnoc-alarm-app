package gr.ote.rdnoc.alarm.nsp;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
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

  /**
   * Must be in the same order as app.rest.nsp.failover.hosts.
   *
   * Example:
   * hosts:
   *   - 172.17.45.132
   *   - 172.17.42.132
   *
   * kafka-bootstrap-servers:
   *   - 172.17.45.132:32100
   *   - 172.17.42.132:32100
   */
  @Value("${app.rest.nsp.failover.kafka-bootstrap-servers:}")
  private List<String> configuredKafkaBootstrapServers;

  private final AtomicReference<String> activeHost = new AtomicReference<>();

  private List<String> hosts = List.of();

  private final Map<String, String> kafkaBootstrapByHost = new LinkedHashMap<>();

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
      throw new IllegalStateException(
          "No NSP host configured. Set app.rest.nsp.host or app.rest.nsp.failover.hosts"
      );
    }

    buildKafkaBootstrapMapping();

    this.activeHost.set(hosts.get(0));

    log.info("NSP site selector initialized. failoverEnabled={}, activeHost={}, hosts={}, kafkaBootstrapByHost={}",
        failoverEnabled, activeHost.get(), hosts, kafkaBootstrapByHost);
  }

  private void buildKafkaBootstrapMapping() {
    kafkaBootstrapByHost.clear();

    if (configuredKafkaBootstrapServers == null || configuredKafkaBootstrapServers.isEmpty()) {
      log.warn("No app.rest.nsp.failover.kafka-bootstrap-servers configured. Kafka failover cannot be site-specific.");
      return;
    }

    if (configuredKafkaBootstrapServers.size() != hosts.size()) {
      log.warn(
          "Kafka bootstrap server count does not match NSP host count. hosts={}, kafkaBootstrapServers={}",
          hosts.size(),
          configuredKafkaBootstrapServers.size()
      );
    }

    int count = Math.min(hosts.size(), configuredKafkaBootstrapServers.size());

    for (int i = 0; i < count; i++) {
      String host = hosts.get(i);
      String bootstrap = configuredKafkaBootstrapServers.get(i);

      if (host != null && !host.isBlank() && bootstrap != null && !bootstrap.isBlank()) {
        kafkaBootstrapByHost.put(host.trim(), bootstrap.trim());
      }
    }
  }

  public boolean isFailoverEnabled() {
    return failoverEnabled;
  }

  public String activeHost() {
    return activeHost.get();
  }

  public String activeKafkaBootstrapServers() {
    return kafkaBootstrapServersFor(activeHost());
  }

  public String kafkaBootstrapServersFor(String host) {
    if (host == null || host.isBlank()) {
      return null;
    }

    String normalized = host.trim();
    String bootstrap = kafkaBootstrapByHost.get(normalized);

    if (bootstrap == null || bootstrap.isBlank()) {
      throw new IllegalStateException(
          "No Kafka bootstrap servers configured for NSP host " + normalized
              + ". Configure app.rest.nsp.failover.kafka-bootstrap-servers in the same order as app.rest.nsp.failover.hosts."
      );
    }

    return bootstrap;
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

    String normalized = host.trim();
    String previous = activeHost.getAndSet(normalized);

    if (!normalized.equals(previous)) {
      log.warn("NSP failover selected active host: {} -> {}", previous, normalized);
    }
  }

  public void markFailure(String host, Exception e) {
    log.warn("NSP site failed: host={}, error={}", host, e.toString());
  }
}