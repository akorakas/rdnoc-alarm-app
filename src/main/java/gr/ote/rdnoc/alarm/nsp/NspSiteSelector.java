package gr.ote.rdnoc.alarm.nsp;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

/**
 * Holds the configured NSP sites and the single ACTIVE site.
 *
 * The active site is one object (REST host + its Kafka), so REST calls, subscriptions,
 * topic checks and the dynamic consumer always target the same site.
 *
 * Which site is active is decided by NspSubscriptionManager using NspSiteProbe;
 * this class only stores the result.
 */
@Slf4j
@Component
public class NspSiteSelector {

  private final boolean failoverEnabled;
  private final List<NspSite> sites;
  private final AtomicReference<NspSite> activeSite = new AtomicReference<>();

  @Autowired
  public NspSiteSelector(
      NspFailoverProperties props,
      @Value("${app.rest.nsp.host:}") String defaultHost,
      KafkaProperties kafkaProperties
  ) {
    this(props, defaultHost, consumerBootstrapServers(kafkaProperties));
  }

  /**
   * @param fallbackKafkaBootstrapServers used only for a single-site setup with no
   *        failover Kafka list configured (spring.kafka.consumer.bootstrap-servers).
   */
  NspSiteSelector(NspFailoverProperties props, String defaultHost, String fallbackKafkaBootstrapServers) {
    this.failoverEnabled = props.isEnabled();
    this.sites = List.copyOf(buildSites(props, trimToNull(defaultHost), trimToNull(fallbackKafkaBootstrapServers)));

    if (sites.isEmpty()) {
      throw new IllegalStateException(
          "No NSP host configured. Set app.rest.nsp.host or app.rest.nsp.failover.sites");
    }

    // Initial choice before any probing: preferred host if it is a configured site, else the first site.
    NspSite initial = findByHost(props.getPreferredHost()).orElse(sites.get(0));
    activeSite.set(initial);

    log.info("NSP site selector initialized. failoverEnabled={}, initialActiveSite={}, sites={}",
        failoverEnabled, initial, sites);
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Site list construction
  // ─────────────────────────────────────────────────────────────────────────

  private static List<NspSite> buildSites(NspFailoverProperties props, String defaultHost, String fallbackKafka) {
    List<NspSite> out = new ArrayList<>();

    // 1) Preferred: explicit site objects.
    if (props.getSites() != null && !props.getSites().isEmpty()) {
      int i = 0;
      for (NspFailoverProperties.SiteConfig sc : props.getSites()) {
        i++;
        String host = trimToNull(sc.getHost());
        String kafka = trimToNull(sc.getKafkaBootstrapServers());
        String name = trimToNull(sc.getName()) != null ? sc.getName().trim() : "site" + i;

        if (host == null) {
          throw new IllegalStateException("app.rest.nsp.failover.sites[" + (i - 1) + "].host is empty");
        }
        if (kafka == null) {
          throw new IllegalStateException("app.rest.nsp.failover.sites[" + (i - 1) + "] (" + host
              + ") has no kafka-bootstrap-servers");
        }
        addUnique(out, new NspSite(name, host, kafka));
      }
      warnIfDefaultHostUnknown(out, defaultHost);
      return out;
    }

    // 2) Legacy: hosts[i] <-> kafka-bootstrap-servers[i], in the order WRITTEN in YAML.
    //    (The old code paired them after moving preferred-host to the front, which swapped sites.)
    List<String> hosts = cleanList(props.getHosts());
    List<String> kafkas = cleanList(props.getKafkaBootstrapServers());

    if (!hosts.isEmpty()) {
      if (!kafkas.isEmpty() && kafkas.size() != hosts.size()) {
        throw new IllegalStateException("app.rest.nsp.failover.hosts (" + hosts.size()
            + ") and kafka-bootstrap-servers (" + kafkas.size() + ") must have the same number of entries");
      }
      for (int i = 0; i < hosts.size(); i++) {
        String kafka = !kafkas.isEmpty() ? kafkas.get(i) : (hosts.size() == 1 ? fallbackKafka : null);
        if (kafka == null) {
          throw new IllegalStateException("No Kafka bootstrap servers configured for NSP host " + hosts.get(i)
              + ". Configure app.rest.nsp.failover.sites (or kafka-bootstrap-servers in the same order as hosts).");
        }
        addUnique(out, new NspSite("site" + (i + 1), hosts.get(i), kafka));
      }
      warnIfDefaultHostUnknown(out, defaultHost);
      return out;
    }

    // 3) Single host: app.rest.nsp.host + spring.kafka.consumer.bootstrap-servers.
    if (defaultHost != null) {
      String kafka = !kafkas.isEmpty() ? kafkas.get(0) : fallbackKafka;
      if (kafka == null) {
        throw new IllegalStateException("No Kafka bootstrap servers configured for NSP host " + defaultHost);
      }
      out.add(new NspSite("default", defaultHost, kafka));
    }
    return out;
  }

  private static void addUnique(List<NspSite> out, NspSite site) {
    for (NspSite s : out) {
      if (s.hasHost(site.host())) {
        throw new IllegalStateException("NSP host configured twice: " + site.host());
      }
    }
    out.add(site);
  }

  private static void warnIfDefaultHostUnknown(List<NspSite> sites, String defaultHost) {
    if (defaultHost != null && sites.stream().noneMatch(s -> s.hasHost(defaultHost))) {
      log.warn("app.rest.nsp.host={} is not one of the configured failover sites and is ignored. sites={}",
          defaultHost, sites);
    }
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Queries
  // ─────────────────────────────────────────────────────────────────────────

  public boolean isFailoverEnabled() {
    return failoverEnabled;
  }

  public List<NspSite> sites() {
    return sites;
  }

  public NspSite activeSite() {
    return activeSite.get();
  }

  public String activeHost() {
    return activeSite.get().host();
  }

  public String activeKafkaBootstrapServers() {
    return activeSite.get().kafkaBootstrapServers();
  }

  public Optional<NspSite> findByHost(String host) {
    String h = trimToNull(host);
    if (h == null) return Optional.empty();
    return sites.stream().filter(s -> s.hasHost(h)).findFirst();
  }

  public String kafkaBootstrapServersFor(String host) {
    if (trimToNull(host) == null) {
      return null;
    }
    return findByHost(host)
        .map(NspSite::kafkaBootstrapServers)
        .orElseThrow(() -> new IllegalStateException(
            "NSP host " + host.trim() + " is not a configured site. sites=" + sites));
  }

  /** Active site first, then the other sites in configured order. */
  public List<NspSite> sitesInProbeOrder() {
    NspSite current = activeSite.get();
    List<NspSite> ordered = new ArrayList<>();
    ordered.add(current);
    for (NspSite s : sites) {
      if (!s.equals(current)) ordered.add(s);
    }
    return ordered;
  }

  /** Hosts for REST failover attempts: only the active one when failover is disabled. */
  public List<String> orderedHostsForAttempt() {
    if (!failoverEnabled) {
      return List.of(activeHost());
    }
    return sitesInProbeOrder().stream().map(NspSite::host).toList();
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Updates
  // ─────────────────────────────────────────────────────────────────────────

  public void setActiveSite(NspSite site) {
    if (site == null) return;
    NspSite previous = activeSite.getAndSet(site);
    if (!site.equals(previous)) {
      log.warn("NSP active site changed: {} -> {}", previous, site);
    }
  }

  public void forceActiveHost(String host) {
    Optional<NspSite> site = findByHost(host);
    if (site.isEmpty()) {
      if (trimToNull(host) != null) {
        log.warn("Requested NSP active host {} is not a configured site; ignoring. sites={}", host, sites);
      }
      return;
    }
    setActiveSite(site.get());
  }

  public void markSuccess(String host) {
    forceActiveHost(host);
  }

  public void markFailure(String host, Exception e) {
    log.warn("NSP site failed: host={}, error={}", host, e.toString());
  }

  // ─────────────────────────────────────────────────────────────────────────
  // helpers
  // ─────────────────────────────────────────────────────────────────────────

  private static String consumerBootstrapServers(KafkaProperties kp) {
    List<String> servers = kp.getConsumer().getBootstrapServers();
    if (servers == null || servers.isEmpty()) {
      servers = kp.getBootstrapServers();
    }
    return servers == null || servers.isEmpty() ? null : String.join(",", servers);
  }

  private static List<String> cleanList(List<String> in) {
    List<String> out = new ArrayList<>();
    if (in == null) return out;
    for (String s : in) {
      String t = trimToNull(s);
      if (t != null) out.add(t);
    }
    return out;
  }

  private static String trimToNull(String s) {
    if (s == null) return null;
    String t = s.trim();
    return t.isEmpty() ? null : t;
  }
}
