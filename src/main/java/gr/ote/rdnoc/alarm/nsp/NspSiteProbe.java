package gr.ote.rdnoc.alarm.nsp;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

/**
 * Decides whether an NSP site is ACTIVE.
 *
 * On the standby site the REST API refuses calls and the Kafka broker is down, so a site
 * counts as active only when BOTH answer:
 *   1. REST: token request succeeds (plus optional probe-path GET), and
 *   2. Kafka: the site's cluster answers describeCluster.
 */
@Slf4j
@Component
public class NspSiteProbe {

  private final NspClient nspClient;
  private final NspKafkaAdminClientFactory adminClientFactory;
  private final long kafkaTimeoutMs;

  public NspSiteProbe(
      NspClient nspClient,
      NspKafkaAdminClientFactory adminClientFactory,
      NspFailoverProperties props
  ) {
    this.nspClient = nspClient;
    this.adminClientFactory = adminClientFactory;
    this.kafkaTimeoutMs = props.getHealthCheck().getKafkaTimeoutMs();
  }

  public boolean isActive(NspSite site) {
    if (site == null) return false;

    try {
      nspClient.probeRest(site.host());
    } catch (Exception e) {
      log.warn("NSP site probe: REST not available on {} -> not active. error={}", site, e.toString());
      return false;
    }

    if (!isKafkaReachable(site)) {
      log.warn("NSP site probe: Kafka not reachable on {} -> not active", site);
      return false;
    }

    log.debug("NSP site probe: {} is active", site);
    return true;
  }

  boolean isKafkaReachable(NspSite site) {
    AdminClient admin = null;
    try {
      admin = adminClientFactory.create(site.kafkaBootstrapServers());
      admin.describeCluster().nodes().get(kafkaTimeoutMs, TimeUnit.MILLISECONDS);
      return true;
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      return false;
    } catch (Exception e) {
      log.debug("Kafka describeCluster failed for {}: {}", site, e.toString());
      return false;
    } finally {
      if (admin != null) {
        // Bounded close: a plain close() waits for in-flight calls up to default.api.timeout.ms.
        admin.close(Duration.ofSeconds(1));
      }
    }
  }
}
