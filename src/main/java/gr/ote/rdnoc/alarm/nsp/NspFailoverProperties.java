package gr.ote.rdnoc.alarm.nsp;

import java.util.ArrayList;
import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * app.rest.nsp.failover.*
 *
 * Preferred site definition (host and Kafka together):
 *
 *   sites:
 *     - name: A
 *       host: 172.17.45.132
 *       kafka-bootstrap-servers: 172.17.45.132:9192
 *     - name: B
 *       host: 172.17.42.132
 *       kafka-bootstrap-servers: 172.17.42.132:9192
 *
 * Legacy form (still supported, paired by position in the order written):
 *
 *   hosts: A,B
 *   kafka-bootstrap-servers: kafkaA,kafkaB
 */
@ConfigurationProperties(prefix = "app.rest.nsp.failover")
public class NspFailoverProperties {

  private boolean enabled = false;

  /** Tie-breaker only: tried first when no site has been selected yet. */
  private String preferredHost;

  private List<SiteConfig> sites = new ArrayList<>();

  /** Legacy parallel lists. */
  private List<String> hosts = new ArrayList<>();
  private List<String> kafkaBootstrapServers = new ArrayList<>();

  /**
   * Optional authenticated GET used as an extra "is this site active?" check,
   * e.g. a lightweight NSP endpoint. Empty = token + Kafka check only.
   */
  private String probePath;

  /** How long startup waits for any site to become active. */
  private long startupWaitTimeoutMs = 180_000L;
  private long startupWaitSleepMs = 5_000L;

  private HealthCheck healthCheck = new HealthCheck();

  public static class SiteConfig {
    private String name;
    private String host;
    private String kafkaBootstrapServers;

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getHost() { return host; }
    public void setHost(String host) { this.host = host; }

    public String getKafkaBootstrapServers() { return kafkaBootstrapServers; }
    public void setKafkaBootstrapServers(String kafkaBootstrapServers) { this.kafkaBootstrapServers = kafkaBootstrapServers; }
  }

  public static class HealthCheck {
    private boolean enabled = true;
    private long intervalMs = 60_000L;
    private long initialDelayMs = 60_000L;

    /** Consecutive failed checks of the active site before switching. */
    private int failureThreshold = 3;

    private long kafkaTimeoutMs = 5_000L;

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }

    public long getIntervalMs() { return intervalMs; }
    public void setIntervalMs(long intervalMs) { this.intervalMs = intervalMs; }

    public long getInitialDelayMs() { return initialDelayMs; }
    public void setInitialDelayMs(long initialDelayMs) { this.initialDelayMs = initialDelayMs; }

    public int getFailureThreshold() { return failureThreshold; }
    public void setFailureThreshold(int failureThreshold) { this.failureThreshold = failureThreshold; }

    public long getKafkaTimeoutMs() { return kafkaTimeoutMs; }
    public void setKafkaTimeoutMs(long kafkaTimeoutMs) { this.kafkaTimeoutMs = kafkaTimeoutMs; }
  }

  public boolean isEnabled() { return enabled; }
  public void setEnabled(boolean enabled) { this.enabled = enabled; }

  public String getPreferredHost() { return preferredHost; }
  public void setPreferredHost(String preferredHost) { this.preferredHost = preferredHost; }

  public List<SiteConfig> getSites() { return sites; }
  public void setSites(List<SiteConfig> sites) { this.sites = sites; }

  public List<String> getHosts() { return hosts; }
  public void setHosts(List<String> hosts) { this.hosts = hosts; }

  public List<String> getKafkaBootstrapServers() { return kafkaBootstrapServers; }
  public void setKafkaBootstrapServers(List<String> kafkaBootstrapServers) { this.kafkaBootstrapServers = kafkaBootstrapServers; }

  public String getProbePath() { return probePath; }
  public void setProbePath(String probePath) { this.probePath = probePath; }

  public long getStartupWaitTimeoutMs() { return startupWaitTimeoutMs; }
  public void setStartupWaitTimeoutMs(long startupWaitTimeoutMs) { this.startupWaitTimeoutMs = startupWaitTimeoutMs; }

  public long getStartupWaitSleepMs() { return startupWaitSleepMs; }
  public void setStartupWaitSleepMs(long startupWaitSleepMs) { this.startupWaitSleepMs = startupWaitSleepMs; }

  public HealthCheck getHealthCheck() { return healthCheck; }
  public void setHealthCheck(HealthCheck healthCheck) { this.healthCheck = healthCheck; }
}
