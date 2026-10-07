package gr.ote.rdnoc.alarm.nsp;

/**
 * One NSP site: the REST host and the Kafka cluster that belongs to it.
 *
 * Host and Kafka are always kept together, so the app can never talk REST
 * to one site while consuming Kafka from the other.
 */
public record NspSite(
    String name,
    String host,
    String kafkaBootstrapServers
) {

  public boolean hasHost(String otherHost) {
    return otherHost != null && host.equalsIgnoreCase(otherHost.trim());
  }

  @Override
  public String toString() {
    return name + "(" + host + ", kafka=" + kafkaBootstrapServers + ")";
  }
}
