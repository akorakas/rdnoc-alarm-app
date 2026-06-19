package gr.ote.rdnoc.alarm.nsp;

public record NspSubscriptionState(
    String subscriptionId,
    String topicId,
    String host,
    String kafkaBootstrapServers
) {

  /**
   * Backward-compatible constructor for old saved JSON/state files.
   */
  public NspSubscriptionState(String subscriptionId, String topicId) {
    this(subscriptionId, topicId, null, null);
  }

  /**
   * Backward-compatible constructor for old saved JSON/state files.
   */
  public NspSubscriptionState(String subscriptionId, String topicId, String host) {
    this(subscriptionId, topicId, host, null);
  }

  public boolean hasHost() {
    return host != null && !host.isBlank();
  }

  public boolean hasKafkaBootstrapServers() {
    return kafkaBootstrapServers != null && !kafkaBootstrapServers.isBlank();
  }
}