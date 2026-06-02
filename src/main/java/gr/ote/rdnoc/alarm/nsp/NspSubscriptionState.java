package gr.ote.rdnoc.alarm.nsp;

public record NspSubscriptionState(String subscriptionId, String topicId, String host) {

  /**
   * Backward-compatible constructor for old saved JSON/state files.
   */
  public NspSubscriptionState(String subscriptionId, String topicId) {
    this(subscriptionId, topicId, null);
  }
}