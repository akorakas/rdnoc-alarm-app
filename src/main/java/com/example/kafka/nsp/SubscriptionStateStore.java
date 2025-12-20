package com.example.kafka.nsp;

import java.util.Optional;

public interface SubscriptionStateStore {
  Optional<NspSubscriptionState> load();
  void save(NspSubscriptionState state);
  void clear();
}
