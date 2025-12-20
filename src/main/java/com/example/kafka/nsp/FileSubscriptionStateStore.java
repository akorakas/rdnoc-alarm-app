package com.example.kafka.nsp;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class FileSubscriptionStateStore implements SubscriptionStateStore {

  private final Path file;
  private final ObjectMapper mapper;

  public FileSubscriptionStateStore(
      @Value("${app.nsp.subscription.state-file}") String filePath,
      ObjectMapper mapper
  ) {
    this.file = Path.of(filePath);
    this.mapper = mapper;
  }

  @Override
  public Optional<NspSubscriptionState> load() {
    try {
      if (!Files.exists(file)) return Optional.empty();

      byte[] data = Files.readAllBytes(file);
      if (data.length == 0) return Optional.empty();

      return Optional.of(mapper.readValue(data, NspSubscriptionState.class));
    } catch (Exception e) {
      log.warn("Failed to load subscription state from {}", file, e);
      return Optional.empty();
    }
  }

  @Override
  public void save(NspSubscriptionState state) {
    try {
      Path parent = file.getParent();
      if (parent != null) {
        Files.createDirectories(parent);
      }

      byte[] data = mapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(state);
      Files.write(file, data);

      log.info("Saved subscription state to {} (subscriptionId={}, topicId={})",
          file, state.subscriptionId(), state.topicId());
    } catch (IOException e) {
      throw new RuntimeException("Failed to save subscription state to " + file, e);
    }
  }

  @Override
  public void clear() {
    try {
      if (Files.exists(file)) {
        Files.delete(file);
        log.info("Deleted subscription state file {}", file);
      }
    } catch (IOException e) {
      log.warn("Failed to delete subscription state file {}", file, e);
    }
  }
}
