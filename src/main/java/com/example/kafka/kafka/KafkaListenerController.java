// src/main/java/com/example/kafka/kafka/KafkaListenerController.java
package com.example.kafka.kafka;

import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class KafkaListenerController {

  private final KafkaListenerEndpointRegistry registry;

  // ─────────────────────────────────────────────────────────────
  // Helpers
  // ─────────────────────────────────────────────────────────────
  private MessageListenerContainer container(String listenerId) {
    return registry.getListenerContainer(listenerId);
  }

  // ─────────────────────────────────────────────────────────────
  // Existing no-arg methods (backward compatible)
  // ─────────────────────────────────────────────────────────────
  public boolean isRunning() {
    return isRunning(InputListener.LISTENER_ID);
  }

  public void start() {
    start(InputListener.LISTENER_ID);
  }

  public void pause() {
    pause(InputListener.LISTENER_ID);
  }

  public void resume() {
    resume(InputListener.LISTENER_ID);
  }

  // ─────────────────────────────────────────────────────────────
  // New id-based methods (what SyncCoordinator expects)
  // ─────────────────────────────────────────────────────────────
  public boolean isRunning(String listenerId) {
    var c = container(listenerId);
    return c != null && c.isRunning();
  }

  public void start(String listenerId) {
    var c = container(listenerId);
    if (c != null && !c.isRunning()) c.start();
  }

  public void pause(String listenerId) {
    var c = container(listenerId);
    if (c != null && c.isRunning()) c.pause();
  }

  public void resume(String listenerId) {
    var c = container(listenerId);
    if (c != null && c.isRunning()) c.resume();
  }
}
