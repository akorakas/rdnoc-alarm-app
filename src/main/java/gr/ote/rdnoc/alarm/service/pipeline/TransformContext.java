package gr.ote.rdnoc.alarm.service.pipeline;

import java.util.HashMap;
import java.util.Map;

import tools.jackson.databind.JsonNode;

public final class TransformContext {
  public final JsonNode root;
  public final Map<String, Object> vars = new HashMap<>();
  public String rendered; // final output if a template step writes to "$"

  public TransformContext(JsonNode root) { this.root = root; }
  @SuppressWarnings("unchecked") public <T> T get(String key) { return (T) vars.get(key); }
  public void put(String key, Object val) { vars.put(key, val); }
}
