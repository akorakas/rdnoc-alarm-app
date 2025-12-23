package com.example.kafka.service.pipeline.steps;

import com.example.kafka.service.pipeline.TransformContext;
import com.example.kafka.service.pipeline.TransformStep;

public class NeNameEnrichmentStep implements TransformStep {

  // which ctx key to read (default "neName")
  private final String sourceKey;
  // which ctx key to write (default "enrichedDataJson")
  private final String targetKey;

  public NeNameEnrichmentStep() {
    this("neName", "enrichedDataJson");
  }

  public NeNameEnrichmentStep(String sourceKey, String targetKey) {
    this.sourceKey = sourceKey;
    this.targetKey = targetKey;
  }

  @Override
  public void apply(TransformContext ctx) {
    Object v = ctx.get(sourceKey);
    if (!(v instanceof String neName) || neName == null || neName.isBlank()) {
      ctx.put(targetKey, "null");
      return;
    }

    String subnetworkName = firstToken(neName, '_');

    String lastUnderscoreToken = lastToken(neName, '_');          // e.g. 9536-763
    String affectedLocationName = firstToken(lastUnderscoreToken, '-'); // e.g. 9536

    // also expose plain string vars if you want them in template
    ctx.put("subnetworkName", subnetworkName);
    ctx.put("affectedLocationName", affectedLocationName);

    // Build RAW JSON so the template can inject it without quotes
    String json =
        "{"
      + "\"affectedLocation\":{\"name\":\"" + esc(affectedLocationName) + "\"},"
      + "\"transport\":{\"subnetworkName\":\"" + esc(subnetworkName) + "\"}"
      + "}";

    ctx.put(targetKey, json);
  }

  private static String firstToken(String s, char delim) {
    int p = s.indexOf(delim);
    return (p < 0) ? s : s.substring(0, p);
  }

  private static String lastToken(String s, char delim) {
    int p = s.lastIndexOf(delim);
    return (p < 0) ? s : s.substring(p + 1);
  }

  // minimal JSON string escape
  private static String esc(String s) {
    if (s == null) return "";
    return s.replace("\\", "\\\\").replace("\"", "\\\"");
  }
}
