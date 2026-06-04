package gr.ote.rdnoc.alarm.mv36.model;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

public class Mv36NetworkElement {

  private String sourceIndex;
  private String mv36NeId;
  private String mv36NeName;
  private String mv36NeUniqueName;
  private String mv36NeTypeStr;
  private Instant lastUpdated;

  public String getSourceIndex() {
    return sourceIndex;
  }

  public void setSourceIndex(String sourceIndex) {
    this.sourceIndex = sourceIndex;
  }

  public String getMv36NeId() {
    return mv36NeId;
  }

  public void setMv36NeId(String mv36NeId) {
    this.mv36NeId = mv36NeId;
  }

  public String getMv36NeName() {
    return mv36NeName;
  }

  public void setMv36NeName(String mv36NeName) {
    this.mv36NeName = clean(mv36NeName);
  }

  public String getMv36NeUniqueName() {
    return mv36NeUniqueName;
  }

  public void setMv36NeUniqueName(String mv36NeUniqueName) {
    this.mv36NeUniqueName = clean(mv36NeUniqueName);
  }

  public String getMv36NeTypeStr() {
    return mv36NeTypeStr;
  }

  public void setMv36NeTypeStr(String mv36NeTypeStr) {
    this.mv36NeTypeStr = clean(mv36NeTypeStr);
  }

  public Instant getLastUpdated() {
    return lastUpdated;
  }

  public void setLastUpdated(Instant lastUpdated) {
    this.lastUpdated = lastUpdated;
  }

  public Map<String, String> toFieldMap() {
    Map<String, String> m = new LinkedHashMap<>();

    put(m, "mv36NeId", mv36NeId);
    put(m, "mv36NeName", mv36NeName);
    put(m, "mv36NeUniqueName", mv36NeUniqueName);
    put(m, "mv36NeTypeStr", mv36NeTypeStr);

    return m;
  }

  public boolean hasUsefulData() {
    return notBlank(mv36NeId)
        || notBlank(mv36NeName)
        || notBlank(mv36NeUniqueName)
        || notBlank(mv36NeTypeStr);
  }

  private static void put(Map<String, String> m, String key, String value) {
    if (notBlank(value)) {
      m.put(key, value);
    }
  }

  private static boolean notBlank(String value) {
    return value != null && !value.isBlank();
  }

  private static String clean(String value) {
    if (value == null) {
      return null;
    }

    String s = value.trim();

    if (s.isEmpty() || "--".equals(s)) {
      return null;
    }

    return s;
  }
}