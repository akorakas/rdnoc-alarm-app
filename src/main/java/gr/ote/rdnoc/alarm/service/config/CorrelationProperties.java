package gr.ote.rdnoc.alarm.service.config;

import java.util.ArrayList;
import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.correlation")
public class CorrelationProperties {

  private boolean enabled = true;

  // EMSId names, e.g. "INFINERA_TNMS"
  private List<String> emsAllowlist = new ArrayList<>();

  // Legacy (keep for backward compatibility)
  private List<String> keyFields = List.of("neName", "neEquipment", "faultId");

  // New (preferred): fully dynamic key definition
  private List<KeyPart> keyParts = new ArrayList<>();

  // Optional: namespace keys per app/system to avoid collisions
  private String redisPrefix = "alarm";

  private int ttlDays = 7;

  public boolean isEnabled() { return enabled; }
  public void setEnabled(boolean enabled) { this.enabled = enabled; }

  public List<String> getEmsAllowlist() { return emsAllowlist; }
  public void setEmsAllowlist(List<String> emsAllowlist) { this.emsAllowlist = emsAllowlist; }

  public List<String> getKeyFields() { return keyFields; }
  public void setKeyFields(List<String> keyFields) { this.keyFields = keyFields; }

  public List<KeyPart> getKeyParts() { return keyParts; }
  public void setKeyParts(List<KeyPart> keyParts) { this.keyParts = keyParts; }

  public String getRedisPrefix() { return redisPrefix; }
  public void setRedisPrefix(String redisPrefix) { this.redisPrefix = redisPrefix; }

  public int getTtlDays() { return ttlDays; }
  public void setTtlDays(int ttlDays) { this.ttlDays = ttlDays; }

  public static class KeyPart {
    // label only (e.g. "neName", "device", etc.)
    private String name;

    // "ue" or "sourceEvent"
    private String from;

    // for from=ue (e.g. "neName", "faultId")
    private String field;

    // for from=sourceEvent (JSON Pointer, e.g. "/fields/egEventParamsName")
    private String jsonPointer;

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getFrom() { return from; }
    public void setFrom(String from) { this.from = from; }

    public String getField() { return field; }
    public void setField(String field) { this.field = field; }

    public String getJsonPointer() { return jsonPointer; }
    public void setJsonPointer(String jsonPointer) { this.jsonPointer = jsonPointer; }
  }
}