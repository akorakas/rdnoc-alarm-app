package com.example.kafka.service.config;

import java.util.ArrayList;
import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.correlation")
public class CorrelationProperties {

  private boolean enabled = true;

  // EMSId names, e.g. "INFINERA_TNMS"
  private List<String> emsAllowlist = new ArrayList<>();

  // UnifiedEvent field names: neName, neEquipment, faultId, alarmIdentifier, etc.
  private List<String> keyFields = List.of("neName", "neEquipment", "faultId");

  private int ttlDays = 7;

  public boolean isEnabled() { return enabled; }
  public void setEnabled(boolean enabled) { this.enabled = enabled; }

  public List<String> getEmsAllowlist() { return emsAllowlist; }
  public void setEmsAllowlist(List<String> emsAllowlist) { this.emsAllowlist = emsAllowlist; }

  public List<String> getKeyFields() { return keyFields; }
  public void setKeyFields(List<String> keyFields) { this.keyFields = keyFields; }

  public int getTtlDays() { return ttlDays; }
  public void setTtlDays(int ttlDays) { this.ttlDays = ttlDays; }
}