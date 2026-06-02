package gr.ote.rdnoc.alarm.mv36.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "app.mv36")
public class Mv36SnmpProperties {

  private Sync sync = new Sync();
  private Snmp snmp = new Snmp();

  public Sync getSync() {
    return sync;
  }

  public void setSync(Sync sync) {
    this.sync = sync;
  }

  public Snmp getSnmp() {
    return snmp;
  }

  public void setSnmp(Snmp snmp) {
    this.snmp = snmp;
  }

  public static class Sync {
    private boolean enabled = false;
    private long fixedDelayMs = 7_200_000L;
    private long initialDelayMs = 120_000L;
    private String kafkaListenerId = "alarm-input-listener";

    public boolean isEnabled() {
      return enabled;
    }

    public void setEnabled(boolean enabled) {
      this.enabled = enabled;
    }

    public long getFixedDelayMs() {
      return fixedDelayMs;
    }

    public void setFixedDelayMs(long fixedDelayMs) {
      this.fixedDelayMs = fixedDelayMs;
    }

    public long getInitialDelayMs() {
      return initialDelayMs;
    }

    public void setInitialDelayMs(long initialDelayMs) {
      this.initialDelayMs = initialDelayMs;
    }

    public String getKafkaListenerId() {
      return kafkaListenerId;
    }

    public void setKafkaListenerId(String kafkaListenerId) {
      this.kafkaListenerId = kafkaListenerId;
    }
  }

  public static class Snmp {
    private String host;
    private int port = 161;
    private String community;
    private int timeoutMs = 10_000;
    private int retries = 2;
    private int maxRepetitions = 25;

    private String sourceEms = "MV36_MOBILE";
    private String emsVendorId = "MV_36";
    private String emsDomain = "TRANSPORT";

    private Oids oids = new Oids();

    public String getHost() {
      return host;
    }

    public void setHost(String host) {
      this.host = host;
    }

    public int getPort() {
      return port;
    }

    public void setPort(int port) {
      this.port = port;
    }

    public String getCommunity() {
      return community;
    }

    public void setCommunity(String community) {
      this.community = community;
    }

    public int getTimeoutMs() {
      return timeoutMs;
    }

    public void setTimeoutMs(int timeoutMs) {
      this.timeoutMs = timeoutMs;
    }

    public int getRetries() {
      return retries;
    }

    public void setRetries(int retries) {
      this.retries = retries;
    }

    public int getMaxRepetitions() {
      return maxRepetitions;
    }

    public void setMaxRepetitions(int maxRepetitions) {
      this.maxRepetitions = maxRepetitions;
    }

    public String getSourceEms() {
      return sourceEms;
    }

    public void setSourceEms(String sourceEms) {
      this.sourceEms = sourceEms;
    }

    public String getEmsVendorId() {
      return emsVendorId;
    }

    public void setEmsVendorId(String emsVendorId) {
      this.emsVendorId = emsVendorId;
    }

    public String getEmsDomain() {
      return emsDomain;
    }

    public void setEmsDomain(String emsDomain) {
      this.emsDomain = emsDomain;
    }

    public Oids getOids() {
      return oids;
    }

    public void setOids(Oids oids) {
      this.oids = oids;
    }
  }

  public static class Oids {
    private String mv36AlarmId;
    private String mv36AlarmSeverity;
    private String mv36AlarmNeId;
    private String mv36AlarmEventType;
    private String mv36AlarmRaisingTime;

    private String mv36AlarmStrNeUniqueName;
    private String mv36AlarmStrShelf;
    private String mv36AlarmStrCard;
    private String mv36AlarmPortId;
    private String mv36AlarmStr;
    private String mv36AlarmStrProbCause;
    private String mv36AlarmStrEventType;

    public String getMv36AlarmId() {
      return mv36AlarmId;
    }

    public void setMv36AlarmId(String mv36AlarmId) {
      this.mv36AlarmId = mv36AlarmId;
    }

    public String getMv36AlarmSeverity() {
      return mv36AlarmSeverity;
    }

    public void setMv36AlarmSeverity(String mv36AlarmSeverity) {
      this.mv36AlarmSeverity = mv36AlarmSeverity;
    }

    public String getMv36AlarmNeId() {
      return mv36AlarmNeId;
    }

    public void setMv36AlarmNeId(String mv36AlarmNeId) {
      this.mv36AlarmNeId = mv36AlarmNeId;
    }

    public String getMv36AlarmEventType() {
      return mv36AlarmEventType;
    }

    public void setMv36AlarmEventType(String mv36AlarmEventType) {
      this.mv36AlarmEventType = mv36AlarmEventType;
    }

    public String getMv36AlarmRaisingTime() {
      return mv36AlarmRaisingTime;
    }

    public void setMv36AlarmRaisingTime(String mv36AlarmRaisingTime) {
      this.mv36AlarmRaisingTime = mv36AlarmRaisingTime;
    }

    public String getMv36AlarmStrNeUniqueName() {
      return mv36AlarmStrNeUniqueName;
    }

    public void setMv36AlarmStrNeUniqueName(String mv36AlarmStrNeUniqueName) {
      this.mv36AlarmStrNeUniqueName = mv36AlarmStrNeUniqueName;
    }

    public String getMv36AlarmStrShelf() {
      return mv36AlarmStrShelf;
    }

    public void setMv36AlarmStrShelf(String mv36AlarmStrShelf) {
      this.mv36AlarmStrShelf = mv36AlarmStrShelf;
    }

    public String getMv36AlarmStrCard() {
      return mv36AlarmStrCard;
    }

    public void setMv36AlarmStrCard(String mv36AlarmStrCard) {
      this.mv36AlarmStrCard = mv36AlarmStrCard;
    }

    public String getMv36AlarmPortId() {
      return mv36AlarmPortId;
    }

    public void setMv36AlarmPortId(String mv36AlarmPortId) {
      this.mv36AlarmPortId = mv36AlarmPortId;
    }

    public String getMv36AlarmStr() {
      return mv36AlarmStr;
    }

    public void setMv36AlarmStr(String mv36AlarmStr) {
      this.mv36AlarmStr = mv36AlarmStr;
    }

    public String getMv36AlarmStrProbCause() {
      return mv36AlarmStrProbCause;
    }

    public void setMv36AlarmStrProbCause(String mv36AlarmStrProbCause) {
      this.mv36AlarmStrProbCause = mv36AlarmStrProbCause;
    }

    public String getMv36AlarmStrEventType() {
      return mv36AlarmStrEventType;
    }

    public void setMv36AlarmStrEventType(String mv36AlarmStrEventType) {
      this.mv36AlarmStrEventType = mv36AlarmStrEventType;
    }
  }
}