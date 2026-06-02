package gr.ote.rdnoc.alarm.mv36.model;

import java.util.LinkedHashMap;
import java.util.Map;

public class Mv36ActiveAlarm {

  private String sourceIndex;

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

  public String getSourceIndex() {
    return sourceIndex;
  }

  public void setSourceIndex(String sourceIndex) {
    this.sourceIndex = sourceIndex;
  }

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

  public Map<String, String> toFieldMap() {
    Map<String, String> m = new LinkedHashMap<>();

    put(m, "sourceIndex", sourceIndex);
    put(m, "mv36AlarmId", mv36AlarmId);
    put(m, "mv36AlarmSeverity", mv36AlarmSeverity);
    put(m, "mv36AlarmNeId", mv36AlarmNeId);
    put(m, "mv36AlarmEventType", mv36AlarmEventType);
    put(m, "mv36AlarmRaisingTime", mv36AlarmRaisingTime);
    put(m, "mv36AlarmStrNeUniqueName", mv36AlarmStrNeUniqueName);
    put(m, "mv36AlarmStrShelf", mv36AlarmStrShelf);
    put(m, "mv36AlarmStrCard", mv36AlarmStrCard);
    put(m, "mv36AlarmPortId", mv36AlarmPortId);
    put(m, "mv36AlarmStr", mv36AlarmStr);
    put(m, "mv36AlarmStrProbCause", mv36AlarmStrProbCause);
    put(m, "mv36AlarmStrEventType", mv36AlarmStrEventType);

    return m;
  }

  private static void put(Map<String, String> m, String key, String value) {
    if (value != null) {
      m.put(key, value);
    }
  }
}