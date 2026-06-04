package gr.ote.rdnoc.alarm.mv36.enrich;

import java.util.Map;
import java.util.Optional;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import gr.ote.rdnoc.alarm.mv36.cache.Mv36NeCache;
import gr.ote.rdnoc.alarm.mv36.model.Mv36ActiveAlarm;
import gr.ote.rdnoc.alarm.mv36.model.Mv36NetworkElement;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "app.mv36.enrichment", name = "enabled", havingValue = "true")
public class Mv36NeEnrichmentService {

  private final Mv36NeCache cache;

  public Optional<Mv36NetworkElement> findByAlarmNeId(String mv36AlarmNeId) {
    return cache.findByNeId(mv36AlarmNeId);
  }

  public Optional<Mv36NetworkElement> findByUniqueName(String mv36NeUniqueName) {
    return cache.findByUniqueName(mv36NeUniqueName);
  }

  public Optional<Mv36NetworkElement> findForAlarm(Mv36ActiveAlarm alarm) {
    if (alarm == null) {
      return Optional.empty();
    }

    Optional<Mv36NetworkElement> byId = findByAlarmNeId(alarm.getMv36AlarmNeId());
    if (byId.isPresent()) {
      return byId;
    }

    return findByUniqueName(alarm.getMv36AlarmStrNeUniqueName());
  }

  public String resolveNeName(String mv36AlarmNeId, String fallbackUniqueName) {
    return findByAlarmNeId(mv36AlarmNeId)
        .map(Mv36NetworkElement::getMv36NeName)
        .filter(this::notBlank)
        .orElseGet(() -> firstNonBlank(fallbackUniqueName, mv36AlarmNeId, ""));
  }

  public String resolveNeName(Mv36ActiveAlarm alarm) {
    if (alarm == null) {
      return "";
    }

    return findForAlarm(alarm)
        .map(Mv36NetworkElement::getMv36NeName)
        .filter(this::notBlank)
        .orElseGet(() -> firstNonBlank(
            alarm.getMv36AlarmStrNeUniqueName(),
            alarm.getMv36AlarmNeId(),
            ""
        ));
  }

  public String resolveNeTypeStr(String mv36AlarmNeId) {
    return findByAlarmNeId(mv36AlarmNeId)
        .map(Mv36NetworkElement::getMv36NeTypeStr)
        .filter(this::notBlank)
        .orElse("");
  }

  /**
   * Adds mv36Ne* fields into a TelegrafGenericEvent fields map.
   *
   * Expected existing key:
   *   mv36AlarmNeId
   *
   * Optional fallback key:
   *   mv36AlarmStrNeUniqueName
   */
  public void enrichFields(Map<String, String> fields) {
    if (fields == null) {
      return;
    }

    String alarmNeId = firstNonBlank(
        fields.get("mv36AlarmNeId"),
        fields.get("mv36NeId")
    );

    String fallbackUniqueName = fields.get("mv36AlarmStrNeUniqueName");

    Optional<Mv36NetworkElement> neOpt = findNe(alarmNeId, fallbackUniqueName);

    if (neOpt.isEmpty()) {
      return;
    }

    Mv36NetworkElement ne = neOpt.get();

    putStringIfNotBlank(fields, "mv36NeId", ne.getMv36NeId());
    putStringIfNotBlank(fields, "mv36NeName", ne.getMv36NeName());
    putStringIfNotBlank(fields, "mv36NeUniqueName", ne.getMv36NeUniqueName());
    putStringIfNotBlank(fields, "mv36NeTypeStr", ne.getMv36NeTypeStr());
  }

  /**
   * Adds mv36Ne* fields into a generic Object map.
   * Useful if your TelegrafGenericEvent fields are Map<String, Object>.
   */
  public void enrichObjectFields(Map<String, Object> fields) {
    if (fields == null) {
      return;
    }

    String alarmNeId = firstNonBlank(
        asString(fields.get("mv36AlarmNeId")),
        asString(fields.get("mv36NeId"))
    );

    String fallbackUniqueName = asString(fields.get("mv36AlarmStrNeUniqueName"));

    Optional<Mv36NetworkElement> neOpt = findNe(alarmNeId, fallbackUniqueName);

    if (neOpt.isEmpty()) {
      return;
    }

    Mv36NetworkElement ne = neOpt.get();

    putObjectIfNotBlank(fields, "mv36NeId", ne.getMv36NeId());
    putObjectIfNotBlank(fields, "mv36NeName", ne.getMv36NeName());
    putObjectIfNotBlank(fields, "mv36NeUniqueName", ne.getMv36NeUniqueName());
    putObjectIfNotBlank(fields, "mv36NeTypeStr", ne.getMv36NeTypeStr());
  }

  private Optional<Mv36NetworkElement> findNe(String alarmNeId, String fallbackUniqueName) {
    Optional<Mv36NetworkElement> neOpt = Optional.empty();

    if (notBlank(alarmNeId)) {
      neOpt = findByAlarmNeId(alarmNeId);
    }

    if (neOpt.isEmpty() && notBlank(fallbackUniqueName)) {
      neOpt = findByUniqueName(fallbackUniqueName);
    }

    return neOpt;
  }

  public int cacheSize() {
    return cache.size();
  }

  private void putStringIfNotBlank(Map<String, String> fields, String key, String value) {
    if (notBlank(value)) {
      fields.put(key, value);
    }
  }

  private void putObjectIfNotBlank(Map<String, Object> fields, String key, String value) {
    if (notBlank(value)) {
      fields.put(key, value);
    }
  }

  private boolean notBlank(String value) {
    return value != null && !value.isBlank();
  }

  private String firstNonBlank(String... values) {
    if (values == null) {
      return "";
    }

    for (String value : values) {
      if (notBlank(value)) {
        return value.trim();
      }
    }

    return "";
  }

  private String asString(Object value) {
    if (value == null) {
      return null;
    }

    String s = String.valueOf(value).trim();

    if (s.isEmpty() || "--".equals(s)) {
      return null;
    }

    return s;
  }
}