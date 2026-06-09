package gr.ote.rdnoc.alarm.mv36.cache;

import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.stereotype.Component;

import gr.ote.rdnoc.alarm.mv36.model.Mv36NetworkElement;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class Mv36NeCache {

  private final ConcurrentHashMap<String, Mv36NetworkElement> byNeId = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, Mv36NetworkElement> byUniqueName = new ConcurrentHashMap<>();

  private volatile Instant lastRefreshTime;
  private volatile int lastRefreshSize;

  public Optional<Mv36NetworkElement> findByNeId(String neId) {
    String key = normalizeNeId(neId);
    if (key == null) {
      return Optional.empty();
    }

    return Optional.ofNullable(byNeId.get(key));
  }

  public Optional<Mv36NetworkElement> findByUniqueName(String uniqueName) {
    String key = normalizeUniqueName(uniqueName);
    if (key == null) {
      return Optional.empty();
    }

    return Optional.ofNullable(byUniqueName.get(key));
  }

  public void replaceAll(Collection<Mv36NetworkElement> elements) {
    ConcurrentHashMap<String, Mv36NetworkElement> newByNeId = new ConcurrentHashMap<>();
    ConcurrentHashMap<String, Mv36NetworkElement> newByUniqueName = new ConcurrentHashMap<>();

    if (elements != null) {
      for (Mv36NetworkElement ne : elements) {
        if (ne == null || !ne.hasUsefulData()) {
          continue;
        }

        ne.setLastUpdated(Instant.now());

        String neId = normalizeNeId(ne.getMv36NeId());
        if (neId != null) {
          newByNeId.put(neId, ne);
        }

        String uniqueName = normalizeUniqueName(ne.getMv36NeUniqueName());
        if (uniqueName != null) {
          newByUniqueName.put(uniqueName, ne);
        }
      }
    }

    byNeId.clear();
    byNeId.putAll(newByNeId);

    byUniqueName.clear();
    byUniqueName.putAll(newByUniqueName);

    lastRefreshTime = Instant.now();
    lastRefreshSize = byNeId.size();

    log.info(
        "MV36 NE cache refreshed. sizeByNeId={}, sizeByUniqueName={}, key819Exists={}, key0dot819Exists={}, uniqueNYMAA83Exists={}",
        byNeId.size(),
        byUniqueName.size(),
        byNeId.containsKey("819"),
        byNeId.containsKey("0.819"),
        byUniqueName.containsKey("NYMA-A.83")
    );
  }

  public int size() {
    return byNeId.size();
  }

  public boolean isEmpty() {
    return byNeId.isEmpty();
  }

  public Instant getLastRefreshTime() {
    return lastRefreshTime;
  }

  public int getLastRefreshSize() {
    return lastRefreshSize;
  }

  public Map<String, Mv36NetworkElement> snapshotByNeId() {
    return Collections.unmodifiableMap(byNeId);
  }

  public void clear() {
    byNeId.clear();
    byUniqueName.clear();
    lastRefreshTime = Instant.now();
    lastRefreshSize = 0;

    log.warn("MV36 NE cache cleared");
  }

  private static String normalizeNeId(String value) {
    String s = clean(value);

    if (s == null) {
      return null;
    }

    // SNMP table index may appear as 0.819, while alarm field is 819.
    if (s.startsWith("0.")) {
      s = s.substring(2).trim();
    }

    return s.isEmpty() ? null : s;
  }

  private static String normalizeUniqueName(String value) {
    return clean(value);
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