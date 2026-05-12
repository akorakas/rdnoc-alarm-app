package gr.ote.rdnoc.alarm.correlate;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.UUID;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import gr.ote.atlas.events.enums.EMSId;
import gr.ote.atlas.events.enums.EventType;
import gr.ote.atlas.events.models.UnifiedEvent;

@Component
public class TnmsRedisAlarmInstanceCorrelator {

  private final StringRedisTemplate redis;

  // Safety TTL for “active alarm” entry if CLEAR never arrives
  private final Duration ttl = Duration.ofDays(7);

  public TnmsRedisAlarmInstanceCorrelator(StringRedisTemplate redis) {
    this.redis = redis;
  }

  public void correlate(UnifiedEvent ue) {
    if (ue == null) return;
    if (ue.getSourceEms() != EMSId.INFINERA_TNMS) return;

    EventType t = ue.getType();
    if (t != EventType.FAULT && t != EventType.CLEAR) return;

    String neName = ue.getNeName();
    String neEquipment = ue.getNeEquipment();
    String faultId = ue.getFaultId();

    if (isBlank(neName) || isBlank(neEquipment) || isBlank(faultId)) return;

    String alarmKey = buildAlarmKey(neName, neEquipment, faultId);
    String redisKey = "tnms:active:" + sha1Hex(alarmKey);

    if (t == EventType.FAULT) {
      // Create instance uuid if not exists, else reuse existing.
      String newUuid = UUID.randomUUID().toString();
      Boolean created = redis.opsForValue().setIfAbsent(redisKey, newUuid, ttl);

      if (Boolean.TRUE.equals(created)) {
        ue.setSerialNo(newUuid);
      } else {
        String existing = redis.opsForValue().get(redisKey);
        if (existing == null || existing.isBlank()) {
          // self-heal
          redis.opsForValue().set(redisKey, newUuid, ttl);
          ue.setSerialNo(newUuid);
        } else {
          ue.setSerialNo(existing);
        }
      }
      return;
    }

    // CLEAR: get then delete (non-atomic, but usually fine)
    String existing = getAndDelete(redisKey);
    ue.setSerialNo(existing); // may be null for orphan CLEAR
  }

  private String getAndDelete(String key) {
    String v = redis.opsForValue().get(key);
    if (v != null) redis.delete(key);
    return v;
  }

  private static String buildAlarmKey(String neName, String neEquipment, String faultId) {
    return "TNMS|" +
        "neName=" + norm(neName) + "|" +
        "neEquipment=" + norm(neEquipment) + "|" +
        "faultId=" + norm(faultId);
  }

  private static String norm(String s) {
    return s.trim().replaceAll("\\s+", " ").toLowerCase();
  }

  private static boolean isBlank(String s) {
    return s == null || s.trim().isEmpty();
  }

  private static String sha1Hex(String input) {
    try {
      MessageDigest md = MessageDigest.getInstance("SHA-1");
      byte[] digest = md.digest(input.getBytes(StandardCharsets.UTF_8));
      StringBuilder sb = new StringBuilder(digest.length * 2);
      for (byte b : digest) sb.append(String.format("%02x", b));
      return sb.toString();
    } catch (Exception e) {
      return Integer.toHexString(input.hashCode());
    }
  }
}