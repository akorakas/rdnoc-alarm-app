package com.example.kafka.correlate;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import com.example.kafka.service.config.CorrelationProperties;

import gr.ote.atlas.events.enums.EMSId;
import gr.ote.atlas.events.enums.EventType;
import gr.ote.atlas.events.models.UnifiedEvent;

@Component
public class RedisAlarmInstanceCorrelator {

  private final StringRedisTemplate redis;
  private final CorrelationProperties props;

  public RedisAlarmInstanceCorrelator(StringRedisTemplate redis, CorrelationProperties props) {
    this.redis = redis;
    this.props = props;
  }

  public void correlate(UnifiedEvent ue) {
    if (ue == null) return;
    if (!props.isEnabled()) return;

    // Allowlist by EMS
    EMSId ems = ue.getSourceEms();
    if (ems == null) return;
    if (!isAllowedEms(ems)) return;

    EventType t = ue.getType();
    if (t != EventType.FAULT && t != EventType.CLEAR) return;

    // Build key from configured fields
    String key = buildKeyFromFields(ue, props.getKeyFields());
    if (key == null) return;

    String redisKey = "alarm:active:" + sha1Hex(key);
    Duration ttl = Duration.ofDays(Math.max(1, props.getTtlDays()));

    if (t == EventType.FAULT) {
      String newUuid = UUID.randomUUID().toString();
      Boolean created = redis.opsForValue().setIfAbsent(redisKey, newUuid, ttl);

      if (Boolean.TRUE.equals(created)) {
        ue.setSerialNo(newUuid);
      } else {
        String existing = redis.opsForValue().get(redisKey);
        if (existing == null || existing.isBlank()) {
          redis.opsForValue().set(redisKey, newUuid, ttl);
          ue.setSerialNo(newUuid);
        } else {
          ue.setSerialNo(existing);
        }
      }
      return;
    }

    // CLEAR
    String existing = getAndDelete(redisKey);
    ue.setSerialNo(existing);
  }

  private boolean isAllowedEms(EMSId ems) {
    List<String> allow = props.getEmsAllowlist();
    if (allow == null || allow.isEmpty()) return false; // safest default
    return allow.stream().anyMatch(s -> s != null && s.equalsIgnoreCase(ems.name()));
  }

  private static String buildKeyFromFields(UnifiedEvent ue, List<String> fields) {
    if (fields == null || fields.isEmpty()) return null;

    StringBuilder sb = new StringBuilder("EMS=").append(ue.getSourceEms().name());
    for (String f : fields) {
      String v = valueOfField(ue, f);
      if (v == null || v.isBlank()) return null; // require all fields
      sb.append('|').append(f).append('=').append(norm(v));
    }
    return sb.toString();
  }

  // Choose whichever UE fields you want to support
  private static String valueOfField(UnifiedEvent ue, String field) {
    if (field == null) return null;
    return switch (field) {
      case "neName" -> ue.getNeName();
      case "neEquipment" -> ue.getNeEquipment();
      case "faultId" -> ue.getFaultId();
      case "alarmIdentifier" -> ue.getAlarmIdentifier();
      case "emsDomain" -> ue.getEmsDomain() != null ? ue.getEmsDomain().name() : null;
      default -> null; // unknown field name -> not supported
    };
  }

  private String getAndDelete(String key) {
    String v = redis.opsForValue().get(key);
    if (v != null) redis.delete(key);
    return v;
  }

  private static String norm(String s) {
    return s.trim().replaceAll("\\s+", " ").toLowerCase();
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