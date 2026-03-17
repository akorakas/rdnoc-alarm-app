package com.example.kafka.correlate;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import com.example.kafka.service.config.CorrelationProperties;
import com.example.kafka.service.config.CorrelationProperties.KeyPart;
import com.fasterxml.jackson.databind.JsonNode;

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

  public void correlate(UnifiedEvent ue, JsonNode sourceEventNode) {
    if (ue == null) return;
    if (!props.isEnabled()) return;

    EMSId ems = ue.getSourceEms();
    if (ems == null) return;
    if (!isAllowedEms(ems)) return;

    EventType t = ue.getType();
    if (t != EventType.FAULT && t != EventType.CLEAR) return;

    String key = buildKey(ue, sourceEventNode, props.getKeyParts(), props.getKeyFields());
    if (key == null) return;

    String prefix = (props.getRedisPrefix() == null || props.getRedisPrefix().isBlank())
        ? "alarm"
        : props.getRedisPrefix().trim();

    String redisKey = prefix + ":active:" + sha1Hex(key);
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
    if (allow == null || allow.isEmpty()) return false;
    return allow.stream().anyMatch(s -> s != null && s.equalsIgnoreCase(ems.name()));
  }

  private static String buildKey(UnifiedEvent ue,
                                JsonNode sourceEventNode,
                                List<KeyPart> keyParts,
                                List<String> legacyKeyFields) {

    StringBuilder sb = new StringBuilder("EMS=").append(ue.getSourceEms().name());

    // Preferred: YAML keyParts (fully dynamic)
    if (keyParts != null && !keyParts.isEmpty()) {
      for (KeyPart p : keyParts) {
        String name = (p.getName() == null || p.getName().isBlank()) ? "part" : p.getName().trim();
        String v = resolvePartValue(ue, sourceEventNode, p);
        if (v == null || v.isBlank()) return null;
        sb.append('|').append(name).append('=').append(norm(v));
      }
      return sb.toString();
    }

    // Back-compat: old keyFields (UE-only)
    if (legacyKeyFields == null || legacyKeyFields.isEmpty()) return null;
    for (String f : legacyKeyFields) {
      String v = valueOfUeField(ue, f);
      if (v == null || v.isBlank()) return null;
      sb.append('|').append(f).append('=').append(norm(v));
    }
    return sb.toString();
  }

  private static String resolvePartValue(UnifiedEvent ue, JsonNode sourceEventNode, KeyPart p) {
    if (p == null) return null;

    String from = p.getFrom();
    if (from == null) return null;

    if ("ue".equalsIgnoreCase(from)) {
      return valueOfUeField(ue, p.getField());
    }

    if ("sourceEvent".equalsIgnoreCase(from)) {
      if (sourceEventNode == null) return null;
      String ptr = p.getJsonPointer();
      if (ptr == null || ptr.isBlank()) return null;

      JsonNode n = sourceEventNode.at(ptr.trim());
      if (n == null || n.isMissingNode() || n.isNull()) return null;
      String v = n.asText();
      return (v == null || v.isBlank()) ? null : v;
    }

    return null;
  }

  // UE fields support (can stay as-is)
  private static String valueOfUeField(UnifiedEvent ue, String field) {
    if (field == null) return null;
    return switch (field) {
      case "neName" -> ue.getNeName();
      case "neEquipment" -> ue.getNeEquipment();
      case "faultId" -> ue.getFaultId();
      case "alarmIdentifier" -> ue.getAlarmIdentifier();
      case "emsDomain" -> ue.getEmsDomain() != null ? ue.getEmsDomain().name() : null;
      default -> null;
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