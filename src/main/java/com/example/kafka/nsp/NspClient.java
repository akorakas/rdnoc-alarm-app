// src/main/java/com/example/kafka/nsp/NspClient.java
package com.example.kafka.nsp;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class NspClient {

  private final ObjectMapper objectMapper;
  private final RestTemplate restTemplate;

  // ───── Values from application.yml ─────
  @Value("${app.rest.nsp.scheme:https}")
  private String scheme;

  @Value("${app.rest.nsp.host}")
  private String host;

  @Value("${app.rest.nsp.paths.token}")
  private String tokenPath;

  @Value("${app.rest.nsp.paths.alarms}")
  private String alarmsPath;

  // NEW: subscriptions base path (used for create + renew)
  @Value("${app.rest.nsp.paths.subscriptions:/nbi-notification/api/v1/notifications/subscriptions}")
  private String subscriptionsPath;

  @Value("${app.rest.nsp.auth.basic}")
  private String basicAuth;

  @Value("${app.rest.nsp.auth.grant-type:client_credentials}")
  private String grantType;

  @Value("${app.rest.nsp.headers.content-type:application/json}")
  private String contentType;

  @Value("${app.rest.nsp.headers.accept:application/json}")
  private String accept;

  // Where the alarms array lives
  @Value("${app.rest.nsp.alarms-array-path:/response/data}")
  private String alarmsArrayPath;

  // Raw alarm filter
  @Value("${app.rest.nsp.alarm-filter:}")
  private String alarmFilter;

  // Subscription request payload parts (optional, but handy)
  @Value("${app.rest.nsp.subscription.category-name:NSP-FAULT}")
  private String subscriptionCategoryName;

  @Value("${app.rest.nsp.subscription.advanced-filter:{\"includeAlarmDetailsOnChangeEvent\":true, \"alarmProperties\": {\"rootCause\": true}}}")
  private String subscriptionAdvancedFilter;

  @Value("${app.rest.nsp.subscription.property-filter:affectedObjectType NOT LIKE 'NmsSystem'}")
  private String subscriptionPropertyFilter;

  // ───────────────────────────────────────

  private String cachedToken;
  private Instant tokenExpiresAt = Instant.EPOCH;

  private String baseUrl() {
    return scheme + "://" + host;
  }

  /**
   * Cache Bearer token until (expires - 60s).
   */
  private synchronized String getAccessToken() throws Exception {
    if (cachedToken != null && Instant.now().isBefore(tokenExpiresAt.minusSeconds(60))) {
      return cachedToken;
    }

    String url = baseUrl() + tokenPath;

    HttpHeaders headers = new HttpHeaders();
    headers.set("Authorization", "Basic " + basicAuth);
    headers.setContentType(MediaType.valueOf(contentType));
    headers.setAccept(List.of(MediaType.valueOf(accept)));

    Map<String, String> body = Map.of("grant_type", grantType);
    HttpEntity<Map<String, String>> request = new HttpEntity<>(body, headers);

    ResponseEntity<String> response = restTemplate.exchange(url, HttpMethod.POST, request, String.class);

    if (!response.getStatusCode().is2xxSuccessful()) {
      throw new IllegalStateException("NSP token request failed: " + response.getStatusCode());
    }

    JsonNode json = objectMapper.readTree(response.getBody());
    String accessToken = json.path("access_token").asText(null);
    int expiresIn = json.path("expires_in").asInt(600);

    if (accessToken == null) {
      throw new IllegalStateException("No access_token in NSP response: " + response.getBody());
    }

    this.cachedToken = accessToken;
    this.tokenExpiresAt = Instant.now().plusSeconds(expiresIn);

    return accessToken;
  }

  // ─────────────────────────────────────────────────────────────────────────
  //  ACTIVE ALARMS (REST snapshot)
  // ─────────────────────────────────────────────────────────────────────────

  public String fetchActiveAlarmsRaw() throws Exception {
    String token = getAccessToken();

    // If no filter configured, call endpoint without alarmFilter
    String url;
    if (alarmFilter == null || alarmFilter.isBlank()) {
      url = baseUrl() + alarmsPath;
    } else {
      // URL-encode once; URLEncoder turns spaces into '+'
      String onceEncoded = URLEncoder.encode(alarmFilter, StandardCharsets.UTF_8);
      // Replace '+' with '%20' for curl-like style
      String encodedForNsp = onceEncoded.replace("+", "%20");
      url = baseUrl() + alarmsPath + "?alarmFilter=" + encodedForNsp;

      log.info("NSP alarms raw filter   : {}", alarmFilter);
      log.info("NSP alarms encoded      : {}", encodedForNsp);
    }

    log.info("NSP alarms request URL  : {}", url);

    HttpHeaders headers = new HttpHeaders();
    headers.setBearerAuth(token);
    headers.setAccept(List.of(MediaType.valueOf(accept)));
    headers.setContentType(MediaType.valueOf(contentType));

    HttpEntity<Void> request = new HttpEntity<>(headers);
    ResponseEntity<String> response = restTemplate.exchange(url, HttpMethod.GET, request, String.class);

    if (!response.getStatusCode().is2xxSuccessful()) {
      throw new IllegalStateException("NSP alarms request failed: " + response.getStatusCode());
    }

    return response.getBody();
  }

  public List<String> fetchActiveAlarmEvents() throws Exception {
    String raw = fetchActiveAlarmsRaw();
    JsonNode root = objectMapper.readTree(raw);
    List<String> result = new ArrayList<>();

    JsonNode arrayNode;
    try {
      JsonPointer ptr = JsonPointer.compile(alarmsArrayPath);
      arrayNode = root.at(ptr);
    } catch (IllegalArgumentException e) {
      log.error("Invalid JSON pointer for alarms-array-path: {}", alarmsArrayPath, e);
      arrayNode = root;
    }

    if (arrayNode != null && arrayNode.isArray()) {
      for (JsonNode node : arrayNode) {
        result.add(objectMapper.writeValueAsString(node));
      }
      log.info("NSP: split {} alarm(s) from {}", result.size(), alarmsArrayPath);
    } else if (root.isArray()) {
      for (JsonNode node : root) {
        result.add(objectMapper.writeValueAsString(node));
      }
      log.warn("NSP: alarms-array-path {} did not resolve to array, used root array instead", alarmsArrayPath);
    } else {
      log.warn("NSP: alarms-array-path {} did not resolve to array; returning single raw payload", alarmsArrayPath);
      result.add(raw);
    }

    return result;
  }

  // ─────────────────────────────────────────────────────────────────────────
  //  SUBSCRIPTION CREATE + RENEW (replaces your shell scripts)
  // ─────────────────────────────────────────────────────────────────────────

  /**
   * Create an NSP notification subscription.
   * Returns (subscriptionId, topicId).
   *
   * Matches your curl body:
   * {
   *   "categories": [{
   *     "advancedFilter": "{...}",
   *     "propertyFilter": "affectedObjectType NOT LIKE 'NmsSystem'",
   *     "name": "NSP-FAULT"
   *   }]
   * }
   */
  public SubscriptionInfo createSubscription() throws Exception {
    String token = getAccessToken();

    String url = baseUrl() + subscriptionsPath;
    log.info("NSP create subscription URL: {}", url);

    HttpHeaders headers = new HttpHeaders();
    headers.setBearerAuth(token);
    headers.setContentType(MediaType.APPLICATION_JSON);
    headers.setAccept(List.of(MediaType.valueOf(accept)));

    // Build JSON string safely (we keep it as String so you can exactly match NSP expectations)
    // advancedFilter must be a JSON string inside JSON => must be quoted/escaped.
    String bodyJson = """
      {
        "categories": [
          {
            "advancedFilter": %s,
            "propertyFilter": %s,
            "name": %s
          }
        ]
      }
      """.formatted(
        objectMapper.writeValueAsString(subscriptionAdvancedFilter),
        objectMapper.writeValueAsString(subscriptionPropertyFilter),
        objectMapper.writeValueAsString(subscriptionCategoryName)
      );

    HttpEntity<String> request = new HttpEntity<>(bodyJson, headers);
    ResponseEntity<String> response = restTemplate.exchange(url, HttpMethod.POST, request, String.class);

    if (!response.getStatusCode().is2xxSuccessful()) {
      throw new IllegalStateException("NSP create subscription failed: " + response.getStatusCode() + " body=" + response.getBody());
    }

    JsonNode root = objectMapper.readTree(response.getBody());
    String subscriptionId = root.at("/response/data/subscriptionId").asText(null);
    String topicId = root.at("/response/data/topicId").asText(null);

    if (subscriptionId == null || subscriptionId.isBlank() || topicId == null || topicId.isBlank()) {
      throw new IllegalStateException("Could not extract subscriptionId/topicId from create response: " + response.getBody());
    }

    log.info("NSP subscription created: subscriptionId={}, topicId={}", subscriptionId, topicId);
    return new SubscriptionInfo(subscriptionId, topicId);
  }

  /**
   * Renew an existing subscription.
   * POST /subscriptions/{subscriptionId}/renewals with form-url-encoded content type (empty body).
   */
  public void renewSubscription(String subscriptionId) throws Exception {
    if (subscriptionId == null || subscriptionId.isBlank()) {
      throw new IllegalArgumentException("subscriptionId is blank");
    }

    String token = getAccessToken();

    String url = baseUrl() + subscriptionsPath + "/" + subscriptionId + "/renewals";
    log.info("NSP renew subscription URL: {}", url);

    HttpHeaders headers = new HttpHeaders();
    headers.setBearerAuth(token);
    headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
    headers.setAccept(List.of(MediaType.valueOf(accept)));

    HttpEntity<String> request = new HttpEntity<>("", headers);
    ResponseEntity<String> response = restTemplate.exchange(url, HttpMethod.POST, request, String.class);

    if (!response.getStatusCode().is2xxSuccessful()) {
      throw new IllegalStateException("NSP renew subscription failed: " + response.getStatusCode() + " body=" + response.getBody());
    }

    log.info("NSP subscription renewed: subscriptionId={}", subscriptionId);
  }

  /**
   * Minimal DTO for createSubscription response.
   */
  public record SubscriptionInfo(String subscriptionId, String topicId) {}
}
