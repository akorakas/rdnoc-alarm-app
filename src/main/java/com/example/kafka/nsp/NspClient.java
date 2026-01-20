// src/main/java/com/example/kafka/nsp/NspClient.java
package com.example.kafka.nsp;

import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
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

  // subscriptions base path (used for create + renew)
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

  // Where the alarms array lives (default works with /response/data)
  @Value("${app.rest.nsp.alarms-array-path:/response/data}")
  private String alarmsArrayPath;

  // Raw alarm filter (read from YAML)
  @Value("${app.rest.nsp.alarm-filter:}")
  private String alarmFilter;

  /**
   * NSP sometimes expects DOUBLE-URL-encoding in alarmFilter:
   * Example: severity%253D'major'
   *
   * In your environment, you verified X1 encoding works, so set:
   *   app.rest.nsp.alarm-filter-double-encode=false
   */
  @Value("${app.rest.nsp.alarm-filter-double-encode:true}")
  private boolean alarmFilterDoubleEncode;

  // Subscription request payload parts
  @Value("${app.rest.nsp.subscription.category-name:NSP-FAULT}")
  private String subscriptionCategoryName;

  @Value("${app.rest.nsp.subscription.advanced-filter:{\"includeAlarmDetailsOnChangeEvent\":true, \"alarmProperties\": {\"rootCause\": true}}}")
  private String subscriptionAdvancedFilter;

  @Value("${app.rest.nsp.subscription.property-filter:affectedObjectType NOT LIKE 'NmsSystem'}")
  private String subscriptionPropertyFilter;

  // ─────────────────────────────────────────────────────────────────────────
  // Pagination
  // ─────────────────────────────────────────────────────────────────────────

  /**
   * Turn pagination ON/OFF for snapshot sync:
   * app.rest.nsp.pagination.enabled=true
   */
  @Value("${app.rest.nsp.pagination.enabled:false}")
  private boolean paginationEnabled;

  /**
   * Page size
   * app.rest.nsp.pagination.limit=1000
   */
  @Value("${app.rest.nsp.pagination.limit:1000}")
  private int paginationLimit;

  /**
   * Safety guard: prevents infinite loop if server behaves strangely
   * app.rest.nsp.pagination.max-pages=200
   */
  @Value("${app.rest.nsp.pagination.max-pages:200}")
  private int paginationMaxPages;

  /**
   * Optional: sort param(s)
   * Example:
   *   app.rest.nsp.pagination.sort=lastTimeDetected,desc
   *
   * Or multiple sorts separated by ';':
   *   app.rest.nsp.pagination.sort=productType,asc;version,desc
   */
  @Value("${app.rest.nsp.pagination.sort:lastTimeDetected,desc}")
  private String paginationSort;

  /**
   * Pagination mode:
   *
   * OFFSET_LIMIT:
   *   ?offset=0&limit=1000
   *
   * START_END:
   *   ?startRow=0&endRow=1000
   *
   * IMPORTANT:
   * Your logs showed NSP ignores offset (returns startRow=0 again),
   * so use START_END mode for this endpoint/build:
   *
   * app.rest.nsp.pagination.mode=START_END
   */
  public enum PaginationMode {
    OFFSET_LIMIT,
    START_END
  }

  @Value("${app.rest.nsp.pagination.mode:OFFSET_LIMIT}")
  private PaginationMode paginationMode;

  // ───────────────────────────────────────

  private String cachedToken;
  private Instant tokenExpiresAt = Instant.EPOCH;

  private String baseUrl() {
    return scheme + "://" + host;
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Encoding helpers
  // ─────────────────────────────────────────────────────────────────────────

  private static String urlEncodeOnce(String s) {
    return URLEncoder.encode(s, StandardCharsets.UTF_8).replace("+", "%20");
  }

  private static String urlEncodeTwice(String s) {
    String once = urlEncodeOnce(s);
    return urlEncodeOnce(once);
  }

  private String encodeAlarmFilter(String rawFilter) {
    if (rawFilter == null || rawFilter.isBlank()) {
      return "";
    }
    return alarmFilterDoubleEncode ? urlEncodeTwice(rawFilter) : urlEncodeOnce(rawFilter);
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

    ResponseEntity<String> response = restTemplate.exchange(
        URI.create(url),
        HttpMethod.POST,
        request,
        String.class
    );

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
  // ACTIVE ALARMS (REST snapshot) + Pagination
  // ─────────────────────────────────────────────────────────────────────────

  /**
   * Backwards-compatible method:
   * - If pagination is enabled -> loops through pages using offset/limit (or startRow/endRow)
   * - Else -> performs single call (old behavior)
   */
  public List<String> fetchActiveAlarmEvents() throws Exception {
    if (!paginationEnabled) {
      String raw = fetchActiveAlarmsRaw();
      return splitAlarmEventsFromRaw(raw);
    }

    final int limit = Math.max(1, paginationLimit);

    int offset = 0;
    int page = 0;

    int lastEndRow = -1;
    List<String> all = new ArrayList<>();

    while (true) {
      page++;
      if (page > paginationMaxPages) {
        log.warn("NSP pagination safety stop: reached max-pages={} at offset={}", paginationMaxPages, offset);
        break;
      }

      AlarmPage alarmPage = fetchActiveAlarmPage(offset, limit);

      log.info("NSP pagination page meta: startRow={}, endRow={}, totalRows={}, count={}",
          alarmPage.startRow(), alarmPage.endRow(), alarmPage.totalRows(), alarmPage.events().size());

      if (alarmPage.events().isEmpty()) {
        log.info("NSP pagination finished: empty page at offset={}, limit={}", offset, limit);
        break;
      }

      all.addAll(alarmPage.events());

      // Stop if we reached end
      if (alarmPage.totalRows() >= 0 && alarmPage.endRow() >= alarmPage.totalRows()) {
        log.info("NSP pagination finished: endRow >= totalRows ({} >= {})",
            alarmPage.endRow(), alarmPage.totalRows());
        break;
      }

      // Safety: ensure forward progress
      if (alarmPage.endRow() <= lastEndRow) {
        log.warn("NSP pagination safety stop: endRow did not increase (prevEndRow={}, endRow={}). offset={}",
            lastEndRow, alarmPage.endRow(), offset);
        break;
      }

      lastEndRow = alarmPage.endRow();

      // Next offset must be endRow
      offset = alarmPage.endRow();
    }

    log.info("NSP pagination finished: total events={}", all.size());
    return all;
  }

  /**
   * Old single-call raw fetch (no pagination).
   */
  public String fetchActiveAlarmsRaw() throws Exception {
    String token = getAccessToken();

    String url = buildAlarmsUrl(null, null);
    log.info("NSP alarms request URL: {}", url);

    HttpHeaders headers = new HttpHeaders();
    headers.setBearerAuth(token);
    headers.setAccept(List.of(MediaType.valueOf(accept)));
    headers.setContentType(MediaType.valueOf(contentType));

    HttpEntity<Void> request = new HttpEntity<>(headers);
    ResponseEntity<String> response = restTemplate.exchange(
        URI.create(url),
        HttpMethod.GET,
        request,
        String.class
    );

    if (!response.getStatusCode().is2xxSuccessful()) {
      throw new IllegalStateException("NSP alarms request failed: " + response.getStatusCode());
    }

    return response.getBody();
  }

  /**
   * Paged raw fetch (+ optional sort).
   */
  public String fetchActiveAlarmsRawPage(int offset, int limit) throws Exception {
    String token = getAccessToken();

    String url = buildAlarmsUrl(offset, limit);
    log.info("NSP alarms page request URL: {}", url);

    HttpHeaders headers = new HttpHeaders();
    headers.setBearerAuth(token);
    headers.setAccept(List.of(MediaType.valueOf(accept)));
    headers.setContentType(MediaType.valueOf(contentType));

    HttpEntity<Void> request = new HttpEntity<>(headers);
    ResponseEntity<String> response = restTemplate.exchange(
        URI.create(url),
        HttpMethod.GET,
        request,
        String.class
    );

    if (!response.getStatusCode().is2xxSuccessful()) {
      throw new IllegalStateException("NSP alarms page request failed: " + response.getStatusCode());
    }

    return response.getBody();
  }

  /**
   * Fetch one page and return AlarmPage metadata + alarm events list.
   */
  public AlarmPage fetchActiveAlarmPage(int offset, int limit) throws Exception {
    String raw = fetchActiveAlarmsRawPage(offset, limit);

    JsonNode root = objectMapper.readTree(raw);
    JsonNode resp = root.path("response");

    int startRow = resp.path("startRow").asInt(offset);
    int endRow = resp.path("endRow").asInt(offset);
    int totalRows = resp.path("totalRows").asInt(-1);

    // Detect ignored paging (your exact symptom)
    if (offset > 0 && startRow == 0) {
      log.warn("NSP paging may be ignored: requested offset={}, but response startRow={}. " +
               "Try app.rest.nsp.pagination.mode=START_END for this endpoint/build.",
          offset, startRow);
    }

    List<String> events = splitAlarmEventsFromRaw(raw);

    return new AlarmPage(startRow, endRow, totalRows, events);
  }

  /**
   * Extracts alarms array from raw JSON using alarmsArrayPath.
   */
  private List<String> splitAlarmEventsFromRaw(String raw) throws Exception {
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
      return result;
    }

    if (root.isArray()) {
      for (JsonNode node : root) {
        result.add(objectMapper.writeValueAsString(node));
      }
      log.warn("NSP: alarms-array-path {} did not resolve to array, used root array instead", alarmsArrayPath);
      return result;
    }

    log.warn("NSP: alarms-array-path {} did not resolve to array; returning single raw payload", alarmsArrayPath);
    return Collections.singletonList(raw);
  }

  /**
   * Builds /alarms/details URL with optional:
   * - alarmFilter
   * - pagination
   * - sort (one or more)
   */
  private String buildAlarmsUrl(Integer offset, Integer limit) {
    StringBuilder sb = new StringBuilder();
    sb.append(baseUrl()).append(alarmsPath);

    boolean hasQuery = false;

    // alarmFilter
    if (alarmFilter != null && !alarmFilter.isBlank()) {
      String encoded = encodeAlarmFilter(alarmFilter);

      sb.append(hasQuery ? "&" : "?");
      sb.append("alarmFilter=").append(encoded);
      hasQuery = true;

      log.info("NSP alarms raw filter : {}", alarmFilter);
      log.info("NSP alarms encoded{}  : {}",
          alarmFilterDoubleEncode ? " x2" : " x1",
          encoded
      );
    }

    // paging params
    if (offset != null && limit != null) {
      sb.append(hasQuery ? "&" : "?");

      if (paginationMode == PaginationMode.START_END) {
        int startRow = offset;
        int endRow = offset + limit;
        sb.append("startRow=").append(startRow);
        sb.append("&endRow=").append(endRow);
      } else {
        sb.append("offset=").append(offset);
        sb.append("&limit=").append(limit);
      }

      hasQuery = true;

      // sort (optional)
      appendSortParams(sb);
    }

    return sb.toString();
  }

  /**
   * Appends one or more &sort= parameters.
   *
   * Accepts:
   * - "lastTimeDetected,desc"
   * - "productType,asc;version,desc"
   */
  private void appendSortParams(StringBuilder sb) {
    if (paginationSort == null || paginationSort.isBlank()) return;

    String[] parts = paginationSort.split(";");
    for (String p : parts) {
      String s = p.trim();
      if (s.isEmpty()) continue;

      String encoded = urlEncodeOnce(s);
      sb.append("&sort=").append(encoded);
    }
  }

  // ─────────────────────────────────────────────────────────────────────────
  // SUBSCRIPTION CREATE + RENEW
  // ─────────────────────────────────────────────────────────────────────────

  /**
   * Create an NSP notification subscription.
   * Returns (subscriptionId, topicId).
   */
  public SubscriptionInfo createSubscription() throws Exception {
    String token = getAccessToken();

    String url = baseUrl() + subscriptionsPath;
    log.info("NSP create subscription URL: {}", url);

    HttpHeaders headers = new HttpHeaders();
    headers.setBearerAuth(token);
    headers.setContentType(MediaType.APPLICATION_JSON);
    headers.setAccept(List.of(MediaType.valueOf(accept)));

    // advancedFilter must be a JSON string inside JSON => must be quoted/escaped
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
    ResponseEntity<String> response = restTemplate.exchange(
        URI.create(url),
        HttpMethod.POST,
        request,
        String.class
    );

    if (!response.getStatusCode().is2xxSuccessful()) {
      throw new IllegalStateException(
          "NSP create subscription failed: " + response.getStatusCode() + " body=" + response.getBody()
      );
    }

    JsonNode root = objectMapper.readTree(response.getBody());
    String subscriptionId = root.at("/response/data/subscriptionId").asText(null);
    String topicId = root.at("/response/data/topicId").asText(null);

    if (subscriptionId == null || subscriptionId.isBlank() || topicId == null || topicId.isBlank()) {
      throw new IllegalStateException(
          "Could not extract subscriptionId/topicId from create response: " + response.getBody()
      );
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
    ResponseEntity<String> response = restTemplate.exchange(
        URI.create(url),
        HttpMethod.POST,
        request,
        String.class
    );

    if (!response.getStatusCode().is2xxSuccessful()) {
      throw new IllegalStateException(
          "NSP renew subscription failed: " + response.getStatusCode() + " body=" + response.getBody()
      );
    }

    log.info("NSP subscription renewed: subscriptionId={}", subscriptionId);
  }

  // ─────────────────────────────────────────────────────────────────────────
  // DTOs
  // ─────────────────────────────────────────────────────────────────────────

  public record SubscriptionInfo(String subscriptionId, String topicId) {}

  public record AlarmPage(int startRow, int endRow, int totalRows, List<String> events) {}
}
