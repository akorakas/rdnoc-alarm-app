// src/main/java/gr/ote/rdnoc/alarm/nsp/NspClient.java
package gr.ote.rdnoc.alarm.nsp;

import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
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
  private final NspSiteSelector siteSelector;

  // ───── Values from application.yml ─────
  @Value("${app.rest.nsp.scheme:https}")
  private String scheme;

  /**
   * Kept for backward compatibility.
   * Actual active host is managed by NspSiteSelector.
   */
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

  // Optional sort (one or more)
  @Value("${app.rest.nsp.sort:lastTimeDetected,desc}")
  private String sortParam;

  // ─────────────────────────────────────────────────────────────────────────
  // Cursor-pagination
  // ─────────────────────────────────────────────────────────────────────────

  @Value("${app.rest.nsp.cursor-pagination.enabled:true}")
  private boolean cursorPaginationEnabled;

  @Value("${app.rest.nsp.cursor-pagination.max-pages:50}")
  private int cursorMaxPages;

  @Value("${app.rest.nsp.cursor-pagination.max-total:200000}")
  private int cursorMaxTotal;

  @Value("${app.rest.nsp.cursor-pagination.field:lastTimeDetected}")
  private String cursorField;

  @Value("${app.rest.nsp.cursor-pagination.dedupe:true}")
  private boolean cursorDedupeEnabled;

  // Subscription request payload parts
  @Value("${app.rest.nsp.subscription.category-name:NSP-FAULT}")
  private String subscriptionCategoryName;

  @Value("${app.rest.nsp.subscription.advanced-filter:{\"includeAlarmDetailsOnChangeEvent\":true, \"alarmProperties\": {\"rootCause\": true}}}")
  private String subscriptionAdvancedFilter;

  @Value("${app.rest.nsp.subscription.property-filter:affectedObjectType NOT LIKE 'NmsSystem'}")
  private String subscriptionPropertyFilter;

  // ───────────────────────────────────────
  // Token cache is host-aware.
  // ───────────────────────────────────────

  private String cachedToken;
  private String cachedTokenHost;
  private Instant tokenExpiresAt = Instant.EPOCH;

  private String baseUrl() {
    return baseUrl(siteSelector.activeHost());
  }

  private String baseUrl(String selectedHost) {
    return scheme + "://" + selectedHost;
  }

  public String activeHost() {
    return siteSelector.activeHost();
  }

  public void forceActiveHost(String host) {
    siteSelector.forceActiveHost(host);
    clearCachedToken();
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Failover helper
  // ─────────────────────────────────────────────────────────────────────────

  private <T> T withFailover(String operationName, NspRestOperation<T> operation) throws Exception {
    Exception last = null;

    for (String candidateHost : siteSelector.orderedHostsForAttempt()) {
      try {
        log.info("NSP REST operation={} trying host={}", operationName, candidateHost);

        T result = operation.run(candidateHost);

        siteSelector.markSuccess(candidateHost);
        return result;

      } catch (Exception e) {
        last = e;
        clearCachedToken();

        siteSelector.markFailure(candidateHost, e);

        if (!siteSelector.isFailoverEnabled()) {
          throw e;
        }
      }
    }

    throw new IllegalStateException(
        "NSP REST operation failed on all configured hosts: " + operationName,
        last
    );
  }

  private synchronized void clearCachedToken() {
    this.cachedToken = null;
    this.cachedTokenHost = null;
    this.tokenExpiresAt = Instant.EPOCH;
  }

  @FunctionalInterface
  private interface NspRestOperation<T> {
    T run(String selectedHost) throws Exception;
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
   * Cache Bearer token per host until (expires - 60s).
   */
  private synchronized String getAccessToken(String selectedHost) throws Exception {
    if (cachedToken != null
        && selectedHost != null
        && selectedHost.equals(cachedTokenHost)
        && Instant.now().isBefore(tokenExpiresAt.minusSeconds(60))) {
      return cachedToken;
    }

    String url = baseUrl(selectedHost) + tokenPath;

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
      throw new IllegalStateException(
          "NSP token request failed on host=" + selectedHost + ": " + response.getStatusCode()
      );
    }

    JsonNode json = objectMapper.readTree(response.getBody());
    String accessToken = json.path("access_token").asText(null);
    int expiresIn = json.path("expires_in").asInt(600);

    if (accessToken == null) {
      throw new IllegalStateException(
          "No access_token in NSP response from host=" + selectedHost + ": " + response.getBody()
      );
    }

    this.cachedToken = accessToken;
    this.cachedTokenHost = selectedHost;
    this.tokenExpiresAt = Instant.now().plusSeconds(expiresIn);

    log.info("NSP token acquired. host={}, expiresIn={}s", selectedHost, expiresIn);

    return accessToken;
  }

  // ─────────────────────────────────────────────────────────────────────────
  // ACTIVE ALARMS (REST snapshot) - Cursor pagination
  // ─────────────────────────────────────────────────────────────────────────

  /**
   * Fetch active alarms as individual alarm JSON strings.
   * Uses failover between NSP sites if enabled.
   */
  public List<String> fetchActiveAlarmEvents() throws Exception {
    return withFailover("fetchActiveAlarmEvents", selectedHost -> {
      if (!cursorPaginationEnabled) {
        String raw = fetchActiveAlarmsRaw(selectedHost, null);
        return splitAlarmEventsFromRaw(raw);
      }

      return fetchActiveAlarmEventsCursorPaged(selectedHost);
    });
  }

  /**
   * Cursor pagination:
   * - call #1: base filter sorted desc -> returns up to server page size
   * - call #2+: add AND lastTimeDetected < cursor
   * - repeat until empty or cursor stops decreasing
   */
  private List<String> fetchActiveAlarmEventsCursorPaged(String selectedHost) throws Exception {
    long cursor = Long.MAX_VALUE;  // exclusive upper bound
    int page = 0;
    int total = 0;

    // Keep order of insertion (stable)
    Map<String, String> dedup = cursorDedupeEnabled ? new LinkedHashMap<>() : null;

    while (true) {
      page++;
      if (page > cursorMaxPages) {
        log.warn("NSP cursor pagination safety stop: reached max-pages={}", cursorMaxPages);
        break;
      }

      String raw = fetchActiveAlarmsRaw(
          selectedHost,
          cursor == Long.MAX_VALUE ? null : cursor
      );

      // Parse page data
      PageData pd = parsePage(raw);

      int count = pd.events().size();
      if (count == 0) {
        log.info("NSP cursor pagination finished: host={} empty page at cursor={}",
            selectedHost, cursor);
        break;
      }

      long pageMinCursor = pd.minCursor();
      log.info("NSP cursor pagination host={} page={} count={} minCursor={} prevCursor={}",
          selectedHost, page, count, pageMinCursor, cursor);

      if (cursorDedupeEnabled) {
        for (JsonNode n : pd.events()) {
          String key = computeDedupeKey(n);
          String jsonStr = objectMapper.writeValueAsString(n);
          dedup.putIfAbsent(key, jsonStr);
        }
        total = dedup.size();
      } else {
        total += count;
      }

      if (total >= cursorMaxTotal) {
        log.warn("NSP cursor pagination safety stop: reached max-total={}", cursorMaxTotal);
        break;
      }

      // Safety: cursor must move backwards
      if (pageMinCursor >= cursor) {
        log.warn("NSP cursor pagination safety stop: cursor did not decrease (host={}, prev={}, newMin={})",
            selectedHost, cursor, pageMinCursor);
        break;
      }

      // Next cursor: strictly older than the oldest record we received
      cursor = pageMinCursor;
    }

    if (cursorDedupeEnabled) {
      log.info("NSP cursor pagination finished: host={} pages={} uniqueEvents={}",
          selectedHost, page, dedup.size());
      return new ArrayList<>(dedup.values());
    }

    // fallback shouldn't be reached often
    return Collections.emptyList();
  }

  /**
   * Raw fetch with optional cursor:
   * - cursor == null: base filter only
   * - cursor != null: base filter AND lastTimeDetected < cursor
   *
   * IMPORTANT:
   * We intentionally DO NOT pass offset/limit because NSP ignores them.
   */
  private String fetchActiveAlarmsRaw(String selectedHost, Long cursorExclusive) throws Exception {
    String token = getAccessToken(selectedHost);

    String url = buildAlarmsUrlWithCursor(selectedHost, cursorExclusive);
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
      throw new IllegalStateException(
          "NSP alarms request failed on host=" + selectedHost + ": "
              + response.getStatusCode() + " body=" + response.getBody()
      );
    }

    return response.getBody();
  }

  /**
   * Build alarms URL with alarmFilter and optional cursor condition.
   */
  private String buildAlarmsUrlWithCursor(String selectedHost, Long cursorExclusive) {
    StringBuilder sb = new StringBuilder();
    sb.append(baseUrl(selectedHost)).append(alarmsPath);

    boolean hasQuery = false;

    // Combine base filter with cursor filter in ONE alarmFilter expression
    String effectiveFilter = alarmFilter == null ? "" : alarmFilter.trim();

    if (cursorExclusive != null) {
      String cursorExpr = cursorField + " < " + cursorExclusive;
      if (effectiveFilter.isBlank()) {
        effectiveFilter = cursorExpr;
      } else {
        effectiveFilter = "(" + effectiveFilter + ") AND " + cursorExpr;
      }
    }

    if (!effectiveFilter.isBlank()) {
      String encoded = encodeAlarmFilter(effectiveFilter);

      sb.append(hasQuery ? "&" : "?");
      sb.append("alarmFilter=").append(encoded);
      hasQuery = true;

      log.info("NSP alarms raw filter : {}", effectiveFilter);
      log.info("NSP alarms encoded{}  : {}",
          alarmFilterDoubleEncode ? " x2" : " x1",
          encoded
      );
    }

    // Sort param (optional)
    appendSortParams(sb, hasQuery);

    return sb.toString();
  }

  private void appendSortParams(StringBuilder sb, boolean hasQueryAlready) {
    if (sortParam == null || sortParam.isBlank()) return;

    boolean hasQuery = hasQueryAlready;

    // docs: use multiple &sort= properties
    // your endpoint supports: sort=field,asc|desc
    String[] parts = sortParam.split(";");
    for (String p : parts) {
      String s = p.trim();
      if (s.isEmpty()) continue;

      sb.append(hasQuery ? "&" : "?");
      sb.append("sort=").append(urlEncodeOnce(s));
      hasQuery = true;
    }
  }

  /**
   * Parse response JSON and return list of alarms + min cursor.
   */
  private PageData parsePage(String raw) throws Exception {
    JsonNode root = objectMapper.readTree(raw);
    JsonNode data = root.at(JsonPointer.compile(alarmsArrayPath));

    if (data == null || !data.isArray()) {
      return new PageData(List.of(), Long.MAX_VALUE);
    }

    List<JsonNode> events = new ArrayList<>();
    long min = Long.MAX_VALUE;

    for (JsonNode n : data) {
      events.add(n);

      long c = readCursorValue(n);
      if (c > 0 && c < min) {
        min = c;
      }
    }

    return new PageData(events, min);
  }

  /**
   * Read lastTimeDetected (ms epoch). If missing/unparseable, return -1.
   */
  private long readCursorValue(JsonNode alarm) {
    JsonNode node = alarm.get(cursorField);
    if (node == null || node.isNull()) return -1;

    if (node.isNumber()) {
      return node.asLong(-1);
    }

    // Sometimes it can be string; try parse numeric
    String s = node.asText("");
    try {
      return Long.parseLong(s);
    } catch (Exception e) {
      return -1;
    }
  }

  /**
   * Dedup key selection (best-effort).
   * Prefers stable unique IDs if present.
   */
  private String computeDedupeKey(JsonNode alarm) {
    // Common candidates
    String[] candidates = { "alarmId", "faultId", "id", "ALA_alarmId" };

    for (String c : candidates) {
      JsonNode v = alarm.get(c);
      if (v != null && !v.isNull()) {
        String s = v.asText("").trim();
        if (!s.isEmpty()) return c + ":" + s;
      }
    }

    // Fallback composite key
    String alarmName = alarm.path("alarmName").asText("");
    String obj = alarm.path("affectedObjectName").asText("");
    long t = readCursorValue(alarm);

    return "fallback:" + alarmName + "|" + obj + "|" + t;
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Extract alarms into JSON strings
  // ─────────────────────────────────────────────────────────────────────────

  /**
   * Extracts alarms array from raw JSON using alarmsArrayPath.
   * Returns a list of individual alarm JSON payloads (string).
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

  // ─────────────────────────────────────────────────────────────────────────
  // SUBSCRIPTION CREATE + RENEW
  // ─────────────────────────────────────────────────────────────────────────

  /**
   * Create an NSP notification subscription.
   * Returns (subscriptionId, topicId, host).
   *
   * This operation uses failover, because a subscription can be created on whichever
   * NSP site is currently active/reachable.
   */
  public SubscriptionInfo createSubscription() throws Exception {
    return withFailover("createSubscription", selectedHost -> {
      String token = getAccessToken(selectedHost);

      String url = baseUrl(selectedHost) + subscriptionsPath;
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
            "NSP create subscription failed on host=" + selectedHost + ": "
                + response.getStatusCode() + " body=" + response.getBody()
        );
      }

      JsonNode root = objectMapper.readTree(response.getBody());
      String subscriptionId = root.at("/response/data/subscriptionId").asText(null);
      String topicId = root.at("/response/data/topicId").asText(null);

      if (subscriptionId == null || subscriptionId.isBlank() || topicId == null || topicId.isBlank()) {
        throw new IllegalStateException(
            "Could not extract subscriptionId/topicId from create response on host="
                + selectedHost + ": " + response.getBody()
        );
      }

      log.info("NSP subscription created: host={}, subscriptionId={}, topicId={}",
          selectedHost, subscriptionId, topicId);

      return new SubscriptionInfo(subscriptionId, topicId, selectedHost);
    });
  }

  /**
   * Backward-compatible renew method.
   * Uses currently active host if caller does not specify one.
   */
  public void renewSubscription(String subscriptionId) throws Exception {
    renewSubscription(subscriptionId, siteSelector.activeHost());
  }

  /**
   * Renew an existing subscription on the host where it was created.
   *
   * Important:
   * We do NOT fail over renew to the other NSP site, because a subscription created
   * on Site A may not exist on Site B. If renew fails, NspSubscriptionManager should
   * recreate the subscription. Recreate will use failover and may create it on the
   * other site.
   */
  public void renewSubscription(String subscriptionId, String hostForSubscription) throws Exception {
    if (subscriptionId == null || subscriptionId.isBlank()) {
      throw new IllegalArgumentException("subscriptionId is blank");
    }

    String selectedHost = (hostForSubscription == null || hostForSubscription.isBlank())
        ? siteSelector.activeHost()
        : hostForSubscription.trim();

    siteSelector.forceActiveHost(selectedHost);

    String token = getAccessToken(selectedHost);

    String url = baseUrl(selectedHost) + subscriptionsPath + "/" + subscriptionId + "/renewals";
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
          "NSP renew subscription failed on host=" + selectedHost + ": "
              + response.getStatusCode() + " body=" + response.getBody()
      );
    }

    log.info("NSP subscription renewed: host={}, subscriptionId={}", selectedHost, subscriptionId);
  }

  // ─────────────────────────────────────────────────────────────────────────
  // DTOs
  // ─────────────────────────────────────────────────────────────────────────

  public record SubscriptionInfo(String subscriptionId, String topicId, String host) {}

  private record PageData(List<JsonNode> events, long minCursor) {}
}
