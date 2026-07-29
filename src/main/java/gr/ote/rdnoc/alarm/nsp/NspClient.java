// src/main/java/gr/ote/rdnoc/alarm/nsp/NspClient.java
package gr.ote.rdnoc.alarm.nsp;

import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
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

import tools.jackson.core.JsonPointer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

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

  @Value("${app.rest.nsp.paths.token}")
  private String tokenPath;

  @Value("${app.rest.nsp.paths.alarms}")
  private String alarmsPath;

  // Subscriptions base path, used for create + renew + delete
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

  // Where the alarms array lives. Default works with /response/data.
  @Value("${app.rest.nsp.alarms-array-path:/response/data}")
  private String alarmsArrayPath;

  // Raw alarm filter, read from YAML.
  @Value("${app.rest.nsp.alarm-filter:}")
  private String alarmFilter;

  /**
   * NSP sometimes expects double URL encoding in alarmFilter.
   *
   * Example:
   * severity%253D'major'
   *
   * In your environment, you verified single encoding works, so use:
   *
   * app.rest.nsp.alarm-filter-double-encode=false
   */
  @Value("${app.rest.nsp.alarm-filter-double-encode:true}")
  private boolean alarmFilterDoubleEncode;

  // Optional sort. Supports one or more sort expressions separated by semicolon.
  @Value("${app.rest.nsp.sort:lastTimeDetected,desc}")
  private String sortParam;

  // ─────────────────────────────────────────────────────────────────────────
  // Cursor pagination
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

  // ─────────────────────────────────────────────────────────────────────────
  // Subscription request payload parts
  // ─────────────────────────────────────────────────────────────────────────

  @Value("${app.rest.nsp.subscription.category-name:NSP-FAULT}")
  private String subscriptionCategoryName;

  @Value("${app.rest.nsp.subscription.advanced-filter:{\"includeAlarmDetailsOnChangeEvent\":true, \"alarmProperties\": {\"rootCause\": true}}}")
  private String subscriptionAdvancedFilter;

  @Value("${app.rest.nsp.subscription.property-filter:affectedObjectType NOT LIKE 'NmsSystem'}")
  private String subscriptionPropertyFilter;

  // ─────────────────────────────────────────────────────────────────────────
  // Host-aware token cache
  // ─────────────────────────────────────────────────────────────────────────

  private String cachedToken;
  private String cachedTokenHost;
  private Instant tokenExpiresAt = Instant.EPOCH;

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

    return alarmFilterDoubleEncode
        ? urlEncodeTwice(rawFilter)
        : urlEncodeOnce(rawFilter);
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Token
  // ─────────────────────────────────────────────────────────────────────────

  /**
   * Cache bearer token per host until expires - 60 seconds.
   */
  private synchronized String getAccessToken(String selectedHost) throws Exception {
    if (selectedHost == null || selectedHost.isBlank()) {
      throw new IllegalArgumentException("selectedHost is blank");
    }

    if (cachedToken != null
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
          "NSP token request failed on host=" + selectedHost
              + ": " + response.getStatusCode()
              + " body=" + response.getBody()
      );
    }

    JsonNode json = objectMapper.readTree(response.getBody());
    String accessToken = json.path("access_token").asString(null);
    int expiresIn = json.path("expires_in").asInt(600);

    if (accessToken == null || accessToken.isBlank()) {
      throw new IllegalStateException(
          "No access_token in NSP response from host=" + selectedHost
              + ": " + response.getBody()
      );
    }

    this.cachedToken = accessToken;
    this.cachedTokenHost = selectedHost;
    this.tokenExpiresAt = Instant.now().plusSeconds(expiresIn);

    log.info("NSP token acquired. host={}, expiresIn={}s", selectedHost, expiresIn);

    return accessToken;
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Active alarms REST snapshot
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
   *
   * 1. First call uses base filter sorted descending.
   * 2. Next calls add: lastTimeDetected < previousMinCursor.
   * 3. Repeat until empty page, max page limit, max total limit, or non-moving cursor.
   */
  private List<String> fetchActiveAlarmEventsCursorPaged(String selectedHost) throws Exception {
    long cursor = Long.MAX_VALUE;
    int page = 0;

    // Always initialize both collections to avoid null warnings and support both modes.
    Map<String, String> dedup = new LinkedHashMap<>();
    List<String> nonDedupedEvents = new ArrayList<>();

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

      PageData pageData = parsePage(raw);

      int count = pageData.events().size();

      if (count == 0) {
        log.info("NSP cursor pagination finished: host={} empty page at cursor={}",
            selectedHost, cursor);
        break;
      }

      long pageMinCursor = pageData.minCursor();

      log.info("NSP cursor pagination host={} page={} count={} minCursor={} prevCursor={}",
          selectedHost, page, count, pageMinCursor, cursor);

      int total;

      if (cursorDedupeEnabled) {
        for (JsonNode alarm : pageData.events()) {
          String key = computeDedupeKey(alarm);
          String jsonStr = objectMapper.writeValueAsString(alarm);
          dedup.putIfAbsent(key, jsonStr);
        }

        total = dedup.size();
      } else {
        for (JsonNode alarm : pageData.events()) {
          nonDedupedEvents.add(objectMapper.writeValueAsString(alarm));
        }

        total = nonDedupedEvents.size();
      }

      if (total >= cursorMaxTotal) {
        log.warn("NSP cursor pagination safety stop: reached max-total={}", cursorMaxTotal);
        break;
      }

      if (pageMinCursor >= cursor) {
        log.warn("NSP cursor pagination safety stop: cursor did not decrease. host={}, prev={}, newMin={}",
            selectedHost, cursor, pageMinCursor);
        break;
      }

      cursor = pageMinCursor;
    }

    if (cursorDedupeEnabled) {
      log.info("NSP cursor pagination finished: host={} pages={} uniqueEvents={}",
          selectedHost, page, dedup.size());

      return new ArrayList<>(dedup.values());
    }

    log.info("NSP cursor pagination finished: host={} pages={} events={}",
        selectedHost, page, nonDedupedEvents.size());

    return nonDedupedEvents;
  }

  /**
   * Raw fetch with optional cursor:
   *
   * cursor == null:
   *   base filter only
   *
   * cursor != null:
   *   base filter AND lastTimeDetected < cursor
   *
   * Important:
   * We intentionally do not pass offset/limit because NSP ignores them in your environment.
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
          "NSP alarms request failed on host=" + selectedHost
              + ": " + response.getStatusCode()
              + " body=" + response.getBody()
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

    String effectiveFilter = alarmFilter == null ? "" : alarmFilter.trim();

    if (cursorExclusive != null) {
      String cursorExpr = cursorField + " < " + cursorExclusive;

      if (effectiveFilter.isBlank()) {
        effectiveFilter = cursorExpr;
      } else {
        effectiveFilter = "(" + effectiveFilter + ") AND " + cursorExpr;
      }
    }

    boolean hasAlarmFilter = !effectiveFilter.isBlank();

    if (hasAlarmFilter) {
      String encoded = encodeAlarmFilter(effectiveFilter);

      sb.append("?");
      sb.append("alarmFilter=").append(encoded);

      log.info("NSP alarms raw filter : {}", effectiveFilter);
      log.info("NSP alarms encoded{}  : {}",
          alarmFilterDoubleEncode ? " x2" : " x1",
          encoded);
    }

    appendSortParams(sb, hasAlarmFilter);

    return sb.toString();
  }

  private void appendSortParams(StringBuilder sb, boolean hasQueryAlready) {
    if (sortParam == null || sortParam.isBlank()) {
      return;
    }

    boolean hasQuery = hasQueryAlready;

    String[] parts = sortParam.split(";");

    for (String part : parts) {
      String sort = part.trim();

      if (sort.isEmpty()) {
        continue;
      }

      sb.append(hasQuery ? "&" : "?");
      sb.append("sort=").append(urlEncodeOnce(sort));
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

    for (JsonNode alarm : data) {
      events.add(alarm);

      long cursor = readCursorValue(alarm);

      if (cursor > 0 && cursor < min) {
        min = cursor;
      }
    }

    return new PageData(events, min);
  }

  /**
   * Read lastTimeDetected, expected as epoch milliseconds.
   * If missing or unparseable, return -1.
   */
  private long readCursorValue(JsonNode alarm) {
    JsonNode node = alarm.get(cursorField);

    if (node == null || node.isNull()) {
      return -1;
    }

    if (node.isNumber()) {
      return node.asLong(-1);
    }

    String value = node.asString("");

    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      return -1;
    }
  }

  /**
   * Dedup key selection.
   * Prefers stable unique IDs if present.
   */
  private String computeDedupeKey(JsonNode alarm) {
    String[] candidates = {
        "alarmId",
        "faultId",
        "id",
        "ALA_alarmId"
    };

    for (String candidate : candidates) {
      JsonNode value = alarm.get(candidate);

      if (value != null && !value.isNull()) {
        String text = value.asString("").trim();

        if (!text.isEmpty()) {
          return candidate + ":" + text;
        }
      }
    }

    String alarmName = alarm.path("alarmName").asString("");
    String affectedObjectName = alarm.path("affectedObjectName").asString("");
    long detectedTime = readCursorValue(alarm);

    return "fallback:" + alarmName + "|" + affectedObjectName + "|" + detectedTime;
  }

  /**
   * Extract alarms array from raw JSON using alarmsArrayPath.
   * Returns a list of individual alarm JSON payloads.
   */
  private List<String> splitAlarmEventsFromRaw(String raw) throws Exception {
    JsonNode root = objectMapper.readTree(raw);
    List<String> result = new ArrayList<>();

    JsonNode arrayNode;

    try {
      JsonPointer pointer = JsonPointer.compile(alarmsArrayPath);
      arrayNode = root.at(pointer);
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

      log.warn("NSP: alarms-array-path {} did not resolve to array, used root array instead",
          alarmsArrayPath);

      return result;
    }

    log.warn("NSP: alarms-array-path {} did not resolve to array; returning single raw payload",
        alarmsArrayPath);

    return List.of(raw);
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Subscription create, renew, delete
  // ─────────────────────────────────────────────────────────────────────────

  /**
   * Create an NSP notification subscription.
   *
   * Returns:
   * - subscriptionId
   * - topicId
   * - host where the subscription was created
   *
   * This operation uses failover because a new subscription may be created on
   * whichever NSP site is currently reachable.
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

      /*
       * advancedFilter must be a JSON string inside JSON.
       *
       * For example, the final payload becomes:
       *
       * {
       *   "categories": [
       *     {
       *       "advancedFilter": "{\"includeAlarmDetailsOnChangeEvent\":true}",
       *       "propertyFilter": "affectedObjectType NOT LIKE 'NmsSystem'",
       *       "name": "NSP-FAULT"
       *     }
       *   ]
       * }
       */
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
            "NSP create subscription failed on host=" + selectedHost
                + ": " + response.getStatusCode()
                + " body=" + response.getBody()
        );
      }

      JsonNode root = objectMapper.readTree(response.getBody());
      String subscriptionId = root.at("/response/data/subscriptionId").asString(null);
      String topicId = root.at("/response/data/topicId").asString(null);

      if (subscriptionId == null || subscriptionId.isBlank()
          || topicId == null || topicId.isBlank()) {
        throw new IllegalStateException(
            "Could not extract subscriptionId/topicId from create response on host="
                + selectedHost
                + ": " + response.getBody()
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
   * We do not fail over renew to the other NSP site, because a subscription
   * created on Site A may not exist on Site B.
   *
   * If renew fails, NspSubscriptionManager should recreate the subscription.
   * Recreate uses failover and may create it on the other site.
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
          "NSP renew subscription failed on host=" + selectedHost
              + ": " + response.getStatusCode()
              + " body=" + response.getBody()
      );
    }

    log.info("NSP subscription renewed: host={}, subscriptionId={}",
        selectedHost, subscriptionId);
  }

  /**
   * Delete an existing subscription from the host where it was created.
   *
   * This is best-effort cleanup. If NSP does not support DELETE in your version,
   * the caller may safely ignore failures and continue creating a new subscription.
   */
  public void deleteSubscription(String subscriptionId, String hostForSubscription) throws Exception {
    if (subscriptionId == null || subscriptionId.isBlank()) {
      throw new IllegalArgumentException("subscriptionId is blank");
    }

    String selectedHost = (hostForSubscription == null || hostForSubscription.isBlank())
        ? siteSelector.activeHost()
        : hostForSubscription.trim();

    String token = getAccessToken(selectedHost);

    String url = baseUrl(selectedHost) + subscriptionsPath + "/" + subscriptionId;
    log.info("NSP delete subscription URL: {}", url);

    HttpHeaders headers = new HttpHeaders();
    headers.setBearerAuth(token);
    headers.setAccept(List.of(MediaType.valueOf(accept)));

    HttpEntity<Void> request = new HttpEntity<>(headers);

    ResponseEntity<String> response = restTemplate.exchange(
        URI.create(url),
        HttpMethod.DELETE,
        request,
        String.class
    );

    if (!response.getStatusCode().is2xxSuccessful()) {
      throw new IllegalStateException(
          "NSP delete subscription failed on host=" + selectedHost
              + ": " + response.getStatusCode()
              + " body=" + response.getBody()
      );
    }

    log.info("NSP subscription deleted. host={}, subscriptionId={}",
        selectedHost, subscriptionId);
  }

  // ─────────────────────────────────────────────────────────────────────────
  // DTOs
  // ─────────────────────────────────────────────────────────────────────────

  public record SubscriptionInfo(
      String subscriptionId,
      String topicId,
      String host
  ) {}

  private record PageData(
      List<JsonNode> events,
      long minCursor
  ) {}
}