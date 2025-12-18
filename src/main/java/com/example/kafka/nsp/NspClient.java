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

    @Value("${app.rest.nsp.auth.basic}")
    private String basicAuth;

    @Value("${app.rest.nsp.auth.grant-type:client_credentials}")
    private String grantType;

    @Value("${app.rest.nsp.headers.content-type:application/json}")
    private String contentType;

    @Value("${app.rest.nsp.headers.accept:application/json}")
    private String accept;

    // ΝΕΟ: σε ποιο JSON path είναι το array με τα alarms
    @Value("${app.rest.nsp.alarms-array-path:/response/data}")
    private String alarmsArrayPath;

    // NEW: raw filter string from YAML
    @Value("${app.rest.nsp.alarm-filter}")
    private String alarmFilter;

    // ───────────────────────────────────────

    private String cachedToken;
    private Instant tokenExpiresAt = Instant.EPOCH;

    private String baseUrl() {
        return scheme + "://" + host;
    }

    private synchronized String getAccessToken() throws Exception {
        if (cachedToken != null &&
            Instant.now().isBefore(tokenExpiresAt.minusSeconds(60))) {
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
                url, HttpMethod.POST, request, String.class);

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

    public String fetchActiveAlarmsRaw() throws Exception {
        String token = getAccessToken();

        // 1) Raw filter from YAML, e.g. "affectedObjectType like '%Equipment%'"
        String rawFilter = alarmFilter;

        // 2) URL-encode once (URLEncoder turns spaces into '+')
        String onceEncoded = URLEncoder.encode(rawFilter, StandardCharsets.UTF_8);
        // e.g. "affectedObjectType+like+%27%25Equipment%25%27"

        // 3) Replace '+' with '%20' so spaces are encoded like in your curl-style URLs
        String encodedForNsp = onceEncoded.replace("+", "%20");
        // e.g. "affectedObjectType%20like%20%27%25Equipment%25%27"

        // 4) Build final URL with a SINGLE-encoded filter
        String url = baseUrl() + alarmsPath + "?alarmFilter=" + encodedForNsp;

        log.info("NSP alarms raw filter   : {}", rawFilter);
        log.info("NSP alarms encoded      : {}", encodedForNsp);
        log.info("NSP alarms request URL  : {}", url);

        HttpHeaders headers = new HttpHeaders();
        headers.setBearerAuth(token);
        headers.setAccept(List.of(MediaType.valueOf(accept)));
        headers.setContentType(MediaType.valueOf(contentType));

        HttpEntity<Void> request = new HttpEntity<>(headers);
        ResponseEntity<String> response = restTemplate.exchange(
                url, HttpMethod.GET, request, String.class);

        if (!response.getStatusCode().is2xxSuccessful()) {
        throw new IllegalStateException("NSP alarms request failed: " + response.getStatusCode());
        }

        return response.getBody();
    }

    public List<String> fetchActiveAlarmEvents() throws Exception {
        String raw = fetchActiveAlarmsRaw();
        JsonNode root = objectMapper.readTree(raw);
        List<String> result = new ArrayList<>();

        // Χρησιμοποιούμε το alarmsArrayPath (π.χ. "/response/data")
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
            // fallback: όλο το root είναι array
            for (JsonNode node : root) {
                result.add(objectMapper.writeValueAsString(node));
            }
            log.warn("NSP: alarms-array-path {} did not resolve to array, used root array instead", alarmsArrayPath);
        } else {
            // τελικό fallback: επέστρεψε το raw ως ένα μόνο message
            log.warn("NSP: alarms-array-path {} did not resolve to array; returning single raw payload", alarmsArrayPath);
            result.add(raw);
        }

        return result;
    }
}
