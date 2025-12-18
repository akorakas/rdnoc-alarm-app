package com.example.kafka.nsp;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.rest.nsp")
public class NspRestProperties {

    private String scheme = "https";
    private String host;
    private long pollIntervalMs = 60000;
    private String alarmsArrayPath = "/response/data";

    private Auth auth = new Auth();
    private Headers headers = new Headers();
    private Paths paths = new Paths();

    public String getBaseUrl() {
        return scheme + "://" + host;
    }

    // ----- nested classes -----
    public static class Auth {
        private String basic;
        private String grantType = "client_credentials";

        public String getBasic() { return basic; }
        public void setBasic(String basic) { this.basic = basic; }
        public String getGrantType() { return grantType; }
        public void setGrantType(String grantType) { this.grantType = grantType; }
    }

    public static class Headers {
        private String contentType = "application/json";
        private String accept = "application/json";

        public String getContentType() { return contentType; }
        public void setContentType(String contentType) { this.contentType = contentType; }
        public String getAccept() { return accept; }
        public void setAccept(String accept) { this.accept = accept; }
    }

    public static class Paths {
        private String token;
        private String alarms;

        public String getToken() { return token; }
        public void setToken(String token) { this.token = token; }
        public String getAlarms() { return alarms; }
        public void setAlarms(String alarms) { this.alarms = alarms; }
    }

    public String getAlarmsArrayPath() {
        return alarmsArrayPath;
    }

    public void setAlarmsArrayPath(String alarmsArrayPath) {
        this.alarmsArrayPath = alarmsArrayPath;
    }
    
    // ----- getters/setters -----
    public String getScheme() { return scheme; }
    public void setScheme(String scheme) { this.scheme = scheme; }

    public String getHost() { return host; }
    public void setHost(String host) { this.host = host; }

    public long getPollIntervalMs() { return pollIntervalMs; }
    public void setPollIntervalMs(long pollIntervalMs) { this.pollIntervalMs = pollIntervalMs; }

    public Auth getAuth() { return auth; }
    public void setAuth(Auth auth) { this.auth = auth; }

    public Headers getHeaders() { return headers; }
    public void setHeaders(Headers headers) { this.headers = headers; }

    public Paths getPaths() { return paths; }
    public void setPaths(Paths paths) { this.paths = paths; }
}
