package gr.ote.rdnoc.alarm.config;

import java.time.Duration;

import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

@Configuration
public class RestClientConfig {

  @Bean
  public RestTemplate restTemplate(RestTemplateBuilder b) {
    Duration connect = Duration.ofSeconds(10);
    Duration read = Duration.ofSeconds(60);

    return b
        .requestFactory(() -> {
          SimpleClientHttpRequestFactory f = new SimpleClientHttpRequestFactory();
          f.setConnectTimeout((int) connect.toMillis());
          f.setReadTimeout((int) read.toMillis());
          return f;
        })
        .build();
  }
}