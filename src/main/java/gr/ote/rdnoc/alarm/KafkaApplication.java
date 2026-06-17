package gr.ote.rdnoc.alarm;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.kafka.annotation.EnableKafka;

import gr.ote.rdnoc.alarm.service.config.CorrelationProperties;
import gr.ote.rdnoc.alarm.service.sync.SyncMarkerProperties;

@SpringBootApplication
@ConfigurationPropertiesScan
@EnableKafka
@EnableConfigurationProperties({
    CorrelationProperties.class,
    SyncMarkerProperties.class
})
public class KafkaApplication {
  public static void main(String[] args) {
    SpringApplication.run(KafkaApplication.class, args);
  }
}