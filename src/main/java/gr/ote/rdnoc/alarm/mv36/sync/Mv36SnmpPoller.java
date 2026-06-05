package gr.ote.rdnoc.alarm.mv36.sync;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import gr.ote.atlas.events.models.UnifiedEvent;
import gr.ote.rdnoc.alarm.mv36.mapper.Mv36ActiveAlarmMapper;
import gr.ote.rdnoc.alarm.mv36.model.Mv36ActiveAlarm;
import gr.ote.rdnoc.alarm.mv36.snmp.Mv36SnmpClient;
import gr.ote.rdnoc.alarm.sink.SinkRouter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class Mv36SnmpPoller {

  private final Mv36SnmpClient snmpClient;
  private final Mv36ActiveAlarmMapper mapper;
  private final SinkRouter sinks;

  private static final ObjectMapper EVENT_OBJECT_MAPPER = buildEventObjectMapper();

  private static ObjectMapper buildEventObjectMapper() {
    JavaTimeModule javaTimeModule = new JavaTimeModule();

    javaTimeModule.addSerializer(Instant.class, new JsonSerializer<Instant>() {
      @Override
      public void serialize(Instant value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        if (value == null) {
          gen.writeNull();
          return;
        }

        String numericTimestamp = value.getEpochSecond() + "." + String.format("%09d", value.getNano());
        gen.writeRawValue(numericTimestamp);
      }
    });

    ObjectMapper mapper = new ObjectMapper();
    mapper.registerModule(javaTimeModule);
    mapper.findAndRegisterModules();

    return mapper;
  }

  public void fetchAndPublishActiveAlarmsOnce() throws Exception {
    Map<String, String> headers = new HashMap<>();
    headers.put("source", "MV36-SNMP");
    headers.put("sourceEms", "MV36_MOBILE");

    int okTotal = 0;
    int failedTotal = 0;

    List<Mv36ActiveAlarm> alarms = snmpClient.fetchActiveAlarms();

    log.info("MV36 SNMP: fetched {} active alarm(s)", alarms.size());

    for (Mv36ActiveAlarm alarm : alarms) {
      try {
        UnifiedEvent event = mapper.toUnifiedEvent(alarm);

        String json = EVENT_OBJECT_MAPPER.writeValueAsString(event);

        String key = event.getAlarmIdentifier();
        sinks.sendOutput(key, json, headers);

        okTotal++;
      } catch (Exception e) {
        failedTotal++;
        log.error("MV36 SNMP: failed to map/send active alarm. sourceIndex={}",
            alarm.getSourceIndex(), e);
      }
    }

    log.info("MV36 SNMP: snapshot publish finished. okTotal={}, failedTotal={}",
        okTotal, failedTotal);
  }
}