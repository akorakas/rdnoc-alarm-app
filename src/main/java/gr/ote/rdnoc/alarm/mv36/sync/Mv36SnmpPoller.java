package gr.ote.rdnoc.alarm.mv36.sync;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

import tools.jackson.core.JacksonException;
import tools.jackson.core.JsonGenerator;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.SerializationContext;
import tools.jackson.databind.ValueSerializer;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.module.SimpleModule;

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
    SimpleModule module = new SimpleModule();
  
    module.addSerializer(Instant.class, new ValueSerializer<Instant>() {
      @Override
      public void serialize(
          Instant value,
          JsonGenerator generator,
          SerializationContext context
      ) throws JacksonException {

        String numericTimestamp =
            value.getEpochSecond()
                + "."
                + String.format("%09d", value.getNano());

        /*
         * Preserve your existing output format as an unquoted
         * numeric JSON value, for example:
         *
         * 1785319341.123456789
         */
        generator.writeRawValue(numericTimestamp);
      }
    });

    return JsonMapper.builder()
        .addModule(module)
        .build();
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