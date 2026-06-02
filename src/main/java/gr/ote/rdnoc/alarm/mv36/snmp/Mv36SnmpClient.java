package gr.ote.rdnoc.alarm.mv36.snmp;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.snmp4j.CommunityTarget;
import org.snmp4j.PDU;
import org.snmp4j.Snmp;
import org.snmp4j.Target;
import org.snmp4j.event.ResponseEvent;
import org.snmp4j.mp.SnmpConstants;
import org.snmp4j.smi.GenericAddress;
import org.snmp4j.smi.Integer32;
import org.snmp4j.smi.OID;
import org.snmp4j.smi.OctetString;
import org.snmp4j.smi.UdpAddress;
import org.snmp4j.smi.Variable;
import org.snmp4j.smi.VariableBinding;
import org.snmp4j.transport.DefaultUdpTransportMapping;
import org.springframework.stereotype.Component;

import gr.ote.rdnoc.alarm.mv36.config.Mv36SnmpProperties;
import gr.ote.rdnoc.alarm.mv36.model.Mv36ActiveAlarm;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class Mv36SnmpClient {

  private static final ZoneId DEFAULT_ZONE = ZoneId.of("Europe/Athens");

  private final Mv36SnmpProperties props;

  private Snmp snmp;

  @PostConstruct
  public void init() throws IOException {
    this.snmp = new Snmp(new DefaultUdpTransportMapping());
    this.snmp.listen();

    log.info("MV36 SNMP client initialized. target={}:{}",
        props.getSnmp().getHost(),
        props.getSnmp().getPort());
  }

  @PreDestroy
  public void close() {
    if (snmp != null) {
      try {
        snmp.close();
      } catch (IOException e) {
        log.warn("Failed to close MV36 SNMP client", e);
      }
    }
  }

  public List<Mv36ActiveAlarm> fetchActiveAlarms() throws IOException {
    var o = props.getSnmp().getOids();

    Map<String, Mv36ActiveAlarm> rows = new LinkedHashMap<>();

    walkAndApply(rows, "mv36AlarmId", o.getMv36AlarmId(), Mv36ActiveAlarm::setMv36AlarmId);
    walkAndApply(rows, "mv36AlarmSeverity", o.getMv36AlarmSeverity(), Mv36ActiveAlarm::setMv36AlarmSeverity);
    walkAndApply(rows, "mv36AlarmNeId", o.getMv36AlarmNeId(), Mv36ActiveAlarm::setMv36AlarmNeId);
    walkAndApply(rows, "mv36AlarmEventType", o.getMv36AlarmEventType(), Mv36ActiveAlarm::setMv36AlarmEventType);
    walkAndApply(rows, "mv36AlarmRaisingTime", o.getMv36AlarmRaisingTime(), Mv36ActiveAlarm::setMv36AlarmRaisingTime);

    walkAndApply(rows, "mv36AlarmStrNeUniqueName", o.getMv36AlarmStrNeUniqueName(), Mv36ActiveAlarm::setMv36AlarmStrNeUniqueName);
    walkAndApply(rows, "mv36AlarmStrShelf", o.getMv36AlarmStrShelf(), Mv36ActiveAlarm::setMv36AlarmStrShelf);
    walkAndApply(rows, "mv36AlarmStrCard", o.getMv36AlarmStrCard(), Mv36ActiveAlarm::setMv36AlarmStrCard);
    walkAndApply(rows, "mv36AlarmPortId", o.getMv36AlarmPortId(), Mv36ActiveAlarm::setMv36AlarmPortId);
    walkAndApply(rows, "mv36AlarmStr", o.getMv36AlarmStr(), Mv36ActiveAlarm::setMv36AlarmStr);
    walkAndApply(rows, "mv36AlarmStrProbCause", o.getMv36AlarmStrProbCause(), Mv36ActiveAlarm::setMv36AlarmStrProbCause);
    walkAndApply(rows, "mv36AlarmStrEventType", o.getMv36AlarmStrEventType(), Mv36ActiveAlarm::setMv36AlarmStrEventType);

    List<Mv36ActiveAlarm> result = new ArrayList<>(rows.values());
    log.info("MV36 SNMP: fetched {} active alarm row(s)", result.size());
    return result;
  }

  private void walkAndApply(
      Map<String, Mv36ActiveAlarm> rows,
      String columnName,
      String baseOidText,
      AlarmSetter setter
  ) throws IOException {

    if (baseOidText == null || baseOidText.isBlank()) {
      log.warn("MV36 SNMP: OID for {} is empty; skipping column", columnName);
      return;
    }

    OID baseOid = new OID(baseOidText);
    Map<String, String> values = walkColumn(baseOid, columnName);

    for (Map.Entry<String, String> e : values.entrySet()) {
      String sourceIndex = e.getKey();
      String value = e.getValue();

      Mv36ActiveAlarm alarm = rows.computeIfAbsent(sourceIndex, idx -> {
        Mv36ActiveAlarm a = new Mv36ActiveAlarm();
        a.setSourceIndex(idx);
        return a;
      });

      setter.set(alarm, value);
    }

    log.info("MV36 SNMP: column={} rows={}", columnName, values.size());
  }

  private Map<String, String> walkColumn(OID baseOid, String columnName) throws IOException {
    Map<String, String> result = new LinkedHashMap<>();

    OID nextOid = new OID(baseOid);
    Target<UdpAddress> target = buildTarget();

    while (true) {
      PDU pdu = new PDU();
      pdu.setType(PDU.GETBULK);
      pdu.setNonRepeaters(0);
      pdu.setMaxRepetitions(props.getSnmp().getMaxRepetitions());
      pdu.add(new VariableBinding(nextOid));

      ResponseEvent<UdpAddress> event = snmp.send(pdu, target);

      if (event == null || event.getResponse() == null) {
        throw new IOException("SNMP timeout/no response while walking " + columnName + " baseOid=" + baseOid);
      }

      PDU response = event.getResponse();

      if (response.getErrorStatus() != PDU.noError) {
        throw new IOException("SNMP error while walking " + columnName
            + ": status=" + response.getErrorStatusText());
      }

      boolean gotAnyInSubtree = false;

      for (VariableBinding vb : response.getVariableBindings()) {
        OID oid = vb.getOid();

        if (!oid.startsWith(baseOid)) {
          return result;
        }

        if (vb.isException()) {
          return result;
        }

        String index = extractIndex(baseOid, oid);
        if (index == null || index.isBlank()) {
          continue;
        }

        String value = variableToString(columnName, vb.getVariable());
        result.put(index, value);

        nextOid = oid;
        gotAnyInSubtree = true;
      }

      if (!gotAnyInSubtree) {
        return result;
      }

      nextOid = new OID(nextOid);
      nextOid.append(0);
    }
  }

  private Target<UdpAddress> buildTarget() {
    String address = "udp:" + props.getSnmp().getHost() + "/" + props.getSnmp().getPort();

    CommunityTarget<UdpAddress> target = new CommunityTarget<>();
    target.setCommunity(new OctetString(props.getSnmp().getCommunity()));
    target.setAddress((UdpAddress) GenericAddress.parse(address));
    target.setVersion(SnmpConstants.version2c);
    target.setTimeout(props.getSnmp().getTimeoutMs());
    target.setRetries(props.getSnmp().getRetries());

    return target;
  }

  private static String extractIndex(OID baseOid, OID fullOid) {
    String base = baseOid.toDottedString();
    String full = fullOid.toDottedString();

    if (!full.startsWith(base + ".")) {
      return null;
    }

    return full.substring(base.length() + 1);
  }

  private static String variableToString(String columnName, Variable v) {
    if (v == null) {
      return null;
    }

    if ("mv36AlarmRaisingTime".equals(columnName) && v instanceof OctetString os) {
      String decoded = decodeDateAndTime(os);
      if (decoded != null) {
        return decoded;
      }
    }

    if (v instanceof OctetString os) {
      return os.toString().trim();
    }

    if (v instanceof Integer32 i) {
      return String.valueOf(i.getValue());
    }

    return v.toString().trim();
  }

  /**
   * SNMP DateAndTime / RFC2579:
   * bytes:
   *   0-1 year
   *   2 month
   *   3 day
   *   4 hour
   *   5 minute
   *   6 second
   *   7 decisecond
   * optional:
   *   8 '+' or '-'
   *   9 timezone hour
   *   10 timezone minute
   *
   * Returns an ISO offset datetime string, for example:
   * 2025-10-01T13:52:53+03:00
   */
  private static String decodeDateAndTime(OctetString os) {
    try {
      byte[] b = os.getValue();

      if (b == null || b.length < 8) {
        return null;
      }

      int year = ((b[0] & 0xFF) << 8) | (b[1] & 0xFF);
      int month = b[2] & 0xFF;
      int day = b[3] & 0xFF;
      int hour = b[4] & 0xFF;
      int min = b[5] & 0xFF;
      int sec = b[6] & 0xFF;
      int deci = b[7] & 0xFF;

      if (year == 0 && month == 1 && day == 1 && hour == 0 && min == 0 && sec == 0 && deci == 0) {
        return null;
      }

      int nanos = deci * 100_000_000;

      ZoneOffset offset;

      if (b.length >= 11) {
        char dir = (char) (b[8] & 0xFF);
        int tzH = b[9] & 0xFF;
        int tzM = b[10] & 0xFF;
        int totalMinutes = tzH * 60 + tzM;

        if (dir == '-') {
          totalMinutes = -totalMinutes;
        }

        offset = ZoneOffset.ofTotalSeconds(totalMinutes * 60);
      } else {
        offset = DEFAULT_ZONE.getRules().getOffset(LocalDateTime.now());
      }

      LocalDateTime ldt = LocalDateTime.of(year, month, day, hour, min, sec, nanos);
      return OffsetDateTime.of(ldt, offset).toString();

    } catch (Exception e) {
      return null;
    }
  }

  @FunctionalInterface
  private interface AlarmSetter {
    void set(Mv36ActiveAlarm alarm, String value);
  }
}