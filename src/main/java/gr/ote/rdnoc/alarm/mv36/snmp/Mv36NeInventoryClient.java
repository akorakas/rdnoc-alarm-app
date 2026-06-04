package gr.ote.rdnoc.alarm.mv36.snmp;

import java.io.IOException;
import java.time.Instant;
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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import gr.ote.rdnoc.alarm.mv36.model.Mv36NetworkElement;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@ConditionalOnProperty(prefix = "app.mv36.enrichment", name = "enabled", havingValue = "true")
public class Mv36NeInventoryClient {

  @Value("${app.mv36.snmp.host}")
  private String host;

  @Value("${app.mv36.snmp.port:161}")
  private int port;

  @Value("${app.mv36.snmp.community}")
  private String community;

  @Value("${app.mv36.snmp.timeout-ms:10000}")
  private int timeoutMs;

  @Value("${app.mv36.snmp.retries:2}")
  private int retries;

  @Value("${app.mv36.snmp.max-repetitions:25}")
  private int maxRepetitions;

  @Value("${app.mv36.enrichment.oids.mv36-ne-id}")
  private String mv36NeIdOid;

  @Value("${app.mv36.enrichment.oids.mv36-ne-name}")
  private String mv36NeNameOid;

  @Value("${app.mv36.enrichment.oids.mv36-ne-unique-name}")
  private String mv36NeUniqueNameOid;

  @Value("${app.mv36.enrichment.oids.mv36-ne-type-str}")
  private String mv36NeTypeStrOid;

  private Snmp snmp;

  @PostConstruct
  public void init() throws IOException {
    this.snmp = new Snmp(new DefaultUdpTransportMapping());
    this.snmp.listen();

    log.info("MV36 NE inventory SNMP client initialized. target={}:{}", host, port);
  }

  @PreDestroy
  public void close() {
    if (snmp != null) {
      try {
        snmp.close();
      } catch (IOException e) {
        log.warn("Failed to close MV36 NE inventory SNMP client", e);
      }
    }
  }

  public List<Mv36NetworkElement> fetchNetworkElements() throws IOException {
    Map<String, Mv36NetworkElement> rows = new LinkedHashMap<>();

    walkAndApply(rows, "mv36NeId", mv36NeIdOid, Mv36NetworkElement::setMv36NeId);
    walkAndApply(rows, "mv36NeName", mv36NeNameOid, Mv36NetworkElement::setMv36NeName);
    walkAndApply(rows, "mv36NeUniqueName", mv36NeUniqueNameOid, Mv36NetworkElement::setMv36NeUniqueName);
    walkAndApply(rows, "mv36NeTypeStr", mv36NeTypeStrOid, Mv36NetworkElement::setMv36NeTypeStr);

    List<Mv36NetworkElement> result = new ArrayList<>(rows.values());

    log.info("MV36 NE inventory fetched. count={}", result.size());

    return result;
  }

  private void walkAndApply(
      Map<String, Mv36NetworkElement> rows,
      String columnName,
      String baseOidText,
      NeSetter setter
  ) throws IOException {

    if (baseOidText == null || baseOidText.isBlank()) {
      log.warn("MV36 NE inventory OID for {} is empty. Skipping column.", columnName);
      return;
    }

    OID baseOid = new OID(baseOidText);
    Map<String, String> values = walkColumn(baseOid, columnName);

    for (Map.Entry<String, String> e : values.entrySet()) {
      String sourceIndex = e.getKey();
      String value = e.getValue();

      Mv36NetworkElement ne = rows.computeIfAbsent(sourceIndex, idx -> {
        Mv36NetworkElement x = new Mv36NetworkElement();
        x.setSourceIndex(idx);
        x.setLastUpdated(Instant.now());
        return x;
      });

      setter.set(ne, value);
    }

    log.info("MV36 NE inventory column={} rows={}", columnName, values.size());
  }

  private Map<String, String> walkColumn(OID baseOid, String columnName) throws IOException {
    Map<String, String> result = new LinkedHashMap<>();

    OID nextOid = new OID(baseOid);
    Target<UdpAddress> target = buildTarget();

    while (true) {
      PDU pdu = new PDU();
      pdu.setType(PDU.GETBULK);
      pdu.setNonRepeaters(0);
      pdu.setMaxRepetitions(maxRepetitions);
      pdu.add(new VariableBinding(nextOid));

      ResponseEvent<UdpAddress> event = snmp.send(pdu, target);

      if (event == null || event.getResponse() == null) {
        throw new IOException("SNMP timeout/no response while walking "
            + columnName + " baseOid=" + baseOid);
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

        String value = variableToString(vb.getVariable());
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
    String address = "udp:" + host + "/" + port;

    CommunityTarget<UdpAddress> target = new CommunityTarget<>();
    target.setCommunity(new OctetString(community));
    target.setAddress((UdpAddress) GenericAddress.parse(address));
    target.setVersion(SnmpConstants.version2c);
    target.setTimeout(timeoutMs);
    target.setRetries(retries);

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

  private static String variableToString(Variable v) {
    if (v == null) {
      return null;
    }

    if (v instanceof OctetString os) {
      return os.toString().trim();
    }

    if (v instanceof Integer32 i) {
      return String.valueOf(i.getValue());
    }

    return v.toString().trim();
  }

  @FunctionalInterface
  private interface NeSetter {
    void set(Mv36NetworkElement ne, String value);
  }
}