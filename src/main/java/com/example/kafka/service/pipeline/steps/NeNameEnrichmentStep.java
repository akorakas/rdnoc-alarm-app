package com.example.kafka.service.pipeline.steps;

import com.example.kafka.service.pipeline.TransformContext;
import com.example.kafka.service.pipeline.TransformStep;

import gr.ote.atlas.events.models.AffectedLocation;
import gr.ote.atlas.events.models.EnrichedData;
import gr.ote.atlas.events.models.TransportEnrichment;

public class NeNameEnrichmentStep implements TransformStep {

  // which ctx key to read (default "neName")
  private final String sourceKey;
  // which ctx key to write (default "enrichedData")
  private final String targetKey;

  public NeNameEnrichmentStep() {
    this("neName", "enrichedData"); // store typed object
  }

  public NeNameEnrichmentStep(String sourceKey, String targetKey) {
    this.sourceKey = sourceKey;
    this.targetKey = targetKey;
  }

  @Override
  public void apply(TransformContext ctx) {
    // ✅ If some previous step already populated enrichment, do nothing
    if (ctx.get(targetKey) != null) {
      return;
    }

    Object v = ctx.get(sourceKey);

    // ✅ DELETE events or missing neName: don't fail, don't overwrite
    if (!(v instanceof String neName) || neName.isBlank()) {
      return;
    }

    String subnetworkName = firstToken(neName, '_');
    String lastUnderscoreToken = lastToken(neName, '_');                // e.g. 9536-763
    String affectedLocationName = firstToken(lastUnderscoreToken, '-'); // e.g. 9536

    // optional convenience vars
    ctx.put("subnetworkName", subnetworkName);
    ctx.put("affectedLocationName", affectedLocationName);

    // ✅ Build typed enrichment object
    EnrichedData ed = new EnrichedData();

    AffectedLocation loc = new AffectedLocation();
    loc.setName(affectedLocationName);
    ed.setAffectedLocation(loc);

    TransportEnrichment tr = new TransportEnrichment();
    tr.setSubnetworkName(subnetworkName);
    ed.setTransport(tr);

    ctx.put(targetKey, ed);
  }

  private static String firstToken(String s, char delim) {
    int p = s.indexOf(delim);
    return (p < 0) ? s : s.substring(0, p);
  }

  private static String lastToken(String s, char delim) {
    int p = s.lastIndexOf(delim);
    return (p < 0) ? s : s.substring(p + 1);
  }
}
