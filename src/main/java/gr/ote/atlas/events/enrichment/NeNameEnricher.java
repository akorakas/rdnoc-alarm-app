package gr.ote.atlas.events.enrichment;

import gr.ote.atlas.events.models.AffectedLocation;
import gr.ote.atlas.events.models.EnrichedData;
import gr.ote.atlas.events.models.TransportEnrichment;
import gr.ote.atlas.events.models.UnifiedEvent;

public class NeNameEnricher {

    public void enrich(UnifiedEvent evt) {
        if (evt == null) return;

        String neName = evt.getNeName();
        if (neName == null || neName.isBlank()) return;

        // Rule #1: subnetworkName = first part until underscore
        String subnetworkName = firstToken(neName, '_');

        // Rule #2: affectedLocation.name = part after last underscore, before '-'
        // Example: ANLAB-01_PSALIDILAB_9536-763 -> last token = 9536-763 -> 9536
        String lastUnderscoreToken = lastToken(neName, '_');
        String affectedLocationName = firstToken(lastUnderscoreToken, '-');

        // Ensure enrichedData exists
        EnrichedData enriched = evt.getEnrichedData();
        if (enriched == null) {
            enriched = new EnrichedData();
            evt.setEnrichedData(enriched);
        }

        // Ensure transport exists
        TransportEnrichment transport = enriched.getTransport();
        if (transport == null) {
            transport = new TransportEnrichment();
            enriched.setTransport(transport);
        }
        transport.setSubnetworkName(subnetworkName);

        // Ensure affectedLocation exists
        AffectedLocation loc = enriched.getAffectedLocation();
        if (loc == null) {
            loc = new AffectedLocation();
            enriched.setAffectedLocation(loc);
        }
        loc.setName(affectedLocationName);
    }

    private static String firstToken(String s, char delim) {
        if (s == null) return null;
        int p = s.indexOf(delim);
        return (p < 0) ? s : s.substring(0, p);
    }

    private static String lastToken(String s, char delim) {
        if (s == null) return null;
        int p = s.lastIndexOf(delim);
        return (p < 0) ? s : s.substring(p + 1);
    }
}
