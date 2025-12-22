package gr.ote.atlas.events.models;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class TransportEnrichment {
    private String subnetworkId;
    private String subnetworkName;
    private String linkId;
    private String linkName;
}
