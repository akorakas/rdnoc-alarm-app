package gr.ote.atlas.events.models;

import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter @Setter
public class EnrichedData {
    private AffectedLocation affectedLocation;
    private TransportEnrichment transport;
    private String affectedSite;
    private String affectedController;
    private String affectedCell;
    private String affectedCellId;
    private List<String> affectedTechnologies;
    private String probableCause;
}
