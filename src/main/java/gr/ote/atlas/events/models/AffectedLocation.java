package gr.ote.atlas.events.models;

import lombok.Getter;
import lombok.Setter;

@Getter @Setter
public class AffectedLocation {
    private String inventoryId;
    private String name;
    private Double longitude;
    private Double latitude;

}
