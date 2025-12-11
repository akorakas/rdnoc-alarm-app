package gr.ote.atlas.events.emsspecificevents;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.util.Map;

@Getter
@Setter
@NoArgsConstructor
@ToString(callSuper = true)
public class TelegrafGenericEvent extends SystemSpecificEvent{
    private Map<String, String> fields;
    private Map<String, String> tags;
    private Long timestamp;
}
