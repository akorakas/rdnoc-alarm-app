package com.example.kafka.debug;

import gr.ote.atlas.events.emsspecificevents.NokiaAtnoiAlarm;
import gr.ote.atlas.events.models.UnifiedEvent;

public class UnifiedEventCompileCheck {
  public static UnifiedEvent build() {
    UnifiedEvent u = new UnifiedEvent();
    u.setSourceEvent(new NokiaAtnoiAlarm());
    return u;
  }
}
