package gr.ote.rdnoc.alarm.sink;

import java.util.Map;

public interface ChannelSender {
  /** Send a record; key may be null; headers are optional. */
  void send(String key, String payload, Map<String, String> headers);
}
