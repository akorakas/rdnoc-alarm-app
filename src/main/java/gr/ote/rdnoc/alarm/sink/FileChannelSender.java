package gr.ote.rdnoc.alarm.sink;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

public class FileChannelSender implements ChannelSender {
  private final Path path;
  private final ReentrantLock lock = new ReentrantLock();

  public FileChannelSender(String filePath) {
    this.path = Paths.get(filePath);
  }

  @Override
  public void send(String key, String payload, Map<String, String> headers) {
    try {
      Files.createDirectories(path.getParent());
      StringBuilder line = new StringBuilder();
      // Write a small envelope + payload as NDJSON (one line per message)
      line.append("{\"ts\":\"").append(Instant.now()).append("\"");
      if (key != null) line.append(",\"key\":").append(jsonQuote(key));
      if (headers != null && !headers.isEmpty()) {
        line.append(",\"headers\":{");
        boolean first = true;
        for (var e : headers.entrySet()) {
          if (!first) line.append(',');
          first = false;
          line.append(jsonQuote(e.getKey())).append(':').append(jsonQuote(e.getValue()));
        }
        line.append('}');
      }
      line.append(",\"payload\":").append(payload).append("}\n"); // payload already JSON string

      byte[] bytes = line.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
      lock.lock();
      try {
        Files.write(path, bytes, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
      } finally {
        lock.unlock();
      }
    } catch (IOException ex) {
      throw new RuntimeException("Failed to write to " + path, ex);
    }
  }

  private static String jsonQuote(String s) {
    if (s == null) return "null";
    // minimal escaper
    String esc = s.replace("\\", "\\\\").replace("\"", "\\\"");
    return "\"" + esc + "\"";
  }
}
