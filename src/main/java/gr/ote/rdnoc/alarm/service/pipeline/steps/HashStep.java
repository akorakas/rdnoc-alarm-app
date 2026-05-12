package gr.ote.rdnoc.alarm.service.pipeline.steps;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.List;

import gr.ote.rdnoc.alarm.service.pipeline.TransformContext;
import gr.ote.rdnoc.alarm.service.pipeline.TransformStep;

public class HashStep implements TransformStep {
  private final String algorithm;
  private final String[] fields;
  private final String target;

  public HashStep(String algorithm, List<String> fields, String target) {
    this.algorithm = (algorithm == null ? "MD5" : algorithm);
    this.fields = fields.toArray(String[]::new);
    this.target = target;
  }

  @Override public void apply(TransformContext ctx) throws Exception {
    MessageDigest md = MessageDigest.getInstance(algorithm);
    StringBuilder sb = new StringBuilder();
    for (String f : fields) {
      Object v = ctx.get(f);
      if (sb.length() > 0) sb.append('|');
      sb.append(v == null ? "" : v.toString());
    }
    byte[] d = md.digest(sb.toString().getBytes(StandardCharsets.UTF_8));
    StringBuilder hex = new StringBuilder();
    for (byte b : d) hex.append(String.format("%02x", b));
    ctx.put(target, hex.toString());
  }
}
