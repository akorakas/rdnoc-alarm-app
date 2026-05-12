package gr.ote.rdnoc.alarm.service.pipeline.steps;

import java.util.regex.Pattern;

import gr.ote.rdnoc.alarm.service.pipeline.TransformContext;
import gr.ote.rdnoc.alarm.service.pipeline.TransformStep;

public class RegexExtractStep implements TransformStep {
  private final String sourceKey, targetKey, fallbackKey;
  private final Pattern pattern;
  private final int group;

  public RegexExtractStep(String sourceKey, String regex, int group, String targetKey, String fallbackKey) {
    this.sourceKey = sourceKey; this.targetKey = targetKey; this.fallbackKey = fallbackKey;
    this.pattern = Pattern.compile(regex, Pattern.DOTALL); this.group = group;
  }

  @Override public void apply(TransformContext ctx) {
    Object v = ctx.get(sourceKey);
    String out = null;
    if (v instanceof String s) {
      var m = pattern.matcher(s);
      if (m.find()) out = m.group(group).trim();
    }
    if (out == null && fallbackKey != null) {
      Object f = ctx.get(fallbackKey);
      out = (f == null) ? null : f.toString().replace("\r", "").trim();
    }
    ctx.put(targetKey, out);
  }
}
