package gr.ote.rdnoc.alarm.service.pipeline.steps;

import gr.ote.rdnoc.alarm.service.pipeline.TransformContext;
import gr.ote.rdnoc.alarm.service.pipeline.TransformStep;
import org.apache.commons.text.StringSubstitutor;

public class TemplateStep implements TransformStep {
  private final String template;
  private final String target;

  public TemplateStep(String template, String target) {
    this.template = template;
    this.target = (target == null ? "$" : target);
  }

  @Override public void apply(TransformContext ctx) {
    var sub = new StringSubstitutor(name -> {
      Object v = ctx.get(name);
      return v == null ? "" : v.toString();
    });
    String s = sub.replace(template);
    if ("$".equals(target)) ctx.rendered = s; else ctx.put(target, s);
  }
}
