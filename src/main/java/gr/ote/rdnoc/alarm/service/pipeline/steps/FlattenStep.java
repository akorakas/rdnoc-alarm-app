package gr.ote.rdnoc.alarm.service.pipeline.steps;

import java.util.List;
import java.util.Map;

import gr.ote.rdnoc.alarm.service.pipeline.TransformContext;
import gr.ote.rdnoc.alarm.service.pipeline.TransformStep;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

public class FlattenStep implements TransformStep {
  private static final ObjectMapper M = new ObjectMapper();
  private final String[] roots;
  private final String[] includeTop;
  private final String target;

  public FlattenStep(List<String> roots, List<String> includeTop, String target) {
    this.roots = (roots == null) ? new String[0] : roots.toArray(String[]::new);
    this.includeTop = (includeTop == null) ? new String[0] : includeTop.toArray(String[]::new);
    this.target = target;
  }

  @Override
  public void apply(TransformContext ctx) {
    ObjectNode out = M.createObjectNode();
    JsonNode root = ctx.root;

    for (String r : roots) {
      JsonNode n = root.path(r);
      if (!n.isMissingNode() && !n.isNull()) {
        flattenInto(r, n, out);
      }
    }

    for (String t : includeTop) {
      if (root.has(t)) out.set(t, root.get(t));
    }

    // store as JSON string for direct injection into the final template
    ctx.put(target, out.toString());
  }

  private static void flattenInto(String prefix, JsonNode node, ObjectNode target) {
    if (node.isObject()) {
      for (Map.Entry<String, JsonNode> e : node.properties()) {
        String next = prefix.isEmpty() ? e.getKey() : prefix + "." + e.getKey();
        flattenInto(next, e.getValue(), target);
      }
    } else if (node.isArray()) {
      int i = 0;
      for (JsonNode el : node) {
        flattenInto(prefix + "[" + i++ + "]", el, target);
      }
    } else {
      target.set(prefix, node);
    }
  }
}
