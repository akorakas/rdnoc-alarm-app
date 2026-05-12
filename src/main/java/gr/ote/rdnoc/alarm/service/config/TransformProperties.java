package gr.ote.rdnoc.alarm.service.config;

import java.util.List;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "transform")
public class TransformProperties {

  private String placeholder = "{EVENT.RECOVERY.DATE} {EVENT.RECOVERY.TIME}";
  private boolean validateOnStart = true;

  // Two pipelines
  private Source kafka = new Source();
  private Source rest  = new Source();

  public String getPlaceholder() { return placeholder; }
  public void setPlaceholder(String placeholder) { this.placeholder = placeholder; }

  public boolean isValidateOnStart() { return validateOnStart; }
  public void setValidateOnStart(boolean validateOnStart) { this.validateOnStart = validateOnStart; }

  public Source getKafka() { return kafka; }
  public void setKafka(Source kafka) { this.kafka = kafka; }

  public Source getRest() { return rest; }
  public void setRest(Source rest) { this.rest = rest; }

  public static class Source {
    private List<Step> pipeline;

    public List<Step> getPipeline() { return pipeline; }
    public void setPipeline(List<Step> pipeline) { this.pipeline = pipeline; }
  }

  public static class Step {
    // common
    private String type;

    // extract
    private Map<String, String> mappings;
    private String fromVar;
    private Boolean failOnMissing;
    private Boolean failOnBadJson;

    // update
    private List<String> stripCr;
    private List<ComputeAssignment> compute;

    // regexExtract
    private String source;
    private String pattern;
    private Integer group;
    private String target;     // ✅ single "target" field (used by multiple step types)
    private String fallback;

    // flatten
    private List<String> roots;
    private List<String> includeTop;

    // hash
    private String algorithm;
    private List<String> fields;

    // template
    private String template;

    // optional future step
    private List<String> required;

    public String getType() { return type; }
    public void setType(String type) { this.type = type; }

    public Map<String, String> getMappings() { return mappings; }
    public void setMappings(Map<String, String> mappings) { this.mappings = mappings; }

    public String getFromVar() { return fromVar; }
    public void setFromVar(String fromVar) { this.fromVar = fromVar; }

    public Boolean getFailOnMissing() { return failOnMissing; }
    public void setFailOnMissing(Boolean failOnMissing) { this.failOnMissing = failOnMissing; }

    public Boolean getFailOnBadJson() { return failOnBadJson; }
    public void setFailOnBadJson(Boolean failOnBadJson) { this.failOnBadJson = failOnBadJson; }

    public List<String> getStripCr() { return stripCr; }
    public void setStripCr(List<String> stripCr) { this.stripCr = stripCr; }

    public List<ComputeAssignment> getCompute() { return compute; }
    public void setCompute(List<ComputeAssignment> compute) { this.compute = compute; }

    public String getSource() { return source; }
    public void setSource(String source) { this.source = source; }

    public String getPattern() { return pattern; }
    public void setPattern(String pattern) { this.pattern = pattern; }

    public Integer getGroup() { return group; }
    public void setGroup(Integer group) { this.group = group; }

    public String getTarget() { return target; }
    public void setTarget(String target) { this.target = target; }

    public String getFallback() { return fallback; }
    public void setFallback(String fallback) { this.fallback = fallback; }

    public List<String> getRoots() { return roots; }
    public void setRoots(List<String> roots) { this.roots = roots; }

    public List<String> getIncludeTop() { return includeTop; }
    public void setIncludeTop(List<String> includeTop) { this.includeTop = includeTop; }

    public String getAlgorithm() { return algorithm; }
    public void setAlgorithm(String algorithm) { this.algorithm = algorithm; }

    public List<String> getFields() { return fields; }
    public void setFields(List<String> fields) { this.fields = fields; }

    public String getTemplate() { return template; }
    public void setTemplate(String template) { this.template = template; }

    public List<String> getRequired() { return required; }
    public void setRequired(List<String> required) { this.required = required; }
  }

  public static class ComputeAssignment {
    private String set;
    private String expr;

    public String getSet() { return set; }
    public void setSet(String set) { this.set = set; }

    public String getExpr() { return expr; }
    public void setExpr(String expr) { this.expr = expr; }
  }
}
