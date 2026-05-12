package gr.ote.rdnoc.alarm.service.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.sinks")
public class SinksProperties {

  public static class Channel {
    /** "kafka" or "file" */
    private String type = "kafka";
    /** Kafka topic name (when type=kafka) */
    private String topic;
    /** Absolute path to file (when type=file) */
    private String file;

    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
    public String getTopic() { return topic; }
    public void setTopic(String topic) { this.topic = topic; }
    public String getFile() { return file; }
    public void setFile(String file) { this.file = file; }
  }

  private Channel output = new Channel();
  private Channel dlt = new Channel();
  private Channel error = new Channel();

  public Channel getOutput() { return output; }
  public void setOutput(Channel output) { this.output = output; }
  public Channel getDlt() { return dlt; }
  public void setDlt(Channel dlt) { this.dlt = dlt; }
  public Channel getError() { return error; }
  public void setError(Channel error) { this.error = error; }
}
