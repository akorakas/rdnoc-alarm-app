package gr.ote.rdnoc.alarm.service.pipeline;

public interface TransformStep {
  void apply(TransformContext ctx) throws Exception;
}
