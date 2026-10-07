package gr.ote.rdnoc.alarm.boot;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.List;

import org.apache.kafka.clients.admin.AdminClient;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;

import gr.ote.rdnoc.alarm.service.config.SinksProperties;

/**
 * The INPUT AdminClient must not be created eagerly: in dynamic (NSP) mode it is unused, and an
 * unreachable spring.kafka.consumer.bootstrap-servers (the standby site) would otherwise produce
 * a background client that retries and logs several times per second.
 */
class RequireTopicsLazyInputAdminTest {

  private ApplicationContextRunner runner() {
    KafkaProperties kp = new KafkaProperties();
    kp.getConsumer().setBootstrapServers(List.of("10.255.255.1:9192")); // unreachable on purpose

    return new ApplicationContextRunner()
        .withUserConfiguration(RequireTopics.class)
        .withBean(KafkaProperties.class, () -> kp)
        .withBean("outputAdminClient", AdminClient.class, () -> mock(AdminClient.class))
        .withBean(KafkaListenerEndpointRegistry.class, () -> mock(KafkaListenerEndpointRegistry.class))
        .withBean(SinksProperties.class, SinksProperties::new);
  }

  @Test
  void inputAdminClientIsNotCreatedAtStartup() {
    runner().run(ctx -> {
      assertThat(ctx).hasNotFailed();
      assertThat(ctx.getBeanFactory().containsSingleton("inputAdminClient")).isFalse();
    });
  }

  @Test
  void inputAdminClientIsCreatedWhenRequested() {
    runner().run(ctx -> {
      ObjectProvider<AdminClient> p = ctx.getBeanProvider(AdminClient.class);
      AdminClient input = (AdminClient) ctx.getBean("inputAdminClient");
      assertThat(input).isNotNull();
      assertThat(ctx.getBeanFactory().containsSingleton("inputAdminClient")).isTrue();
      assertThat(p).isNotNull();
    });
  }
}
