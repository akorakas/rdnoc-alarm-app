package gr.ote.rdnoc.alarm.nsp;

import org.apache.kafka.clients.admin.AdminClient;

public interface NspKafkaAdminClientFactory {

  AdminClient create(String bootstrapServers);
}