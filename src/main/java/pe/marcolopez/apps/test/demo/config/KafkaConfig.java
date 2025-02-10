package pe.marcolopez.apps.test.demo.config;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

@ApplicationScoped
public class KafkaConfig {

  @ConfigProperty(name = "app.kafka.bootstrap-servers", defaultValue = "localhost:9092")
  String kafkaBrokers;

  @ConfigProperty(name = "app.kafka.consumer.group-id", defaultValue = "kafka-client-quarkus-consumer")
  String consumerGroupId;

  @ConfigProperty(name = "app.kafka.consumer.client-id", defaultValue = "kafka-client-quarkus-consumer-client")
  String consumerClientId;

  @ConfigProperty(name = "app.kafka.consumer.max-pool-records", defaultValue = "1000")
  String maxPoolRecords;

  @ConfigProperty(name = "app.kafka.consumer.offset-reset", defaultValue = "earliest")
  String offsetReset;

  @ConfigProperty(name = "app.kafka.consumer.auto-commit", defaultValue = "false")
  String autoCommit;

  @Produces
  public Consumer<String, String> getConsumer() {
    Properties props = new Properties();

    /*
     * Kafka Bootstrap
     */
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers);

    /*
     * With group id, kafka broker ensures that the same message is not consumed more then once by a
     * consumer group meaning a message can be only consumed by any one member a consumer group.
     *
     * Consumer groups is also a way of supporting parallel consumption of the data i.e. different consumers of
     * the same consumer group consume data in parallel from different partitions.
     */
    props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);

    /*
     * In addition to group.id, each consumer also identifies itself to the Kafka broker using consumer.id.
     * This is used by Kafka to identify the currently ACTIVE consumers of a particular consumer group.
     */
    props.put(ConsumerConfig.CLIENT_ID_CONFIG, consumerClientId + "-" + getHostname());

    /*
     * Deserializers for Keys and Values
     */
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

    /*
     * Pool size
     */
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPoolRecords);

    /*
     * If true the consumer's offset will be periodically committed in the background.
     * Disabled to allow commit or not under some circumstances
     */
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, autoCommit);

    /*
     * What to do when there is no initial offset in Kafka or if the current offset does not exist any more on the
     * server:
     *   earliest: automatically reset the offset to the earliest offset
     *   latest: automatically reset the offset to the latest offset
     */
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetReset);

    return new KafkaConsumer<>(props);
  }

  private String getHostname() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      return "UnknownHost";
    }
  }
}
