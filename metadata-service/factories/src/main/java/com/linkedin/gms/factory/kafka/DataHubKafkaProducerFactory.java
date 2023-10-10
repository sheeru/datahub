package com.linkedin.gms.factory.kafka;

import com.linkedin.gms.factory.common.TopicConventionFactory;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.kafka.schemaregistry.AwsGlueSchemaRegistryFactory;
import com.linkedin.gms.factory.kafka.schemaregistry.InternalSchemaRegistryFactory;
import com.linkedin.gms.factory.kafka.schemaregistry.KafkaSchemaRegistryFactory;
import com.linkedin.metadata.spring.YamlPropertySourceFactory;

import java.util.HashMap;
import java.util.Map;

import com.linkedin.mxe.TopicConvention;
import com.paypal.infra.messaging.kafka.config.builder.KafkaConfigServiceInputBuilder;
import com.paypal.infra.messaging.kafka.config.client.ConfigClient;
import com.paypal.infra.messaging.kafka.config.client.KafkaConfigClient;
import com.paypal.infra.messaging.kafka.config.exception.ConfigClientException;
import com.paypal.infra.messaging.kafka.config.input.ConfigServiceInput;
import com.paypal.infra.messaging.kafka.config.model.ClientEnvironment;
import com.paypal.infra.messaging.kafka.config.model.ClientRole;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;


@Configuration
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
@Import({KafkaSchemaRegistryFactory.class, AwsGlueSchemaRegistryFactory.class, InternalSchemaRegistryFactory.class})
public class DataHubKafkaProducerFactory {

  @Autowired
  @Qualifier(TopicConventionFactory.TOPIC_CONVENTION_BEAN)
  private TopicConvention topicConvention;

  @Bean(name = "kafkaProducer")
  protected Producer<String, IndexedRecord> createInstance(@Qualifier("configurationProvider") ConfigurationProvider
      provider) {
    return new KafkaProducer<>(buildProducerProperties(provider, topicConvention)
    );
  }

  public static Map<String, Object> buildProducerProperties(ConfigurationProvider configurationProvider,
                                                            TopicConvention topicConvention) {

    Map<String, Object> kafkaProperties = getKafkaProducerProperties(configurationProvider);


    // Below code shows how to create KafkaConfigServiceInput and KafkaProducer object
    ClientEnvironment clientEnv = new ClientEnvironment("java", "kafka-clients",
            "2.2.1-PAYPAL-v4.1");
    ClientRole clientRole = ClientRole.PRODUCER;
    String clientColo = "dev51";	//for qa
    String host = "msmaster.qa.paypal.com";
    String port = "22643";
    String clientId = kafkaProperties.get("client.id").toString();


    ConfigServiceInput kafkaConfigServiceInput = new KafkaConfigServiceInputBuilder()
            .withColo(clientColo)
            .withClientId(clientId)
            .withClientRole(clientRole)
            .withTopics(getTopics(topicConvention))
            .withClientEnvironment(clientEnv)
            .withSecurityZone("dev51")
            .withEnvironment("qa")
            .enableSASL(true)
            .get();

    // Using ConfigServiceHelper to get Kafka configs
    ConfigClient configClient = new KafkaConfigClient(host, port, 0);
    Map<String, Object> result = null;
    try {
      result = configClient.getConfigs(kafkaConfigServiceInput, kafkaProperties);
    } catch (ConfigClientException e) {
      throw new RuntimeException(e);
    }

    return result;
  }

  @NotNull
  private static Map<String, Object> getKafkaProducerProperties(ConfigurationProvider configurationProvider) {
    Map<String, Object> properties = new HashMap<>();

    properties.put("acks", "1");
    // client.id must be unique for each producer/consumer instance!
    properties.put("client.id", "datahub-consumer");
    properties.put("retries", 10);
    properties.put("linger.ms", 10);
    properties.put("buffer.memory", 33554432);
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    properties.put("value.serializer", "com.linkedin.gms.factory.kafka.serializers.KafkaAvroSerializer");

    properties.put("paypal.nonraptor.client", "true");
    // This is one of the ways of adding appcontext. Alternative ways to load appcontext can be found at:
    // https://github.paypal.com/Messaging-R/kafka-security#kafka-security-lib-configuration-file
    properties.put("ssl.paypal.client.appcontext", configurationProvider.getKeymaker().getAppcontext());
    return properties;
  }

  public static String getTopics(TopicConvention topicConvention) {
    return topicConvention.getPlatformEventTopicName() + "," +
            topicConvention.getDataHubUpgradeHistoryTopicName() + "," +
//            topicConvention.getFailedMetadataChangeProposalTopicName() + "," +
            topicConvention.getMetadataChangeProposalTopicName() + "," +
            topicConvention.getMetadataChangeLogVersionedTopicName() + "," +
            topicConvention.getMetadataChangeLogTimeseriesTopicName();
  }

}