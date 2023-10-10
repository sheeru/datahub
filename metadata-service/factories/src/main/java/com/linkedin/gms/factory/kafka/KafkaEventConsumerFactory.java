package com.linkedin.gms.factory.kafka;

import com.linkedin.gms.factory.common.TopicConventionFactory;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.kafka.schemaregistry.AwsGlueSchemaRegistryFactory;
import com.linkedin.gms.factory.kafka.schemaregistry.InternalSchemaRegistryFactory;
import com.linkedin.gms.factory.kafka.schemaregistry.KafkaSchemaRegistryFactory;
import com.linkedin.gms.factory.kafka.schemaregistry.SchemaRegistryConfig;

import java.time.Duration;
import java.util.Arrays;
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
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

@Slf4j
@Configuration
@Import({KafkaSchemaRegistryFactory.class, AwsGlueSchemaRegistryFactory.class, InternalSchemaRegistryFactory.class})
public class KafkaEventConsumerFactory {


    private int kafkaEventConsumerConcurrency;

    @Autowired
    @Qualifier(TopicConventionFactory.TOPIC_CONVENTION_BEAN)
    private TopicConvention topicConvention;

    @Bean(name = "kafkaConsumerFactory")
    protected DefaultKafkaConsumerFactory<String, GenericRecord> createConsumerFactory(
            @Qualifier("configurationProvider") ConfigurationProvider provider) {
        kafkaEventConsumerConcurrency = provider.getKafka().getListener().getConcurrency();

        Map<String, Object> customizedProperties = buildCustomizedProperties(provider, topicConvention);

        return new DefaultKafkaConsumerFactory<>(customizedProperties);
    }

    @Bean(name = "duheKafkaConsumerFactory")
    protected DefaultKafkaConsumerFactory<String, GenericRecord> duheKafkaConsumerFactory(
            @Qualifier("configurationProvider") ConfigurationProvider provider) {

        Map<String, Object> customizedProperties = buildCustomizedProperties(provider, topicConvention);

        return new DefaultKafkaConsumerFactory<>(customizedProperties);
    }

    private static Map<String, Object> buildCustomizedProperties(ConfigurationProvider provider, TopicConvention topicConvention) {
//        KafkaProperties.Consumer consumerProps = baseKafkaProperties.getConsumer();
//
//        // Specify (de)serializers for record keys and for record values.
//        consumerProps.setKeyDeserializer(StringDeserializer.class);
//        // Records will be flushed every 10 seconds.
//        consumerProps.setEnableAutoCommit(true);
//        consumerProps.setAutoCommitInterval(Duration.ofSeconds(10));
//
//        // KAFKA_BOOTSTRAP_SERVER has precedence over SPRING_KAFKA_BOOTSTRAP_SERVERS
//        if (kafkaConfiguration.getBootstrapServers() != null && kafkaConfiguration.getBootstrapServers().length() > 0) {
//            consumerProps.setBootstrapServers(Arrays.asList(kafkaConfiguration.getBootstrapServers().split(",")));
//        } // else we rely on KafkaProperties which defaults to localhost:9092
//
//        Map<String, Object> customizedProperties = baseKafkaProperties.buildConsumerProperties();
//        customizedProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, schemaRegistryConfig.getDeserializer());
//
//        // Override KafkaProperties with SchemaRegistryConfig only for non-empty values
//        schemaRegistryConfig.getProperties().entrySet()
//                .stream()
//                .filter(entry -> entry.getValue() != null && !entry.getValue().toString().isEmpty())
//                .forEach(entry -> customizedProperties.put(entry.getKey(), entry.getValue()));

        return getKafkaConsumerProperties(provider, topicConvention);
    }

    @Bean(name = "kafkaEventConsumer")
    protected KafkaListenerContainerFactory<?> createInstance(
            @Qualifier("kafkaConsumerFactory") DefaultKafkaConsumerFactory<String, GenericRecord> kafkaConsumerFactory) {

        ConcurrentKafkaListenerContainerFactory<String, GenericRecord> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(kafkaConsumerFactory);
        factory.setContainerCustomizer(new ThreadPoolContainerCustomizer());
        factory.setConcurrency(kafkaEventConsumerConcurrency);

        log.info(String.format("Event-based KafkaListenerContainerFactory built successfully. Consumer concurrency = %s",
                kafkaEventConsumerConcurrency));

        return factory;
    }

    @Bean(name = "duheKafkaEventConsumer")
    protected KafkaListenerContainerFactory<?> duheKafkaEventConsumer(
            @Qualifier("duheKafkaConsumerFactory") DefaultKafkaConsumerFactory<String, GenericRecord> kafkaConsumerFactory) {

        ConcurrentKafkaListenerContainerFactory<String, GenericRecord> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(kafkaConsumerFactory);
        factory.setContainerCustomizer(new ThreadPoolContainerCustomizer());
        factory.setConcurrency(1);

        log.info("Event-based DUHE KafkaListenerContainerFactory built successfully. Consumer concurrency = 1");
        return factory;
    }

    public static Map<String,Object>  getKafkaConsumerProperties(ConfigurationProvider provider, TopicConvention topicConvention) {

        Map<String, Object> properties = getPaypalKafaConsumerProperties(provider);

        // Below code shows how to create KafkaConfigServiceInput and KafkaConsumer object
        ClientEnvironment clientEnv = new ClientEnvironment("java", "kafka-clients",
                "2.2.1-PAYPAL-v4.1");
        ClientRole clientRole = ClientRole.CONSUMER;
        String clientColo = "dev51";	//for qa
        String host = "msmaster.qa.paypal.com";
        String port = "22643";
        String clientId = properties.get("client.id").toString();

        ConfigServiceInput kafkaConfigServiceInput = new KafkaConfigServiceInputBuilder()
                .withColo(clientColo)
                .withClientId(clientId)
                .withClientRole(clientRole)
                .withClientEnvironment(clientEnv)
                .withTopics(DataHubKafkaProducerFactory.getTopics(topicConvention))
                .enableSASL(true)
                .get();

        // Using ConfigServiceHelper to get Kafka configs
        Map<String, Object> result = null;
        ConfigClient configClient = new KafkaConfigClient(host, port, 0);
        try {
            result= configClient.getConfigs(kafkaConfigServiceInput, properties);
        } catch (ConfigClientException e) {
            throw new RuntimeException(e);
        }

        return result;
    }

    @NotNull
    private static Map<String, Object> getPaypalKafaConsumerProperties(ConfigurationProvider provider) {
        Map<String,Object>  properties = new HashMap<>();
        properties.put("group.id", "datahub-consumer");
        properties.put("client.id", "datahub-consumer");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "com.linkedin.gms.factory.kafka.serializers.KafkaAvroDeserializer");
        properties.put("max.poll.records", 100);

        properties.put("paypal.nonraptor.client", "true");
        // This is one of the ways of adding appcontext. Alternative ways to load appcontext can be found at:
        // https://github.paypal.com/Messaging-R/kafka-security#kafka-security-lib-configuration-file
        properties.put("ssl.paypal.client.appcontext", provider.getKeymaker().getAppcontext());
        return properties;
    }
}