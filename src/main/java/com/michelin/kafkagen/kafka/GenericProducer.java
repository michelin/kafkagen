package com.michelin.kafkagen.kafka;

import com.michelin.kafkagen.config.KafkagenConfig;
import com.michelin.kafkagen.models.Dataset;
import com.michelin.kafkagen.models.Record;
import io.confluent.avro.random.generator.Generator;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import jakarta.enterprise.context.ApplicationScoped;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.stream.LongStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;

@Slf4j
@ApplicationScoped
public class GenericProducer {

    private final Random random = new Random();

    /**
     * Produce records from a list of datasets (e.g. records for different topics)
     *
     * @param datasets    - the datasets to produce
     * @param maxInterval - the max interval in ms between 2 records
     * @param context     - the user's context
     */
    public void produce(List<Dataset> datasets, Integer maxInterval, KafkagenConfig.Context context) {
        datasets.forEach(d -> {
            produce(d.getTopic(), d, maxInterval, context);
        });
    }

    /**
     * Produce records from a list of datasets (e.g. records for different topics)
     *
     * @param topicName   - the topic to insert records
     * @param dataset     - the single dataset to produce
     * @param maxInterval - the max interval in ms between 2 records
     * @param context     - the user's context
     */
    public void produce(String topicName, Dataset dataset, Integer maxInterval, KafkagenConfig.Context context) {
        // Get the producer with the right serializer depending on the key type (String / Avro)
        var producer = getProducer(context, dataset.getKeySerializer(), dataset.getValueSerializer());

        // For each message, build the key/value avro message and send it to the topic
        dataset.getRecords().forEach(message -> {
            var record = new ProducerRecord<>(topicName,
                null,
                message.getTimestamp(),
                message.getKey(),
                message.getValue());

            if (message.getHeaders() != null) {
                message.getHeaders().forEach((k, v) -> record.headers().add(k, v.getBytes()));
            }

            producer.send(record, (recordMetadata, e) -> {
                if (e != null) {
                    log.debug("", e);
                    throw new RuntimeException(e);
                }
            });

            if (maxInterval != null) {
                try {
                    Thread.sleep(random.nextInt(maxInterval));
                } catch (InterruptedException e) {
                    log.debug("", e);
                }
            }
        });
    }

    public void producerRandom(String topicName, String avroFile, Long iterations, Integer maxInterval,
                               KafkagenConfig.Context context) {
        var schemaParser = new Schema.Parser();
        Schema schema = null;

        try (var stream = new FileInputStream(avroFile)) {
            schema = schemaParser.parse(stream);
        } catch (Exception e) {
            log.debug("", e);
        }

        var generator = new Generator.Builder()
            .generation(iterations)
            .schema(schema)
            .build();

        var records = new ArrayList<Record>();
        LongStream.range(0, iterations)
            .forEach(i ->
                records.add(Record.builder()
                    .value(generator.generate())
                    .build()) // JSON
            );

        produce(topicName,
            Dataset.builder()
                .topic(topicName)
                .records(records)
                .keySerializer(StringSerializer.class)
                .valueSerializer(KafkaAvroSerializer.class).build(),
            maxInterval, context);
    }

    /**
     * Build a Kafka producer.
     *
     * @param context         - the user's context
     * @param keySerializer   - the key serializer to use
     * @param valueSerializer - the value serializer to use
     * @return a Kafak producer
     */
    @SuppressWarnings("rawtypes")
    private KafkaProducer<Object, Object> getProducer(KafkagenConfig.Context context,
                                                      Class<? extends Serializer> keySerializer,
                                                      Class<? extends Serializer> valueSerializer) {
        final var settings = new Properties();
        settings.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
        settings.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);

        // Boostrap servers configuration
        settings.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, context.definition().bootstrapServers());
        context.definition().saslJaasConfig().ifPresentOrElse(saslJaasConfig -> {
            var securityProtocol = context.definition().securityProtocol();
            var saslMechanism = context.definition().saslMechanism();
            if (securityProtocol.isEmpty() || saslMechanism.isEmpty()) {
                throw new RuntimeException("security-protocol and sasl-mechanism settings are mandatory");
            }

            settings.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, securityProtocol.get());
            settings.put(SaslConfigs.SASL_MECHANISM, saslMechanism.get());
            settings.put(SaslConfigs.SASL_JAAS_CONFIG, saslJaasConfig);
        }, () -> settings.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT"));

        // Schema registry configuration is present
        context.definition().registryUrl().ifPresent(registryUrl -> {
            settings.put(AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION, true);
            settings.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, false);
            settings.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, registryUrl);

            var registryUsername = context.definition().registryUsername();
            var registryPassword = context.definition().registryPassword();

            if (registryUsername.isPresent() && registryPassword.isPresent()) {
                settings.put("basic.auth.credentials.source", "USER_INFO");
                settings.put("basic.auth.user.info",
                    String.format("%s:%s", registryUsername.get(), registryPassword.get()));
            }
        });

        context.definition().partitionerClass().ifPresent(partitionerClass -> {
            settings.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, partitionerClass);
        });

        final var producer = new KafkaProducer<>(settings);

        Runtime.getRuntime().addShutdownHook(new Thread(producer::close));

        return producer;
    }
}
