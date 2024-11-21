package com.michelin.kafkagen.kafka;

import com.michelin.kafkagen.config.KafkagenConfig;
import com.michelin.kafkagen.models.Record;
import com.michelin.kafkagen.services.SchemaService;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

@Slf4j
@ApplicationScoped
public class GenericConsumer {

    private final SchemaService schemaService;

    @Getter
    private KafkaConsumer<byte[], byte[]> kafkaConsumer;
    private Deserializer<?> keyDeserializer;
    private Deserializer<?> valueDeserializer;

    @Inject
    public GenericConsumer(SchemaService schemaService) {
        this.schemaService = schemaService;
    }

    /**
     * Poll Kafka ConsumerRecord from the Kafka topic until the end offsets are reached.
     * If no endOffsets is given, it will poll only once
     *
     * @param endOffsets The end offsets for each partition
     * @return The list of ConsumerRecord
     */
    public List<ConsumerRecord<byte[], byte[]>> pollConsumerRecords(Map<TopicPartition, Long> endOffsets) {
        var records = new ArrayList<ConsumerRecord<byte[], byte[]>>();

        if (endOffsets != null) {
            var topicEndOffsets = kafkaConsumer.endOffsets(kafkaConsumer.assignment());

            // Deal with input offsets that are greater than the end offsets
            // Replace by the real end offsets to avoid infinite loop
            endOffsets.forEach((tp, offset) -> {
                if (topicEndOffsets.get(tp) < offset) {
                    endOffsets.put(tp, topicEndOffsets.get(tp));
                }
            });
        }

        do {
            kafkaConsumer.poll(Duration.ofSeconds(5)).forEach(records::add);
        } while (endOffsets != null && endOffsets.entrySet().stream()
            .anyMatch(e -> kafkaConsumer.position(e.getKey()) < e.getValue()));

        return records;
    }

    /**
     * Poll Kafkagen Record from the Kafka topic until the end offsets are reached.
     * If no endOffsets is given, it will poll only once
     *
     * @param endOffsets The end offsets for each partition
     * @return The list of Record
     */
    public List<Record> pollRecords(Map<TopicPartition, Long> endOffsets) {
        return convert(pollConsumerRecords(endOffsets));
    }

    /**
     * Convert a list of Kafka ConsumerRecord to a list of Kafkagen Record.
     *
     * @param rawRecords The list of ConsumerRecord
     * @return The list of Record
     */
    public List<Record> convert(List<ConsumerRecord<byte[], byte[]>> rawRecords) {
        var records = new ArrayList<Record>();

        rawRecords.forEach(record -> {
            Object deserializedKey;
            Object deserializedValue;

            // Deserialize the key and value from byte[] to Object
            try {
                deserializedKey = keyDeserializer.deserialize(record.topic(), record.key());
                deserializedValue = valueDeserializer.deserialize(record.topic(), record.value());
            } catch (Exception e) {
                return;
            }

            // Build the Kafkagen record
            var headers = new HashMap<String, String>();
            record.headers().iterator()
                .forEachRemaining(h -> headers.put(h.key(), new String(h.value(), StandardCharsets.UTF_8)));

            records.add(Record.builder()
                .topic(record.topic())
                .timestamp(record.timestamp())
                .offset(record.offset())
                .headers(headers)
                .key(deserializedKey)
                .value(deserializedValue)
                .build());
        });

        return records;
    }

    public KafkaConsumer<byte[], byte[]> init(String topic, KafkagenConfig.Context context) {
        final var settings = new Properties();

        context.definition().groupIdPrefix()
            .ifPresentOrElse(groudIdPrefix -> settings.put(ConsumerConfig.GROUP_ID_CONFIG, groudIdPrefix + "kafkagen"),
                () -> settings.put(ConsumerConfig.GROUP_ID_CONFIG, "kafkagen"));
        settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        settings.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        settings.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        settings.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);

        // Boostrap servers
        settings.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, context.definition().bootstrapServers());
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

        // Schema registry
        context.definition().registryUrl().ifPresent(registryUrl -> {
            settings.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, registryUrl);

            var registryUsername = context.definition().registryUsername();
            var registryPassword = context.definition().registryPassword();
            if (registryUsername.isPresent() && registryPassword.isPresent()) {
                settings.put("basic.auth.credentials.source", "USER_INFO");
                settings.put("basic.auth.user.info",
                    String.format("%s:%s", registryUsername.get(), registryPassword.get()));
            }
        });

        settings.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 15000);

        kafkaConsumer = new KafkaConsumer<>(settings);

        ParsedSchema keySchema = schemaService.getLatestSchema(topic + "-key", context);
        ParsedSchema valueSchema = schemaService.getLatestSchema(topic + "-value", context);

        keyDeserializer = createDeserializer(keySchema);
        valueDeserializer = createDeserializer(valueSchema);

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaConsumer::close));

        return kafkaConsumer;
    }

    private Deserializer<?> createDeserializer(ParsedSchema schema) {
        if (schema == null) {
            return new StringDeserializer();
        } else {
            return switch (schema.schemaType()) {
                case "AVRO" -> switch (((AvroSchema) schema).rawSchema().getType()) {
                    case STRING -> new StringDeserializer();
                    // TODO: support the other types
                    default -> new KafkaAvroDeserializer(schemaService.getSchemaRegistryClient());
                };
                case "JSON" -> new KafkaJsonSchemaDeserializer<>(schemaService.getSchemaRegistryClient());
                case "PROTOBUF" -> new KafkaProtobufDeserializer<>(schemaService.getSchemaRegistryClient());
                default -> new StringDeserializer();
            };
        }
    }

    @PreDestroy
    public void close() {
        kafkaConsumer.close();
    }
}
