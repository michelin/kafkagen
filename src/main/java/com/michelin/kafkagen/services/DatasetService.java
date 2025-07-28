/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.michelin.kafkagen.services;

import static java.util.stream.Collectors.toList;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.michelin.kafkagen.config.KafkagenConfig;
import com.michelin.kafkagen.exceptions.FileFormatException;
import com.michelin.kafkagen.kafka.GenericConsumer;
import com.michelin.kafkagen.models.Dataset;
import com.michelin.kafkagen.models.Record;
import com.michelin.kafkagen.utils.AvroUtils;
import com.michelin.kafkagen.utils.BytesToStringSerializer;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaUtils;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaUtils;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.util.Utf8;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

@Slf4j
@Singleton
public class DatasetService {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final SchemaService schemaService;
    private final AssertService assertService;
    private final GenericConsumer genericConsumer;

    @Inject
    public DatasetService(SchemaService schemaService, AssertService assertService, GenericConsumer genericConsumer) {
        this.schemaService = schemaService;
        this.assertService = assertService;
        this.genericConsumer = genericConsumer;
    }

    /**
     * Create datasets from a dataset file that contains records for different topics.
     *
     * @param datasetFile - a dataset file
     * @param context     - the user's context
     * @return a list of datasets to produce
     */
    public List<Dataset> getDataset(File datasetFile, KafkagenConfig.Context context) {
        var datasets = new ArrayList<Dataset>();
        List<Record> rawRecords = getRawRecord(datasetFile);

        // Because there is not a single topic to produce in, creates a dataset by record
        rawRecords.forEach(record -> {
            ParsedSchema keySchema = schemaService.getSchema(
                record.getTopic() + "-key", Optional.ofNullable(record.getKeySubjectVersion()), context);
            ParsedSchema valueSchema = schemaService.getSchema(
                record.getTopic() + "-value", Optional.ofNullable(record.getValueSubjectVersion()), context);

            // If we have at least one schema, proceed with AVRO/JSON/PROTOBUF
            if (valueSchema != null || keySchema != null) {
                switch (valueSchema != null ? valueSchema.schemaType() : keySchema.schemaType()) {
                    case "AVRO" -> datasets.add(getAvroDataset(record.getTopic(), (AvroSchema) keySchema,
                        (AvroSchema) valueSchema, List.of(record)));
                    case "JSON" -> datasets.add(getJsonRecords(record.getTopic(), (JsonSchema) keySchema,
                        (JsonSchema) valueSchema, List.of(record)));
                    case "PROTOBUF" -> datasets.add(getProtobufRecords(record.getTopic(), (ProtobufSchema) keySchema,
                        (ProtobufSchema) valueSchema, List.of(record)));
                }
            } else {
                // Otherwise, use plain text records
                datasets.add(Dataset.builder()
                    .topic(record.getTopic())
                    .records(List.of(buildPlainTextRecord(record)))
                    .keySerializer(StringSerializer.class)
                    .valueSerializer(StringSerializer.class)
                    .build());
            }
        });

        return datasets;
    }

    /**
     * Create datasets from a dataset file for a single topic.
     *
     * @param datasetFile - a dataset file
     * @param topic       - the topic to use for all the records
     * @param keySubjectVersion - the version of the key subject to use for the schemas
     * @param valueSubjectVersion - the version of the value subject to use for the schemas
     * @param context     - the user's context
     * @return a list of datasets to produce
     */
    public Dataset getDataset(File datasetFile, String topic, Optional<Integer> keySubjectVersion,
                              Optional<Integer> valueSubjectVersion, KafkagenConfig.Context context) {
        Dataset dataset = null;

        ParsedSchema keySchema = schemaService.getSchema(topic + "-key", keySubjectVersion, context);
        ParsedSchema valueSchema = schemaService.getSchema(topic + "-value", valueSubjectVersion, context);

        // If we have at least one schema, proceed with AVRO/JSON/PROTOBUF
        if (valueSchema != null || keySchema != null) {
            switch (valueSchema != null ? valueSchema.schemaType() : keySchema.schemaType()) {
                case "AVRO" -> dataset = getAvroDataset(topic, (AvroSchema) keySchema, (AvroSchema) valueSchema,
                    getRawRecord(datasetFile));
                case "JSON" -> dataset = getJsonRecords(topic, (JsonSchema) keySchema, (JsonSchema) valueSchema,
                    getRawRecord(datasetFile));
                case "PROTOBUF" ->
                    dataset = getProtobufRecords(topic, (ProtobufSchema) keySchema, (ProtobufSchema) valueSchema,
                        getRawRecord(datasetFile));
            }
        } else {
            // Otherwise, use plain text records
            dataset = Dataset.builder()
                .topic(topic)
                .records(getRawRecord(datasetFile).stream()
                    .map(this::buildPlainTextRecord)
                    .collect(toList()))
                .keySerializer(StringSerializer.class)
                .valueSerializer(StringSerializer.class)
                .build();
        }

        return dataset;
    }


    /**
     * Get Avro messages from a template file and a list of variables.
     *
     * @param templateFile - the file containing the records template
     * @param variables    - variables list to put in the template
     * @param topic        - name of the topic
     * @param context      - the user's context
     * @return Avro-formatted messages
     */
    @SuppressWarnings("unchecked")
    public Dataset getDatasetFromTemplate(String templateFile, Map<String, Map <String, Object>> variables,
                                          String topic, KafkagenConfig.Context context) {
        List<Record> rawRecords = getRawRecord(new File(templateFile));
        List<Record> enrichedRecords = new ArrayList<>();
        var templateSize = rawRecords.size();

        if (variables.size() % templateSize != 0) {
            throw new RuntimeException(
                "Discrepancies between the number of record in the template and the given variables");
        }

        AtomicInteger index = new AtomicInteger();

        variables.keySet().forEach(key -> {
            String clone;
            Record record;
            try {
                clone = objectMapper.writeValueAsString(rawRecords.get(index.getAndIncrement() % (rawRecords.size())));
                record = objectMapper.readValue(clone, Record.class);
            } catch (JsonProcessingException e) {
                log.debug("", e);
                throw new RuntimeException(e);
            }

            record.setKey(key);
            ((Map<String, Object>) record.getValue()).putAll(variables.get(key));

            enrichedRecords.add(record);
        });

        ParsedSchema keySchema = null;
        try {
            // Get key/value schema from registry to Avro serializations
            keySchema = schemaService.getLatestSchema(topic + "-key", context);
        } catch (Exception e) {
            log.trace("No key schema found for subject <{}>, continuing with String serialization", topic + "-key");
        }
        var valueSchema = schemaService.getLatestSchema(topic + "-value", context);

        return getAvroDataset(topic, (AvroSchema) keySchema, (AvroSchema) valueSchema, enrichedRecords);
    }

    public List<Record> getDatasetForKey(String topic, Object key, boolean prettyPrint,
                                         KafkagenConfig.Context context) {
        try (KafkaConsumer<?, ?> kafkaConsumer = genericConsumer.init(topic, context)) {
            var topicPartitions = kafkaConsumer.partitionsFor(topic).stream().map(partitionInfo ->
                new TopicPartition(partitionInfo.topic(), partitionInfo.partition())
            ).toList();

            kafkaConsumer.assign(topicPartitions);
            kafkaConsumer.seekToBeginning(topicPartitions);

            Map<TopicPartition, Long> endOffsets = kafkaConsumer.endOffsets(kafkaConsumer.assignment());

            var mapper = new ObjectMapper();
            try {
                key = mapper.readValue(key.toString(), Object.class);
            } catch (JsonProcessingException e) {
                // Do nothing, key is a string
            }
            Object finalKey = key;

            // Stop if we reached the end offset of all the partitions
            var polledRecords = genericConsumer.pollRecords(endOffsets);

            List<Record> records = new ArrayList<>(polledRecords.stream()
                .filter(r -> {
                    Object k;
                    try {
                        if (r.getKey() != null) {
                            k = r.getKey().getClass().isAssignableFrom(String.class)
                                ? r.getKey()
                                : mapper.readValue(r.getKey().toString(), Object.class);
                        } else {
                            k = null;
                        }

                        return assertService.match(finalKey, k, false);
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }

                }).toList());

            if (prettyPrint) {
                // If needed, pretty print the date and time fields
                records.forEach(DatasetService::prettyRecord);
            }

            return records;
        }
    }

    private static void prettyRecord(Record record) {
        if (record.getKey() instanceof GenericData.Record) {
            AvroUtils.prettyPrintDateAndTimeFields((GenericData.Record) record.getKey());
        }
        if (record.getValue() instanceof GenericData.Record) {
            AvroUtils.prettyPrintDateAndTimeFields((GenericData.Record) record.getValue());
        }
    }

    /**
     * Get a dataset from a topic.
     *
     * @param topic       - the topic to consume
     * @param partition   - the partition to consume
     * @param startOffset - the start offset to consume from
     * @param endOffset   - the end offset to consume to
     * @param offsets     - a list of offsets to consume
     * @param context     - the user's context
     * @return a list of records
     */
    public List<Record> getDatasetFromTopic(String topic, int partition, Long startOffset, Long endOffset,
                                            List<Long> offsets, boolean prettyPrint, KafkagenConfig.Context context) {
        List<ConsumerRecord<byte[], byte[]>> polledRecords;
        List<Record> records;

        try (KafkaConsumer<?, ?> kafkaConsumer = genericConsumer.init(topic, context)) {
            var topicPartition = new TopicPartition(topic, partition);
            kafkaConsumer.assign(List.of(topicPartition));

            // Offset is a range
            if (startOffset != null && endOffset != null) {
                kafkaConsumer.seek(topicPartition, startOffset);
                polledRecords = genericConsumer.pollConsumerRecords(
                        new HashMap<>(Map.of(topicPartition, endOffset)))
                    .stream()
                    .filter(record -> record.offset() <= endOffset)
                    .toList();
            } else if (offsets != null) {
                // Offset is a list of offsets, consume the partition from the first offset and filter the records
                kafkaConsumer.seek(topicPartition, offsets.getFirst());
                polledRecords = genericConsumer.pollConsumerRecords(
                        new HashMap<>(Map.of(topicPartition,
                            offsets.stream().max(Long::compareTo).orElse(offsets.getFirst()))))
                    .stream()
                    .filter(record -> offsets.contains(record.offset()))
                    .toList();
            } else if (startOffset != null) {
                // Offset is a single offset
                kafkaConsumer.seek(topicPartition, startOffset);
                polledRecords = genericConsumer.pollConsumerRecords(Map.of(topicPartition, startOffset)).subList(0, 1);
            } else {
                // No specific offset, seek to beginning
                kafkaConsumer.seekToBeginning(List.of(topicPartition));
                polledRecords =
                    genericConsumer.pollConsumerRecords(kafkaConsumer.endOffsets(kafkaConsumer.assignment()));
            }
        }

        records = genericConsumer.convert(polledRecords);

        if (prettyPrint) {
            // If needed, pretty print the date and time fields
            records.forEach(DatasetService::prettyRecord);
        }
        return records;
    }

    /**
     * Get Avro messages from key/value schemas and map-formatted messages.
     *
     * @param keySchema   key schema of the messages
     * @param valueSchema value schema of the messages
     * @param rawRecords
     * @return Avro-formatted messages
     */
    private Dataset getAvroDataset(String topic, AvroSchema keySchema, AvroSchema valueSchema,
                                   List<Record> rawRecords) {
        var records = new ArrayList<Record>();

        rawRecords.forEach(message -> {
            try {
                records.add(Record.builder()
                    .timestamp(message.getTimestamp())
                    .headers(message.getHeaders())
                    .key(message.getKey() != null && keySchema != null
                        ? jsonToAvro(objectMapper.writeValueAsString(message.getKey()), keySchema.rawSchema())
                        : message.getKey())
                    .value(message.getValue() != null && valueSchema != null
                        ? jsonToAvro(objectMapper.writeValueAsString(message.getValue()), valueSchema.rawSchema())
                        : message.getValue())
                    .build());
            } catch (JsonProcessingException e) {
                log.debug("", e);
                throw new RuntimeException(e.getOriginalMessage());
            }
        });

        return Dataset.builder()
            .topic(topic)
            .keySerializer(keySchema != null ? KafkaAvroSerializer.class : StringSerializer.class)
            .valueSerializer(valueSchema != null ? KafkaAvroSerializer.class : StringSerializer.class)
            .records(records)
            .build();
    }

    /**
     * Get Protobuf records from key/value schemas and map-formatted messages.
     *
     * @param keySchema   - key schema of the messages
     * @param valueSchema - value schema of the messages
     * @param rawRecords  - records to convert to Protobuf records
     * @return Protobuf-formatted messages
     */
    private Dataset getProtobufRecords(String topic, ProtobufSchema keySchema, ProtobufSchema valueSchema,
                                       List<Record> rawRecords) {
        var records = new ArrayList<Record>();

        rawRecords.forEach(message -> {
            try {
                records.add(Record.builder()
                    .timestamp(message.getTimestamp())
                    .headers(message.getHeaders())
                    .key(message.getKey() != null && keySchema != null
                        ? ProtobufSchemaUtils.toObject(objectMapper.writeValueAsString(message.getKey()), keySchema)
                        : message.getKey())
                    .value(message.getValue() != null && valueSchema != null
                        ? ProtobufSchemaUtils.toObject(objectMapper.writeValueAsString(message.getValue()), valueSchema)
                        : message.getValue())
                    .build());
            } catch (Exception e) {
                log.debug("", e);
                throw new RuntimeException(e);
            }
        });

        return Dataset.builder()
            .topic(topic)
            .keySerializer(keySchema != null ? KafkaProtobufSerializer.class : StringSerializer.class)
            .valueSerializer(valueSchema != null ? KafkaProtobufSerializer.class : StringSerializer.class)
            .records(records)
            .build();
    }

    /**
     * Get JsonSchema records from key/value schemas and map-formatted messages.
     *
     * @param keySchema   - key schema of the messages
     * @param valueSchema - value schema of the messages
     * @param rawRecords  - records to convert to JsonSchema records
     * @return JsonSchema-formatted messages
     */
    private Dataset getJsonRecords(String topic, JsonSchema keySchema, JsonSchema valueSchema,
                                   List<Record> rawRecords) {
        var records = new ArrayList<Record>();

        rawRecords.forEach(message -> {
            records.add(Record.builder()
                .timestamp(message.getTimestamp())
                .headers(message.getHeaders())
                .key(message.getKey() != null && keySchema != null
                    ? JsonSchemaUtils.envelope(keySchema, objectMapper.valueToTree(message.getKey()))
                    : message.getKey())
                .value(message.getValue() != null && valueSchema != null
                    ? JsonSchemaUtils.envelope(valueSchema, objectMapper.valueToTree(message.getValue()))
                    : message.getValue())
                .build());
        });

        return Dataset.builder()
            .topic(topic)
            .keySerializer(keySchema != null ? KafkaJsonSchemaSerializer.class : StringSerializer.class)
            .valueSerializer(valueSchema != null ? KafkaJsonSchemaSerializer.class : StringSerializer.class)
            .records(records)
            .build();
    }

    /**
     * Get messages from a dataset file.
     *
     * @param datasetFile - the file containing the dataset
     * @return a list of messages (Map)
     */
    public List<Record> getRawRecord(File datasetFile) {
        var extension = getExtensionByStringHandling(datasetFile.getName());

        if (extension.isEmpty() || !extension.get().matches("json|yml|yaml")) {
            throw new RuntimeException(
                String.format("Supported file extensions are .json, .yml and .yaml. File given as input is: %s",
                    datasetFile.getPath()));
        }

        Record[] rawRecords = {};
        String fileContent;
        try {
            fileContent = Files.readString(datasetFile.toPath());
        } catch (IOException e) {
            log.debug("", e);
            throw new FileFormatException(datasetFile.getPath());
        }

        switch (extension.get()) {
            case "json" -> {
                objectMapper.enable(JsonParser.Feature.ALLOW_COMMENTS);
                try {
                    rawRecords = objectMapper.readValue(fileContent, Record[].class);
                } catch (JsonProcessingException e) {
                    log.debug("", e);
                    throw new RuntimeException("The dataset format is incorrect: " + e.getOriginalMessage());
                }
            }
            case "yml", "yaml" -> {
                var yaml = new Yaml(new Constructor(Record[].class, new LoaderOptions()));
                rawRecords = yaml.load(fileContent);
            }
        }

        return new ArrayList<>(List.of(rawRecords));
    }

    /**
     * Get plain-text records from key/value schemas and map-formatted messages.
     *
     * @param rawRecord - record to convert to plain-text record
     * @return plain-text-formatted messages
     */
    private Record buildPlainTextRecord(Record rawRecord) {
        try {
            return Record.builder()
                .timestamp(rawRecord.getTimestamp())
                .headers(rawRecord.getHeaders())
                .key(rawRecord.getKey() == null || rawRecord.getKey() instanceof String
                    ? rawRecord.getKey()
                    : objectMapper.writeValueAsString(rawRecord.getKey()))
                .value(rawRecord.getValue() == null || rawRecord.getValue() instanceof String
                    ? rawRecord.getValue()
                    : objectMapper.writeValueAsString(rawRecord.getValue()))
                .build();
        } catch (JsonProcessingException e) {
            log.debug("", e);
            throw new RuntimeException("Key/value not either a string or a valid JSON object");
        }
    }

    /**
     * Taken from https://github.com/confluentinc/schema-registry/blob/master/avro-serializer/src/main/java/io/confluent/kafka/formatter/AvroMessageReader.java#L116
     *
     * @param jsonString json string representing the dataset
     * @param schema     topic key or value schema
     * @return a GenericData.Record object corresponding to the jsonString
     */
    public Object jsonToAvro(String jsonString, Schema schema) {
        if (schema == null) {
            throw new RuntimeException("Schema can't be null");
        }

        if (!schema.getType().equals(Schema.Type.STRING)) {
            try {
                // For Avro schema, enrich the message with type specification if needed
                jsonString = enrichJsonString(jsonString, schema);
            } catch (Exception e) {
                log.debug("", e);
                throw new RuntimeException(e);
            }
        }

        try {
            DatumReader<Object> reader = new GenericDatumReader<>(schema);
            var object = reader.read(null, DecoderFactory.get().jsonDecoder(schema, jsonString));

            if (schema.getType().equals(Schema.Type.STRING)) {
                object = ((Utf8) object).toString();
            }
            return object;
        } catch (IOException | AvroRuntimeException e) {
            log.debug("", e);
            throw new SerializationException(
                String.format("Error serializing json to Avro. Cause: %s - Message: %s", e.getMessage(), jsonString),
                e);

        }
    }

    /**
     * Enrich the json dataset with type specification for union fields.
     * Allow user to give simplified value for union fields and let us enrich it before producing the record.
     * <br/>
     * For example this nullable field
     * <pre>
     *  "fields": [{
     *      "name": "field_name",
     *      "type": ["null", "string"],
     *       "default": null
     *  }]
     * </pre>
     *
     * <p>
     * Should be passed as
     * <pre>
     *  {
     *      "field_name": {"string": "some string"}
     *  }
     * </pre>
     *
     * <p>
     * But to ease the dataset writing we can simply give
     * <pre>
     *  {
     *      "field_name": "some string"
     *  }
     * </pre>
     *
     * @param jsonString json string representing the dataset
     * @param schema     topic key or value schema
     * @return the enriched string with type specification for union fields
     * @throws JsonProcessingException
     */
    @SuppressWarnings("unchecked")
    private String enrichJsonString(String jsonString, Schema schema) throws JsonProcessingException {
        Map<String, Object> record;
        objectMapper.enable(JsonParser.Feature.ALLOW_COMMENTS);
        SimpleModule module = new SimpleModule();
        // Prevent base64 encoding of byte arrays
        module.addSerializer(byte[].class, new BytesToStringSerializer());
        objectMapper.registerModule(module);

        try {
            record = objectMapper.readValue(jsonString, LinkedHashMap.class);
        } catch (IOException e) {
            log.debug("", e);
            throw new RuntimeException(e);
        }

        enrich(record, schema);

        return objectMapper.writeValueAsString(record);
    }

    /**
     * Check for each field in the schema if it's a union field and put the type specification in the record
     * to comply with the expected format for producing union fields
     *
     * @param record the record to enrich
     * @param schema topic key or value schema
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private void enrich(Map<String, Object> record, Schema schema) {
        schema.getFields().forEach(f -> {
            switch (f.schema().getType()) {
                case UNION -> {
                    Schema fieldSchema = f.schema().getTypes().stream()
                        .filter(s -> !s.getType().equals(Schema.Type.NULL))
                        .findFirst()
                        .get();

                    if (record.containsKey(f.name())) {
                        // If there is a Map and this map is not a Record type,
                        // it means that dataset is not using the pretty format. Do nothing
                        if (record.get(f.name()) != null
                            && (!Map.class.isAssignableFrom(record.get(f.name()).getClass())
                            || fieldSchema.getType() == Schema.Type.RECORD)) {

                            // Depending on the not-null type, we have to go deeper and potentially enrich
                            // sub records, arrays or enum
                            // TODO try to simplify this switch
                            switch (fieldSchema.getType()) {
                                case ARRAY -> {
                                    record.put(f.name(), Map.of(fieldSchema.getName(), record.get(f.name())));
                                    ((List) ((Map) record.get(f.name())).get("array"))
                                        .forEach(item -> {
                                            if (fieldSchema.getElementType().getType().equals(Schema.Type.RECORD)) {
                                                enrich((Map<String, Object>) item, fieldSchema.getElementType());
                                            }
                                        });
                                }
                                case RECORD -> {
                                    var type = schema.getNamespace() + "." + fieldSchema.getName();
                                    record.put(f.name(), Map.of(type, record.get(f.name())));
                                    enrich((Map) ((Map) record.get(f.name())).get(type), fieldSchema);
                                }
                                case ENUM -> record.put(f.name(),
                                    Map.of(schema.getNamespace() + "." + fieldSchema.getName(),
                                        record.get(f.name())));
                                default -> {
                                    if (fieldSchema.getLogicalType() != null
                                        && record.get(f.name()) instanceof String) {
                                        record.put(f.name(), Map.of(fieldSchema.getName(),
                                            convertToLogicalType((String) record.get(f.name()),
                                                fieldSchema.getLogicalType())));
                                    } else {
                                        record.put(f.name(), Map.of(fieldSchema.getName(), record.get(f.name())));
                                    }
                                }
                            }
                        }
                    } else {
                        record.put(f.name(), null);
                    }
                }
                case ARRAY -> {
                    Schema s = f.schema().getElementType();
                    if (s.getType() == Schema.Type.UNION || s.getType() == Schema.Type.RECORD) {
                        ((List) record.get(f.name())).forEach(item -> {
                            enrich((Map<String, Object>) item, f.schema().getElementType());
                        });
                    } else if ((s.getType() == Schema.Type.INT || s.getType() == Schema.Type.LONG)
                        && s.getLogicalType() != null) {
                        record.put(f.name(), ((List) record.get(f.name())).stream().map(item -> {
                            if (item instanceof String) {
                                return convertToLogicalType((String) item, s.getLogicalType());
                            }
                            return item;
                        }).toList());
                    }
                }
                case RECORD -> enrich((Map<String, Object>) record.get(f.name()), f.schema());
                case BYTES -> {
                    if (schema.getField(f.name()).schema().getLogicalType() != null
                        && record.get(f.name()) instanceof String) {
                        record.put(f.name(),
                            convertToLogicalType((String) record.get(f.name()), f.schema().getLogicalType()));
                    } else if (record.get(f.name()) instanceof String) {
                        // If the field is a string, we assume it's a Base64-encoded string
                        record.put(f.name(), Base64.getDecoder().decode((String) record.get(f.name())));
                    }
                }
                case FIXED -> record.put(f.name(), Base64.getDecoder().decode((String) record.get(f.name())));
                case INT, LONG -> {
                    if (f.schema().getLogicalType() != null && record.get(f.name()) instanceof String) {
                        record.put(f.name(),
                            convertToLogicalType((String) record.get(f.name()), f.schema().getLogicalType()));
                    }
                }
                default -> {
                }
            }
        });
    }

    public Object convertToLogicalType(String stringValue, LogicalType logicalType) {
        Object value;

        switch (logicalType.getName()) {
            case "decimal" -> {
                BigDecimal decimal = new BigDecimal(stringValue);
                decimal = decimal.setScale(((LogicalTypes.Decimal) logicalType).getScale(), RoundingMode.HALF_EVEN);
                value = new Conversions.DecimalConversion().toBytes(decimal, null, logicalType).array();
            }
            case "date" -> value = (int) LocalDate.parse(stringValue).toEpochDay();
            case "time-millis" -> value = (int) (LocalTime.parse(stringValue).toNanoOfDay() / 1000000);
            case "time-micros" -> value = LocalTime.parse(stringValue).toNanoOfDay() / 1000;
            case "timestamp-millis", "local-timestamp-millis" -> value = LocalDateTime.parse(stringValue,
                DateTimeFormatter.ISO_DATE_TIME).toInstant(ZoneOffset.UTC).toEpochMilli();
            case "timestamp-micros", "local-timestamp-micros" -> {
                Instant instant = LocalDateTime.parse(stringValue,
                        DateTimeFormatter.ISO_DATE_TIME)
                    .toInstant(ZoneOffset.UTC);
                value = instant.getEpochSecond() * 1000000 + (instant.getNano() / 1000);
            }
            default -> throw new RuntimeException("Logical type not supported: " + logicalType.getName());
        }

        return value;
    }

    public Optional<String> getExtensionByStringHandling(String filename) {
        return Optional.ofNullable(filename)
            .filter(f -> f.contains("."))
            .map(f -> f.substring(filename.lastIndexOf(".") + 1));
    }

    /**
     * Compact the list of records to keeop the latest value for each message key.
     *
     * @param records the list to compact
     * @return a compacted list of record
     */
    public List<Record> compact(List<Record> records) {
        // Remove messages that should be deleted by compaction
        return records.stream()
            .collect(Collectors.toMap(
                Record::getKey,
                message -> message,
                (existing, replacement) -> replacement
            ))
            .values().stream().filter(message -> message.getValue() != null)
            .toList();
    }
}
