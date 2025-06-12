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

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.michelin.kafkagen.AbstractIntegrationTest;
import com.michelin.kafkagen.KafkaTestResource;
import com.michelin.kafkagen.kafka.GenericProducer;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.quarkus.test.InjectMock;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.PrimitivesArrays;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
@QuarkusTestResource(value = KafkaTestResource.class, restrictToAnnotatedClass = true)
public class DatasetServiceIntegrationTest extends AbstractIntegrationTest {

    @InjectMock
    MockedContext context;

    @InjectMock
    MockedContext.MockedKafkaContext kafkaContext;

    @Inject
    DatasetService datasetService;
    @Inject
    GenericProducer genericProducer;

    @BeforeEach
    public void init() {
        Mockito.when(kafkaContext.registryUrl()).thenReturn(Optional.of(String.format("http://%s:%d", registryHost, Integer.parseInt(registryPort))));
        Mockito.when(context.definition()).thenReturn(kafkaContext);
    }

    @Test
    public void getDatasetForFileWithAvroSchema() throws Exception {
        String keyStringSchema =
                new String(Files.readAllBytes(Paths.get("src/test/resources/avro/schemas/avroKeySchema.avsc")));
        String valueStringSchema =
                new String(Files.readAllBytes(Paths.get("src/test/resources/avro/schemas/avroValueSchema.avsc")));

        createSubjects("avroTopicWithKey-key", new AvroSchema(new Schema.Parser().parse(keyStringSchema)));
        createSubjects("avroTopicWithKey-value", new AvroSchema(new Schema.Parser().parse(valueStringSchema)));

        var dataset = datasetService.getDataset(new File(getClass().getClassLoader().getResource("avro/datasets/dataset.json").toURI()), "avroTopicWithKey", Optional.empty(), Optional.empty(), context);

        assertNotNull(dataset);
        assertEquals(2, dataset.getRecords().size());
        assertEquals(GenericData.Record.class, dataset.getRecords().getFirst().getKey().getClass());
        assertEquals("key1", ((GenericData.Record) dataset.getRecords().getFirst().getKey()).get("keyFieldString").toString());
        assertEquals(GenericData.Record.class, dataset.getRecords().get(1).getKey().getClass());
        assertEquals("key2", ((GenericData.Record) dataset.getRecords().get(1).getKey()).get("keyFieldString").toString());
        assertEquals(GenericData.Record.class, dataset.getRecords().getFirst().getValue().getClass());
        assertEquals("value1", ((GenericData.Record) dataset.getRecords().getFirst().getValue()).get("fieldString").toString());
        assertEquals(GenericData.Record.class, dataset.getRecords().get(1).getValue().getClass());
        assertEquals("value2", ((GenericData.Record) dataset.getRecords().get(1).getValue()).get("fieldString").toString());
    }


    @Test
    public void getDatasetForFileWithAvroSchemaAndPrettyDateAndTime() throws Exception {
        String valueStringSchema =
                new String(Files.readAllBytes(Paths.get("src/test/resources/avro/schemas/avroValueWithLogicalTypes.avsc")));

        createSubjects("datasetWithPrettyDateAndTime-value", new AvroSchema(new Schema.Parser().parse(valueStringSchema)));

        var dataset = datasetService.getDataset(new File(getClass().getClassLoader().getResource("avro/datasets/datasetWithPrettyDateAndTime.json").toURI()), "datasetWithPrettyDateAndTime", Optional.empty(), Optional.empty(), context);

        assertNotNull(dataset);
        assertEquals(1, dataset.getRecords().size());
        assertEquals(GenericData.Record.class, dataset.getRecords().getFirst().getValue().getClass());
        assertEquals(54747100, ((GenericData.Record) dataset.getRecords().getFirst().getValue()).get("timeMillis"));
        assertEquals(54747429954L, ((GenericData.Record) dataset.getRecords().getFirst().getValue()).get("timeMicros"));
        assertEquals(1725376347100L, ((GenericData.Record) dataset.getRecords().getFirst().getValue()).get("timestampMillis"));
        assertEquals(1725376347429990L, ((GenericData.Record) dataset.getRecords().getFirst().getValue()).get("timestampMicros"));
        assertEquals(19969, ((GenericData.Record) dataset.getRecords().getFirst().getValue()).get("date"));
        assertEquals(1725376347100L, ((GenericData.Record) dataset.getRecords().getFirst().getValue()).get("localTimestampMillis"));
        assertEquals(19969, ((GenericData.Record) dataset.getRecords().getFirst().getValue()).get("fieldUnion"));
        assertEquals(1725376347100L, ((GenericData.Record)((GenericData.Record) dataset.getRecords().getFirst().getValue()).get("fieldRecord")).get("recordTimestampMillis"));
        assertEquals(19969, ((GenericData.Record)((GenericData.Record) dataset.getRecords().getFirst().getValue()).get("fieldRecord")).get("recordFieldUnion"));
        assertEquals(1725376347100L, ((PrimitivesArrays.LongArray)((GenericData.Record) dataset.getRecords().getFirst().getValue()).get("fieldArray")).get(0));
        assertEquals(1725376347200L, ((PrimitivesArrays.LongArray)((GenericData.Record) dataset.getRecords().getFirst().getValue()).get("fieldArray")).get(1));
        assertEquals(1725376347100L, ((GenericData.Record)((GenericData.Array)((GenericData.Record) dataset.getRecords().getFirst().getValue()).get("fieldArrayRecord")).get(0)).get("timestampMillis"));
    }

    @Test
    public void getDatasetForFileWithJsonSchema() throws Exception {
        String keyStringSchema =
                new String(Files.readAllBytes(Paths.get("src/test/resources/jsonschema/schemas/jsonKeySchema.json")));
        String valueStringSchema =
                new String(Files.readAllBytes(Paths.get("src/test/resources/jsonschema/schemas/jsonValueSchema.json")));

        createSubjects("jsonTopicWithKey-key", new JsonSchema(keyStringSchema));
        createSubjects("jsonTopicWithKey-value", new JsonSchema(valueStringSchema));

        var dataset = datasetService.getDataset(new File(getClass().getClassLoader().getResource("jsonschema/datasets/dataset.json").toURI()), "jsonTopicWithKey", Optional.empty(), Optional.empty(), context);

        assertNotNull(dataset);
        assertEquals(2, dataset.getRecords().size());
        assertEquals("key1", ((ObjectNode) dataset.getRecords().getFirst().getKey()).get("payload").get("keyFieldString").asText());
        assertEquals("value1", ((ObjectNode) dataset.getRecords().getFirst().getValue()).get("payload").get("fieldString").asText());
        assertEquals("key2", ((ObjectNode) dataset.getRecords().get(1).getKey()).get("payload").get("keyFieldString").asText());
        assertEquals("value2", ((ObjectNode) dataset.getRecords().get(1).getValue()).get("payload").get("fieldString").asText());
    }

    @Test
    public void getDatasetForFileWithProtobuf() throws Exception {
        String keyStringSchema =
                new String(Files.readAllBytes(Paths.get("src/test/resources/protobuf/schemas/protobufKeySchema.proto")));
        String valueStringSchema =
                new String(Files.readAllBytes(Paths.get("src/test/resources/protobuf/schemas/protobufValueSchema.proto")));

        createSubjects("protobufTopicWithKey-key", new ProtobufSchema(keyStringSchema));
        createSubjects("protobufTopicWithKey-value", new ProtobufSchema(valueStringSchema));

        var dataset = datasetService.getDataset(new File(getClass().getClassLoader().getResource("protobuf/datasets/dataset.json").toURI()), "protobufTopicWithKey", Optional.empty(), Optional.empty(), context);

        assertNotNull(dataset);
        assertEquals(2, dataset.getRecords().size());

        Descriptors.FieldDescriptor keyFieldStringDescriptor = ((DynamicMessage) dataset.getRecords().getFirst().getKey()).getDescriptorForType().findFieldByName("keyFieldString");
        Descriptors.FieldDescriptor valueFieldStringDescriptor = ((DynamicMessage) dataset.getRecords().getFirst().getValue()).getDescriptorForType().findFieldByName("fieldString");

        assertEquals("key1", ((DynamicMessage) dataset.getRecords().getFirst().getKey()).getField(keyFieldStringDescriptor));
        assertEquals("value1", ((DynamicMessage) dataset.getRecords().getFirst().getValue()).getField(valueFieldStringDescriptor));
        assertEquals("key2", ((DynamicMessage) dataset.getRecords().get(1).getKey()).getField(keyFieldStringDescriptor));
        assertEquals("value2", ((DynamicMessage) dataset.getRecords().get(1).getValue()).getField(valueFieldStringDescriptor));
    }

    @Test
    public void getDatasetForFileWithoutSchema() throws Exception {
        var dataset = datasetService.getDataset(new File(getClass().getClassLoader().getResource("noSchemas/datasets/dataset.json").toURI()), "noSchemaTopic", Optional.empty(), Optional.empty(), context);

        assertNotNull(dataset);
        assertEquals(2, dataset.getRecords().size());

        assertEquals(String.class, dataset.getRecords().getFirst().getKey().getClass());
        assertEquals(String.class, dataset.getRecords().getFirst().getValue().getClass());
        assertEquals(String.class, dataset.getRecords().get(1).getKey().getClass());
        assertEquals(String.class, dataset.getRecords().get(1).getValue().getClass());

        assertTrue(dataset.getRecords().getFirst().getKey().toString().contains("\"keyFieldString\":\"key1\""));
        assertTrue(dataset.getRecords().getFirst().getValue().toString().contains("\"fieldString\":\"value1\""));
        assertTrue(dataset.getRecords().get(1).getKey().toString().contains("\"keyFieldString\":\"key2\""));
        assertTrue(dataset.getRecords().get(1).getValue().toString().contains("\"fieldString\":\"value2\""));
    }

    @Test
    public void getDatasetForDifferentAvroTopicsInSameFile() throws Exception {
        String keyStringSchema =
                new String(Files.readAllBytes(Paths.get("src/test/resources/avro/schemas/avroKeySchema.avsc")));
        String valueStringSchema =
                new String(Files.readAllBytes(Paths.get("src/test/resources/avro/schemas/avroValueSchema.avsc")));
        String valueStringSchemaSelfReference =
                new String(Files.readAllBytes(Paths.get("src/test/resources/avro/schemas/avroWithSelfReferenceSchema.avsc")));

        createSubjects("avroTopicWithKey-key", new AvroSchema(new Schema.Parser().parse(keyStringSchema)));
        createSubjects("avroTopicWithKey-value", new AvroSchema(new Schema.Parser().parse(valueStringSchema)));
        createSubjects("selfReferencedTopic-value", new AvroSchema(new Schema.Parser().parse(valueStringSchemaSelfReference)));

        var datasets = datasetService.getDataset(new File(getClass().getClassLoader().getResource("avro/datasets/datasetMultiTopic.json").toURI()), context);

        assertNotNull(datasets);
        assertEquals(2, datasets.size());
        assertEquals("avroTopicWithKey", datasets.getFirst().getTopic());
        assertEquals(1, datasets.getFirst().getRecords().size());
        assertEquals("selfReferencedTopic", datasets.get(1).getTopic());
        assertEquals(1, datasets.get(1).getRecords().size());

        assertEquals(GenericData.Record.class, datasets.getFirst().getRecords().getFirst().getKey().getClass());
        assertEquals("key1", ((GenericData.Record) datasets.getFirst().getRecords().getFirst().getKey()).get("keyFieldString").toString());
        assertEquals(GenericData.Record.class, datasets.getFirst().getRecords().getFirst().getValue().getClass());
        assertEquals("value1", ((GenericData.Record) datasets.getFirst().getRecords().getFirst().getValue()).get("fieldString").toString());

        assertEquals(String.class, datasets.get(1).getRecords().getFirst().getKey().getClass());
        assertEquals("key2", datasets.get(1).getRecords().getFirst().getKey().toString());
        assertEquals(GenericData.Record.class, datasets.get(1).getRecords().getFirst().getValue().getClass());
        assertEquals("value2", ((GenericData.Record) datasets.get(1).getRecords().getFirst().getValue()).get("fieldString").toString());
    }

    @Test
    public void getDatasetForDifferentJsonTopicsInSameFile() throws Exception {
        String keyStringSchema =
                new String(Files.readAllBytes(Paths.get("src/test/resources/jsonschema/schemas/jsonKeySchema.json")));
        String valueStringSchema =
                new String(Files.readAllBytes(Paths.get("src/test/resources/jsonschema/schemas/jsonValueSchema.json")));

        createSubjects("jsonTopicWithKey-key", new JsonSchema(keyStringSchema));
        createSubjects("jsonTopicWithKey-value", new JsonSchema(valueStringSchema));
        createSubjects("jsonTopicWithoutKey-value", new JsonSchema(valueStringSchema));

        var datasets = datasetService.getDataset(new File(getClass().getClassLoader().getResource("jsonschema/datasets/datasetMultiTopic.json").toURI()), context);

        assertNotNull(datasets);
        assertEquals(2, datasets.size());
        assertEquals("jsonTopicWithKey", datasets.getFirst().getTopic());
        assertEquals(1, datasets.getFirst().getRecords().size());
        assertEquals("jsonTopicWithoutKey", datasets.get(1).getTopic());
        assertEquals(1, datasets.get(1).getRecords().size());

        assertEquals("key1", ((ObjectNode) datasets.getFirst().getRecords().getFirst().getKey()).get("payload").get("keyFieldString").asText());
        assertEquals("value1", ((ObjectNode) datasets.getFirst().getRecords().getFirst().getValue()).get("payload").get("fieldString").asText());
        assertEquals("key2", datasets.get(1).getRecords().getFirst().getKey());
        assertEquals("value2", ((ObjectNode) datasets.get(1).getRecords().getFirst().getValue()).get("payload").get("fieldString").asText());
    }

    @Test
    public void getDatasetForDifferentProtobufTopicsInSameFile() throws Exception {
        String keyStringSchema =
                new String(Files.readAllBytes(Paths.get("src/test/resources/protobuf/schemas/protobufKeySchema.proto")));
        String valueStringSchema =
                new String(Files.readAllBytes(Paths.get("src/test/resources/protobuf/schemas/protobufValueSchema.proto")));

        createSubjects("protobufTopicWithKey-key", new ProtobufSchema(keyStringSchema));
        createSubjects("protobufTopicWithKey-value", new ProtobufSchema(valueStringSchema));
        createSubjects("protobufTopicWithoutKey-value", new ProtobufSchema(valueStringSchema));

        var datasets = datasetService.getDataset(new File(getClass().getClassLoader().getResource("protobuf/datasets/datasetMultiTopic.json").toURI()), context);

        assertNotNull(datasets);
        assertEquals(2, datasets.size());
        assertEquals("protobufTopicWithKey", datasets.getFirst().getTopic());
        assertEquals(1, datasets.getFirst().getRecords().size());
        assertEquals("protobufTopicWithoutKey", datasets.get(1).getTopic());
        assertEquals(1, datasets.get(1).getRecords().size());

        Descriptors.FieldDescriptor keyFieldStringDescriptor = ((DynamicMessage) datasets.getFirst().getRecords().getFirst().getKey()).getDescriptorForType().findFieldByName("keyFieldString");
        Descriptors.FieldDescriptor valueFieldStringDescriptor = ((DynamicMessage) datasets.getFirst().getRecords().getFirst().getValue()).getDescriptorForType().findFieldByName("fieldString");
        Descriptors.FieldDescriptor valueFieldStringDescriptor2 = ((DynamicMessage) datasets.get(1).getRecords().getFirst().getValue()).getDescriptorForType().findFieldByName("fieldString");

        assertEquals("key1", ((DynamicMessage) datasets.getFirst().getRecords().getFirst().getKey()).getField(keyFieldStringDescriptor));
        assertEquals("value1", ((DynamicMessage) datasets.getFirst().getRecords().getFirst().getValue()).getField(valueFieldStringDescriptor));
        assertEquals("key2", datasets.get(1).getRecords().getFirst().getKey());
        assertEquals("value2", ((DynamicMessage) datasets.get(1).getRecords().getFirst().getValue()).getField(valueFieldStringDescriptor2));
    }

    @Test
    public void getDatasetForDifferentTopicsWithoutSchemaInSameFile() throws Exception {
        var datasets = datasetService.getDataset(new File(getClass().getClassLoader().getResource("noSchemas/datasets/datasetMultiTopic.json").toURI()), context);

        assertNotNull(datasets);
        assertEquals(2, datasets.size());
        assertEquals("noSchemaTopic", datasets.getFirst().getTopic());
        assertEquals(1, datasets.getFirst().getRecords().size());
        assertEquals("noSchemaTopic2", datasets.get(1).getTopic());
        assertEquals(1, datasets.get(1).getRecords().size());

        assertEquals(String.class, datasets.getFirst().getRecords().getFirst().getKey().getClass());
        assertEquals(String.class, datasets.getFirst().getRecords().getFirst().getValue().getClass());
        assertEquals(String.class, datasets.get(1).getRecords().getFirst().getKey().getClass());
        assertEquals(String.class, datasets.get(1).getRecords().getFirst().getValue().getClass());

        assertTrue(datasets.getFirst().getRecords().getFirst().getKey().toString().contains("\"keyFieldString\":\"key1\""));
        assertTrue(datasets.getFirst().getRecords().getFirst().getValue().toString().contains("\"fieldString\":\"value1\""));
        assertEquals("key2", datasets.get(1).getRecords().getFirst().getKey());
        assertTrue(datasets.get(1).getRecords().getFirst().getValue().toString().contains("\"fieldString\":\"value2\""));
    }

    @Test
    public void getEnrichedDatasetForFileWithAvroSchema() throws Exception {
       String valueStringSchema =
                new String(Files.readAllBytes(Paths.get("src/test/resources/avro/schemas/avroValueSchema.avsc")));

        createSubjects("avroTopicWithoutKey-value", new AvroSchema(new Schema.Parser().parse(valueStringSchema)));

        var dataset = datasetService.getDataset(new File(getClass().getClassLoader().getResource("avro/datasets/datasetToEnrich.json").toURI()), "avroTopicWithoutKey", Optional.empty(), Optional.empty(), context);

        assertNotNull(dataset);
        assertEquals(1, dataset.getRecords().size());
        assertEquals(GenericData.Record.class, dataset.getRecords().getFirst().getValue().getClass());
        assertEquals("String_value", ((GenericData.Record) dataset.getRecords().getFirst().getValue()).get("fieldUnion").toString());

        dataset = datasetService.getDataset(new File(getClass().getClassLoader().getResource("avro/datasets/datasetToEnrichWithoutNullField.json").toURI()), "avroTopicWithoutKey", Optional.empty(), Optional.empty(), context);

        assertNotNull(dataset);
        assertEquals(1, dataset.getRecords().size());
        assertEquals(GenericData.Record.class, dataset.getRecords().getFirst().getValue().getClass());
        assertNull(((GenericData.Record) dataset.getRecords().getFirst().getValue()).get("fieldUnion"));
    }

    @Test
    public void getDatasetFromTemplate() throws Exception {
        String valueStringSchema =
                new String(Files.readAllBytes(Paths.get("src/test/resources/avro/schemas/avroValueSchema.avsc")));

        createSubjects("avroTopicWithoutKey-value", new AvroSchema(new Schema.Parser().parse(valueStringSchema)));

        var variables = new HashMap<String, Map<String, Object>>();
        var value1 = new HashMap<String, Object>();
        value1.put("fieldString", "value1");
        variables.put("key1", value1);

        var value2 = new HashMap<String, Object>();
        value2.put("fieldString", "value2");
        variables.put("key2", value2);

        var value3 = new HashMap<String, Object>();
        value3.put("fieldString", "value3");
        variables.put("key3", value3);

        var value4 = new HashMap<String, Object>();
        value4.put("fieldString", "value4");
        variables.put("key4", value4);

        var dataset = datasetService.getDatasetFromTemplate("src/test/resources/avro/datasets/templateDataset.json", variables, "avroTopicWithoutKey", context);

        assertNotNull(dataset);
        assertEquals(4, dataset.getRecords().size());

        assertEquals(String.class, dataset.getRecords().getFirst().getKey().getClass());
        assertEquals("key1", dataset.getRecords().getFirst().getKey());
        assertEquals(GenericData.Record.class, dataset.getRecords().getFirst().getValue().getClass());
        assertEquals("value1", ((GenericData.Record) dataset.getRecords().getFirst().getValue()).get("fieldString").toString());
        assertEquals(1, ((GenericData.Record) dataset.getRecords().getFirst().getValue()).get("fieldInt"));
        assertEquals(42L, ((GenericData.Record) dataset.getRecords().getFirst().getValue()).get("fieldLong"));

        assertEquals("key2", dataset.getRecords().get(1).getKey());
        assertEquals(GenericData.Record.class, dataset.getRecords().get(1).getValue().getClass());
        assertEquals("value2", ((GenericData.Record) dataset.getRecords().get(1).getValue()).get("fieldString").toString());
        assertEquals(2, ((GenericData.Record) dataset.getRecords().get(1).getValue()).get("fieldInt"));
        assertEquals(42L, ((GenericData.Record) dataset.getRecords().get(1).getValue()).get("fieldLong"));

        assertEquals("key3", dataset.getRecords().get(2).getKey());
        assertEquals(GenericData.Record.class, dataset.getRecords().get(2).getValue().getClass());
        assertEquals("value3", ((GenericData.Record) dataset.getRecords().get(2).getValue()).get("fieldString").toString());
        assertEquals(1, ((GenericData.Record) dataset.getRecords().get(2).getValue()).get("fieldInt"));
        assertEquals(42L, ((GenericData.Record) dataset.getRecords().get(2).getValue()).get("fieldLong"));

        assertEquals("key4", dataset.getRecords().get(3).getKey());
        assertEquals(GenericData.Record.class, dataset.getRecords().get(3).getValue().getClass());
        assertEquals("value4", ((GenericData.Record) dataset.getRecords().get(3).getValue()).get("fieldString").toString());
        assertEquals(2, ((GenericData.Record) dataset.getRecords().get(3).getValue()).get("fieldInt"));
        assertEquals(42L, ((GenericData.Record) dataset.getRecords().get(3).getValue()).get("fieldLong"));
    }

    @Test
    public void getDatasetFromTopic() throws Exception {
        Mockito.when(kafkaContext.registryUrl()).thenReturn(Optional.of(String.format("http://%s:%d", registryHost, Integer.parseInt(registryPort))));
        Mockito.when(kafkaContext.bootstrapServers()).thenReturn(host + ":" + Integer.parseInt(port));
        Mockito.when(context.definition()).thenReturn(kafkaContext);

        String valueStringSchema =
                new String(Files.readAllBytes(Paths.get("src/test/resources/avro/schemas/avroValueSchema.avsc")));

        createSubjects("avroTopicWithoutKey-value", new AvroSchema(new Schema.Parser().parse(valueStringSchema)));
        getAdminClient().createTopics(List.of(new NewTopic("avroTopicWithoutKey", 3, (short) 1)));
        var dataset = datasetService.getDataset(new File(getClass().getClassLoader().getResource("avro/datasets/datasetToProduce.json").toURI()), "avroTopicWithoutKey", Optional.empty(), Optional.empty(), context);

        Thread.sleep(1000);

        // Partition 0: key4, key6, key7
        // Partition 1: key3, key5
        // Partition 2: key1, key2
        genericProducer.produce("avroTopicWithoutKey", dataset, 1, context);

        var records = datasetService.getDatasetFromTopic("avroTopicWithoutKey", 0, null, null, null, false, context);
        assertEquals(3, records.size());
        assertEquals("key4", records.getFirst().getKey());
        assertEquals("key6", records.get(1).getKey());
        assertEquals("key7", records.get(2).getKey());

        records = datasetService.getDatasetFromTopic("avroTopicWithoutKey", 1, 1L, null, null, false, context);
        assertEquals(1, records.size());
        assertEquals("key5", records.getFirst().getKey());

        records = datasetService.getDatasetFromTopic("avroTopicWithoutKey", 1, 0L, 1L, null, false, context);
        assertEquals(2, records.size());
        assertEquals("key3", records.getFirst().getKey());
        assertEquals("key5", records.get(1).getKey());

        records = datasetService.getDatasetFromTopic("avroTopicWithoutKey", 0, null, null, List.of(0L, 2L), false, context);
        assertEquals(2, records.size());
        assertEquals("key4", records.getFirst().getKey());
        assertEquals("key7", records.get(1).getKey());
    }

    @Test
    public void getDatasetForKey() throws Exception {
        Mockito.when(kafkaContext.registryUrl()).thenReturn(Optional.of(String.format("http://%s:%d", registryHost, Integer.parseInt(registryPort))));
        Mockito.when(kafkaContext.bootstrapServers()).thenReturn(host + ":" + Integer.parseInt(port));
        Mockito.when(context.definition()).thenReturn(kafkaContext);

        String valueStringSchema =
                new String(Files.readAllBytes(Paths.get("src/test/resources/avro/schemas/avroValueSchema.avsc")));

        createSubjects("avroTopicWithoutKeyForKeySearch-value", new AvroSchema(new Schema.Parser().parse(valueStringSchema)));
        getAdminClient().createTopics(List.of(new NewTopic("avroTopicWithoutKeyForKeySearch", 3, (short) 1)));
        var dataset = datasetService.getDataset(new File(getClass().getClassLoader().getResource("avro/datasets/datasetForKeySearch.json").toURI()), "avroTopicWithoutKeyForKeySearch", Optional.empty(), Optional.empty(), context);

        Thread.sleep(1000);

        // Partition 0: key4, key6, key7
        // Partition 1: key3, key5
        // Partition 2: key1, key2
        genericProducer.produce("avroTopicWithoutKeyForKeySearch", dataset, 1, context);

        var records = datasetService.getDatasetForKey("avroTopicWithoutKeyForKeySearch", "key2", true, context);
        assertNotNull(records);
        assertEquals(2, records.size());
        assertEquals("key2", records.getFirst().getKey());
        assertEquals("value2", ((GenericData.Record) records.getFirst().getValue()).get("fieldString").toString());
        assertEquals("key2", records.get(1).getKey());
        assertEquals("value2_2", ((GenericData.Record) records.get(1).getValue()).get("fieldString").toString());
    }
    @Test
    public void getDatasetForAvroKey() throws Exception {
        Mockito.when(kafkaContext.registryUrl()).thenReturn(Optional.of(String.format("http://%s:%d", registryHost, Integer.parseInt(registryPort))));
        Mockito.when(kafkaContext.bootstrapServers()).thenReturn(host + ":" + Integer.parseInt(port));
        Mockito.when(context.definition()).thenReturn(kafkaContext);

        String keyStringSchema =
                new String(Files.readAllBytes(Paths.get("src/test/resources/avro/schemas/avroKeySchema.avsc")));
        String valueStringSchema =
                new String(Files.readAllBytes(Paths.get("src/test/resources/avro/schemas/avroValueSchema.avsc")));
        createSubjects("avroTopicWithKey-key", new AvroSchema(new Schema.Parser().parse(keyStringSchema)));
        createSubjects("avroTopicWithKey-value", new AvroSchema(new Schema.Parser().parse(valueStringSchema)));
        getAdminClient().createTopics(List.of(new NewTopic("avroTopicWithKey", 3, (short) 1)));
        var dataset = datasetService.getDataset(new File(getClass().getClassLoader().getResource("avro/datasets/datasetForAvroKeySearch.json").toURI()), "avroTopicWithKey", Optional.empty(), Optional.empty(), context);

        Thread.sleep(1000);

        genericProducer.produce("avroTopicWithKey", dataset, 1, context);

        var records = datasetService.getDatasetForKey("avroTopicWithKey", "{ \"keyFieldString\" : \"key2\" }", true, context);
        assertNotNull(records);
        assertEquals(2, records.size());
        assertEquals("{\"keyFieldString\": \"key2\", \"keyFieldInt\": 42}", records.getFirst().getKey().toString());
        assertEquals("value2", ((GenericData.Record) records.getFirst().getValue()).get("fieldString").toString());
        assertEquals("{\"keyFieldString\": \"key2\", \"keyFieldInt\": 42}", records.get(1).getKey().toString());
        assertEquals("value2_2", ((GenericData.Record) records.get(1).getValue()).get("fieldString").toString());
    }

    @Test
    public void getRawRecordsForFileWithWrongExtension() {
        assertThrows(RuntimeException.class, () -> datasetService.getRawRecord(new File("dataset")));
        assertThrows(RuntimeException.class, () -> datasetService.getRawRecord(new File("dataset.xml")));
    }
}
