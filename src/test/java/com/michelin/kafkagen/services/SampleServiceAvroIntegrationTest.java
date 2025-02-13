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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.json.WriterBasedJsonGenerator;
import com.michelin.kafkagen.AbstractIntegrationTest;
import com.michelin.kafkagen.KafkaTestResource;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.quarkus.test.InjectMock;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.junit.jupiter.api.*;
import org.mockito.Mockito;

import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Slf4j
@QuarkusTest
@QuarkusTestResource(value = KafkaTestResource.class, restrictToAnnotatedClass = true)
public class SampleServiceAvroIntegrationTest extends AbstractIntegrationTest {
    @Inject
    SampleService sampleService;

    @InjectMock
    MockedContext context;
    @InjectMock
    MockedContext.MockedKafkaContext kafkaContext;

    WriterBasedJsonGenerator generator;

    @BeforeEach
    public void init() {
        Mockito.when(kafkaContext.registryUrl()).thenReturn(Optional.of(String.format("http://%s:%d", registryHost, Integer.parseInt(registryPort))));
        Mockito.when(context.definition()).thenReturn(kafkaContext);
    }

    @Test
    public void avroSampleTopicWithKey() throws Exception {
        String keyStringSchema =
                new String(Files.readAllBytes(Paths.get("src/test/resources/avro/schemas/avroKeySchema.avsc")));
        String valueStringSchema =
                new String(Files.readAllBytes(Paths.get("src/test/resources/avro/schemas/avroValueSchema.avsc")));

        createSubjects("avroTopicWithKey-key", new AvroSchema(new Schema.Parser().parse(keyStringSchema)));
        createSubjects("avroTopicWithKey-value", new AvroSchema(new Schema.Parser().parse(valueStringSchema)));

        JsonFactory factory = new JsonFactory();
        StringWriter out = new StringWriter();
        generator = (WriterBasedJsonGenerator) factory.createGenerator(out);
        generator.useDefaultPrettyPrinter();

        sampleService.generateSample("avroTopicWithKey", context, generator, false, false);

        Assertions.assertEquals(
                readFileFromResources("avro/expected/topicWithKeySample.json"),
                out.toString());
    }

    @Test
    public void avroSampleTopicWithoutKey() throws Exception {
        String valueStringSchema =
                new String(Files.readAllBytes(Paths.get("src/test/resources/avro/schemas/avroValueSchema.avsc")));

        createSubjects("avroTopicWithoutKey-value", new AvroSchema(new Schema.Parser().parse(valueStringSchema)));

        JsonFactory factory = new JsonFactory();
        StringWriter out = new StringWriter();
        generator = (WriterBasedJsonGenerator) factory.createGenerator(out);
        generator.useDefaultPrettyPrinter();

        sampleService.generateSample("avroTopicWithoutKey", context, generator, false, false);

        Assertions.assertEquals(
                readFileFromResources("avro/expected/topicWithoutKeySample.json"),
                out.toString());
    }

    @Test
    public void avroSampleTopicWithReference() throws Exception {
        String referencedSchema =
                new String(Files.readAllBytes(Paths.get("src/test/resources/avro/schemas/avroValueSchema.avsc")));
        String schemaWithReference =
                new String(Files.readAllBytes(Paths.get("src/test/resources/avro/schemas/avroWithReferenceSchema.avsc")));

        createSubjects("referencedTopic-value", new AvroSchema(new Schema.Parser().parse(referencedSchema)));

        var schema = new AvroSchema(schemaWithReference, List.of(new SchemaReference("AvroValue", "referencedTopic-value", 1)), Map.of("AvroValue", referencedSchema), 1);
        createSubjects("topicWithReference-value", schema);

        JsonFactory factory = new JsonFactory();
        StringWriter out = new StringWriter();
        generator = (WriterBasedJsonGenerator) factory.createGenerator(out);
        generator.useDefaultPrettyPrinter();

        sampleService.generateSample("topicWithReference", context, generator, false, false);

        Assertions.assertEquals(
                readFileFromResources("avro/expected/topicWithReferenceSample.json"),
                out.toString());
    }

    @Test
    public void avroSampleTopicWithSelfReference() throws Exception {
        String schemaWithSelfReference =
                new String(Files.readAllBytes(Paths.get("src/test/resources/avro/schemas/avroWithSelfReferenceSchema.avsc")));

        createSubjects("selfReferencedTopic-value", new AvroSchema(new Schema.Parser().parse(schemaWithSelfReference)));

        JsonFactory factory = new JsonFactory();
        StringWriter out = new StringWriter();
        generator = (WriterBasedJsonGenerator) factory.createGenerator(out);
        generator.useDefaultPrettyPrinter();

        sampleService.generateSample("selfReferencedTopic", context, generator, false, false);

        Assertions.assertEquals(
                readFileFromResources("avro/expected/topicWithSelfReferenceSample.json"),
                out.toString());
    }

    @Test
    public void avroSampleTopicWithRecursiveReference() throws Exception {
        String schemaWithSelfReference =
                new String(Files.readAllBytes(Paths.get("src/test/resources/avro/schemas/avroWithRecursiveReferenceSchema.avsc")));

        createSubjects("recursiveReferencedTopic-value", new AvroSchema(new Schema.Parser().parse(schemaWithSelfReference)));

        JsonFactory factory = new JsonFactory();
        StringWriter out = new StringWriter();
        generator = (WriterBasedJsonGenerator) factory.createGenerator(out);
        generator.useDefaultPrettyPrinter();

        sampleService.generateSample("recursiveReferencedTopic", context, generator, false, false);

        Assertions.assertEquals(
                readFileFromResources("avro/expected/topicWithRecursiveReferenceSample.json"),
                out.toString());
    }
}
