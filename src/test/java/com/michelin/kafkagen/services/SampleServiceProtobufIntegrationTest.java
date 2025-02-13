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
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.quarkus.test.InjectMock;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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
public class SampleServiceProtobufIntegrationTest extends AbstractIntegrationTest {
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
    public void jsonSampleTopicWithKey() throws Exception {
        String keyStringSchema =
                new String(Files.readAllBytes(Paths.get("src/test/resources/protobuf/schemas/protobufKeySchema.proto")));
        String valueStringSchema =
                new String(Files.readAllBytes(Paths.get("src/test/resources/protobuf/schemas/protobufValueSchema.proto")));

        createSubjects("jsonTopicWithKey-key", new ProtobufSchema(keyStringSchema));
        createSubjects("jsonTopicWithKey-value", new ProtobufSchema(valueStringSchema));

        JsonFactory factory = new JsonFactory();
        StringWriter out = new StringWriter();
        generator = (WriterBasedJsonGenerator) factory.createGenerator(out);
        generator.useDefaultPrettyPrinter();

        sampleService.generateSample("jsonTopicWithKey", context, generator, false, false);

        Assertions.assertEquals(
                readFileFromResources("protobuf/expected/topicWithKeySample.json"),
                out.toString());
    }

    @Test
    public void jsonSampleTopicWithoutKey() throws Exception {
        String valueStringSchema =
                new String(Files.readAllBytes(Paths.get("src/test/resources/protobuf/schemas/protobufValueSchema.proto")));

        createSubjects("jsonTopicWithoutKey-value", new ProtobufSchema(valueStringSchema));

        JsonFactory factory = new JsonFactory();
        StringWriter out = new StringWriter();
        generator = (WriterBasedJsonGenerator) factory.createGenerator(out);
        generator.useDefaultPrettyPrinter();

        sampleService.generateSample("jsonTopicWithoutKey", context, generator, false, false);

        Assertions.assertEquals(
                readFileFromResources("protobuf/expected/topicWithoutKeySample.json"),
                out.toString());
    }

    @Test
    public void jsonSampleTopicWithReference() throws Exception {
        String referencedSchema =
                new String(Files.readAllBytes(Paths.get("src/test/resources/protobuf/schemas/protobufValueSchema.proto")));
        String schemaWithReference =
                new String(Files.readAllBytes(Paths.get("src/test/resources/protobuf/schemas/protobufWithReferenceSchema.proto")));

        createSubjects("referencedTopic-value", new ProtobufSchema(referencedSchema));

        var schema = new ProtobufSchema(schemaWithReference, List.of(new SchemaReference("ProtobufValue", "referencedTopic-value", 1)), Map.of("ProtobufValue", referencedSchema), 1, "schemaName");
        createSubjects("topicWithReference-value", schema);

        JsonFactory factory = new JsonFactory();
        StringWriter out = new StringWriter();
        generator = (WriterBasedJsonGenerator) factory.createGenerator(out);
        generator.useDefaultPrettyPrinter();

        sampleService.generateSample("topicWithReference", context, generator, false, false);

        Assertions.assertEquals(
                readFileFromResources("protobuf/expected/topicWithReferenceSample.json"),
                out.toString());
    }
}
