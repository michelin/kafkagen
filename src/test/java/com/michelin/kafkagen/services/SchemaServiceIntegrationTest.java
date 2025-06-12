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

import com.michelin.kafkagen.AbstractIntegrationTest;
import com.michelin.kafkagen.KafkaTestResource;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.quarkus.test.InjectMock;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Optional;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
@QuarkusTest
@QuarkusTestResource(value = KafkaTestResource.class, restrictToAnnotatedClass = true)
public class SchemaServiceIntegrationTest extends AbstractIntegrationTest {

    @Inject
    SchemaService schemaService;

    @InjectMock
    MockedContext context;
    @InjectMock
    MockedContext.MockedKafkaContext kafkaContext;

    static AvroSchema schema;
    static AvroSchema schemaV2;

    @BeforeAll
    public static void setup() throws IOException {
        schema = new AvroSchema(new Schema.Parser().parse(
            new String(Files.readAllBytes(Paths.get("src/test/resources/avro/schemas/avroValueSchema.avsc")))));
        schemaV2 = new AvroSchema(new Schema.Parser().parse(
            new String(Files.readAllBytes(Paths.get("src/test/resources/avro/schemas/avroValueSchemaV2.avsc")))));
    }

    @BeforeEach
    public void init() {
        Mockito.when(kafkaContext.registryUrl()).thenReturn(Optional.of(String.format("http://%s:%d", registryHost, Integer.parseInt(registryPort))));
        Mockito.when(context.definition()).thenReturn(kafkaContext);
    }

    @Test
    public void getFirstSchemaVersion() throws IOException {
        createSubjects("getFirstSchemaVersion-value", schema);
        createSubjects("getFirstSchemaVersion-value", schemaV2);

        ParsedSchema actualSchema = schemaService.getSchema("getFirstSchemaVersion-value", Optional.of(1), context);

        assertEquals(schema, actualSchema);
    }

    @Test
    public void getSecondSchemaVersion() throws IOException {
        createSubjects("getSecondSchemaVersion-value", schema);
        createSubjects("getSecondSchemaVersion-value", schemaV2);

        ParsedSchema actualSchema = schemaService.getSchema("getSecondSchemaVersion-value", Optional.of(2), context);

        assertEquals(schemaV2, actualSchema);
    }


    @Test
    public void getLatestSchema() throws IOException {
        createSubjects("getLatestSchema-value", schema);
        createSubjects("getLatestSchema-value", schemaV2);

        ParsedSchema actualSchema = schemaService.getLatestSchema("getLatestSchema-value", context);

        assertEquals(schemaV2, actualSchema);
    }

    @Test
    public void schemaVersionNotFound() throws IOException {
        createSubjects("schemaVersionNotFound-value", schema);
        createSubjects("schemaVersionNotFound-value", schemaV2);

        assertNull(schemaService.getSchema("schemaVersionNotFound-value", Optional.of(3), context));
    }

    @Test
    public void schemaNotFound() {
        assertNull(schemaService.getSchema("unknownSubject-value", Optional.empty(), context));
    }
}
