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
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Slf4j
@QuarkusTest
@QuarkusTestResource(value = KafkaTestResource.class, restrictToAnnotatedClass = true)
public class SchemaServiceIntegrationTest extends AbstractIntegrationTest {

    @Inject
    SchemaService schemaService;

    @Test
    public void getLatestSchema() throws IOException {
        var schema = new AvroSchema(new Schema.Parser().parse(
                new String(Files.readAllBytes(Paths.get("src/test/resources/avro/schemas/avroValueSchema.avsc")))));
        var schemaV2 = new AvroSchema(new Schema.Parser().parse(
                new String(Files.readAllBytes(Paths.get("src/test/resources/avro/schemas/avroValueSchemaV2.avsc")))));

        createSubjects("subject-value", schema);
        createSubjects("subject-value", schemaV2);

        ParsedSchema actualSchema = schemaService.getLatestSchema(String.format("http://%s:%s", registryHost, registryPort), "subject-value", Optional.empty(), Optional.empty());

        assertEquals(schemaV2, actualSchema);
    }

    @Test
    public void schemaNotFound() {
        assertThrows(RuntimeException.class, () ->
                schemaService.getLatestSchema(String.format("http://%s:%s", registryHost, registryPort),
                        "unknownSubject-value", Optional.empty(), Optional.empty()));
    }
}
