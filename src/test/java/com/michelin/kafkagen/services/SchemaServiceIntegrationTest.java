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
