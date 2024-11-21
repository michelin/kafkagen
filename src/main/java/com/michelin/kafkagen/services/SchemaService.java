package com.michelin.kafkagen.services;

import com.michelin.kafkagen.config.KafkagenConfig;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import jakarta.inject.Singleton;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Singleton
public class SchemaService {

    private final Map<String, ParsedSchema> cachedSchemas = new HashMap<>();

    @Getter
    private SchemaRegistryClient schemaRegistryClient;

    /**
     * Get the latest schema from the registry for a subject.
     *
     * @param subject - the subject to search
     * @param context - the user's context
     * @return the schema if found, null otherwise
     */
    public ParsedSchema getLatestSchema(String subject, KafkagenConfig.Context context) {
        try {
            return getLatestSchema(context.definition().registryUrl().get(), subject,
                context.definition().registryUsername(), context.definition().registryPassword());
        } catch (Exception e) {
            log.debug("Subject {} not found", subject);
            return null;
        }
    }

    /**
     * Get the latest schema in the registry for a subject.
     *
     * @param registryUrl      - the registry URL
     * @param subject          - the subject name
     * @param registryUsername - the registry username
     * @param registryPassword - the registry password
     * @return the latest schema for the input subject
     * @throws RuntimeException if the schema is not found
     */
    public ParsedSchema getLatestSchema(String registryUrl, String subject, Optional<String> registryUsername,
                                        Optional<String> registryPassword) {
        if (cachedSchemas.containsKey(subject)) {
            return cachedSchemas.get(subject);
        }

        if (schemaRegistryClient == null) {
            Map<String, String> props = new HashMap<>();
            if (registryUsername.isPresent() && registryPassword.isPresent()) {
                props = Map.of("basic.auth.credentials.source", "USER_INFO",
                    "basic.auth.user.info", String.format("%s:%s", registryUsername.get(), registryPassword.get()));
            }
            schemaRegistryClient = new CachedSchemaRegistryClient(registryUrl, 20,
                List.of(new AvroSchemaProvider(), new JsonSchemaProvider(), new ProtobufSchemaProvider()), props);
        }

        try {
            List<ParsedSchema> schemas = schemaRegistryClient.getSchemas(subject, false, true);

            if (schemas.isEmpty()) {
                throw new RuntimeException(String.format(
                    "Schema not found. Please check that the subject <%s> exists or use a String serializer.",
                    subject));
            }

            cachedSchemas.put(subject, schemas.getFirst());
            return schemas.getFirst();
        } catch (Exception e) {
            log.debug("", e);
            throw new RuntimeException(e);
        }
    }
}
