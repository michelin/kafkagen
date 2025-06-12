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

import com.michelin.kafkagen.config.KafkagenConfig;
import com.michelin.kafkagen.exceptions.SchemaNotFoundException;
import com.michelin.kafkagen.utils.SSLUtils;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.SslConfigs;

@Slf4j
@Singleton
public class SchemaService {

    ConfigService configService;

    private final Map<String, ParsedSchema> cachedSchemas = new HashMap<>();

    @Getter
    private SchemaRegistryClient schemaRegistryClient;

    @Inject
    public SchemaService(ConfigService configService) {
        this.configService = configService;
    }

    /**
     * Get the latest schema from the registry for a subject.
     *
     * @param subject - the subject to search
     * @param context - the user's context
     * @return the schema if found, null otherwise
     */
    public ParsedSchema getLatestSchema(String subject, KafkagenConfig.Context context) {
        return getSchema(subject, Optional.empty(), context);
    }

    /**
     * Get the latest schema in the registry for a subject.
     *
     * @param subject          - the subject name
     * @param context - the current context
     * @return the latest schema for the input subject
     * @throws RuntimeException if the schema is not found
     */
    public ParsedSchema getSchema(String subject, Optional<Integer> subjectVersion, KafkagenConfig.Context context) {
        log.debug("Getting schema for subject: {}, version: {}", subject, subjectVersion.isEmpty() ? "latest" : subjectVersion);

        if (cachedSchemas.containsKey(subject)) {
            return cachedSchemas.get(subject);
        }

        if (schemaRegistryClient == null) {
            initSchemaRegistryClient(context);
        }

        try {
            ParsedSchema schema;

            if (subjectVersion.isEmpty()) {
                List<ParsedSchema> schemas = schemaRegistryClient.getSchemas(subject, false, true);

                if (schemas.isEmpty()) {
                    throw new SchemaNotFoundException(subject);
                }

                schema = schemas.getFirst();
            } else {
                try {
                    SchemaMetadata schemaMetadata = schemaRegistryClient.getSchemaMetadata(subject, subjectVersion.get());
                    schema = schemaRegistryClient.getSchemaById(schemaMetadata.getId());
                } catch (RestClientException e) {
                    throw new SchemaNotFoundException(subject, subjectVersion.get());
                }
            }

            log.debug("Found schema for subject {}", subject);
            cachedSchemas.put(subject, schema);
            return schema;
        } catch (SchemaNotFoundException e) {
            log.debug("{}. Using StringSerializer instead.", e.getMessage());
            return null;
        } catch (Exception e) {
            log.debug("", e);
            return null;
        }
    }

    /**
     * Initialize the Schema Registry client based on the current context.
     *
     * @param context - the current Kafkagen context
     */
    private void initSchemaRegistryClient(KafkagenConfig.Context context) {
        if (context.definition().registryUrl().isPresent()) {
            Map<String, String> props = new HashMap<>();
            if (context.definition().registryUsername().isPresent() &&
                context.definition().registryPassword().isPresent()) {
                props.put("basic.auth.credentials.source", "USER_INFO");
                props.put("basic.auth.user.info",
                    String.format("%s:%s", context.definition().registryUsername().get(),
                        context.definition().registryPassword().get()));
            }

            // Setup the custom truststore location if given
            props.put(SchemaRegistryClientConfig.CLIENT_NAMESPACE + SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
                configService.kafkagenConfig.truststoreLocation().orElse(""));

            schemaRegistryClient = new CachedSchemaRegistryClient(context.definition().registryUrl().get(), 20,
                List.of(new AvroSchemaProvider(), new JsonSchemaProvider(), new ProtobufSchemaProvider()), props);

            if (configService.kafkagenConfig.insecureSsl().isPresent()
                && configService.kafkagenConfig.insecureSsl().get()) {
                // Disable SSL for the Kafka cluster connection
                SSLUtils.turnSchemaRegistryClientInsecure(schemaRegistryClient);
            }
        }
    }
}
