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
import com.michelin.kafkagen.utils.SSLUtils;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
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
                props.put("basic.auth.credentials.source", "USER_INFO");
                props.put("basic.auth.user.info",
                    String.format("%s:%s", registryUsername.get(), registryPassword.get()));
            }

            // Setup the custom truststore location if given
            props.put(SchemaRegistryClientConfig.CLIENT_NAMESPACE + SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
                configService.kafkagenConfig.truststoreLocation().orElse(""));

            schemaRegistryClient = new CachedSchemaRegistryClient(registryUrl, 20,
                List.of(new AvroSchemaProvider(), new JsonSchemaProvider(), new ProtobufSchemaProvider()), props);

            if (configService.kafkagenConfig.insecureSsl().isPresent()
                && configService.kafkagenConfig.insecureSsl().get()) {
                // Disable SSL for the Kafka cluster connection
                SSLUtils.turnSchemaRegistryClientInsecure(schemaRegistryClient);
            }
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
