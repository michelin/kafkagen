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

package com.michelin.kafkagen.kafka;

import com.michelin.kafkagen.config.KafkagenConfig;
import com.michelin.kafkagen.utils.InsecureSslEngineFactory;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.quarkus.runtime.annotations.RegisterForReflection;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.security.oauthbearer.ClientCredentialsJwtRetriever;
import org.apache.kafka.common.security.oauthbearer.ClientJwtValidator;
import org.apache.kafka.common.security.oauthbearer.DefaultJwtRetriever;
import org.apache.kafka.common.security.oauthbearer.DefaultJwtValidator;
import org.apache.kafka.common.security.oauthbearer.FileJwtRetriever;
import org.apache.kafka.common.security.oauthbearer.JwtBearerJwtRetriever;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;

/**
 * Build the Kafka and Schema Registry client properties from a Kafkagen context.
 * Named context fields are applied first, then the free-form kafka-properties / registry-properties
 * maps are applied last so they can override anything.
 */
@Slf4j
@RegisterForReflection(targets = {
    OAuthBearerLoginModule.class,
    OAuthBearerLoginCallbackHandler.class,
    DefaultJwtRetriever.class,
    ClientCredentialsJwtRetriever.class,
    JwtBearerJwtRetriever.class,
    FileJwtRetriever.class,
    DefaultJwtValidator.class,
    ClientJwtValidator.class
})
public final class KafkaClientPropertiesFactory {
    static final String BASIC_AUTH_CREDENTIALS_SOURCE = "basic.auth.credentials.source";
    static final String BASIC_AUTH_USER_INFO = "basic.auth.user.info";
    static final String BEARER_AUTH_CREDENTIALS_SOURCE = "bearer.auth.credentials.source";
    static final String BEARER_AUTH_ISSUER_ENDPOINT_URL = "bearer.auth.issuer.endpoint.url";

    private KafkaClientPropertiesFactory() {
    }

    /**
     * Build the Kafka client (producer/consumer) connection and security properties.
     *
     * @param context        - the Kafkagen context
     * @param kafkagenConfig - the global Kafkagen configuration
     * @return the Kafka client properties
     */
    public static Map<String, Object> kafkaProperties(KafkagenConfig.Context context,
                                                      KafkagenConfig kafkagenConfig) {
        var definition = context.definition();
        var settings = new HashMap<String, Object>();

        settings.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, definition.bootstrapServers());
        definition.securityProtocol()
            .ifPresent(securityProtocol -> settings.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol));
        definition.saslMechanism().ifPresent(saslMechanism -> settings.put(SaslConfigs.SASL_MECHANISM, saslMechanism));
        definition.saslJaasConfig().ifPresent(saslJaasConfig -> settings.put(SaslConfigs.SASL_JAAS_CONFIG, saslJaasConfig));

        kafkagenConfig.truststoreLocation()
            .ifPresent(truststore -> settings.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststore));

        if (kafkagenConfig.insecureSsl().orElse(false)) {
            settings.put(SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG, InsecureSslEngineFactory.class);
        }

        settings.putAll(definition.kafkaProperties());

        if (settings.containsKey(SaslConfigs.SASL_JAAS_CONFIG)
            && (!settings.containsKey(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG)
            || !settings.containsKey(SaslConfigs.SASL_MECHANISM))) {
            throw new IllegalArgumentException(
                "security-protocol and sasl-mechanism settings are mandatory when sasl-jaas-config is set");
        }

        settings.putIfAbsent(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT");

        allowOAuthBearerUrls(context);

        return settings;
    }

    /**
     * Build the Schema Registry client properties (also used by the Confluent serializers).
     *
     * @param context        - the Kafkagen context
     * @param kafkagenConfig - the global Kafkagen configuration
     * @return the Schema Registry client properties, empty if no registry is configured
     */
    @SuppressWarnings("deprecation")
    public static Map<String, Object> registryProperties(KafkagenConfig.Context context,
                                                         KafkagenConfig kafkagenConfig) {
        var definition = context.definition();
        var settings = new HashMap<String, Object>();

        if (definition.registryUrl().isEmpty()) {
            return settings;
        }

        settings.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, definition.registryUrl().get());

        var registryProperties = definition.registryProperties();
        var registryUsername = definition.registryUsername();
        var registryPassword = definition.registryPassword();

        if (registryUsername.isPresent() && registryPassword.isPresent()) {
            if (registryProperties.containsKey(BEARER_AUTH_CREDENTIALS_SOURCE)) {
                throw new IllegalArgumentException("registry-username/registry-password cannot be used together with "
                    + "registry-properties." + BEARER_AUTH_CREDENTIALS_SOURCE);
            }

            log.warn("registry-username and registry-password are deprecated and will be removed in a future release. "
                + "Use registry-properties with " + BASIC_AUTH_CREDENTIALS_SOURCE + " and "
                + BASIC_AUTH_USER_INFO + " instead.");
            settings.put(BASIC_AUTH_CREDENTIALS_SOURCE, "USER_INFO");
            settings.put(BASIC_AUTH_USER_INFO, String.format("%s:%s", registryUsername.get(), registryPassword.get()));
        }

        kafkagenConfig.truststoreLocation().ifPresent(truststore -> settings.put(
            SchemaRegistryClientConfig.CLIENT_NAMESPACE + SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststore));

        settings.putAll(registryProperties);

        allowOAuthBearerUrls(context);

        return settings;
    }

    /**
     * Since Kafka 4.0, OAuth token endpoints must be explicitly allowed through a JVM system property.
     * Add the endpoints declared in the context to that allow-list, keeping any value already set by the user.
     *
     * @param context - the Kafkagen context
     */
    static void allowOAuthBearerUrls(KafkagenConfig.Context context) {
        var definition = context.definition();
        var contextUrls = Stream.of(
                definition.kafkaProperties().get(SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL),
                definition.kafkaProperties().get(SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_URL),
                definition.registryProperties().get(BEARER_AUTH_ISSUER_ENDPOINT_URL))
            .filter(Objects::nonNull)
            .map(String::trim)
            .filter(url -> !url.isEmpty())
            .toList();

        if (contextUrls.isEmpty()) {
            return;
        }

        var property = BrokerSecurityConfigs.ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG;
        Set<String> allowedUrls = new LinkedHashSet<>();
        var existing = System.getProperty(property);
        if (existing != null && !existing.isBlank()) {
            Arrays.stream(existing.split(","))
                .map(String::trim)
                .filter(url -> !url.isEmpty())
                .forEach(allowedUrls::add);
        }
        allowedUrls.addAll(contextUrls);

        System.setProperty(property, allowedUrls.stream().collect(Collectors.joining(",")));
    }
}
