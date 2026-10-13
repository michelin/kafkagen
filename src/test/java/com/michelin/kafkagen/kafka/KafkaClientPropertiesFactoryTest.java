package com.michelin.kafkagen.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.michelin.kafkagen.config.KafkagenConfig;
import com.michelin.kafkagen.utils.InsecureSslEngineFactory;
import io.smallrye.config.SmallRyeConfigBuilder;
import io.smallrye.config.source.yaml.YamlConfigSource;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class KafkaClientPropertiesFactoryTest {
    private static final String ALLOWED_URLS = BrokerSecurityConfigs.ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG;

    @AfterEach
    void tearDown() {
        System.clearProperty(ALLOWED_URLS);
    }

    private static KafkagenConfig load(String yaml) {
        return new SmallRyeConfigBuilder()
            .withSources(new YamlConfigSource("test", yaml))
            .withMapping(KafkagenConfig.class)
            .build()
            .getConfigMapping(KafkagenConfig.class);
    }

    private static KafkagenConfig loadContext(String context) {
        return load("""
            kafkagen:
              contexts:
                - name: test
                  context:
            """ + context.indent(8));
    }

    @Test
    void plaintextByDefault() {
        var config = loadContext("bootstrap-servers: localhost:9092");
        var context = config.contexts().getFirst();

        assertTrue(context.definition().kafkaProperties().isEmpty());
        assertTrue(context.definition().registryProperties().isEmpty());

        var props = KafkaClientPropertiesFactory.kafkaProperties(context, config);
        assertEquals("localhost:9092", props.get("bootstrap.servers"));
        assertEquals("PLAINTEXT", props.get("security.protocol"));
        assertFalse(props.containsKey("sasl.jaas.config"));
        assertTrue(KafkaClientPropertiesFactory.registryProperties(context, config).isEmpty());
        assertNull(System.getProperty(ALLOWED_URLS));
    }

    @Test
    void sslSecurityProtocolWithoutSasl() {
        var config = loadContext("""
            bootstrap-servers: localhost:9093
            security-protocol: SSL
            kafka-properties:
              ssl.keystore.location: /path/to/keystore.p12
              ssl.keystore.password: changeit
            """);
        var props = KafkaClientPropertiesFactory.kafkaProperties(config.contexts().getFirst(), config);

        assertEquals("SSL", props.get("security.protocol"));
        assertEquals("/path/to/keystore.p12", props.get("ssl.keystore.location"));
        assertEquals("changeit", props.get("ssl.keystore.password"));
    }

    @Test
    void saslJaasConfigRequiresProtocolAndMechanism() {
        var config = loadContext("""
            bootstrap-servers: localhost:9092
            sasl-jaas-config: jaas
            """);
        var context = config.contexts().getFirst();

        assertThrows(IllegalArgumentException.class,
            () -> KafkaClientPropertiesFactory.kafkaProperties(context, config));
    }

    @Test
    void oauthBearer() {
        var config = load("""
            kafkagen:
              truststore-location: /path/to/truststore.jks
              insecure-ssl: true
              contexts:
                - name: oauth
                  context:
                    bootstrap-servers: broker:9093
                    security-protocol: SASL_SSL
                    sasl-mechanism: OAUTHBEARER
                    sasl-jaas-config: org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required clientId="id" clientSecret="secret";
                    kafka-properties:
                      sasl.login.callback.handler.class: org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler
                      sasl.oauthbearer.token.endpoint.url: https://idp/oauth2/token
                    registry-url: https://registry
                    registry-properties:
                      bearer.auth.credentials.source: OAUTHBEARER
                      bearer.auth.issuer.endpoint.url: https://idp/oauth2/registry-token
                      bearer.auth.client.id: id
                      bearer.auth.client.secret: secret
            """);
        var context = config.contexts().getFirst();

        var kafkaProps = KafkaClientPropertiesFactory.kafkaProperties(context, config);
        assertEquals("SASL_SSL", kafkaProps.get("security.protocol"));
        assertEquals("OAUTHBEARER", kafkaProps.get("sasl.mechanism"));
        assertEquals("org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler",
            kafkaProps.get("sasl.login.callback.handler.class"));
        assertEquals("https://idp/oauth2/token", kafkaProps.get("sasl.oauthbearer.token.endpoint.url"));
        assertEquals("/path/to/truststore.jks", kafkaProps.get("ssl.truststore.location"));
        assertEquals(InsecureSslEngineFactory.class, kafkaProps.get("ssl.engine.factory.class"));

        var registryProps = KafkaClientPropertiesFactory.registryProperties(context, config);
        assertEquals("https://registry", registryProps.get("schema.registry.url"));
        assertEquals("OAUTHBEARER", registryProps.get("bearer.auth.credentials.source"));
        assertEquals("https://idp/oauth2/registry-token", registryProps.get("bearer.auth.issuer.endpoint.url"));
        assertEquals("/path/to/truststore.jks", registryProps.get("schema.registry.ssl.truststore.location"));
        assertFalse(registryProps.containsKey("basic.auth.credentials.source"));

        assertEquals("https://idp/oauth2/token,https://idp/oauth2/registry-token", System.getProperty(ALLOWED_URLS));
    }

    @Test
    void kafkaPropertiesOverrideNamedSettings() {
        var config = loadContext("""
            bootstrap-servers: localhost:9092
            security-protocol: SASL_PLAINTEXT
            kafka-properties:
              security.protocol: SASL_SSL
              sasl.mechanism: PLAIN
              sasl.jaas.config: jaas
            """);
        var props = KafkaClientPropertiesFactory.kafkaProperties(config.contexts().getFirst(), config);

        assertEquals("SASL_SSL", props.get("security.protocol"));
        assertEquals("PLAIN", props.get("sasl.mechanism"));
        assertEquals("jaas", props.get("sasl.jaas.config"));
    }

    @Test
    void existingAllowedUrlsArePreserved() {
        System.setProperty(ALLOWED_URLS, "https://other/token");
        var config = loadContext("""
            bootstrap-servers: localhost:9092
            kafka-properties:
              sasl.oauthbearer.token.endpoint.url: https://idp/oauth2/token
            """);
        var context = config.contexts().getFirst();

        KafkaClientPropertiesFactory.kafkaProperties(context, config);
        KafkaClientPropertiesFactory.kafkaProperties(context, config);

        assertEquals("https://other/token,https://idp/oauth2/token", System.getProperty(ALLOWED_URLS));
    }

    @Test
    void registryPropertiesOverrideSchemaRegistryUrl() {
        var config = loadContext("""
            bootstrap-servers: localhost:9092
            registry-url: http://registry
            registry-properties:
              schema.registry.url: http://overridden-registry
            """);
        var props = KafkaClientPropertiesFactory.registryProperties(config.contexts().getFirst(), config);

        assertEquals("http://overridden-registry", props.get("schema.registry.url"));
    }

    @Test
    void deprecatedRegistryCredentials() {
        var config = loadContext("""
            bootstrap-servers: localhost:9092
            registry-url: http://registry
            registry-username: user
            registry-password: pass
            """);
        var props = KafkaClientPropertiesFactory.registryProperties(config.contexts().getFirst(), config);

        assertEquals("USER_INFO", props.get("basic.auth.credentials.source"));
        assertEquals("user:pass", props.get("basic.auth.user.info"));
    }

    @Test
    void deprecatedRegistryCredentialsConflictWithBearer() {
        var config = loadContext("""
            bootstrap-servers: localhost:9092
            registry-url: http://registry
            registry-username: user
            registry-password: pass
            registry-properties:
              bearer.auth.credentials.source: OAUTHBEARER
            """);
        var context = config.contexts().getFirst();

        assertThrows(IllegalArgumentException.class,
            () -> KafkaClientPropertiesFactory.registryProperties(context, config));
    }
}
