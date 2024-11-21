package com.michelin.kafkagen;

import com.michelin.kafkagen.config.KafkagenConfig;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.kafka.clients.admin.Admin;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public abstract class AbstractIntegrationTest {
    @ConfigProperty(name = "kafka.host")
    protected String host;
    @ConfigProperty(name = "kafka.port")
    protected String port;
    @ConfigProperty(name = "registry.host")
    protected String registryHost;
    @ConfigProperty(name = "registry.port")
    protected String registryPort;

    private static Admin adminClient;

    /**
     * Getter for admin client service.
     *
     * @return The admin client
     */
    public Admin getAdminClient() {
        if (adminClient == null) {
            adminClient = Admin.create(Map.of("bootstrap.servers", host + ":" + Integer.parseInt(port)));
        }
        return adminClient;
    }

    public String readFileFromResources(String path) throws IOException {
        return new String(Files.readAllBytes(Paths.get("src/test/resources/" + path))).trim();
    }

    protected void createSubjects(String subjectName, ParsedSchema schema) {
        try {
            var schemaRegistryClient = new CachedSchemaRegistryClient(
                    String.format("http://%s:%d", registryHost, Integer.parseInt(registryPort)), 1000);
            schemaRegistryClient.register(subjectName, schema);
            schemaRegistryClient.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @ApplicationScoped
    @io.quarkus.test.Mock
    public static class MockedConfig implements KafkagenConfig {

        @Override
        public String currentContext() {
            return null;
        }

        @Override
        public List<Context> contexts() {
            return null;
        }
    }

    @ApplicationScoped
    public static class MockedContext implements KafkagenConfig.Context {

        @Override
        public String name() {
            return "context";
        }

        @Override
        public KafkaContext definition() {
            return new MockedKafkaContext();
        }

        @ApplicationScoped
        public static class MockedKafkaContext implements KafkaContext {

            @Override
            public String bootstrapServers() {
                return "bootstrapServers";
            }

            @Override
            public Optional<String> groupIdPrefix() {
                return Optional.of("groupIdPrefix");
            }

            @Override
            public Optional<String> securityProtocol() {
                return Optional.of("securityProtocol");
            }

            @Override
            public Optional<String> saslMechanism() {
                return Optional.of("saslMechanism");
            }

            @Override
            public Optional<String> saslJaasConfig() {
                return Optional.of("saslJaasConfig");
            }

            @Override
            public Optional<String> registryUrl() {
                return Optional.of("registryUrl");
            }

            @Override
            public Optional<String> registryUsername() {
                return Optional.of("registryUsername");
            }

            @Override
            public Optional<String> registryPassword() {
                return Optional.of("registryPassword");
            }

            @Override
            public Optional<String> partitionerClass() {
                return Optional.of("partitionerClass");
            }
        }
    }
}
