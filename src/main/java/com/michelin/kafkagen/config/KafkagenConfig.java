package com.michelin.kafkagen.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;
import java.util.List;
import java.util.Optional;

@ConfigMapping(prefix = "kafkagen")
public interface KafkagenConfig {
    @WithDefault("no-context")
    String currentContext();

    Optional<Boolean> insecureSsl();

    Optional<String> truststoreLocation();

    List<Context> contexts();

    interface Context {
        String name();

        @WithName("context")
        KafkaContext definition();

        interface KafkaContext {
            String bootstrapServers();

            Optional<String> groupIdPrefix();

            Optional<String> securityProtocol();

            Optional<String> saslMechanism();

            Optional<String> saslJaasConfig();

            Optional<String> registryUrl();

            Optional<String> registryUsername();

            Optional<String> registryPassword();

            Optional<String> partitionerClass();
        }
    }
}
