package com.michelin.kafkagen.config;

import io.smallrye.config.ConfigSourceContext;
import io.smallrye.config.ConfigSourceFactory;
import io.smallrye.config.source.yaml.YamlConfigSource;
import java.net.URL;
import java.util.Collections;
import java.util.OptionalInt;
import org.eclipse.microprofile.config.spi.ConfigSource;

public class KafkagenConfigSourceFactory implements ConfigSourceFactory {

    @Override
    public Iterable<ConfigSource> getConfigSources(final ConfigSourceContext context) {
        YamlConfigSource yamlConfigSource = null;
        try {
            if (System.getenv().keySet().stream().noneMatch(s -> s.startsWith("KAFKAGEN_"))) {
                yamlConfigSource = new YamlConfigSource(
                    new URL("file:///" + System.getProperty("user.home") + "/.kafkagen/config.yml"));
            }

            if (System.getenv("KAFKAGEN_CONFIG") != null) {
                yamlConfigSource = new YamlConfigSource("kafkagen", System.getenv("KAFKAGEN_CONFIG"));
            }

            return Collections.singletonList(yamlConfigSource);
        } catch (Exception e) {
            return Collections.emptyList();
        }
    }

    @Override
    public OptionalInt getPriority() {
        return OptionalInt.of(290);
    }
}
