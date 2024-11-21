package com.michelin.kafkagen.services;

import com.michelin.kafkagen.config.KafkagenConfig;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

@Singleton
public class ConfigService {

    public KafkagenConfig kafkagenConfig;

    @Inject
    public ConfigService(KafkagenConfig kafkagenConfig) {
        this.kafkagenConfig = kafkagenConfig;
    }

    /**
     * Return the name of the current context.
     *
     * @return The current context name
     */
    public String getCurrentContextName() {
        return kafkagenConfig.contexts()
            .stream()
            .filter(context -> context.name().equals(kafkagenConfig.currentContext()))
            .findFirst()
            .map(KafkagenConfig.Context::name)
            .orElse(null);
    }

    /**
     * Get the current context infos if it exists.
     *
     * @return The current context
     */
    public Optional<KafkagenConfig.Context> getContextByName(String name) {
        return kafkagenConfig.contexts()
            .stream()
            .filter(context -> context.name().equals(name))
            .findFirst();
    }

    /**
     * Update the current configuration context with the given new context.
     *
     * @param contextToSet The context to set
     * @throws IOException Any exception during file writing
     */
    public void updateConfigurationContext(KafkagenConfig.Context contextToSet) throws IOException {
        var yaml = new Yaml();
        var initialFile = new File(System.getProperty("user.home") + "/.kafkagen/config.yml");
        var targetStream = new FileInputStream(initialFile);
        Map<String, LinkedHashMap<String, Object>> rootNodeConfig = yaml.load(targetStream);

        LinkedHashMap<String, Object> kafkagenNodeConfig = rootNodeConfig.get("kafkagen");
        kafkagenNodeConfig.put("current-context", contextToSet.name());

        var options = new DumperOptions();
        options.setIndent(2);
        options.setPrettyFlow(true);
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);

        var yamlMapper = new Yaml(options);
        var writer = new FileWriter(System.getProperty("user.home") + "/.kafkagen/config.yml");
        yamlMapper.dump(rootNodeConfig, writer);
    }
}
