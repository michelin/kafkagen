package com.michelin.kafkagen;

import com.michelin.kafkagen.config.KafkagenConfig;
import com.michelin.kafkagen.services.ConfigService;
import jakarta.inject.Inject;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.concurrent.Callable;
import lombok.Getter;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;
import picocli.CommandLine;

@CommandLine.Command(name = "config",
    headerHeading = "@|bold Usage|@:",
    synopsisHeading = " ",
    descriptionHeading = "%n@|bold Description|@:%n%n",
    description = "Manage configuration.",
    parameterListHeading = "%n@|bold Parameters|@:%n",
    optionListHeading = "%n@|bold Options|@:%n",
    commandListHeading = "%n@|bold Commands|@:%n",
    usageHelpAutoWidth = true,
    mixinStandardHelpOptions = true)
public class ConfigSubcommand implements Callable<Integer> {
    @CommandLine.Parameters(index = "0", description = "Action to perform (${COMPLETION-CANDIDATES}).", arity = "1")
    public ConfigAction action;

    @CommandLine.Parameters(index = "1", defaultValue = "", description = "Context to use.", arity = "1")
    public String context;

    @CommandLine.Spec
    public CommandLine.Model.CommandSpec commandSpec;

    public KafkagenConfig kafkagenConfig;
    public ConfigService configService;

    @Inject
    public ConfigSubcommand(KafkagenConfig kafkagenConfig, ConfigService configService) {
        this.kafkagenConfig = kafkagenConfig;
        this.configService = configService;
    }

    /**
     * Run the "config" command.
     *
     * @return The command return code
     * @throws Exception Any exception during the run
     */
    @Override
    public Integer call() throws Exception {
        if (action.equals(ConfigAction.CURRENT_CONTEXT)) {
            commandSpec.commandLine().getOut().println(kafkagenConfig.currentContext());
            return CommandLine.ExitCode.OK;
        }

        if (kafkagenConfig.contexts().isEmpty()) {
            commandSpec.commandLine().getOut().println("No context pre-defined.");
            return CommandLine.ExitCode.OK;
        }

        if (action.equals(ConfigAction.GET_CONTEXTS)) {
            var contexts = new HashMap<String, Object>();
            kafkagenConfig.contexts().forEach(c -> {
                var config = new LinkedHashMap<String, String>();

                config.put("bootstrap-servers", c.definition().bootstrapServers());
                if (c.definition().saslJaasConfig().isPresent()) {
                    config.put("sasl-jaas-config", c.definition().saslJaasConfig().get());
                }
                if (c.definition().securityProtocol().isPresent()) {
                    config.put("security-protocol", c.definition().securityProtocol().get());
                }
                if (c.definition().saslMechanism().isPresent()) {
                    config.put("sasl-mechanism", c.definition().saslMechanism().get());
                }

                if (c.definition().registryUrl().isPresent()) {
                    config.put("registry-url", c.definition().registryUrl().get());
                }
                if (c.definition().registryUsername().isPresent() && c.definition().registryPassword().isPresent()) {
                    config.put("registry-username", c.definition().registryUsername().get());
                    config.put("registry-password", c.definition().registryPassword().get());
                }
                if (c.definition().partitionerClass().isPresent()) {
                    config.put("partitioner-class", c.definition().partitionerClass().get());
                }

                contexts.put(c.name(), config);
            });
            var options = new DumperOptions();
            options.setExplicitStart(true);
            options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
            options.setDefaultScalarStyle(DumperOptions.ScalarStyle.PLAIN);
            var yaml = new Yaml(options);
            commandSpec.commandLine().getOut().println(yaml.dump(contexts));
            return CommandLine.ExitCode.OK;
        }

        var optionalContextToSet = configService.getContextByName(context);
        if (optionalContextToSet.isEmpty()) {
            commandSpec.commandLine().getErr().println("No context exists with the name: " + context);
            return CommandLine.ExitCode.USAGE;
        }

        var contextToSet = optionalContextToSet.get();
        if (action.equals(ConfigAction.USE_CONTEXT)) {
            configService.updateConfigurationContext(contextToSet);
            commandSpec.commandLine().getOut().println("Switched to context \"" + context + "\".");
            return CommandLine.ExitCode.OK;
        }

        return CommandLine.ExitCode.OK;
    }
}

@SuppressWarnings("checkstyle:OneTopLevelClass")
@Getter
enum ConfigAction {
    GET_CONTEXTS("get-contexts"),
    CURRENT_CONTEXT("current-context"),
    USE_CONTEXT("use-context");

    private final String name;

    ConfigAction(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }
}
