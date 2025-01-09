package com.michelin.kafkagen;

import com.michelin.kafkagen.config.KafkagenConfig;
import com.michelin.kafkagen.services.ConfigService;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.logging.LogManager;
import org.jboss.logmanager.Level;
import picocli.CommandLine;

public abstract class ValidCurrentContextHook implements Callable<Integer> {

    @CommandLine.Option(names = {"-v", "--verbose"}, description = "Show more information about the execution")
    public boolean verbose;

    @CommandLine.Spec
    CommandLine.Model.CommandSpec commandSpec;

    public ConfigService configService;

    protected Optional<KafkagenConfig.Context> currentContext;

    public ValidCurrentContextHook(ConfigService configService) {
        this.configService = configService;
    }

    public Integer call() throws Exception {
        if (verbose) {
            LogManager.getLogManager().getLogger("com.michelin").setLevel(Level.DEBUG);
        }

        String currentContextName = configService.getCurrentContextName();
        currentContext = configService.getContextByName(currentContextName);

        if (currentContext.isEmpty()) {
            commandSpec.commandLine().getErr()
                .println("No context selected. Please list/set the context with the config command");
            return CommandLine.ExitCode.SOFTWARE;
        }

        return callSubCommand();
    }

    protected abstract Integer callSubCommand() throws Exception;

}
