package com.michelin.kafkagen;

import com.fasterxml.jackson.core.base.GeneratorBase;
import com.michelin.kafkagen.config.KafkagenConfig;
import com.michelin.kafkagen.services.ConfigService;
import com.michelin.kafkagen.services.DatasetService;
import com.michelin.kafkagen.services.SampleService;
import com.michelin.kafkagen.services.SchemaService;
import com.michelin.kafkagen.utils.IOUtils;
import jakarta.inject.Inject;
import java.io.File;
import java.io.StringWriter;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.logging.LogManager;
import lombok.extern.slf4j.Slf4j;
import org.jboss.logmanager.Level;
import picocli.CommandLine;

@Slf4j
@CommandLine.Command(name = "sample",
    headerHeading = "@|bold Usage|@:",
    synopsisHeading = " ",
    descriptionHeading = "%n@|bold Description|@:%n%n",
    description = "Create a sample record for the given topic",
    parameterListHeading = "%n@|bold Parameters|@:%n",
    optionListHeading = "%n@|bold Options|@:%n",
    commandListHeading = "%n@|bold Commands|@:%n",
    usageHelpAutoWidth = true,
    mixinStandardHelpOptions = true)
public class SampleSubcommand implements Callable<Integer> {

    @CommandLine.Parameters(description = "Name of the topic", arity = "1")
    public String topic;

    @CommandLine.Option(names = {"-f", "--format"}, description = "Format (json or yaml) of the sample record")
    public String format = "json";

    @CommandLine.Option(names = {"-s", "--save"}, description = "File to save the sample record")
    public Optional<File> file;

    @CommandLine.Option(names = {"-d", "--doc"}, description = "Include fields doc as comment in JSON sample record")
    public boolean withDoc;

    @CommandLine.Option(names = {"-p", "--pretty"}, description = "Include type specification for union field")
    public boolean pretty;

    @CommandLine.Option(names = {"-v", "--verbose"}, description = "Show more information about the execution")
    public boolean verbose;

    @CommandLine.Spec
    CommandLine.Model.CommandSpec commandSpec;

    public SchemaService schemaService;
    public ConfigService configService;
    public DatasetService datasetService;
    public SampleService sampleService;

    @Inject
    public SampleSubcommand(SchemaService schemaService, ConfigService configService, DatasetService datasetService,
                            SampleService sampleService) {
        this.schemaService = schemaService;
        this.configService = configService;
        this.datasetService = datasetService;
        this.sampleService = sampleService;
    }

    @Override
    public Integer call() throws Exception {
        if (verbose) {
            LogManager.getLogManager().getLogger("com.michelin").setLevel(Level.DEBUG);
        }

        String currentContextName = configService.getCurrentContextName();
        Optional<KafkagenConfig.Context> currentContext = configService.getContextByName(currentContextName);

        if (currentContext.isEmpty()) {
            commandSpec.commandLine().getErr()
                .println("No context selected. Please list/set the context with the config command");
            return CommandLine.ExitCode.SOFTWARE;
        }

        StringWriter out = new StringWriter();
        GeneratorBase generator = IOUtils.getGenerator(format, out);

        try {
            sampleService.generateSample(topic, currentContext.get(), generator, pretty, withDoc);
        } catch (Exception e) {
            commandSpec.commandLine().getErr().println("Sample failed due to the following error: " + e.getMessage());
            return CommandLine.ExitCode.SOFTWARE;
        }

        if (file.isPresent() && !out.toString().isEmpty()) {
            IOUtils.writeOutToFile(file.get(), out);
        }

        commandSpec.commandLine().getOut().println(out);

        generator.close();

        return CommandLine.ExitCode.OK;
    }
}
