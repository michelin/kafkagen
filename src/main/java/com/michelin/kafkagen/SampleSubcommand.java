package com.michelin.kafkagen;

import com.fasterxml.jackson.core.base.GeneratorBase;
import com.michelin.kafkagen.services.ConfigService;
import com.michelin.kafkagen.services.DatasetService;
import com.michelin.kafkagen.services.SampleService;
import com.michelin.kafkagen.services.SchemaService;
import com.michelin.kafkagen.utils.IOUtils;
import jakarta.inject.Inject;
import java.io.File;
import java.io.StringWriter;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
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
public class SampleSubcommand extends ValidCurrentContextHook {

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

    public SchemaService schemaService;
    public DatasetService datasetService;
    public SampleService sampleService;

    @Inject
    public SampleSubcommand(ConfigService configService, SchemaService schemaService, DatasetService datasetService,
                            SampleService sampleService) {
        super(configService);
        this.schemaService = schemaService;
        this.configService = configService;
        this.datasetService = datasetService;
        this.sampleService = sampleService;
    }

    @Override
    public Integer callSubCommand() {
        StringWriter out = new StringWriter();
        GeneratorBase generator = IOUtils.getGenerator(format, out);

        try {
            sampleService.generateSample(topic, currentContext.get(), generator, pretty, withDoc);

            if (file.isPresent() && !out.toString().isEmpty()) {
                IOUtils.writeOutToFile(file.get(), out);
            }

            commandSpec.commandLine().getOut().println(out);

            generator.close();
        } catch (Exception e) {
            commandSpec.commandLine().getErr().println("Sample failed due to the following error: " + e.getMessage());
            return CommandLine.ExitCode.SOFTWARE;
        }

        return CommandLine.ExitCode.OK;
    }
}
