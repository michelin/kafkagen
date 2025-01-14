package com.michelin.kafkagen;

import com.michelin.kafkagen.kafka.GenericProducer;
import com.michelin.kafkagen.models.Dataset;
import com.michelin.kafkagen.services.ConfigService;
import com.michelin.kafkagen.services.DatasetService;
import jakarta.inject.Inject;
import java.io.File;
import java.util.List;
import java.util.Optional;
import picocli.CommandLine;

@CommandLine.Command(name = "produce",
    headerHeading = "@|bold Usage|@:",
    synopsisHeading = " ",
    descriptionHeading = "%n@|bold Description|@:%n%n",
    description = "Produce a dataset into a given topic",
    parameterListHeading = "%n@|bold Parameters|@:%n",
    optionListHeading = "%n@|bold Options|@:%n",
    commandListHeading = "%n@|bold Commands|@:%n",
    usageHelpAutoWidth = true,
    mixinStandardHelpOptions = true)
public class ProduceSubcommand extends ValidCurrentContextHook {

    @CommandLine.Parameters(description = "Name of the topic", arity = "0..1")
    public Optional<String> topic;

    // YAML file or directory containing YAML resources to apply
    @CommandLine.Option(names = {"-f", "--file"}, description = "YAML/JSON File containing the dataset to insert")
    public Optional<File> file;

    public GenericProducer genericProducer;
    public DatasetService datasetService;

    @Inject
    public ProduceSubcommand(ConfigService configService, GenericProducer genericProducer,
                             DatasetService datasetService) {
        super(configService);
        this.genericProducer = genericProducer;
        this.datasetService = datasetService;
    }

    @Override
    public Integer callSubCommand() {
        if (file.isEmpty()) {
            throw new CommandLine.ParameterException(commandSpec.commandLine(), "No dataset given. Try --help to see "
                + "the command usage");
        }

        try {
            // No topic: each record define its topic
            if (topic.isEmpty()) {
                List<Dataset> datasets = datasetService.getDataset(file.get(), currentContext.get());
                genericProducer.produce(datasets, 1, currentContext.get());

                datasets.forEach(d ->
                    commandSpec.commandLine().getOut().println(String.format("Produced %d records in %s with %s (key) "
                            + "- %s (value)",
                        d.getRecords().size(), d.getTopic(), d.getKeySerializer().getSimpleName(),
                        d.getValueSerializer().getSimpleName()))
                );
            } else {
                // Otherwise, 1 topic for all the record
                Dataset dataset = datasetService.getDataset(file.get(), topic.get(), currentContext.get());

                genericProducer.produce(topic.get(), dataset, 1, currentContext.get());
                commandSpec.commandLine().getOut()
                    .println(String.format("Produced %d records in %s with %s (key) - %s (value)",
                        dataset.getRecords().size(),
                        dataset.getTopic(),
                        dataset.getKeySerializer().getSimpleName(),
                        dataset.getValueSerializer().getSimpleName()));
            }
        } catch (RuntimeException e) {
            commandSpec.commandLine().getErr().println("Produce failed due to the following error: " + e.getMessage());
            return CommandLine.ExitCode.SOFTWARE;
        }

        return CommandLine.ExitCode.OK;
    }
}
