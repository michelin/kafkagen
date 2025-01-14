package com.michelin.kafkagen;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.base.GeneratorBase;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.michelin.kafkagen.models.Record;
import com.michelin.kafkagen.services.ConfigService;
import com.michelin.kafkagen.services.DatasetService;
import com.michelin.kafkagen.services.SchemaService;
import com.michelin.kafkagen.utils.IOUtils;
import jakarta.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine;
import picocli.CommandLine.Option;

@Slf4j
@CommandLine.Command(name = "dataset",
    headerHeading = "@|bold Usage|@:",
    synopsisHeading = " ",
    descriptionHeading = "%n@|bold Description|@:%n%n",
    description = "Create a dataset for the given topic",
    parameterListHeading = "%n@|bold Parameters|@:%n",
    optionListHeading = "%n@|bold Options|@:%n",
    commandListHeading = "%n@|bold Commands|@:%n",
    usageHelpAutoWidth = true,
    mixinStandardHelpOptions = true)
public class DatasetSubcommand extends ValidCurrentContextHook {

    @CommandLine.Parameters(description = "Name of the topic", arity = "1")
    public String topic;

    @CommandLine.Option(names = {"-f", "--format"}, description = "Format (json or yaml) of the sample record")
    public String format = "json";

    @CommandLine.Option(names = {"-s", "--save"}, description = "File to save the sample record")
    public Optional<File> file;

    @CommandLine.Option(names = {"-p", "--pretty"}, description = "Include type specification for union field")
    public boolean pretty;
    @CommandLine.Option(names = {"-c", "--compact"}, description = "Compact the dataset to keep only the most recent "
        + "record for each key, removing tombstones")
    public boolean compact;

    @CommandLine.Option(names = {"-k", "--key"}, description = "Export a dataset by its key")
    public Optional<String> key;

    @CommandLine.Option(names = {"-o", "--offset"}, arity = "1..*",
        description = "Details of the messages offset to export. Ex: 0=0-10 1=8,10 2 will export messages from the "
            + "partition 0 from offset 0 to 10, plus messages with offset 8 and 10 from the partition 1 and all the "
            + "messages from partition 2",
        mapFallbackValue = Option.NULL_VALUE)
    public Map<String, Optional<String>> offsets;

    public SchemaService schemaService;
    public DatasetService datasetService;

    @Inject
    public DatasetSubcommand(ConfigService configService, SchemaService schemaService, DatasetService datasetService) {
        super(configService);
        this.schemaService = schemaService;
        this.datasetService = datasetService;
    }

    @Override
    public Integer callSubCommand() {
        if (offsets == null && key.isEmpty()) {
            commandSpec.commandLine().getErr().println("No offset (-o) or key (-k) provided. Please provide at least "
                + "one offset or key");
            return CommandLine.ExitCode.USAGE;
        }

        StringWriter out = new StringWriter();
        final GeneratorBase generator = IOUtils.getGenerator(format, out);

        List<Record> records;

        if (key.isPresent()) {
            records = datasetService.getDatasetForKey(topic, key.get(), pretty, currentContext.get());
        } else {
            // Get the records from the topic
            records = offsets.keySet().stream().map((String key) -> {
                int partition = Integer.parseInt(key);
                Long startOffset = null;
                Long endOffset = null;
                List<Long> offsetsList = null;

                Optional<String> offsetOpt = offsets.get(key);
                if (offsetOpt.isPresent()) {
                    String offset = offsetOpt.get();
                    // Range of offsets
                    if (offset.contains("-")) {
                        String[] offsetsRange = offset.split("-");
                        startOffset = Long.parseLong(offsetsRange[0]);
                        endOffset = Long.parseLong(offsetsRange[1]);
                    } else if (offset.contains(",")) {
                        // List of offsets
                        offsetsList = Arrays.stream(offset.split(","))
                            .map(Long::parseLong)
                            .toList();
                    } else {
                        // Single offset
                        startOffset = Long.parseLong(offset);
                    }
                }

                return datasetService.getDatasetFromTopic(topic, partition, startOffset, endOffset, offsetsList,
                    pretty, currentContext.get());
                }
            )
            .flatMap(List::stream)
            .toList();
        }

        var mapper = new ObjectMapper();
        mapper.setSerializationInclusion(Include.NON_NULL);

        // Convert the records to a list of Kafkagen Record
        var kafkagenRecords = records.stream().map(record -> {
            Object key = null;
            Object value = null;

            try {
                if (record.getKey() != null) {
                    key = record.getKey().getClass().isAssignableFrom(String.class)
                        ? record.getKey()
                        : mapper.readValue(record.getKey().toString(), Object.class);
                }

                if (record.getValue() != null) {
                    value = record.getValue().getClass().isAssignableFrom(String.class)
                        ? record.getValue()
                        : mapper.readValue(record.getValue().toString(), Object.class);
                }
            } catch (IOException e) {
                log.debug("", e);
            }

            return Record.builder()
                .mostRecent(null)
                .timestamp(record.getTimestamp())
                .headers(record.getHeaders())
                .key(key)
                .value(value)
                .build();
        }).toList();

        if (compact) {
            kafkagenRecords = datasetService.compact(kafkagenRecords);
        }

        generator.setCodec(mapper);

        // Write the records to the output
        try {
            generator.writeObject(kafkagenRecords);

            if (file.isPresent() && !out.toString().isEmpty()) {
                IOUtils.writeOutToFile(file.get(), out);
            }

            commandSpec.commandLine().getOut().println(out);

            generator.close();
        } catch (IOException e) {
            commandSpec.commandLine().getErr().println("Dataset export failed due to the following error: " + e.getMessage());
            return CommandLine.ExitCode.SOFTWARE;
        }

        return CommandLine.ExitCode.OK;
    }

}
