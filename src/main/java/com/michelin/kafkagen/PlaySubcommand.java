/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.michelin.kafkagen;

import com.michelin.kafkagen.config.KafkagenConfig;
import com.michelin.kafkagen.kafka.GenericProducer;
import com.michelin.kafkagen.models.Dataset;
import com.michelin.kafkagen.models.Scenario;
import com.michelin.kafkagen.services.ConfigService;
import com.michelin.kafkagen.services.DatasetService;
import com.michelin.kafkagen.services.FileService;
import io.quarkus.runtime.util.StringUtil;
import jakarta.inject.Inject;
import java.io.File;
import java.util.List;
import java.util.Optional;
import picocli.CommandLine;

@CommandLine.Command(name = "play",
    headerHeading = "@|bold Usage|@:",
    synopsisHeading = " ",
    descriptionHeading = "%n@|bold Description|@:%n%n",
    description = "Play a scenario to insert dataset into a given topic",
    parameterListHeading = "%n@|bold Parameters|@:%n",
    optionListHeading = "%n@|bold Options|@:%n",
    commandListHeading = "%n@|bold Commands|@:%n",
    usageHelpAutoWidth = true,
    mixinStandardHelpOptions = true)
public class PlaySubcommand extends ValidCurrentContextHook {

    // YAML file or directory containing YAML resources to apply
    @CommandLine.Option(names = {"-f", "--file"}, description = "YAML File or Directory containing YAML resources")
    public Optional<File> file;

    public FileService fileService;
    public GenericProducer genericProducer;
    public DatasetService datasetService;

    @Inject
    public PlaySubcommand(ConfigService configService, FileService fileService, GenericProducer genericProducer,
                          DatasetService datasetService) {
        super(configService);
        this.fileService = fileService;
        this.genericProducer = genericProducer;
        this.datasetService = datasetService;
    }

    @Override
    public Integer callSubCommand() {
        if (file.isEmpty()) {
            throw new CommandLine.ParameterException(commandSpec.commandLine(), "No scenario given. Try --help to see "
                + "the command usage");
        }

        // List all files to process
        List<File> yamlFiles = fileService.computeYamlFileList(file.get(), true);
        if (yamlFiles.isEmpty()) {
            throw new CommandLine.ParameterException(commandSpec.commandLine(), "Could not find yaml/yml files in "
                + file.get().getName());
        }

        try {
            // Load each files
            var scenarios = fileService.parseResourceListFromFiles(yamlFiles);
            scenarios.forEach(s -> playScenario(s, file.get().getParentFile().getAbsolutePath(), currentContext.get()));
        } catch (Exception e) {
            commandSpec.commandLine().getErr().println("Play failed due to the following error: " + e.getMessage());
            return CommandLine.ExitCode.SOFTWARE;
        }

        return CommandLine.ExitCode.OK;
    }

    private void playScenario(Scenario scenario, String absolutePath, KafkagenConfig.Context currentContext) {
        Scenario.ScenarioSpec spec = scenario.getSpec();
        try {
            // Dataset file
            if (!StringUtil.isNullOrEmpty(spec.getDatasetFile())) {
                // Same topic given in the scenario description file
                if (!StringUtil.isNullOrEmpty(spec.getTopic())) {
                    Dataset dataset = datasetService.getDataset(new File(absolutePath + "/" + spec.getDatasetFile()),
                        spec.getTopic(), Optional.ofNullable(spec.getKeySubjectVersion()), Optional.ofNullable(spec.getValueSubjectVersion()), currentContext);

                    genericProducer.produce(spec.getTopic(), dataset, spec.getMaxInterval(), currentContext);
                    commandSpec.commandLine().getOut().println(String.format("Produced %d records in %s with %s (key) "
                            + "- %s (value)",
                        dataset.getRecords().size(),
                        dataset.getTopic(),
                        dataset.getKeySerializer().getSimpleName(),
                        dataset.getValueSerializer().getSimpleName()));
                } else {
                    // Multiple topics given for each record
                    List<Dataset> datasets =
                        datasetService.getDataset(new File(absolutePath + "/" + spec.getDatasetFile()), currentContext);
                    genericProducer.produce(datasets, 1, currentContext);

                    datasets.forEach(d ->
                        commandSpec.commandLine().getOut()
                            .println(String.format("Produced %d records in %s with %s (key) - %s (value)",
                                d.getRecords().size(), d.getTopic(), d.getKeySerializer().getSimpleName(),
                                d.getValueSerializer().getSimpleName()))
                    );
                }
            } else if (!StringUtil.isNullOrEmpty(spec.getTemplateFile())) {
                // Template file
                Dataset dataset = datasetService.getDatasetFromTemplate(absolutePath + "/" + spec.getTemplateFile(),
                    spec.getVariables(), spec.getTopic(), currentContext);

                genericProducer.produce(spec.getTopic(), dataset, spec.getMaxInterval(), currentContext);

                commandSpec.commandLine().getOut()
                    .println(String.format("Produced %d records in %s with %s (key) - %s (value)",
                        dataset.getRecords().size(),
                        dataset.getTopic(),
                        dataset.getKeySerializer().getSimpleName(),
                        dataset.getValueSerializer().getSimpleName()));
            } else {
                // Random message generation
                genericProducer.producerRandom(spec.getTopic(), absolutePath + "/" + spec.getAvroFile(),
                    spec.getIterations(), spec.getMaxInterval(), currentContext);
                commandSpec.commandLine().getOut().println(
                    String.format("Produced %d random records from avro schema (%s) in %s",
                        spec.getIterations(), spec.getAvroFile(), spec.getTopic()));
            }
        } catch (RuntimeException e) {
            commandSpec.commandLine().getErr()
                .println("Unable to produce records due to the following error: " + e.getMessage());
        }
    }
}
