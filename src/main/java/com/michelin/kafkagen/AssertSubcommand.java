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

import com.michelin.kafkagen.models.CompactedAssertState;
import com.michelin.kafkagen.models.Record;
import com.michelin.kafkagen.services.AssertService;
import com.michelin.kafkagen.services.ConfigService;
import com.michelin.kafkagen.services.DatasetService;
import jakarta.inject.Inject;
import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import picocli.CommandLine;

@CommandLine.Command(name = "assert",
    headerHeading = "@|bold Usage|@:",
    synopsisHeading = " ",
    descriptionHeading = "%n@|bold Description|@:%n%n",
    description = "Assert that a dataset exists in a topic",
    parameterListHeading = "%n@|bold Parameters|@:%n",
    optionListHeading = "%n@|bold Options|@:%n",
    commandListHeading = "%n@|bold Commands|@:%n",
    usageHelpAutoWidth = true,
    mixinStandardHelpOptions = true)
public class AssertSubcommand extends ValidCurrentContextHook {

    @CommandLine.Parameters(description = "Name of the topic", arity = "0..1")
    public Optional<String> topic;

    /**
     * YAML file or directory containing YAML resources to apply.
     */
    @CommandLine.Option(names = {"-f", "--file"}, description = "YAML/JSON File containing the dataset to assert")
    public Optional<File> file;

    @CommandLine.Option(names = {"-s",
        "--strict"}, description = "True when message fields should be strictly checked (false to ignore unset fields)")
    public boolean strict;

    @CommandLine.Option(names = {"-t",
        "--timestamp"}, description = "Timestamp milliseconds to start asserting from", arity = "0..1")
    public Optional<Long> startTimestamp;

    public DatasetService datasetService;
    public AssertService assertService;

    @Inject
    public AssertSubcommand(ConfigService configService, DatasetService datasetService, AssertService assertService) {
        super(configService);
        this.datasetService = datasetService;
        this.assertService = assertService;
    }

    @Override
    public Integer callSubCommand() {
        if (file.isEmpty()) {
            throw new CommandLine.ParameterException(commandSpec.commandLine(),
                "No dataset given. Try --help to see the command usage");
        }

        if (topic.isEmpty()) {
            throw new CommandLine.ParameterException(commandSpec.commandLine(),
                "No topic given. Try --help to see the command usage");
        }

        var exitCode = CommandLine.ExitCode.USAGE;

        try {
            List<Record> expectedRecords = datasetService.getRawRecord(file.get());

            Map<Object, CompactedAssertState> mostRecentAssertResult = expectedRecords.stream()
                .filter(r -> r.getMostRecent().equals(true))
                .collect(Collectors.toMap(Record::getKey, r -> CompactedAssertState.NOT_FOUND));

            var ok = assertService.assertThatTopicContains(topic.get(), expectedRecords, mostRecentAssertResult,
                currentContext.get(),
                strict, startTimestamp);

            if (ok) {
                commandSpec.commandLine().getOut().println("The dataset has been found in the topic");
                exitCode = CommandLine.ExitCode.OK;
            } else {
                commandSpec.commandLine().getOut()
                    .println(String.format("%d records do not match", expectedRecords.size()));
                expectedRecords.forEach(r -> {
                    commandSpec.commandLine().getOut().println(String.format("%s: %s",
                        mostRecentAssertResult.getOrDefault(r.getKey(), CompactedAssertState.NOT_FOUND).getMessage(),
                        r));
                });
            }
        } catch (Exception e) {
            commandSpec.commandLine().getErr().println("Assert failed due to the following error: " + e.getMessage());
            return CommandLine.ExitCode.SOFTWARE;
        }
        return exitCode;
    }
}
