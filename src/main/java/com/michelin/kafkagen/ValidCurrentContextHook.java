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
import com.michelin.kafkagen.services.ConfigService;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.logging.LogManager;
import org.jboss.logmanager.Level;
import picocli.CommandLine;

/**
 * Abstract class to check if a context is selected before executing a command.
 */
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

    public Integer call() {
        if (verbose) {
            LogManager.getLogManager().getLogger("com.michelin").setLevel(Level.DEBUG);
        }

        String currentContextName = configService.getCurrentContextName();
        currentContext = configService.getContextByName(currentContextName);

        if (currentContext.isEmpty()) {
            commandSpec.commandLine().getErr()
                .println("No context selected. Please list/set the context with the config command");
            return CommandLine.ExitCode.USAGE;
        }

        return callSubCommand();
    }

    protected abstract Integer callSubCommand();

}
