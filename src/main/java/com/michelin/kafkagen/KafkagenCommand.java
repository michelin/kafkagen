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

import io.quarkus.picocli.runtime.annotations.TopCommand;
import java.util.concurrent.Callable;
import org.eclipse.microprofile.config.ConfigProvider;
import picocli.CommandLine;
import picocli.CommandLine.Command;

@TopCommand
@Command(name = "kafkagen",
        subcommands = {
            ProduceSubcommand.class,
            PlaySubcommand.class,
            SampleSubcommand.class,
            DatasetSubcommand.class,
            AssertSubcommand.class,
            ConfigSubcommand.class
        },
        mixinStandardHelpOptions = true,
        versionProvider = KafkagenCommand.ManifestVersionProvider.class)
public class KafkagenCommand implements Callable<Integer> {

    public Integer call() {
        var cmd = new CommandLine(new KafkagenCommand());
        cmd.usage(System.out);
        return 0;
    }

    static class ManifestVersionProvider implements CommandLine.IVersionProvider {

        public String[] getVersion() throws Exception {
            return new String[] { ConfigProvider.getConfig().getValue("quarkus.application.version", String.class) };
        }
    }
}
