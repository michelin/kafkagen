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
import io.quarkus.test.InjectMock;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import picocli.CommandLine;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

@QuarkusTest
@QuarkusTestResource(value = KafkaTestResource.class, restrictToAnnotatedClass = true)
public class ConfigSubcommandIntegrationTest extends AbstractIntegrationTest {

    @InjectMock
    MockedConfig config;
    @InjectMock
    MockedContext context;
    @InjectMock
    MockedContext2 context2;
    @Inject
    MockedContext.MockedKafkaContext kafkaContext;
    @Inject
    MockedContext2.MockedKafkaContext2 kafkaContext2;
    @Mock
    CommandLine.Model.CommandSpec commandSpec;
    @Mock
    public ConfigService configService;

    private final StringWriter out = new StringWriter();

    @BeforeEach
    public void init() {
        MockitoAnnotations.openMocks(this);
        Mockito.when(configService.getCurrentContextName()).thenReturn("dev");
        Mockito.when(configService.getContextByName(Mockito.anyString())).thenReturn(Optional.of(context));
    }

    @Test
    public void callGetContextsActionWithoutContexts() throws Exception {
        Mockito.when(config.contexts()).thenReturn(List.of());

        var configSubcommand = new ConfigSubcommand(config, configService);
        initCommandLine(configSubcommand);
        configSubcommand.commandSpec = commandSpec;
        configSubcommand.action = ConfigAction.GET_CONTEXTS;

        assertEquals(CommandLine.ExitCode.OK, configSubcommand.call());
        assertEquals("No context pre-defined.", out.toString().trim());
    }
    @Test
    public void callWithGetContextsAction() throws Exception {
        String expected = """
                ---
                context2:
                  bootstrap-servers: bootstrapServers2
                  sasl-jaas-config: saslJaasConfig2
                  security-protocol: securityProtocol2
                  sasl-mechanism: saslMechanism2
                  registry-url: registryUrl2
                  registry-username: registryUsername2
                  registry-password: registryPassword2
                  partitioner-class: partitionerClass2
                context1:
                  bootstrap-servers: bootstrapServers
                  sasl-jaas-config: saslJaasConfig
                  security-protocol: securityProtocol
                  sasl-mechanism: saslMechanism
                  registry-url: registryUrl
                  registry-username: registryUsername
                  registry-password: registryPassword
                  partitioner-class: partitionerClass""";

        Mockito.when(context.name()).thenReturn("context1");
        Mockito.when(context.definition()).thenReturn(kafkaContext);
        Mockito.when(context2.name()).thenReturn("context2");
        Mockito.when(context2.definition()).thenReturn(kafkaContext2);
        Mockito.when(config.contexts()).thenReturn(List.of(context,context2));

        var configSubcommand = new ConfigSubcommand(config, configService);
        initCommandLine(configSubcommand);
        configSubcommand.commandSpec = commandSpec;
        configSubcommand.action = ConfigAction.GET_CONTEXTS;

        assertEquals(CommandLine.ExitCode.OK, configSubcommand.call());
        assertEquals(expected, out.toString().trim());
    }

    private void initCommandLine(ConfigSubcommand configSubcommand) {
        var commandLine = new CommandLine(configSubcommand);
        commandLine.setOut(new PrintWriter(out));
        commandLine.setErr(new PrintWriter(out));
        Mockito.when(commandSpec.commandLine()).thenReturn(commandLine);
    }

    @ApplicationScoped
    public static class MockedContext2 implements KafkagenConfig.Context {

        @Override
        public String name() {
            return "context2";
        }

        @Override
        public KafkaContext definition() {
            return new MockedKafkaContext2();
        }

        @ApplicationScoped
        public static class MockedKafkaContext2 implements KafkaContext {

            @Override
            public String bootstrapServers() {
                return "bootstrapServers2";
            }

            @Override
            public Optional<String> groupIdPrefix() {
                return Optional.of("groupIdPrefix2");
            }

            @Override
            public Optional<String> securityProtocol() {
                return Optional.of("securityProtocol2");
            }

            @Override
            public Optional<String> saslMechanism() {
                return Optional.of("saslMechanism2");
            }

            @Override
            public Optional<String> saslJaasConfig() {
                return Optional.of("saslJaasConfig2");
            }

            @Override
            public Optional<String> registryUrl() {
                return Optional.of("registryUrl2");
            }

            @Override
            public Optional<String> registryUsername() {
                return Optional.of("registryUsername2");
            }

            @Override
            public Optional<String> registryPassword() {
                return Optional.of("registryPassword2");
            }

            @Override
            public Optional<String> partitionerClass() {
                return Optional.of("partitionerClass2");
            }
        }
    }
}
