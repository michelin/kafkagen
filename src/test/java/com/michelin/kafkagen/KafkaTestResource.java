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

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.util.Map;

public class KafkaTestResource implements QuarkusTestResourceLifecycleManager {
    public static final String CONFLUENT_VERSION = "7.9.0";

    public static KafkaContainer kafka;
    public static GenericContainer registry;
    public static Network network;

    @Override
    public Map<String, String> start() {
        network = Network.newNetwork();

        kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:" + CONFLUENT_VERSION).asCompatibleSubstituteFor("confluentinc/cp-kafka"))
                .withNetworkAliases("kafka")
                .withNetwork(network);
        kafka.start();

        registry = new GenericContainer(DockerImageName.parse("confluentinc/cp-schema-registry:" + CONFLUENT_VERSION).asCompatibleSubstituteFor("confluentinc/cp-schema-registry"))
                .withNetwork(network)
                .withNetworkAliases("registry")
                .withEnv("SCHEMA_REGISTRY_HOST_NAME", "localhost")
                .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "kafka:9092")
                .withExposedPorts(8081)
                .waitingFor(Wait.forHttp("/subjects"));

        registry.start();

        return Map.of("kafka.host", kafka.getHost(), "kafka.port", kafka.getFirstMappedPort().toString(),
                "registry.host", registry.getHost(), "registry.port", registry.getMappedPort(8081).toString());
    }

    @Override
    public void stop() {
        if(kafka != null)
            kafka.stop();
        if(registry != null)
            registry.stop();
    }
}
