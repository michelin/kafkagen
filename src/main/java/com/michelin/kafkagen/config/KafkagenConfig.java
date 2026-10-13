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

package com.michelin.kafkagen.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@ConfigMapping(prefix = "kafkagen")
public interface KafkagenConfig {
    @WithDefault("no-context")
    String currentContext();

    Optional<Boolean> insecureSsl();

    Optional<String> truststoreLocation();

    List<Context> contexts();

    interface Context {
        String name();

        @WithName("context")
        KafkaContext definition();

        interface KafkaContext {
            String bootstrapServers();

            Optional<String> groupIdPrefix();

            Optional<String> securityProtocol();

            Optional<String> saslMechanism();

            Optional<String> saslJaasConfig();

            /**
             * Additional Kafka client properties (e.g. OAuth settings). They override the named settings.
             */
            Map<String, String> kafkaProperties();

            Optional<String> registryUrl();

            /**
             * Use registry-properties with basic.auth.credentials.source and basic.auth.user.info instead.
             *
             * @deprecated will be removed in a future release
             */
            @Deprecated
            Optional<String> registryUsername();

            /**
             * Use registry-properties with basic.auth.credentials.source and basic.auth.user.info instead.
             *
             * @deprecated will be removed in a future release
             */
            @Deprecated
            Optional<String> registryPassword();

            /**
             * Additional Schema Registry client properties (e.g. basic or bearer authentication).
             */
            Map<String, String> registryProperties();

            Optional<String> partitionerClass();
        }
    }
}
